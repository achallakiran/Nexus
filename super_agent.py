
import os
import sys
import json
import asyncio
import signal
import shutil
import yaml
from pathlib import Path
from typing import Dict, List, Tuple, Set
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from interactive_agent import InteractiveAgent

CONFIG_PATH = os.getenv("MCP_SERVERS_CONFIG", "config/servers.yaml")
DATA_DIR = Path("data")

def _pretty(obj_or_text: str, header: str = "") -> str:
    header_line = f"\n**{header}**\n" if header else ""
    txt = obj_or_text
    try:
        if isinstance(obj_or_text, str):
            parsed = json.loads(obj_or_text)
            txt = json.dumps(parsed, indent=2, sort_keys=True)
        elif isinstance(obj_or_text, (dict, list)):
            txt = json.dumps(obj_or_text, indent=2, sort_keys=True)
    except Exception:
        pass
    return f"{header_line}```\n{txt}\n```"

def load_config() -> dict:
    p = Path(CONFIG_PATH)
    print(f"[DEBUG] Loading config from: {p.resolve()}", file=sys.stderr)
    return yaml.safe_load(p.read_text("utf-8")) if p.exists() else {"poll_interval_seconds": 30, "servers": []}

def classify_severity(error_log: str) -> str:
    txt = (error_log or "").lower()
    if any(k in txt for k in ["data loss", "corruption", "pipeline down", "outage", "sla breach", "p1"]):
        return "P1"
    if any(k in txt for k in ["timeout", "retry", "degraded", "transient"]):
        return "P2"
    return "P3"

def load_team_errors(team: str) -> Dict[str, str]:
    p = DATA_DIR / f"{team}_errors.json"
    try:
        if p.exists():
            print(f"[DEBUG] Loading error logs for {team} from {p}", file=sys.stderr)
            return json.loads(p.read_text("utf-8"))
    except Exception as e:
        print(f"[WARN] Could not read {p}: {e}", file=sys.stderr)
    return {}

async def call(session: ClientSession, tool: str, **kwargs) -> str:
    try:
        print(f"[DEBUG] Calling tool: {tool} with args: {kwargs}", file=sys.stderr)
        res = await session.call_tool(tool, arguments=kwargs)
        print(f"[DEBUG] Tool {tool} returned: {res}", file=sys.stderr)
        if hasattr(res, "structuredContent") and isinstance(res.structuredContent, dict) and "result" in res.structuredContent:
            result = res.structuredContent["result"]
            print(f"[DEBUG] Extracted .structuredContent['result']: {result}", file=sys.stderr)
            return result
        elif hasattr(res, "content") and isinstance(res.content, list) and len(res.content) > 0:
            text = getattr(res.content[0], "text", None)
            if text:
                print(f"[DEBUG] Extracted .content[0].text: {text}", file=sys.stderr)
                return text
        elif isinstance(res, str):
            return res
        return json.dumps(str(res))
    except Exception as e:
        print(f"[ERROR] Exception calling tool {tool}: {e}", file=sys.stderr)
        return json.dumps({"error": str(e)})

async def connect_server(stack: AsyncExitStack, name: str, cmd: str, args: List[str], env: Dict[str, str]):
    if os.path.isabs(cmd) and not os.path.exists(cmd):
        raise FileNotFoundError(f"Server '{name}' command not found: {cmd}")
    if not os.path.isabs(cmd) and shutil.which(cmd) is None:
        raise FileNotFoundError(f"Server '{name}' command not found in PATH: {cmd}")
    print(f"[DEBUG] Spawning MCP server: {name} with command: {cmd} {args}", file=sys.stderr)
    params = StdioServerParameters(command=cmd, args=args, env={**os.environ, **(env or {})})
    read, write = await stack.enter_async_context(stdio_client(params))
    sess = await stack.enter_async_context(ClientSession(read, write))
    await sess.initialize()
    print(f"[DEBUG] MCP server {name} initialized.", file=sys.stderr)
    return sess

async def collect_all_jobs(sessions_by_name: Dict[str, ClientSession]) -> Dict[str, dict]:
    jobs: Dict[str, dict] = {}
    for name, sess in sessions_by_name.items():
        try:
            print(f"[DEBUG] Collecting jobs from team: {name}", file=sys.stderr)
            raw = await call(sess, f"{name}_list_jobs")
            print(f"[DEBUG] Raw jobs from {name}: {raw}", file=sys.stderr)
            lst = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(lst, list):
                for j in lst:
                    k = j.get("key")
                    if k:
                        jobs[k] = j
        except Exception as e:
            print(f"[ERROR] Could not collect jobs from {name}: {e}", file=sys.stderr)
    return jobs

def build_dependents_index(all_jobs: Dict[str, dict]) -> Dict[str, Set[str]]:
    idx: Dict[str, Set[str]] = {}
    for jk, j in (all_jobs or {}).items():
        upstream = j.get("upstream")
        ups = upstream if isinstance(upstream, list) else ([upstream] if upstream else [])
        for u in ups:
            if not u:
                continue
            idx.setdefault(u, set()).add(jk)
    return idx

def transitive_dependents(source_job: str, idx: Dict[str, Set[str]]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    stack: List[str] = list(idx.get(source_job, []))
    while stack:
        d = stack.pop()
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
        stack.extend(list(idx.get(d, [])))
    return sorted(out)

class CommsAgent:
    def __init__(self, sessions_by_name: Dict[str, ClientSession]):
        self.sessions = sessions_by_name

    async def send_incident_email_via_team_tool(self, team: str, subject: str, body: str, to: str, cc: str = "") -> str:
        print(f"[DEBUG] Sending incident email via team tool: team={team}, to={to}, subject={subject}", file=sys.stderr)
        sess = self.sessions.get(team)
        if not sess:
            print(f"[ERROR] No session for team {team} when sending email.", file=sys.stderr)
            return "No session for team"
        return await call(sess, f"{team}_send_incident_email", subject=subject, body=body, to=to, cc=cc)

    async def schedule_zoom_bridge(self, team: str, job_key: str) -> Tuple[str, str]:
        print(f"[DEBUG] Scheduling Zoom bridge for team={team}, job_key={job_key}", file=sys.stderr)
        sess = self.sessions.get(team)
        if not sess:
            print(f"[ERROR] No session for team {team} when scheduling Zoom.", file=sys.stderr)
            return ("", "")
        poc_raw = await call(sess, f"{team}_get_poc_for_job", job_key=job_key)
        try:
            poc = json.loads(poc_raw)
            return (poc.get("zoom", ""), poc.get("email", ""))
        except Exception:
            return ("", "")

class RemediationAgent:
    def __init__(self, sessions_by_name: Dict[str, ClientSession], comms: CommsAgent, poll_seconds: int):
        self.sessions = sessions_by_name
        self.comms = comms
        self.poll = poll_seconds

    async def _scan_team_once(self, team: str, sess: ClientSession):
        try:
            print(f"[SCAN] Requesting jobs for team: {team}", file=sys.stderr)
            raw = await call(sess, f"{team}_list_jobs")
            print(f"[SCAN] Raw jobs response for {team}: {raw}", file=sys.stderr)
            jobs = json.loads(raw) if isinstance(raw, str) else raw
        except Exception as e:
            print(f"[SCAN] Could not list jobs for {team}: {e}", file=sys.stderr)
            return

        if not jobs or not isinstance(jobs, list):
            print(f"[SCAN] No jobs found for {team}.", file=sys.stderr)
            return

        print(f"[SCAN] Team {team} -> checking {len(jobs)} job(s)", file=sys.stderr)
        team_errors = load_team_errors(team)

        for job in jobs:
            job_key = job.get("key") or "<unknown>"
            status = job.get("status") or "<unknown>"
            print(f"[SCAN] {team} job {job_key} status: {status}", file=sys.stderr)
            if (status or "").upper() != "ERROR":
                continue

            error_log = team_errors.get(job_key, "Timeout contacting upstream")
            severity = classify_severity(error_log)
            print(f"[ACTION] Attempt playbook for {job_key} (severity {severity}) with error_log='{error_log}'", file=sys.stderr)
            pb = await call(sess, f"{team}_run_playbook", job_key=job_key, error_log=error_log)
            try:
                pbj = json.loads(pb) if pb else {}
            except Exception:
                pbj = {"status": "FAILURE", "reason": "invalid_json_from_playbook"}
            print(f"[DEBUG] Playbook result for {job_key}: {pbj}", file=sys.stderr)

            status_flag = (pbj.get("status") or "").upper()
            if status_flag == "SUCCESS":
                print(f"[OK] {team} {job_key} fixed via playbook: {pbj.get('action')} (rule={pbj.get('rule_id')})", file=sys.stderr)
                continue

            print(f"[ACTION] Playbook result={status_flag} for {job_key}; escalate and notify downstreams.", file=sys.stderr)
            poc_raw = await call(sess, f"{team}_get_poc_for_job", job_key=job_key)
            try:
                poc = json.loads(poc_raw) if poc_raw else {}
            except Exception:
                poc = {}
            owner_email = poc.get("email", "")

            all_jobs = await collect_all_jobs(self.sessions)
            idx = build_dependents_index(all_jobs)
            deps = transitive_dependents(job_key, idx)
            print(f"[FANOUT] Downstreams for {job_key}: {', '.join(deps) if deps else '(none)'}", file=sys.stderr)

            recipients: List[str] = [owner_email] if owner_email else []
            for dep in deps:
                dep_team = dep.split(":")[0].lower()
                dep_sess = self.sessions.get(dep_team)
                if not dep_sess:
                    continue
                dep_poc_raw = await call(dep_sess, f"{dep_team}_get_poc_for_job", job_key=dep)
                try:
                    dep_poc = json.loads(dep_poc_raw)
                    if dep_poc.get("email"):
                        recipients.append(dep_poc["email"])
                except Exception:
                    pass

            subject = f"[{severity}] {job_key} failed: {error_log}"
            body = (
                f"Job: {job_key}\n"
                f"Team: {team.upper()}\n"
                f"Severity: {severity}\n"
                f"Error: {error_log}\n"
                f"Downstreams affected: {', '.join(deps) if deps else '(none)'}\n"
            )
            for r in recipients:
                print(f"[DEBUG] Sending escalation email to: {r}", file=sys.stderr)
                await self.comms.send_incident_email_via_team_tool(team, subject, body, to=r)
            print(f"[ESCALATE] {team} {job_key} -> mailed {len(recipients)} recipient(s)", file=sys.stderr)

            if severity == "P1":
                zoom, host = await self.comms.schedule_zoom_bridge(team, job_key)
                if zoom:
                    print(f"[BRIDGE] Join Zoom: {zoom} (POC: {host})", file=sys.stderr)
                else:
                    print("[BRIDGE] No Zoom link in POC; please add to POC JSON.", file=sys.stderr)

    async def run(self, stop: asyncio.Event):
        cycle = 0
        while not stop.is_set():
            cycle += 1
            print(f"===== Background Scan Cycle #{cycle} =====", file=sys.stderr)
            try:
                for team, sess in self.sessions.items():
                    await self._scan_team_once(team, sess)
            except Exception as e:
                print("[SCAN ERROR]", e, file=sys.stderr)
            try:
                await asyncio.sleep(self.poll)
            except asyncio.CancelledError:
                break

async def run_interactive_console(interactive: InteractiveAgent, stop_evt: asyncio.Event):
    print(
        "💬 Interactive mode\n"
        "Commands: fix | escalate | notify | scan | list jobs | list failures | list rules | run playbook | mark ok\n"
        "Examples:\n"
        "  fix cxo CXO:reporting_export\n"
        "  run playbook data_lake DATA_LAKE:ingest_orders with Disk full at /mnt/raw\n"
        "Type 'quit' to exit.\n"
    )
    while not stop_evt.is_set():
        try:
            text = await asyncio.to_thread(input, "You> ")
        except (EOFError, KeyboardInterrupt):
            text = "quit"
        if not text:
            continue
        if text.strip().lower() in {"quit", "exit"}:
            stop_evt.set()
            break
        out = await interactive.handle_text(text)
        print(out)  # Only user output to stdout!

async def main():
    cfg = load_config()
    poll = int(cfg.get("poll_interval_seconds", 30))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with AsyncExitStack() as stack:
        sessions: Dict[str, ClientSession] = {}
        for srv in cfg.get("servers", []):
            try:
                sess = await connect_server(stack, srv["name"], srv["command"], srv["args"], srv.get("env", {}))
                sessions[srv["name"]] = sess
                print(f"[MCP] connected -> {srv['name']}", file=sys.stderr)
            except FileNotFoundError as e:
                print(f"[WARN] Skipping server '{srv['name']}': {e}", file=sys.stderr)
            except Exception as e:
                print(f"[ERROR] Failed to start server '{srv.get('name','?')}': {e}", file=sys.stderr)

        if not sessions:
            print("[WARN] No MCP servers connected. Interactive console is available; some commands will be limited.", file=sys.stderr)

        comms = CommsAgent(sessions)
        remediation = RemediationAgent(sessions, comms, poll_seconds=poll)
        interactive = InteractiveAgent(sessions)
        print("✅ Multi-agent system READY (Remediation + Interactive). Ctrl+C to stop.")

        await asyncio.gather(
            remediation.run(stop),
            run_interactive_console(interactive, stop)
        )

        print("Shutting down agents...", file=sys.stderr)

if __name__ == "__main__":
    asyncio.run(main())

