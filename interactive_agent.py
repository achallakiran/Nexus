# interactive_agent.py
# -*- coding: utf-8 -*-
"""
InteractiveAgent: human text -> MCP tools.

Two NLU modes:
 - Optional LLM adapter (enabled when AZURE_OPENAI_* env vars are set).
 - Deterministic parser (regex/keyword) fallback.

Supported verbs:
 - fix (restart/resolve/repair)
 - escalate (bridge)
 - notify (alert/inform)
 - scan
 - list_jobs / list_failures
 - list_rules
 - run_playbook (with custom error text)
 - mark_ok (manual override)
"""

import os
import re
import json
from typing import Dict, Optional
from mcp import ClientSession

# --- Pretty formatting helpers (same as super_agent.py) ---
def _pretty(obj_or_text: str, header: str = "") -> str:
    """
    Render obj_or_text as readable text. If it's JSON, indent it.
    Optional header printed as a bold line.
    """
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

USE_LLM = all(os.getenv(k) for k in ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_DEPLOYMENT", "AZURE_OPENAI_API_KEY"])


async def llm_parse_intent(text: str) -> Optional[Dict]:
    if not USE_LLM:
        return None
    import httpx
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT").rstrip("/") + "/v1/chat/completions"
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")

    prompt = (
        "Extract intent from user text for reliability operations.\n"
        "Return strict JSON with keys: verb, team, job_key, reason, context, error_text.\n"
        "Allowed verbs: fix\nescalate\nnotify\nscan\nlist_jobs\nlist_failures\nlist_rules\nrun_playbook\nmark_ok.\n"
        "Teams: cxo, oms, data_lake.\n"
        "If team appears as 'data lake', normalize to 'data_lake'.\n"
        "If user says 'mark ok', 'resolve manually', set verb=mark_ok.\n"
        "If user provides custom error after 'with', populate error_text.\n"
        f"Text: {text}\n"
    )
    payload = {
        "model": deployment,
        "messages": [
            {"role": "system", "content": "You are an intent tagger for reliability operations."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"}
    }
    async with httpx.AsyncClient(headers={"api-key": api_key}, timeout=30, verify=os.getenv("SSL_CERT_FILE") or True) as client:
        r = await client.post(endpoint, json=payload)
        r.raise_for_status()
        data = r.json()
        content = data["choices"][0]["message"]["content"]
        try:
            return json.loads(content)
        except Exception:
            return None


VERB_MAP = {
    "restart": "fix", "fix": "fix", "resolve": "fix", "repair": "fix",
    "escalate": "escalate", "bridge": "escalate",
    "notify": "notify", "alert": "notify", "inform": "notify",
    "scan": "scan",
    "list": "list_jobs", "show": "list_jobs", "jobs": "list_jobs",
    "failures": "list_failures", "failed": "list_failures", "errors": "list_failures", "incidents": "list_failures",
    "rules": "list_rules", "playbook rules": "list_rules",
    "run playbook": "run_playbook",
    "mark ok": "mark_ok", "resolve manually": "mark_ok", "set ok": "mark_ok"
}

TEAM_ALIASES = {
    "cxo": "cxo",
    "oms": "oms",
    "data_lake": "data_lake",
    "data lake": "data_lake"
}

TEAM_RE = r"\b(cxo|oms|data[\s\-\_]lake)\b"
JOBKEY_RE = r"([A-Z_]+:[A-Za-z0-9_\-]+)"


def det_parse(text: str) -> Dict:
    t = text.lower()
    verb = None
    for k, v in VERB_MAP.items():
        if re.search(rf"\b{k}\b", t):
            verb = v
            break
    m_team = re.search(TEAM_RE, t)
    team = m_team.group(1).replace(" ", "_").replace("-", "_") if m_team else None
    m_jobkey = re.search(JOBKEY_RE, text)
    job_key = m_jobkey.group(1) if m_jobkey else None
    # custom error after "with ..."
    m_err = re.search(r"\bwith\b\s+(.+)$", text, re.IGNORECASE)
    error_text = m_err.group(1).strip() if m_err else None
    # reason/context after 'because'/'due to'
    reason = None
    m_reason = re.search(r"(because|due to)\s+(.+)$", text, re.IGNORECASE)
    if m_reason:
        reason = m_reason.group(2).strip()
    return {"verb": verb, "team": team, "job_key": job_key, "reason": reason, "context": None, "error_text": error_text}


def normalize_team(team: Optional[str], sessions_by_team: Dict[str, ClientSession], text_lower: str) -> Optional[str]:
    if team and team in sessions_by_team:
        return team
    for alias, canon in TEAM_ALIASES.items():
        if re.search(rf"\b{alias}\b", text_lower):
            if canon in sessions_by_team:
                return canon
    for candidate in sessions_by_team.keys():
        if re.search(rf"\b{candidate}\b", text_lower):
            return candidate
    return team


async def infer_job_key(team: str, job_key: Optional[str], text: str, sess: ClientSession) -> Optional[str]:
    if job_key:
        return job_key
    # try matching tokens against list_jobs
    try:
        raw = await sess.call_tool(f"{team}_list_jobs", arguments={})
        jobs = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        jobs = []
    text_tokens = set(re.findall(r"[A-Za-z0-9_]+", text.lower()))

    def tokens(s: str) -> set:
        return set(re.findall(r"[A-Za-z0-9_]+", s.lower()))

    best_key, best_score = None, 0
    for j in jobs if isinstance(jobs, list) else []:
        name = j.get("name") or ""
        key = j.get("key") or ""
        if not key:
            continue
        tks = tokens(name) | tokens(key)  # union of tokens from name and key
        score = len(text_tokens & tks)
        if score > best_score:
            best_score, best_key = score, key
    return best_key


class InteractiveAgent:
    def __init__(self, sessions_by_team: Dict[str, ClientSession]):
        self.sessions = sessions_by_team

    async def handle_text(self, text: str) -> str:
        parsed = await llm_parse_intent(text) if USE_LLM else None
        if not parsed:
            parsed = det_parse(text)

        verb = parsed.get("verb")
        team = parsed.get("team")
        job_key = parsed.get("job_key")
        reason = parsed.get("reason") or "Interactive request"
        context = parsed.get("context") or "Interactive request"
        error_text = parsed.get("error_text")  # optional for run_playbook

        team = normalize_team(team, self.sessions, text.lower())
        if not verb:
            return (
                "I couldn't infer an action.\n"
                "Try: fix/escalate/notify/scan/list jobs/list failures/list rules/run playbook/mark ok + team/job."
            )

        # Aggregate listing across all teams (when team omitted)
        if team is None and verb in {"list_jobs", "list_failures", "list_rules"}:
            outputs = []
            for tname, sess in self.sessions.items():
                if verb == "list_jobs":
                    res = await sess.call_tool(f"{tname}_list_jobs", arguments={})
                    outputs.append(f"=== {tname.upper()} jobs ===\n" + _pretty(res))
                elif verb == "list_failures":
                    res = await sess.call_tool(f"{tname}_list_failures", arguments={})
                    outputs.append(f"=== {tname.upper()} failures ===\n" + _pretty(res))
                else:  # list_rules
                    res = await sess.call_tool(f"{tname}_list_playbook_rules", arguments={})
                    outputs.append(f"=== {tname.upper()} playbook rules ===\n" + _pretty(res))
            return "\n\n".join(outputs)

        if not team:
            return "Please specify a team (cxo, oms, data_lake)."

        sess = self.sessions.get(team)
        if not sess:
            return f"No MCP session for {team}."

        # hydrate job_key if needed for verbs that require it
        if verb in {"fix", "escalate", "notify", "run_playbook", "mark_ok"}:
            job_key = await infer_job_key(team, job_key, text, sess)
            if not job_key:
                return (
                    "I couldn't determine the job. Provide a job key "
                    "(e.g., CXO:reporting_export) or mention the job name in text."
                )

        if verb == "scan":
            return "Scan requested. The background agent runs scans continuously."

        if verb == "list_jobs":
            res = await sess.call_tool(f"{team}_list_jobs", arguments={})
            return f"Jobs for **{team.upper()}**:" + _pretty(res)

        if verb == "list_failures":
            res = await sess.call_tool(f"{team}_list_failures", arguments={})
            return f"Failed jobs for **{team.upper()}**:" + _pretty(res)

        if verb == "list_rules":
            res = await sess.call_tool(f"{team}_list_playbook_rules", arguments={})
            return f"Playbook rules for **{team.upper()}**:" + _pretty(res)

        if verb == "run_playbook":
            if not error_text:
                return (
                    "Provide an error after 'with', e.g., "
                    "'run playbook data_lake DATA_LAKE:ingest_orders with Disk full at /mnt/raw'."
                )
            res = await sess.call_tool(
                f"{team}_run_playbook",
                arguments={"job_key": job_key, "error_log": error_text}
            )
            return (f"Playbook result for **{job_key}** (team **{team.upper()}**)") + _pretty(res)

        if verb == "fix":
            res = await sess.call_tool(
                f"{team}_run_playbook",
                arguments={"job_key": job_key, "error_log": "Interactive fix"}
            )
            return (f"Fix attempted for **{job_key}** (team **{team.upper()}**)") + _pretty(res)

        if verb == "escalate":
            poc = await sess.call_tool(f"{team}_get_poc_for_job", arguments={"job_key": job_key})
            deps = await sess.call_tool(
                f"{team}_get_dependents",
                arguments={"job_key": job_key, "transitive": True}
            )
            return (
                f"Escalation captured for **{job_key}** (team **{team.upper()}**)\n"
                f"{_pretty(poc, 'Point of Contact')}\n"
                f"{_pretty(deps, 'Downstreams')}"
            )

        if verb == "notify":
            deps = await sess.call_tool(
                f"{team}_get_dependents",
                arguments={"job_key": job_key, "transitive": True}
            )
            return (
                f"Notify intent for **{job_key}** (team **{team.upper()}**)\n"
                f"{_pretty(deps, 'Downstreams affected')}"
            )

        if verb == "mark_ok":
            res = await sess.call_tool(
                f"{team}_set_job_status",
                arguments={"job_key": job_key, "status": "OK"}
            )
            return (f"Manual override → **OK** for **{job_key}** (team **{team.upper()}**)") + _pretty(res)

        return "Intent understood but action not implemented."


# --- Optional standalone runner (lets you run `python interactive_agent.py`) ---
if __name__ == "__main__":
    import asyncio

    async def demo():
        # Minimal standalone: no MCP sessions – lets you test parsing/formatting.
        agent = InteractiveAgent(sessions_by_team={})
        print(
            "Standalone InteractiveAgent demo.\n"
            "Type text; 'quit' to exit.\n"
            "Examples:\n"
            "  list jobs cxo\n"
            "  fix oms OMS:load_inventory\n"
            "  run playbook data_lake DATA_LAKE:ingest_orders with Disk full at /mnt/raw\n"
        )
        while True:
            try:
                txt = input("You> ")
            except (EOFError, KeyboardInterrupt):
                break
            if txt.strip().lower() in {"quit", "exit"}:
                break
            print(await agent.handle_text(txt))

    asyncio.run(demo())
