#!/usr/bin/env python3
# servers/team_mcp_min.py
# -*- coding: utf-8 -*-
"""
Minimal MCP Server (per team)
Exposes capabilities:
 1) {TEAM}_get_job_info(job_key)
 2) {TEAM}_get_poc_for_job(job_key)
 3) {TEAM}_run_playbook(job_key, error_log) <-- reads data/{team}_playbook.json
 4) {TEAM}_get_dependents(job_key, transitive=True)
 5) {TEAM}_list_jobs
 6) {TEAM}_list_failures
 7) {TEAM}_list_playbook_rules <-- NEW
 8) {TEAM}_set_job_status(job_key, status) <-- NEW (manual override)
Optional:
 - {TEAM}_send_incident_email

Transport: FastMCP.run() for STDIO.

CLI usage:
  python servers/team_mcp_min.py <team>
Where <team> ∈ { cxo, oms, data_lake } (aliases team1->cxo, team2->oms, team3->data_lake).
"""

import sys
import json
import re
from pathlib import Path
from datetime import datetime, timezone
from fastmcp import FastMCP

def now_iso(): return datetime.now(timezone.utc).isoformat()

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"

TEAM_ALIASES = {
    "team1": "cxo", "team2": "oms", "team3": "data_lake",
    "cxo": "cxo", "oms": "oms", "data_lake": "data_lake"
}

def read_json(p: Path):
    try:
        return json.loads(p.read_text("utf-8")) if p.exists() else {}
    except Exception as e:
        print(f"[MCP:WARN] Invalid JSON at {p}: {e}", file=sys.stderr)
        return {}

def write_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def normalize_upstreams(u):
    if u is None: return []
    return u if isinstance(u, list) else [u]

def build_dependents_index(jobs: dict):
    idx = {}
    for jk, j in (jobs or {}).items():
        for u in normalize_upstreams(j.get("upstream")):
            if not u: continue
            idx.setdefault(u, set()).add(jk)
    return idx

if len(sys.argv) < 2:
    print("Usage: python servers/team_mcp_min.py <team>", file=sys.stderr)
    sys.exit(1)

TEAM = TEAM_ALIASES.get(sys.argv[1].strip().lower(), sys.argv[1].strip().lower())
FILES = {
    "jobs":     DATA_DIR / f"{TEAM}_jobs.json",
    "poc":      DATA_DIR / f"{TEAM}_poc.json",
    "playbook": DATA_DIR / f"{TEAM}_playbook.json",
    "email_out":DATA_DIR / f"{TEAM}_emails.json"
}

# Sample seeds for dev/demo
def sample_jobs(team):
    return {
        f"{team.upper()}:reporting_export": {
            "key": f"{team.upper()}:reporting_export",
            "name": "Reporting Export", "team": team.upper(),
            "status": "ERROR", "upstream": f"{team.upper()}:analytics_rollup"
        },
        f"{team.upper()}:analytics_rollup": {
            "key": f"{team.upper()}:analytics_rollup",
            "name": "Analytics Rollup", "team": team.upper(), "status": "OK"
        }
    }

def sample_poc(team):
    return {
        "name": f"{team.capitalize()} Oncall",
        "email": f"{team.replace('_','-')}.oncall@example.com",
        "zoom": "https://zoom.us/j/1111111111"
    }

mcp = FastMCP(f"{TEAM}_Server")

# ---------- Playbook loading & matching ----------
def _load_playbook() -> dict:
    pb = read_json(FILES["playbook"])
    if not pb or "rules" not in pb:
        print(f"[MCP:INFO] No playbook found for {TEAM}; using empty rules.", file=sys.stderr)
        return {"rules": []}
    print(f"[MCP:INFO] Loaded playbook for {TEAM}: {FILES['playbook']}", file=sys.stderr)
    return pb

def _rule_matches(rule: dict, error_log: str) -> bool:
    """
    Returns True iff the rule matches the error_log considering:
    - mode: 'contains' | 'regex'
    - search_type: 'search' | 'fullmatch' (regex only; default 'search')
    - requires: list[str] (all must be present as substrings, case-insensitive)
    """
    mode = (rule.get("mode") or "contains").lower()
    match = rule.get("match") or ""
    search_type = (rule.get("search_type") or "search").lower()
    requires = rule.get("requires") or []
    text_lower = (error_log or "").lower()

    # All required substrings must be present
    for req in requires:
        if req and req.lower() not in text_lower:
            return False

    if mode == "contains":
        return match.lower() in text_lower

    if mode == "regex":
        try:
            if search_type == "fullmatch":
                return re.fullmatch(match, error_log or "", flags=re.IGNORECASE) is not None
            # default: search
            return re.search(match, error_log or "", flags=re.IGNORECASE) is not None
        except re.error as e:
            print(f"[MCP:WARN] Bad regex in rule '{rule.get('id','<unnamed>')}': {match} ({e})", file=sys.stderr)
            return False

    return False

def _apply_playbook(job_key: str, error_log: str, jobs: dict) -> dict:
    """
    Iterate rules; return one of:
    - {"status": "SUCCESS", "action": ..., "rule_id": ...}
      -> auto-fix performed, status updated if configured
    - {"status": "NOAUTO", "action": ..., "rule_id": ..., "reason": "manual_rule"}
      -> rule matched but marked manual; agent should escalate
    - {"status": "FAILURE", "reason": "no_matching_rule"}
      -> no rule matched; agent should escalate
    """
    pb = _load_playbook()
    rules = pb.get("rules", [])
    for rule in rules:
        rid = rule.get("id") or "<unnamed>"
        action = rule.get("action") or "unknown_action"
        target = rule.get("updates_status_to")  # e.g., "OK" or null
        auto = bool(rule.get("auto", False))
        if not _rule_matches(rule, error_log):
            continue

        print(f"[MCP:PLAYBOOK] Team={TEAM} job={job_key} matched rule={rid} action={action} auto={auto}", file=sys.stderr)

        # AUTO-FIX path
        if auto:
            if target and isinstance(target, str):
                try:
                    if job_key in jobs:
                        jobs[job_key]["status"] = target
                        write_json(FILES["jobs"], jobs)
                except Exception as e:
                    print(f"[MCP:WARN] Failed to update job status: {e}", file=sys.stderr)
            return {"status": "SUCCESS", "action": action, "rule_id": rid}

        # MANUAL path (no auto-fix)
        return {"status": "NOAUTO", "action": action, "rule_id": rid, "reason": "manual_rule"}

    # No rule matched
    return {"status": "FAILURE", "reason": "no_matching_rule"}


# ---------- Tools ----------
@mcp.tool(name=f"{TEAM}_get_job_info")
def get_job_info(job_key: str) -> str:
    jobs = read_json(FILES["jobs"]) or sample_jobs(TEAM)
    job = jobs.get(job_key, {})
    if not job:
        return json.dumps({"error": f"Unknown job {job_key}"}, indent=2)
    return json.dumps(job, indent=2)

@mcp.tool(name=f"{TEAM}_get_poc_for_job")
def get_poc_for_job(job_key: str) -> str:
    poc = read_json(FILES["poc"]) or sample_poc(TEAM)
    return json.dumps(poc, indent=2)

@mcp.tool(name=f"{TEAM}_run_playbook")
def run_playbook(job_key: str, error_log: str) -> str:
    jobs = read_json(FILES["jobs"]) or sample_jobs(TEAM)
    result = _apply_playbook(job_key, error_log, jobs)
    return json.dumps(result, indent=2)

@mcp.tool(name=f"{TEAM}_get_dependents")
def get_dependents(job_key: str, transitive: bool = True) -> str:
    jobs = read_json(FILES["jobs"]) or sample_jobs(TEAM)
    idx = build_dependents_index(jobs)
    out = []
    stack = list(idx.get(job_key, []))
    seen = set()
    while stack:
        d = stack.pop()
        if d in seen:
            continue
        seen.add(d); out.append(d)
        if transitive:
            stack.extend(list(idx.get(d, [])))
    return json.dumps(sorted(out), indent=2)

@mcp.tool(name=f"{TEAM}_list_jobs")
def list_jobs() -> str:
    jobs = read_json(FILES["jobs"]) or sample_jobs(TEAM)
    print(f"[MCP:DEBUG] list_jobs -> {len(jobs)} items for {TEAM}", file=sys.stderr)
    return json.dumps(list(jobs.values()), indent=2)

@mcp.tool(name=f"{TEAM}_list_failures")
def list_failures() -> str:
    jobs = read_json(FILES["jobs"]) or sample_jobs(TEAM)
    failed = []
    for j in jobs.values():
        status = (j.get("status") or "").upper()
        if status in {"ERROR", "FAILED"}:
            failed.append({"key": j.get("key"), "name": j.get("name"), "status": status})
    print(f"[MCP:DEBUG] list_failures -> {len(failed)} items for {TEAM}", file=sys.stderr)
    return json.dumps(failed, indent=2)

# NEW: list playbook rules (for interactive discovery)
@mcp.tool(name=f"{TEAM}_list_playbook_rules")
def list_playbook_rules() -> str:
    pb = _load_playbook()
    return json.dumps(pb.get("rules", []), indent=2)

# NEW: manual override (mark job OK/ERROR/etc.)
@mcp.tool(name=f"{TEAM}_set_job_status")
def set_job_status(job_key: str, status: str) -> str:
    jobs = read_json(FILES["jobs"]) or sample_jobs(TEAM)
    if job_key not in jobs:
        return json.dumps({"status":"ERROR","reason":f"Unknown job {job_key}"}, indent=2)
    jobs[job_key]["status"] = status
    write_json(FILES["jobs"], jobs)
    return json.dumps({"status":"SUCCESS","job_key":job_key,"new_status":status}, indent=2)

@mcp.tool(name=f"{TEAM}_send_incident_email")
def send_incident_email(subject: str, body: str, to: str, cc: str = "") -> str:
    outbox = read_json(FILES["email_out"])
    if not isinstance(outbox, list): outbox = []
    outbox.append({"ts": now_iso(), "to": to, "cc": cc, "subject": subject, "body": body})
    write_json(FILES["email_out"], outbox)
    return json.dumps({"status":"QUEUED","to":to}, indent=2)

if __name__ == "__main__":
    mcp.run()  # stdio transport
