# Support Super Agent (MCP, two agents: interactive + background)

## Overview
- **InteractiveAgent**: understands human text, calls MCP tools. Uses **LLM intent parser** when `AZURE_OPENAI_*` env vars are set; otherwise uses deterministic regex.
- **RemediationAgent**: continuous background scan, runs playbooks, sends emails via team tool, schedules Zoom for **P1**.

MCP servers expose 4 capabilities:
1. `{TEAM}_get_job_info(job_key)`
2. `{TEAM}_get_poc_for_job(job_key)`
3. `{TEAM}_run_playbook(job_key, error_log)`
4. `{TEAM}_get_dependents(job_key, transitive=True)`

## Prereqs
- Python 3.10+ (your existing venv)
- Packages: `fastmcp`, `mcp`, `PyYAML`, `httpx`
- Optionally: Azure OpenAI env vars for LLM intent parsing:
  - `AZURE_OPENAI_ENDPOINT`
  - `AZURE_OPENAI_DEPLOYMENT`
  - `AZURE_OPENAI_API_KEY`

## Setup
```bash
# activate your existing venv
source /path/to/venv/bin/activate

# install minimal deps
pip install -r requirements.txt
