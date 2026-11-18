#!/usr/bin/env python3
"""Streamlined, Prefetch-First dbt Cloud MCP — Gemini Enterprise (Sobi) A2UI v0.9 Canvas CLI.

Design Philosophy (Prefetch-First + Action-Oriented + On-Demand Deep Dives):
  1. Fast Parallel Workspace Prefetch (`hydrate_workspace_bundle` via ThreadPoolExecutor):
     - Pulls projects, jobs, recent runs (all statuses), error diagnostics for failed/latest runs,
       mart models, all models, raw sources (with freshness), BI exposures, governed metrics,
       saved queries, macros, plus initial model lineage/health and initial metric slice in parallel.
     - Users can switch across ALL 4 tabs, search/filter runs by status or job, filter models/sources/
       exposures/metrics/macros, inspect failed run errors, and stage job/SQL actions in 0ms client-side
       WITHOUT waiting for extra tool turns!
  2. Clear Review & Take Action Workflows:
     - Tab 1 (Action Center & Runs): KPI health strip, "Needs Attention" triage cards for failed/active
       runs & stale sources, instant 0ms status/job/search run filtering, and 2-step Confirmation Modals
       for Safe Retry (`retry_job_run`), Trigger Job / Selective Model Build (`trigger_job_run`), and
       Cancel Run (`cancel_job_run`).
     - Tab 2 (Models, Health & Lineage): Unified 0ms searchable explorer across Marts, Models, Sources,
       and Exposures with Master-Detail (`<- All Resources`), instant metadata review, 1-click Selective
       Rebuild (`dbt build --select +model+`) or Open in SQL Runner, and on-demand Deep Dive.
     - Tab 3 (Semantic Metrics): Instant 0ms search across governed MetricFlow metrics & saved queries,
       visual bar/table/SQL toggle, and on-demand custom slice query execution.
     - Tab 4 (SQL & Macros): Instant 0ms mart/source/macro picker with 1-click SQL template insertion,
       on-demand AI NL-to-dbt-SQL (`text_to_sql`), and guarded Warehouse SQL Execution (`execute_sql`).
  3. Additional Tool Calls Relegated Strictly to Deep Dives & Confirmed Executions:
     - Deep-dive run artifacts & dbt product docs (`list_job_run_artifacts`, `search_product_docs`)
     - Deep-dive uncached model lineage, columns, SQL & run performance (`get_lineage`, `get_node_details`,
       `get_model_health`, `get_model_performance`, `get_related_models`)
     - Custom Semantic Layer metric slicing (`query_metrics`, `get_dimensions`, `get_dimension_values`)
     - AI NL-to-SQL (`text_to_sql`) & Confirmed Mutations (`retry_job_run`, `trigger_job_run`,
       `cancel_job_run`, `execute_sql`).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import datetime
import glob
import json
import os
import pathlib
import re
import subprocess
import sys
from typing import Any

# ==============================================================================
# 1. PORTABLE CONSTANTS, ALLOW-LISTS & dbt LABS BRAND THEME
# ==============================================================================
SKILL_SLUG = "dbt_a2ui"
SELF_B64_MARKER = "DBT_A2UI_CLI_B64:"
DEFAULT_SURFACE_ID = "dbt_a2ui_workbench"
DEFAULT_A2UI_OUT_PATH = "/tmp/dbt_a2ui.json"
DEFAULT_STATE_CACHE_PATH = "/tmp/dbt_a2ui_state_cache.json"

GE_COMPOSITE_CATALOG_V09 = (
    "https://www.gstatic.com/vertexaisearch/a2ui/v0_9/"
    "gemini_enterprise_composite_catalog.json"
)

ALLOWED_REMOTE_CLI_BINS: set[str] = {
    "remote_cli",
    "/usr/local/bin/remote_cli",
    "/usr/bin/remote_cli",
}

ALLOWED_DBT_TOOLS: set[str] = {
    "list_projects",
    "get_all_models",
    "get_mart_models",
    "get_related_models",
    "get_node_details",
    "get_all_sources",
    "get_exposures",
    "get_all_macros",
    "get_lineage",
    "get_model_health",
    "get_model_performance",
    "list_metrics",
    "get_dimensions",
    "get_entities",
    "get_dimension_values",
    "query_metrics",
    "get_metrics_compiled_sql",
    "list_saved_queries",
    "text_to_sql",
    "execute_sql",
    "list_jobs",
    "get_job_details",
    "list_jobs_runs",
    "get_job_run_details",
    "get_job_run_error",
    "list_job_run_artifacts",
    "get_job_run_artifacts",
    "trigger_job_run",
    "cancel_job_run",
    "retry_job_run",
    "search_product_docs",
    "get_product_doc_pages",
}

CONNECTOR_RE = re.compile(
    r"^projects/\d{5,25}/locations/[A-Za-z0-9_-]+/collections/[A-Za-z0-9_.-]+/dataConnector$"
)
ENDPOINT_RE = re.compile(r"^https://[A-Za-z0-9.-]+(?::\d+)?(?:/[A-Za-z0-9_./~%-]*)?$")
NUMERIC_ID_RE = re.compile(r"^\d{1,22}$")
SAFE_UNIQUE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,180}$")
SAFE_GRAIN_SET = {"DAY", "WEEK", "MONTH", "QUARTER", "YEAR"}
EMAIL_PII_RE = re.compile(r"\b([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*@([A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")

BRAND_THEME: dict[str, str] = {
    "header_bg": "#040A14",
    "accent": "#FF694B",
    "primary": "#E04F33",
    "primary_dark": "#C23D22",
    "navy": "#0C192C",
    "navy_hover": "#162844",
    "canvas_bg": "#F4F6F9",
    "surface_bg": "#FFFFFF",
    "subtle_bg": "#F8FAFC",
    "border": "#E2E8F0",
    "text_primary": "#0F172A",
    "text_secondary": "#334155",
    "text_muted": "#64748B",
    "ok_bg": "#ECFDF5",
    "ok_text": "#15803D",
    "warn_bg": "#FFFBEB",
    "warn_text": "#B45309",
    "crit_bg": "#FEF2F2",
    "crit_text": "#B91C1C",
}


class CliError(RuntimeError):
    """Safe user-facing CLI error."""


# ==============================================================================
# 2. SAFE FILE PATHS, INPUT VALIDATION, PII MASKING & SESSION CACHE
# ==============================================================================
def _safe_tmp_path(env_key: str, default_path: str) -> pathlib.Path:
    raw = os.environ.get(env_key) or default_path
    resolved = pathlib.Path(raw).resolve()
    if not str(resolved).startswith(("/tmp/", "/private/tmp/", "/workspace/")):
        return pathlib.Path(default_path)
    return resolved


def mask_pii(value: Any) -> str:
    text = str(value if value is not None else "")
    return EMAIL_PII_RE.sub(r"\1***@\2", text)


def validate_int_id(value: Any, label: str = "ID", *, allow_empty: bool = False) -> int | None:
    clean = str(value if value is not None else "").strip()
    if not clean:
        if allow_empty:
            return None
        raise CliError(f"Missing required dbt {label}.")
    if not NUMERIC_ID_RE.match(clean):
        raise CliError(f"Invalid numeric dbt {label}.")
    parsed = int(clean)
    if parsed <= 0:
        raise CliError(f"dbt {label} must be positive.")
    return parsed


def validate_unique_id(value: Any, *, allow_empty: bool = True) -> str:
    clean = str(value if value is not None else "").strip()
    if not clean:
        if allow_empty:
            return ""
        raise CliError("Missing dbt resource identifier.")
    if not SAFE_UNIQUE_ID_RE.match(clean):
        raise CliError("Invalid dbt resource identifier format.")
    return clean


def load_cache_root() -> dict[str, Any]:
    cache_path = _safe_tmp_path("DBT_A2UI_STATE_CACHE", DEFAULT_STATE_CACHE_PATH)
    if not cache_path.is_file():
        return {}
    with contextlib.suppress(Exception):
        loaded = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            return loaded
    return {}


def save_cache_root(root: dict[str, Any]) -> None:
    cache_path = _safe_tmp_path("DBT_A2UI_STATE_CACHE", DEFAULT_STATE_CACHE_PATH)
    with contextlib.suppress(Exception):
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(root, indent=2), encoding="utf-8")


def load_state() -> dict[str, Any]:
    root = load_cache_root()
    st = root.get("state")
    return st if isinstance(st, dict) else {}


def save_state(state: dict[str, Any]) -> None:
    root = load_cache_root()
    root["state"] = state
    save_cache_root(root)


def _now_utc_short() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")


# ==============================================================================
# 3. 4-TIER PORTABLE CONNECTOR & ENDPOINT AUTO-DISCOVERY
# ==============================================================================
def discover_dbt_mcp_connection(
    cli_connector: str = "",
    cli_endpoint: str = "",
) -> tuple[str, str]:
    """Resolves dbt MCP (connector, endpoint) across any GCP project without hardcoded IDs."""
    root = load_cache_root()
    cached_conn_info = root.get("_connection") if isinstance(root.get("_connection"), dict) else {}

    conn_candidates = [
        (cli_connector or "").strip(),
        (os.environ.get("DBT_MCP_CONNECTOR") or "").strip(),
        (os.environ.get("A2UI_MCP_CONNECTOR") or "").strip(),
        str(cached_conn_info.get("connector") or "").strip(),
    ]
    endp_candidates = [
        (cli_endpoint or "").strip(),
        (os.environ.get("DBT_MCP_ENDPOINT") or "").strip(),
        (os.environ.get("A2UI_MCP_ENDPOINT") or "").strip(),
        str(cached_conn_info.get("endpoint") or "").strip(),
    ]

    conn = next((c for c in conn_candidates if CONNECTOR_RE.match(c)), "")
    endp = next((e for e in endp_candidates if ENDPOINT_RE.match(e) and "<" not in e), "")

    if not conn or not endp:
        conn_pat = re.compile(
            r"(projects/\d{5,25}/locations/[A-Za-z0-9_-]+/collections/[A-Za-z0-9_.-]*dbt[A-Za-z0-9_.-]*/dataConnector)",
            flags=re.IGNORECASE,
        )
        dbt_endp_pat = re.compile(
            r"(https://[A-Za-z0-9.-]*dbt\.com/[A-Za-z0-9_./~%-]*mcp[A-Za-z0-9_./~%-]*)",
            flags=re.IGNORECASE,
        )
        generic_endp_pat = re.compile(
            r"--endpoint\s+['\"]?(https://[A-Za-z0-9.-]+(?::\d+)?(?:/[A-Za-z0-9_./~%-]*)?)['\"]?",
            flags=re.IGNORECASE,
        )
        search_paths = sorted(glob.glob("/workspace/skills/*/SKILL.md")) + sorted(
            glob.glob("/workspace/.gemini/**/*.md", recursive=True)
        )
        for path_str in search_paths:
            with contextlib.suppress(Exception):
                txt = pathlib.Path(path_str).read_text(encoding="utf-8")
                if SELF_B64_MARKER in txt:
                    continue
                if not conn:
                    mc = conn_pat.search(txt)
                    if mc:
                        conn = mc.group(1)
                if not endp and "dbt" in txt.lower():
                    me = dbt_endp_pat.search(txt) or generic_endp_pat.search(txt)
                    if me:
                        endp = me.group(1)
                if conn and endp:
                    break

    if conn or endp:
        root["_connection"] = {"connector": conn, "endpoint": endp}
        save_cache_root(root)
    return conn, endp


# ==============================================================================
# 4. `remote_cli mcp` GATEWAY & CONCURRENT EXECUTION ENGINE
# ==============================================================================
def _resolve_remote_cli_bin() -> str:
    for cand in ("/usr/local/bin/remote_cli", "/usr/bin/remote_cli", "remote_cli"):
        if cand == "remote_cli" or os.path.isfile(cand):
            if cand in ALLOWED_REMOTE_CLI_BINS:
                return cand
    raise CliError("Untrusted or missing remote_cli executable.")


def _try_parse_json(raw: str) -> Any:
    txt = (raw or "").strip()
    if not txt:
        return None
    with contextlib.suppress(json.JSONDecodeError):
        return json.loads(txt)
    fence = re.search(r"```(?:json)?\s*([\[{].*?[\]}])\s*```", txt, flags=re.DOTALL)
    if fence:
        with contextlib.suppress(json.JSONDecodeError):
            return json.loads(fence.group(1))
    for line in reversed(txt.splitlines()):
        cand = line.strip()
        if cand.startswith("{") or cand.startswith("["):
            with contextlib.suppress(json.JSONDecodeError):
                return json.loads(cand)
    return None


def _unwrap_mcp_payload(parsed: Any) -> tuple[Any, str, bool]:
    """Unwraps remote_cli mcp envelope -> (data_obj, text_str, is_error)."""
    if isinstance(parsed, list):
        return parsed, "", False
    if not isinstance(parsed, dict):
        return {}, str(parsed or ""), False

    is_err = bool(parsed.get("isError"))
    inner = parsed.get("result") if ("result" in parsed and ("tool" in parsed or "isError" in parsed)) else parsed

    if isinstance(inner, list):
        return inner, "", is_err
    if isinstance(inner, str):
        maybe = _try_parse_json(inner)
        if maybe is not None:
            return maybe, inner, is_err
        return {"text": inner}, inner, is_err
    if isinstance(inner, dict):
        if isinstance(inner.get("structuredContent"), (dict, list)):
            sc = inner["structuredContent"]
            txt = str(inner.get("text") or "")
            return sc, txt, is_err
        if isinstance(inner.get("content"), list):
            texts: list[str] = []
            for item in inner["content"]:
                if isinstance(item, dict) and item.get("text") is not None:
                    texts.append(str(item["text"]))
            joined = "\n".join(texts).strip()
            maybe = _try_parse_json(joined)
            if maybe is not None:
                return maybe, joined, is_err
            return {"text": joined}, joined, is_err
        return inner, str(inner.get("text") or ""), is_err
    return {}, "", is_err


class DbtMcpGateway:
    """Executes allow-listed dbt Cloud MCP tools concurrently via `remote_cli mcp`."""

    def __init__(self, connector: str = "", endpoint: str = "", mock_fixtures_path: str = "") -> None:
        self.connector, self.endpoint = discover_dbt_mcp_connection(connector, endpoint)
        self.fixtures: dict[str, Any] | None = None
        if mock_fixtures_path:
            fp = pathlib.Path(mock_fixtures_path).resolve()
            if fp.is_file():
                self.fixtures = json.loads(fp.read_text(encoding="utf-8"))

    def ensure_ready(self) -> None:
        if self.fixtures is not None:
            return
        if not self.connector or not CONNECTOR_RE.match(self.connector):
            raise CliError(
                "dbt MCP dataConnector was not auto-discovered in this workspace. "
                "Re-run with --connector 'projects/<PROJECT_NUMBER>/locations/global/collections/<DBT_CONNECTOR_ID>/dataConnector' "
                "and --endpoint 'https://<your-dbt-host>/api/ai/v1/mcp' from your connected dbt MCP server."
            )
        if not self.endpoint or not ENDPOINT_RE.match(self.endpoint):
            raise CliError(
                "dbt MCP endpoint URL was not auto-discovered. "
                "Re-run with --endpoint 'https://<your-dbt-host>/api/ai/v1/mcp'."
            )

    def call(self, tool_name: str, arguments: dict[str, Any] | None = None, *, timeout_s: int = 55) -> tuple[Any, str]:
        if tool_name not in ALLOWED_DBT_TOOLS:
            raise CliError(f"Tool '{tool_name}' is not in the allow-listed dbt MCP catalog.")
        clean_args = arguments or {}

        if self.fixtures is not None:
            val = self.fixtures.get(tool_name, {})
            if isinstance(val, dict) and "_by_id" in val:
                lookup_key = str(
                    clean_args.get("run_id")
                    or clean_args.get("unique_id")
                    or clean_args.get("name")
                    or clean_args.get("job_id")
                    or ""
                )
                val = val["_by_id"].get(lookup_key, val.get("_default", {}))
            return val, json.dumps(val) if not isinstance(val, str) else val

        self.ensure_ready()
        cli_bin = _resolve_remote_cli_bin()
        payload_str = json.dumps(clean_args, separators=(",", ":"))
        cmd = [
            cli_bin,
            "mcp",
            self.connector,
            tool_name,
            "--json",
            payload_str,
            "--endpoint",
            self.endpoint,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, check=False)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise CliError(f"dbt MCP '{tool_name}' failed (exit {proc.returncode}): {mask_pii(detail[:500])}")

        parsed = _try_parse_json(proc.stdout)
        if parsed is None:
            return {"text": proc.stdout.strip()}, proc.stdout.strip()
        data_obj, text_str, is_err = _unwrap_mcp_payload(parsed)
        if is_err:
            raise CliError(f"dbt MCP '{tool_name}' error: {mask_pii((text_str or json.dumps(data_obj))[:500])}")
        return data_obj, text_str

    def call_soft(self, tool_name: str, arguments: dict[str, Any] | None = None, *, timeout_s: int = 40) -> tuple[Any, str, str]:
        """Calls a tool without aborting the batch if an optional endpoint is unavailable."""
        try:
            data, txt = self.call(tool_name, arguments, timeout_s=timeout_s)
            return data, txt, ""
        except Exception as err:
            return {}, "", str(err)

    def call_parallel(
        self,
        specs: dict[str, tuple[str, dict[str, Any] | None]],
        *,
        max_workers: int = 8,
        timeout_s: int = 42,
    ) -> dict[str, tuple[Any, str, str]]:
        """Executes multiple read-only dbt MCP tools concurrently so initial prefetch is fast."""
        if not specs:
            return {}
        results: dict[str, tuple[Any, str, str]] = {}
        workers = min(max_workers, max(1, len(specs)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            fut_map = {
                pool.submit(self.call_soft, t_name, t_args, timeout_s=timeout_s): key
                for key, (t_name, t_args) in specs.items()
            }
            for fut in concurrent.futures.as_completed(fut_map):
                key = fut_map[fut]
                try:
                    results[key] = fut.result()
                except Exception as exc:
                    results[key] = ({}, "", str(exc))
        return results


# ==============================================================================
# 5. RESILIENT RECORD / LIST EXTRACTORS FOR dbt CLOUD RESPONSES
# ==============================================================================
def extract_items(obj: Any, preferred_keys: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    """Extracts a list of dicts from varied dbt Cloud REST / Discovery GraphQL / MetricFlow shapes."""
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if not isinstance(obj, dict):
        return []

    search_keys = list(preferred_keys) + [
        "data",
        "items",
        "results",
        "projects",
        "models",
        "mart_models",
        "sources",
        "exposures",
        "macros",
        "metrics",
        "saved_queries",
        "savedQueries",
        "dimensions",
        "entities",
        "jobs",
        "runs",
        "nodes",
        "rows",
        "pages",
    ]
    for key in search_keys:
        val = obj.get(key)
        if isinstance(val, list):
            if val and all(isinstance(x, dict) for x in val):
                return val
            if val and all(isinstance(x, str) for x in val):
                return [{"name": x} for x in val]
            if not val:
                return []
        if isinstance(val, dict):
            if isinstance(val.get("edges"), list):
                return [e["node"] for e in val["edges"] if isinstance(e, dict) and isinstance(e.get("node"), dict)]
            nested = extract_items(val, preferred_keys)
            if nested:
                return nested
    if isinstance(obj.get("edges"), list):
        return [e["node"] for e in obj["edges"] if isinstance(e, dict) and isinstance(e.get("node"), dict)]
    return []


def resolve_active_project(
    gw: DbtMcpGateway,
    state: dict[str, Any],
    requested_project_id: int | None = None,
    force_refresh: bool = False,
) -> tuple[int, str, list[dict[str, Any]]]:
    """Discovers active dbt Cloud projects via `list_projects` and caches them in session state."""
    gw.ensure_ready()
    projects = state.get("projects") if isinstance(state.get("projects"), list) else []
    if not projects or requested_project_id is not None or force_refresh:
        raw_proj, _, _ = gw.call_soft("list_projects", {"limit": 50})
        extracted = extract_items(raw_proj, ("projects", "data", "items"))
        norm_projects: list[dict[str, Any]] = []
        for p in extracted:
            pid = p.get("id") or p.get("project_id") or p.get("projectId")
            try:
                pid_int = int(pid)
            except (TypeError, ValueError):
                continue
            pname = str(p.get("name") or p.get("displayName") or f"Project {pid_int}")
            norm_projects.append({"id": str(pid_int), "name": pname})
        if norm_projects:
            projects = norm_projects
            state["projects"] = projects

    if requested_project_id is not None:
        match = next((p for p in projects if str(p.get("id")) == str(requested_project_id)), None)
        pname = match["name"] if match else f"Project {requested_project_id}"
        state["active_project_id"] = str(requested_project_id)
        state["active_project_name"] = pname
        return requested_project_id, pname, projects

    cached_pid = state.get("active_project_id")
    if cached_pid and NUMERIC_ID_RE.match(str(cached_pid)):
        pid_int = int(cached_pid)
        match = next((p for p in projects if str(p.get("id")) == str(pid_int)), None)
        pname = match["name"] if match else str(state.get("active_project_name") or f"Project {pid_int}")
        return pid_int, pname, projects

    if projects:
        first = projects[0]
        pid_int = int(first["id"])
        state["active_project_id"] = str(pid_int)
        state["active_project_name"] = first["name"]
        return pid_int, first["name"], projects

    raise CliError("No active dbt Cloud projects were returned by list_projects.")


# ==============================================================================
# 6. PARALLEL PREFETCH BUNDLE & TARGETED DEEP-DIVE HYDRATORS
# ==============================================================================
def _normalize_run_status_bucket(raw_status: str) -> str:
    s = (raw_status or "").lower()
    if "err" in s or "fail" in s:
        return "error"
    if "run" in s or "queue" in s or "start" in s:
        return "running"
    if "succ" in s or "pass" in s or "ok" in s:
        return "success"
    if "cancel" in s:
        return "cancelled"
    return "other"


def _normalize_source_freshness(s: dict[str, Any]) -> tuple[str, str]:
    fresh = s.get("freshness") or s.get("freshnessStatus") or s.get("maxLoadedAt") or s.get("state") or "Tracked"
    loaded_at = str(s.get("maxLoadedAt") or "")[:19]
    if isinstance(fresh, dict):
        st = str(fresh.get("status") or fresh.get("freshnessStatus") or fresh.get("maxLoadedAt") or "Healthy")
        if not loaded_at and fresh.get("maxLoadedAt"):
            loaded_at = str(fresh.get("maxLoadedAt"))[:19]
        fresh = st
    fresh_str = str(fresh)[:60]
    low = fresh_str.lower()
    if "err" in low or "stale" in low or "fail" in low:
        bucket = "error"
    elif "warn" in low:
        bucket = "warn"
    elif "pass" in low or "health" in low or "fresh" in low or "ok" in low:
        bucket = "healthy"
    else:
        bucket = "tracked"
    return fresh_str, bucket


def _extract_run_diagnostic(raw_rd: Any, txt_rd: str, raw_err: Any, txt_err: str, run_id_str: str) -> dict[str, Any]:
    steps_out: list[dict[str, str]] = []
    git_branch = ""
    git_sha = ""
    status_str = ""
    summary_text = ""

    if isinstance(raw_rd, dict):
        rd_core = raw_rd.get("data") if isinstance(raw_rd.get("data"), dict) else raw_rd
        status_str = str(rd_core.get("status_humanized") or rd_core.get("status") or "")
        git_branch = str(rd_core.get("git_branch") or "")
        git_sha = str(rd_core.get("git_sha") or "")[:10]
        steps_raw = extract_items(rd_core, ("run_steps", "steps"))
        for idx, stp in enumerate(steps_raw[:12]):
            steps_out.append({
                "name": str(stp.get("name") or f"Step {idx + 1}"),
                "status": str(stp.get("status_humanized") or stp.get("status") or ""),
                "duration": str(stp.get("duration_humanized") or stp.get("duration") or ""),
            })
        if not steps_out and txt_rd:
            summary_text = txt_rd[:600]

    error_text = ""
    if isinstance(raw_err, dict) and raw_err:
        if "text" in raw_err and len(raw_err) == 1:
            error_text = str(raw_err.get("text") or "")[:2400]
        else:
            error_text = json.dumps(raw_err, indent=2)[:2400]
    elif txt_err:
        error_text = str(txt_err)[:2400]

    # Try to spot a failed dbt model name inside error_text so the UI can offer 1-click selective rebuild
    failed_model_hint = ""
    m_mod = re.search(r"\bmodel\.[A-Za-z0-9_]+\.([A-Za-z0-9_]+)\b", error_text)
    if m_mod:
        failed_model_hint = m_mod.group(1)

    return {
        "id": run_id_str,
        "status": status_str,
        "git_branch": git_branch,
        "git_sha": git_sha,
        "steps": steps_out,
        "summary_text": summary_text,
        "error_text": mask_pii(error_text),
        "failed_model_hint": failed_model_hint,
        "deep_dive_loaded": False,
        "artifacts": [],
        "docs_hits": [],
    }


def _parse_lineage_and_node(
    lin_uid: str,
    raw_lin: Any,
    raw_nd: Any,
    txt_nd: str,
    raw_h: Any,
    txt_h: str,
    raw_p: Any = None,
    *,
    include_perf: bool = False,
) -> dict[str, Any]:
    nodes_raw = extract_items(raw_lin, ("nodes", "items", "data"))
    edges_raw = extract_items(raw_lin, ("edges", "links", "relationships"))

    node_map: dict[str, dict[str, Any]] = {}
    for n in nodes_raw:
        nid = str(n.get("uniqueId") or n.get("unique_id") or n.get("id") or "").strip()
        if not nid:
            continue
        rtype = str(n.get("resourceType") or n.get("resource_type") or n.get("type") or nid.split(".")[0]).lower()
        node_map[nid] = {
            "unique_id": nid,
            "name": str(n.get("name") or nid.split(".")[-1]),
            "resource_type": rtype,
        }

    direct_parents: set[str] = set()
    direct_children: set[str] = set()
    all_ancestors: set[str] = set()
    all_descendants: set[str] = set()
    for edge in edges_raw:
        src = str(edge.get("source") or edge.get("from") or edge.get("parent") or "")
        dst = str(edge.get("target") or edge.get("to") or edge.get("child") or "")
        if dst == lin_uid and src:
            direct_parents.add(src)
        elif src == lin_uid and dst:
            direct_children.add(dst)
        if src and src != lin_uid:
            all_ancestors.add(src)
        if dst and dst != lin_uid:
            all_descendants.add(dst)

    if direct_parents or direct_children:
        up_ids = list(direct_parents) + [x for x in all_ancestors if x not in direct_parents and x not in direct_children]
        down_ids = list(direct_children) + [x for x in all_descendants if x not in direct_children and x not in direct_parents]
    else:
        up_ids = [nid for nid, info in node_map.items() if nid != lin_uid and info["resource_type"] in ("source", "seed")]
        down_ids = [nid for nid in node_map if nid != lin_uid and nid not in up_ids]

    upstream_nodes = [
        node_map.get(nid, {"unique_id": nid, "name": nid.split(".")[-1], "resource_type": nid.split(".")[0]})
        for nid in up_ids[:14]
    ]
    downstream_nodes = [
        node_map.get(nid, {"unique_id": nid, "name": nid.split(".")[-1], "resource_type": nid.split(".")[0]})
        for nid in down_ids[:14]
    ]

    guessed_rtype = lin_uid.split(".")[0].lower() if "." in lin_uid else "model"
    nd_dict = raw_nd if isinstance(raw_nd, dict) else {}
    if isinstance(nd_dict.get("data"), dict):
        nd_dict = nd_dict["data"]
    if isinstance(nd_dict.get("model"), dict):
        nd_dict = nd_dict["model"]

    cols_out: list[dict[str, str]] = []
    raw_cols = nd_dict.get("columns") or nd_dict.get("catalogColumns") or []
    if isinstance(raw_cols, dict):
        for cname, cinfo in list(raw_cols.items())[:35]:
            if isinstance(cinfo, dict):
                cols_out.append({
                    "name": str(cname),
                    "type": str(cinfo.get("type") or cinfo.get("data_type") or "ANY"),
                    "description": str(cinfo.get("description") or "")[:140],
                })
    elif isinstance(raw_cols, list):
        for cinfo in raw_cols[:35]:
            if isinstance(cinfo, dict) and cinfo.get("name"):
                cols_out.append({
                    "name": str(cinfo.get("name")),
                    "type": str(cinfo.get("type") or cinfo.get("data_type") or "ANY"),
                    "description": str(cinfo.get("description") or "")[:140],
                })

    health_kv: dict[str, Any] = {}
    health_text = ""
    if isinstance(raw_h, dict) and raw_h:
        for k, v in list(raw_h.items())[:12]:
            if k == "text" and len(raw_h) == 1:
                health_text = str(v)[:800]
            else:
                health_kv[str(k)] = str(v)[:180] if not isinstance(v, (int, float, bool)) else v
    elif txt_h:
        health_text = txt_h[:800]

    perf_runs: list[dict[str, Any]] = []
    if raw_p is not None:
        for r in extract_items(raw_p, ("runs", "performance", "history", "items", "data"))[:8]:
            exec_s = (
                r.get("executionTime")
                or r.get("execution_time")
                or r.get("duration")
                or r.get("model_execution_time")
                or 0
            )
            try:
                exec_val = round(float(exec_s), 2)
            except (TypeError, ValueError):
                exec_val = 0.0
            perf_runs.append({
                "run_id": str(r.get("runId") or r.get("run_id") or ""),
                "status": str(r.get("status") or "success"),
                "execution_seconds": exec_val,
                "completed_at": str(r.get("executeCompletedAt") or r.get("created_at") or "")[:19],
            })

    return {
        "unique_id": lin_uid,
        "name": str(nd_dict.get("name") or lin_uid.split(".")[-1]),
        "resource_type": guessed_rtype,
        "materialization": str(
            nd_dict.get("materializedType")
            or nd_dict.get("materialization")
            or (nd_dict.get("config") or {}).get("materialized")
            or "table/view"
        ),
        "schema": str(nd_dict.get("schema") or ""),
        "database": str(nd_dict.get("database") or ""),
        "description": str(nd_dict.get("description") or "")[:320],
        "raw_sql": str(nd_dict.get("rawCode") or nd_dict.get("raw_code") or nd_dict.get("rawSql") or "")[:3000],
        "compiled_sql": str(nd_dict.get("compiledCode") or nd_dict.get("compiled_code") or nd_dict.get("compiledSql") or "")[:3000],
        "columns": cols_out,
        "upstream": upstream_nodes,
        "downstream": downstream_nodes,
        "health_kv": health_kv,
        "health_text": health_text,
        "performance": perf_runs,
        "has_perf": include_perf or bool(perf_runs),
        "fallback_text": txt_nd[:900] if not cols_out and not nd_dict else "",
    }


def hydrate_workspace_bundle(
    gw: DbtMcpGateway,
    state: dict[str, Any],
    *,
    project_id: int,
    force_refresh: bool = False,
) -> None:
    """Prefetches the core dbt workspace dataset in parallel so 90% of user filtering/viewing is 0ms."""
    already_pid = str(state.get("_bundle_project_id") or "")
    if not force_refresh and already_pid == str(project_id) and state.get("bundle_ready"):
        return

    # Wave 1: Fetch all catalog & operational lists concurrently
    wave1 = gw.call_parallel({
        "jobs": ("list_jobs", {"project_id": project_id, "limit": 50}),
        "runs": ("list_jobs_runs", {"order_by": "-created_at", "limit": 40}),
        "marts": ("get_mart_models", {"project_id": project_id, "limit": 60}),
        "models": ("get_all_models", {"project_id": project_id, "limit": 100}),
        "sources": ("get_all_sources", {"project_id": project_id, "limit": 60}),
        "exposures": ("get_exposures", {"project_id": project_id, "limit": 40}),
        "metrics": ("list_metrics", {"project_id": project_id}),
        "saved_queries": ("list_saved_queries", {"project_id": project_id}),
        "macros": ("get_all_macros", {"project_id": project_id, "include_default_dbt_packages": False, "limit": 40}),
    })

    # 1. Jobs & Runs
    norm_jobs: list[dict[str, Any]] = []
    for j in extract_items(wave1["jobs"][0], ("jobs", "data", "items"))[:40]:
        jid = j.get("id") or j.get("job_id")
        try:
            jid_int = int(jid)
        except (TypeError, ValueError):
            continue
        steps_raw = j.get("execute_steps") or j.get("executeSteps") or []
        norm_jobs.append({
            "id": str(jid_int),
            "name": str(j.get("name") or f"Job {jid_int}"),
            "environment_id": str(j.get("environment_id") or ""),
            "steps": [str(s) for s in steps_raw[:6]] if isinstance(steps_raw, list) else [],
            "schedule": str((j.get("schedule") or {}).get("cron") if isinstance(j.get("schedule"), dict) else ""),
        })

    job_name_by_id = {j["id"]: j["name"] for j in norm_jobs}
    norm_runs: list[dict[str, Any]] = []
    for r in extract_items(wave1["runs"][0], ("runs", "data", "items"))[:40]:
        rid = r.get("id") or r.get("run_id")
        try:
            rid_int = int(rid)
        except (TypeError, ValueError):
            continue
        r_jid = str(r.get("job_definition_id") or r.get("job_id") or "")
        r_proj = r.get("project_id")
        if r_proj and str(r_proj) != str(project_id) and norm_jobs and r_jid not in job_name_by_id:
            continue
        st_human = str(r.get("status_humanized") or r.get("status") or "unknown").lower()
        norm_runs.append({
            "id": str(rid_int),
            "job_id": r_jid,
            "job_name": job_name_by_id.get(r_jid) or str((r.get("job") or {}).get("name") or f"Job {r_jid or 'Run'}"),
            "status": st_human,
            "status_bucket": _normalize_run_status_bucket(st_human),
            "duration": str(r.get("duration_humanized") or r.get("duration") or ""),
            "created_at": str(r.get("created_at") or r.get("finished_at") or "")[:19],
            "git_branch": str(r.get("git_branch") or ""),
            "trigger_cause": mask_pii(
                str(r.get("trigger", {}).get("cause") if isinstance(r.get("trigger"), dict) else (r.get("cause") or ""))[:90]
            ),
        })

    # 2. Unified Catalog (Marts + All Models + Sources + Exposures)
    mart_uids: set[str] = set()
    norm_marts: list[dict[str, Any]] = []
    for m in extract_items(wave1["marts"][0], ("models", "mart_models", "items", "data"))[:60]:
        uid = str(m.get("uniqueId") or m.get("unique_id") or "").strip()
        name = str(m.get("name") or (uid.split(".")[-1] if uid else "")).strip()
        if not name:
            continue
        key_uid = uid or f"model.{name}"
        mart_uids.add(key_uid)
        mart_uids.add(name)
        norm_marts.append({
            "unique_id": key_uid,
            "name": name,
            "kind": "mart",
            "materialization": str(m.get("materializedType") or m.get("materialization") or "table"),
            "schema": str(m.get("schema") or ""),
            "description": str(m.get("description") or "")[:200],
            "ref_snippet": f"{{{{ ref('{name}') }}}}",
            "status_badge": "Governed Mart",
            "status_bucket": "healthy",
        })

    seen_model_uids: set[str] = {m["unique_id"] for m in norm_marts}
    norm_models: list[dict[str, Any]] = list(norm_marts)
    for m in extract_items(wave1["models"][0], ("models", "items", "data"))[:100]:
        uid = str(m.get("uniqueId") or m.get("unique_id") or "").strip()
        name = str(m.get("name") or (uid.split(".")[-1] if uid else "")).strip()
        if not name:
            continue
        key_uid = uid or f"model.{name}"
        if key_uid in seen_model_uids:
            continue
        seen_model_uids.add(key_uid)
        is_mart = (key_uid in mart_uids) or (name in mart_uids) or name.startswith(("fct_", "dim_", "mart_"))
        entry = {
            "unique_id": key_uid,
            "name": name,
            "kind": "mart" if is_mart else "model",
            "materialization": str(m.get("materializedType") or m.get("materialization") or "view/table"),
            "schema": str(m.get("schema") or ""),
            "description": str(m.get("description") or "")[:200],
            "ref_snippet": f"{{{{ ref('{name}') }}}}",
            "status_badge": "Governed Mart" if is_mart else "Model",
            "status_bucket": "healthy",
        }
        norm_models.append(entry)
        if is_mart and len(norm_marts) < 60:
            norm_marts.append(entry)

    norm_sources: list[dict[str, Any]] = []
    for s in extract_items(wave1["sources"][0], ("sources", "items", "data"))[:60]:
        uid = str(s.get("uniqueId") or s.get("unique_id") or "").strip()
        sname = str(s.get("name") or s.get("identifier") or (uid.split(".")[-1] if uid else "")).strip()
        src_group = str(s.get("sourceName") or s.get("source_name") or s.get("schema") or "raw").strip()
        if not sname:
            continue
        fresh_str, fresh_bucket = _normalize_source_freshness(s)
        full_name = f"{src_group}.{sname}" if src_group else sname
        norm_sources.append({
            "unique_id": uid or f"source.{full_name}",
            "name": full_name,
            "source_name": src_group,
            "table_name": sname,
            "kind": "source",
            "freshness": fresh_str,
            "status_badge": fresh_str,
            "status_bucket": fresh_bucket,
            "description": str(s.get("description") or "")[:180],
            "ref_snippet": f"{{{{ source('{src_group}', '{sname}') }}}}",
        })

    norm_exposures: list[dict[str, Any]] = []
    for ex in extract_items(wave1["exposures"][0], ("exposures", "items", "data"))[:40]:
        uid = str(ex.get("uniqueId") or ex.get("unique_id") or "").strip()
        ename = str(ex.get("name") or ex.get("label") or uid).strip()
        if not ename:
            continue
        etype = str(ex.get("type") or ex.get("exposureType") or "Dashboard")
        owner_raw = ex.get("owner") if isinstance(ex.get("owner"), dict) else {}
        owner_name = mask_pii(str(owner_raw.get("name") or owner_raw.get("email") or ""))
        norm_exposures.append({
            "unique_id": uid or f"exposure.{ename}",
            "name": ename,
            "kind": "exposure",
            "exposure_type": etype,
            "owner": owner_name,
            "url": str(ex.get("url") or ""),
            "status_badge": etype,
            "status_bucket": "tracked",
            "description": mask_pii(str(ex.get("description") or "")[:180]),
        })

    # 3. Metrics, Saved Queries & Macros
    metrics_list: list[dict[str, Any]] = []
    for m in extract_items(wave1["metrics"][0], ("metrics", "items", "data"))[:60]:
        mname = str(m.get("name") or m.get("id") or "").strip()
        if not mname:
            continue
        metrics_list.append({
            "name": mname,
            "label": str(m.get("label") or mname.replace("_", " ").title()),
            "description": str(m.get("description") or "")[:220],
            "type": str(m.get("type") or "metric"),
        })

    saved_list: list[dict[str, Any]] = []
    for sq in extract_items(wave1["saved_queries"][0], ("saved_queries", "savedQueries", "items"))[:25]:
        sq_name = str(sq.get("name") or "").strip()
        if sq_name:
            saved_list.append({
                "name": sq_name,
                "label": str(sq.get("label") or sq_name),
                "description": str(sq.get("description") or "")[:180],
            })

    macros_list: list[dict[str, str]] = []
    for mc in extract_items(wave1["macros"][0], ("macros", "items", "data"))[:35]:
        mname = str(mc.get("name") or "").strip()
        if mname:
            macros_list.append({
                "name": mname,
                "package": str(mc.get("packageName") or mc.get("package_name") or "project"),
                "description": str(mc.get("description") or "")[:140],
                "snippet": f"{{{{ {mname}() }}}}",
            })

    # Wave 2: Targeted parallel pre-hydration of high-probability immediate items:
    # - Up to 2 failed runs + 1 latest run -> immediate 0ms error & step triage!
    # - Focus mart model -> immediate 0ms lineage, SQL, columns & health!
    # - Primary metric -> immediate 0ms dimensions, entities, bars & compiled SQL!
    wave2_specs: dict[str, tuple[str, dict[str, Any] | None]] = {}

    failed_runs = [r for r in norm_runs if r["status_bucket"] == "error"]
    runs_to_prefetch: list[str] = [r["id"] for r in failed_runs[:2]]
    if norm_runs and norm_runs[0]["id"] not in runs_to_prefetch:
        runs_to_prefetch.append(norm_runs[0]["id"])

    for rid_str in runs_to_prefetch[:3]:
        rid_i = int(rid_str)
        wave2_specs[f"rd_{rid_str}"] = ("get_job_run_details", {"run_id": rid_i})
        wave2_specs[f"re_{rid_str}"] = ("get_job_run_error", {"run_id": rid_i, "include_warnings": True})

    focus_model_uid = ""
    if norm_marts:
        focus_model_uid = norm_marts[0]["unique_id"]
    elif norm_models:
        mart_like = next((c for c in norm_models if not c["name"].startswith("stg_")), norm_models[0])
        focus_model_uid = mart_like["unique_id"]

    if focus_model_uid:
        id_arg = {"unique_id": focus_model_uid} if "." in focus_model_uid else {"name": focus_model_uid}
        wave2_specs["focus_lin"] = (
            "get_lineage",
            {"project_id": project_id, "unique_id": focus_model_uid, "direction": "both", "depth": 2, "limit": 60},
        )
        wave2_specs["focus_nd"] = ("get_node_details", {"project_id": project_id, "resource_type": "model", **id_arg})
        wave2_specs["focus_mh"] = ("get_model_health", {"project_id": project_id, **id_arg})

    first_metric = metrics_list[0]["name"] if metrics_list else ""
    if first_metric:
        q_init = {
            "project_id": project_id,
            "metrics": [first_metric],
            "group_by": [{"name": "metric_time", "type": "time_dimension", "grain": "MONTH"}],
            "limit": 24,
        }
        wave2_specs["met_dims"] = ("get_dimensions", {"project_id": project_id, "metrics": [first_metric]})
        wave2_specs["met_ents"] = ("get_entities", {"project_id": project_id, "metrics": [first_metric]})
        wave2_specs["met_q"] = ("query_metrics", q_init)
        wave2_specs["met_sql"] = ("get_metrics_compiled_sql", q_init)

    wave2 = gw.call_parallel(wave2_specs, max_workers=8)

    run_diagnostics: dict[str, dict[str, Any]] = (
        state.get("run_diagnostics") if isinstance(state.get("run_diagnostics"), dict) and not force_refresh else {}
    )
    for rid_str in runs_to_prefetch[:3]:
        raw_rd, txt_rd, _ = wave2.get(f"rd_{rid_str}", ({}, "", ""))
        raw_re, txt_re, _ = wave2.get(f"re_{rid_str}", ({}, "", ""))
        run_diagnostics[rid_str] = _extract_run_diagnostic(raw_rd, txt_rd, raw_re, txt_re, rid_str)

    model_deep_dives: dict[str, dict[str, Any]] = (
        state.get("model_deep_dives") if isinstance(state.get("model_deep_dives"), dict) and not force_refresh else {}
    )
    if focus_model_uid:
        raw_lin, _, _ = wave2.get("focus_lin", ({}, "", ""))
        raw_nd, txt_nd, _ = wave2.get("focus_nd", ({}, "", ""))
        raw_mh, txt_mh, _ = wave2.get("focus_mh", ({}, "", ""))
        model_deep_dives[focus_model_uid] = _parse_lineage_and_node(
            focus_model_uid, raw_lin, raw_nd, txt_nd, raw_mh, txt_mh
        )

    # Initial Metric Slice
    dims_list: list[dict[str, str]] = []
    entities_list: list[str] = []
    rows_out: list[dict[str, Any]] = []
    compiled_sql = ""
    raw_preview = ""
    metric_err = wave1["metrics"][2]

    if first_metric:
        raw_dims, _, _ = wave2.get("met_dims", ({}, "", ""))
        for d in extract_items(raw_dims, ("dimensions", "items", "data"))[:35]:
            dname = str(d.get("name") or d.get("qualified_name") or d.get("qualifiedName") or "").strip()
            dtype = str(d.get("type") or "categorical").lower()
            if dname:
                dims_list.append({"name": dname, "type": "time_dimension" if "time" in dtype else "dimension"})

        raw_ents, _, _ = wave2.get("met_ents", ({}, "", ""))
        for e in extract_items(raw_ents, ("entities", "items", "data"))[:20]:
            ename = str(e.get("name") or "").strip()
            if ename:
                entities_list.append(ename)

        raw_qm, txt_qm, err_q = wave2.get("met_q", ({}, "", ""))
        if err_q:
            raw_qm, txt_qm, err_q2 = gw.call_soft(
                "query_metrics", {"project_id": project_id, "metrics": [first_metric], "limit": 24}
            )
            if err_q2:
                metric_err = err_q
        extracted_rows = extract_items(raw_qm, ("rows", "data", "items", "results"))
        if not extracted_rows and isinstance(raw_qm, dict) and isinstance(raw_qm.get("table"), dict):
            extracted_rows = extract_items(raw_qm["table"], ("rows", "data"))
        rows_out = extracted_rows[:24]
        if not rows_out and txt_qm:
            raw_preview = txt_qm[:1200]

        raw_sql, txt_sql, _ = wave2.get("met_sql", ({}, "", ""))
        if isinstance(raw_sql, dict):
            compiled_sql = str(
                raw_sql.get("sql") or raw_sql.get("compiled_sql") or raw_sql.get("compiledSql") or raw_sql.get("text") or txt_sql
            )[:3000]
        else:
            compiled_sql = str(txt_sql or "")[:3000]

    default_run_id = failed_runs[0]["id"] if failed_runs else (norm_runs[0]["id"] if norm_runs else "")
    default_job_id = ""
    if default_run_id:
        mr = next((r for r in norm_runs if r["id"] == default_run_id and r.get("job_id")), None)
        if mr:
            default_job_id = mr["job_id"]
    if not default_job_id and norm_jobs:
        default_job_id = norm_jobs[0]["id"]

    prev_sql = state.get("sql_view") if isinstance(state.get("sql_view"), dict) and not force_refresh else {}
    default_sql = str(prev_sql.get("generated_sql") or "")
    if not default_sql:
        if norm_marts:
            default_sql = f"SELECT *\nFROM {norm_marts[0]['ref_snippet']}\nLIMIT 25"
        elif norm_models:
            default_sql = f"SELECT *\nFROM {norm_models[0]['ref_snippet']}\nLIMIT 25"

    state["_bundle_project_id"] = str(project_id)
    state["bundle_ready"] = True
    state["last_synced"] = _now_utc_short()
    state["jobs"] = norm_jobs
    state["runs"] = norm_runs
    state["selected_run_id"] = default_run_id
    state["selected_job_id"] = default_job_id
    state["run_diagnostics"] = run_diagnostics

    state["marts"] = norm_marts
    state["models"] = norm_models
    state["sources"] = norm_sources
    state["exposures"] = norm_exposures
    state["macros"] = macros_list
    state["focus_unique_id"] = focus_model_uid
    state["catalog_view_mode"] = "list"
    state["catalog_subtab"] = "overview"
    state["lineage_orientation"] = "lr"
    state["model_deep_dives"] = model_deep_dives

    state["metrics_view"] = {
        "metrics": metrics_list,
        "saved_queries": saved_list,
        "selected_metric": first_metric,
        "grain": "MONTH",
        "group_dim": "",
        "where_dim": "",
        "where_val": "",
        "dimensions": dims_list,
        "entities": entities_list,
        "dimension_values": [],
        "rows": rows_out,
        "compiled_sql": compiled_sql,
        "raw_preview": raw_preview,
        "error": metric_err,
    }

    state["sql_view"] = {
        "nl_prompt": str(prev_sql.get("nl_prompt") or ""),
        "generated_sql": default_sql,
        "query_rows": prev_sql.get("query_rows") or [],
        "query_text": prev_sql.get("query_text") or "",
    }


def deep_dive_run(
    gw: DbtMcpGateway,
    state: dict[str, Any],
    *,
    project_id: int,
    run_id: int,
    fetch_docs: bool = True,
) -> None:
    """On-demand Deep Dive for a specific dbt Job Run (steps + errors + artifacts + dbt Docs)."""
    hydrate_workspace_bundle(gw, state, project_id=project_id, force_refresh=False)
    rid_str = str(run_id)
    batch = gw.call_parallel({
        "rd": ("get_job_run_details", {"run_id": run_id}),
        "re": ("get_job_run_error", {"run_id": run_id, "include_warnings": True}),
        "art": ("list_job_run_artifacts", {"run_id": run_id}),
    })
    raw_rd, txt_rd, _ = batch["rd"]
    raw_re, txt_re, _ = batch["re"]
    raw_art, _, _ = batch["art"]

    diag = _extract_run_diagnostic(raw_rd, txt_rd, raw_re, txt_re, rid_str)
    artifacts_list: list[str] = []
    if isinstance(raw_art, list):
        artifacts_list = [str(a) for a in raw_art[:18]]
    elif isinstance(raw_art, dict):
        arr = raw_art.get("artifacts") or raw_art.get("data") or raw_art.get("items") or []
        if isinstance(arr, list):
            artifacts_list = [str(a.get("path") if isinstance(a, dict) else a) for a in arr[:18]]
    diag["artifacts"] = artifacts_list

    docs_hits: list[dict[str, str]] = []
    if fetch_docs and diag.get("error_text"):
        err_txt = str(diag["error_text"])
        first_line = next(
            (ln.strip() for ln in err_txt.splitlines() if ln.strip() and not ln.strip().startswith("{")),
            "dbt build run error",
        )
        raw_docs, _, _ = gw.call_soft("search_product_docs", {"query": first_line[:90]})
        for d in extract_items(raw_docs, ("results", "pages", "items", "data"))[:4]:
            docs_hits.append({
                "title": str(d.get("title") or d.get("path") or "dbt Documentation"),
                "url": str(d.get("url") or d.get("path") or ""),
                "snippet": str(d.get("snippet") or d.get("description") or "")[:200],
            })
    diag["docs_hits"] = docs_hits
    diag["deep_dive_loaded"] = True

    state.setdefault("run_diagnostics", {})[rid_str] = diag
    state["selected_run_id"] = rid_str


def deep_dive_model(
    gw: DbtMcpGateway,
    state: dict[str, Any],
    *,
    project_id: int,
    unique_id: str = "",
    search_query: str = "",
    orientation: str = "",
    subtab: str = "",
) -> None:
    """On-demand Deep Dive for a specific dbt resource (lineage + columns/SQL + health + run perf) or AI search."""
    hydrate_workspace_bundle(gw, state, project_id=project_id, force_refresh=False)
    if orientation in ("lr", "td"):
        state["lineage_orientation"] = orientation

    if search_query:
        raw_rel, _, _ = gw.call_soft("get_related_models", {"project_id": project_id, "search_query": search_query[:120]})
        rel_items: list[dict[str, Any]] = []
        for m in extract_items(raw_rel, ("models", "items", "data"))[:30]:
            uid = str(m.get("uniqueId") or m.get("unique_id") or "").strip()
            mname = str(m.get("name") or (uid.split(".")[-1] if uid else "")).strip()
            if mname:
                rel_items.append({
                    "unique_id": uid or f"model.{mname}",
                    "name": mname,
                    "description": str(m.get("description") or "")[:180],
                })
        state["semantic_search_query"] = search_query
        state["semantic_search_results"] = rel_items
        if not unique_id and rel_items:
            unique_id = rel_items[0]["unique_id"]

    target_uid = validate_unique_id(unique_id or state.get("focus_unique_id") or "")
    if not target_uid:
        return

    # Resolve bare model name to full unique_id if present in prefetched catalog
    if "." not in target_uid:
        all_res = (state.get("models") or []) + (state.get("sources") or []) + (state.get("exposures") or [])
        matched = next((r["unique_id"] for r in all_res if r.get("name") == target_uid and "." in str(r.get("unique_id"))), "")
        if matched:
            target_uid = matched

    guessed_rtype = target_uid.split(".")[0].lower() if "." in target_uid else "model"
    if guessed_rtype not in ("model", "source", "exposure", "test", "seed", "snapshot", "macro", "semantic_model"):
        guessed_rtype = "model"

    id_arg = {"unique_id": target_uid} if "." in target_uid else {"name": target_uid}
    specs: dict[str, tuple[str, dict[str, Any] | None]] = {
        "lin": ("get_lineage", {"project_id": project_id, "unique_id": target_uid, "direction": "both", "depth": 2, "limit": 60}),
        "nd": ("get_node_details", {"project_id": project_id, "resource_type": guessed_rtype, **id_arg}),
    }
    if guessed_rtype == "model":
        specs["mh"] = ("get_model_health", {"project_id": project_id, **id_arg})
        specs["mp"] = ("get_model_performance", {"project_id": project_id, **id_arg, "num_runs": 8, "include_tests": True})

    batch = gw.call_parallel(specs)
    raw_lin, _, _ = batch["lin"]
    raw_nd, txt_nd, _ = batch["nd"]
    raw_mh, txt_mh, _ = batch.get("mh", ({}, "", ""))
    raw_mp, _, _ = batch.get("mp", (None, "", ""))

    parsed = _parse_lineage_and_node(
        target_uid, raw_lin, raw_nd, txt_nd, raw_mh, txt_mh, raw_mp, include_perf=True
    )
    state.setdefault("model_deep_dives", {})[target_uid] = parsed
    state["focus_unique_id"] = target_uid
    state["catalog_view_mode"] = "detail"
    if subtab in ("overview", "lineage", "sql"):
        state["catalog_subtab"] = subtab


def deep_dive_metric_slice(
    gw: DbtMcpGateway,
    state: dict[str, Any],
    *,
    project_id: int,
    selected_metric: str = "",
    grain: str = "",
    group_dim: str = "",
    where_dim: str = "",
    where_val: str = "",
) -> None:
    """On-demand custom Semantic Layer slice query (`query_metrics` + dimensions + compiled SQL)."""
    hydrate_workspace_bundle(gw, state, project_id=project_id, force_refresh=False)
    mv = state.get("metrics_view") if isinstance(state.get("metrics_view"), dict) else {}
    metrics_list = mv.get("metrics") if isinstance(mv.get("metrics"), list) else []

    chosen_metric = (selected_metric or str(mv.get("selected_metric") or "")).strip()
    if not chosen_metric and metrics_list:
        chosen_metric = metrics_list[0]["name"]

    chosen_grain = (grain or str(mv.get("grain") or "MONTH")).upper()
    if chosen_grain not in SAFE_GRAIN_SET:
        chosen_grain = "MONTH"

    chosen_dim = (group_dim if group_dim != "__keep__" else str(mv.get("group_dim") or "")).strip()
    if not chosen_metric:
        return

    group_by: list[dict[str, Any]] = [{"name": "metric_time", "type": "time_dimension", "grain": chosen_grain}]
    if chosen_dim and chosen_dim != "metric_time":
        group_by.append({"name": chosen_dim, "type": "dimension"})

    q_args: dict[str, Any] = {
        "project_id": project_id,
        "metrics": [chosen_metric],
        "group_by": group_by,
        "limit": 24,
    }
    if where_dim and where_val:
        safe_dim = re.sub(r"[^A-Za-z0-9_]", "", where_dim)
        safe_val = re.sub(r"['\"\\]", "", where_val)[:80]
        if safe_dim and safe_val:
            q_args["where"] = f"{{{{ Dimension('{safe_dim}') }}}} = '{safe_val}'"

    specs: dict[str, tuple[str, dict[str, Any] | None]] = {
        "dims": ("get_dimensions", {"project_id": project_id, "metrics": [chosen_metric]}),
        "ents": ("get_entities", {"project_id": project_id, "metrics": [chosen_metric]}),
        "qm": ("query_metrics", q_args),
        "sql": ("get_metrics_compiled_sql", q_args),
    }
    if where_dim:
        specs["dv"] = (
            "get_dimension_values",
            {"project_id": project_id, "dimension": where_dim, "metrics": [chosen_metric], "limit": 25},
        )

    batch = gw.call_parallel(specs)
    dims_list: list[dict[str, str]] = []
    for d in extract_items(batch["dims"][0], ("dimensions", "items", "data"))[:35]:
        dname = str(d.get("name") or d.get("qualified_name") or d.get("qualifiedName") or "").strip()
        dtype = str(d.get("type") or "categorical").lower()
        if dname:
            dims_list.append({"name": dname, "type": "time_dimension" if "time" in dtype else "dimension"})

    entities_list = [
        str(e.get("name") or "").strip()
        for e in extract_items(batch["ents"][0], ("entities", "items", "data"))[:20]
        if str(e.get("name") or "").strip()
    ]

    dim_values: list[str] = []
    if "dv" in batch:
        raw_dv = batch["dv"][0]
        if isinstance(raw_dv, list):
            dim_values = [str(v) for v in raw_dv[:25]]
        elif isinstance(raw_dv, dict):
            vals = raw_dv.get("values") or raw_dv.get("data") or raw_dv.get("items") or []
            if isinstance(vals, list):
                dim_values = [str(v.get("value") if isinstance(v, dict) else v) for v in vals[:25]]

    raw_qm, txt_qm, err_q = batch["qm"]
    query_err = ""
    if err_q:
        raw_qm, txt_qm, err_q2 = gw.call_soft(
            "query_metrics", {"project_id": project_id, "metrics": [chosen_metric], "limit": 24}
        )
        if err_q2:
            query_err = err_q
    rows_out = extract_items(raw_qm, ("rows", "data", "items", "results"))
    if not rows_out and isinstance(raw_qm, dict) and isinstance(raw_qm.get("table"), dict):
        rows_out = extract_items(raw_qm["table"], ("rows", "data"))

    raw_sql, txt_sql, _ = batch["sql"]
    if isinstance(raw_sql, dict):
        compiled_sql = str(
            raw_sql.get("sql") or raw_sql.get("compiled_sql") or raw_sql.get("compiledSql") or raw_sql.get("text") or txt_sql
        )[:3000]
    else:
        compiled_sql = str(txt_sql or "")[:3000]

    mv.update({
        "selected_metric": chosen_metric,
        "grain": chosen_grain,
        "group_dim": chosen_dim,
        "where_dim": where_dim,
        "where_val": where_val,
        "dimensions": dims_list,
        "entities": entities_list,
        "dimension_values": dim_values,
        "rows": rows_out[:24],
        "compiled_sql": compiled_sql,
        "raw_preview": txt_qm[:1200] if not rows_out and txt_qm else "",
        "error": query_err,
    })
    state["metrics_view"] = mv


def deep_dive_generate_sql(
    gw: DbtMcpGateway,
    state: dict[str, Any],
    *,
    project_id: int,
    nl_prompt: str = "",
    sql_override: str = "",
) -> None:
    """On-demand AI NL-to-dbt-SQL generation (`text_to_sql`)."""
    hydrate_workspace_bundle(gw, state, project_id=project_id, force_refresh=False)
    sv = state.get("sql_view") if isinstance(state.get("sql_view"), dict) else {}
    generated_sql = sql_override or str(sv.get("generated_sql") or "")
    if nl_prompt:
        raw_t2s, txt_t2s, err_t2s = gw.call_soft("text_to_sql", {"project_id": project_id, "text": nl_prompt[:400]})
        if isinstance(raw_t2s, dict):
            generated_sql = str(raw_t2s.get("sql") or raw_t2s.get("query") or raw_t2s.get("text") or txt_t2s).strip()
        elif txt_t2s:
            generated_sql = txt_t2s.strip()
        if err_t2s and not generated_sql:
            marts = state.get("marts") or state.get("models") or []
            if marts:
                generated_sql = f"SELECT *\nFROM {marts[0]['ref_snippet']}\nLIMIT 25"
    sv["nl_prompt"] = nl_prompt or str(sv.get("nl_prompt") or "")
    sv["generated_sql"] = generated_sql[:3500]
    state["sql_view"] = sv


# ==============================================================================
# 7. SELF-CONTAINED PURE-DOM HTML SYNTHESIZER (0ms FILTERING & ACTION WORKBENCH)
# ==============================================================================
def render_dbt_workbench_html(sc: dict[str, Any]) -> str:
    """Synthesizes a 100% self-contained, instant-filtering dbt Cloud A2UI Workbench."""
    serialized = (
        json.dumps(sc or {}, ensure_ascii=True)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )
    t = BRAND_THEME

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0" />
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; img-src data:; connect-src 'none'; form-action 'none'; base-uri 'none';" />
  <title>dbt Cloud Action &amp; Intelligence Workbench</title>
  <style>
    :root {{
      --header-bg: {t["header_bg"]};
      --accent: {t["accent"]};
      --primary: {t["primary"]};
      --primary-dark: {t["primary_dark"]};
      --navy: {t["navy"]};
      --navy-hover: {t["navy_hover"]};
      --canvas: {t["canvas_bg"]};
      --surface: {t["surface_bg"]};
      --subtle: {t["subtle_bg"]};
      --border: {t["border"]};
      --text: {t["text_primary"]};
      --secondary: {t["text_secondary"]};
      --muted: {t["text_muted"]};
      --ok-bg: {t["ok_bg"]};
      --ok-text: {t["ok_text"]};
      --warn-bg: {t["warn_bg"]};
      --warn-text: {t["warn_text"]};
      --crit-bg: {t["crit_bg"]};
      --crit-text: {t["crit_text"]};
      --sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      --mono: 'SFMono-Regular', ui-monospace, Menlo, Consolas, monospace;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }}
    html, body {{ width: 100%; max-width: 100%; overflow-x: hidden; background: var(--canvas); color: var(--text); font-family: var(--sans); font-size: 13.5px; line-height: 1.48; }}
    .shell {{ max-width: 860px; margin: 0 auto; padding: 12px 14px 44px; display: flex; flex-direction: column; gap: 12px; }}
    .topbar {{ background: var(--header-bg); color: #fff; border-bottom: 2px solid var(--accent); padding: 10px 16px; display: flex; align-items: center; justify-content: space-between; gap: 10px; position: sticky; top: 0; z-index: 30; }}
    .brand-row {{ display: flex; align-items: center; gap: 9px; min-width: 0; }}
    .brand-title {{ font-weight: 800; font-size: 13.5px; letter-spacing: 0.01em; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .proj-select {{ background: rgba(255,255,255,0.12); color: #fff; border: 1px solid rgba(255,255,255,0.25); border-radius: 8px; padding: 4px 8px; font-size: 12px; font-weight: 600; max-width: 185px; width: auto; }}
    .proj-select option {{ color: #0f172a; background: #fff; }}
    .back-nav-btn {{ padding: 5px 10px; border-radius: 8px; border: 1px solid rgba(255,255,255,0.28); background: rgba(255,255,255,0.12); color: #fff; font-family: inherit; font-size: 12px; font-weight: 700; cursor: pointer; white-space: nowrap; }}
    .nav-tabs {{ display: flex; gap: 5px; overflow-x: auto; scrollbar-width: none; background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 5px; }}
    .nav-tabs::-webkit-scrollbar {{ display: none; }}
    .nav-tab {{ border: none; background: transparent; color: var(--secondary); font-family: inherit; font-size: 12.5px; font-weight: 700; padding: 7px 12px; border-radius: 8px; cursor: pointer; white-space: nowrap; display: inline-flex; align-items: center; gap: 6px; }}
    .nav-tab.active {{ background: var(--navy); color: #fff; }}
    .tab-badge {{ font-size: 10.5px; padding: 1px 6px; border-radius: 999px; background: rgba(100,116,139,0.16); color: inherit; }}
    .nav-tab.active .tab-badge {{ background: rgba(255,255,255,0.2); color: #fff; }}
    .tab-badge-crit {{ background: var(--crit-bg); color: var(--crit-text); }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; }}
    .kpi-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 11px 13px; cursor: pointer; display: flex; flex-direction: column; gap: 2px; transition: border-color 0.15s; text-align: left; }}
    .kpi-card:hover {{ border-color: var(--accent); }}
    .kpi-val {{ font-size: 20px; font-weight: 800; line-height: 1.15; }}
    .kpi-lbl {{ font-size: 11.5px; font-weight: 700; color: var(--muted); }}
    .card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 14px; padding: 15px 16px; display: flex; flex-direction: column; gap: 10px; box-shadow: 0 1px 3px rgba(15,23,42,0.04); }}
    .card-triage {{ border-left: 4px solid var(--primary); background: #FFFCFB; }}
    .row-between {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }}
    .seg-bar, .chip-bar {{ display: flex; gap: 6px; overflow-x: auto; scrollbar-width: none; padding-bottom: 2px; }}
    .seg-bar::-webkit-scrollbar, .chip-bar::-webkit-scrollbar {{ display: none; }}
    .chip {{ border: 1px solid var(--border); background: var(--subtle); color: var(--secondary); border-radius: 999px; padding: 5px 11px; font-size: 12px; font-weight: 600; cursor: pointer; white-space: nowrap; font-family: inherit; }}
    .chip.active {{ background: #FFF1EE; border-color: var(--accent); color: #B9381E; font-weight: 700; }}
    .pill {{ display: inline-flex; align-items: center; gap: 5px; padding: 2px 9px; border-radius: 999px; font-size: 11.5px; font-weight: 700; white-space: nowrap; }}
    .pill-ok {{ background: var(--ok-bg); color: var(--ok-text); }}
    .pill-warn {{ background: var(--warn-bg); color: var(--warn-text); }}
    .pill-crit {{ background: var(--crit-bg); color: var(--crit-text); }}
    .pill-neutral {{ background: var(--subtle); color: var(--secondary); border: 1px solid var(--border); }}
    .btn {{ border: 1px solid #CBD5E1; background: var(--surface); color: var(--text); border-radius: 9px; padding: 7px 13px; min-height: 36px; font-family: inherit; font-size: 12.5px; font-weight: 700; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; gap: 6px; white-space: nowrap; }}
    .btn:hover {{ border-color: #94A3B8; background: #F8FAFC; }}
    .btn-sm {{ padding: 5px 10px; min-height: 30px; font-size: 11.8px; border-radius: 7px; }}
    .btn-primary {{ background: var(--primary); border-color: var(--primary); color: #fff; }}
    .btn-primary:hover {{ background: var(--primary-dark); border-color: var(--primary-dark); }}
    .btn-navy {{ background: var(--navy); border-color: var(--navy); color: #fff; }}
    .btn-navy:hover {{ background: var(--navy-hover); }}
    .action-footer {{ display: flex; justify-content: flex-end; align-items: center; gap: 8px; flex-wrap: wrap; padding-top: 10px; border-top: 1px solid var(--border); }}
    input[type="text"], input[type="search"], select, textarea {{ width: 100%; border: 1px solid #CBD5E1; border-radius: 8px; padding: 8px 10px; font-family: inherit; font-size: 13px; color: var(--text); background: #fff; }}
    textarea.code-input {{ font-family: var(--mono); font-size: 12px; background: #0F172A; color: #F8FAFC; border-color: #1E293B; }}
    .code-box {{ background: #0F172A; color: #E2E8F0; border-radius: 10px; padding: 11px 13px; font-family: var(--mono); font-size: 11.8px; overflow-x: auto; white-space: pre-wrap; word-break: break-word; max-height: 260px; overflow-y: auto; }}
    .queue-list {{ display: flex; flex-direction: column; gap: 8px; }}
    .queue-item {{ border: 1px solid var(--border); border-radius: 11px; padding: 11px 13px; display: flex; align-items: center; justify-content: space-between; gap: 10px; background: #fff; cursor: pointer; transition: border-color 0.12s; }}
    .queue-item:hover, .queue-item.selected {{ border-color: var(--accent); background: #FFFBFA; }}
    .lineage-grid {{ background-color: #F8FAFC; background-image: radial-gradient(#CBD5E1 1.1px, transparent 1.1px); background-size: 15px 15px; border: 1px solid var(--border); border-radius: 12px; padding: 14px; }}
    .lin-flow-lr {{ display: flex; flex-direction: row; align-items: stretch; justify-content: space-between; gap: 10px; overflow-x: auto; }}
    .lin-flow-td {{ display: flex; flex-direction: column; align-items: stretch; gap: 8px; }}
    .lin-col {{ flex: 1; min-width: 170px; display: flex; flex-direction: column; gap: 7px; }}
    .lin-node {{ background: #fff; border: 1px solid #CBD5E1; border-radius: 9px; padding: 8px 10px; cursor: pointer; }}
    .lin-node:hover {{ border-color: var(--accent); }}
    .lin-node.focus {{ border: 2px solid var(--primary); background: #FFF5F2; cursor: default; }}
    .bar-row {{ display: flex; align-items: center; gap: 8px; font-size: 12px; }}
    .bar-track {{ flex: 1; height: 9px; background: #E2E8F0; border-radius: 999px; overflow: hidden; }}
    .bar-fill {{ height: 100%; background: var(--accent); border-radius: 999px; }}
    .banner {{ border-radius: 10px; padding: 11px 14px; font-size: 12.8px; border: 1px solid var(--border); }}
    .banner-ok {{ background: var(--ok-bg); color: var(--ok-text); border-color: #86EFAC; }}
    .banner-warn {{ background: var(--warn-bg); color: var(--warn-text); border-color: #FCD34D; }}
    .banner-crit {{ background: var(--crit-bg); color: var(--crit-text); border-color: #FCA5A5; }}
    .toast {{ position: fixed; bottom: 16px; right: 16px; background: #0F172A; color: #fff; padding: 8px 14px; border-radius: 999px; font-size: 12px; font-weight: 600; z-index: 120; box-shadow: 0 4px 12px rgba(0,0,0,0.2); }}
    .modal-backdrop {{ position: fixed; inset: 0; background: rgba(15,23,42,0.52); backdrop-filter: blur(2px); display: flex; align-items: center; justify-content: center; padding: 16px; z-index: 110; }}
    .modal-card {{ background: #fff; border-radius: 16px; width: 100%; max-width: 490px; padding: 18px; display: flex; flex-direction: column; gap: 12px; box-shadow: 0 20px 40px rgba(15,23,42,0.25); }}
    .loading-popup-card {{ background: #fff; border: 1px solid var(--border); border-top: 4px solid var(--accent); border-radius: 16px; width: 100%; max-width: 430px; padding: 18px 20px; display: flex; flex-direction: column; gap: 12px; box-shadow: 0 22px 44px rgba(15,23,42,0.28); }}
    .refresh-icon-wrap {{ width: 42px; height: 42px; min-width: 42px; border-radius: 12px; background: #FFF1EE; border: 1px solid #FFD5CC; color: var(--primary); display: inline-flex; align-items: center; justify-content: center; }}
    .refresh-svg {{ width: 22px; height: 22px; animation: dbtSpin 1.05s linear infinite; }}
    .popup-progress-track {{ height: 6px; width: 100%; background: #F1F5F9; border-radius: 999px; overflow: hidden; }}
    .popup-progress-bar {{ width: 42%; height: 100%; border-radius: 999px; background: linear-gradient(90deg, var(--primary), var(--accent), #FBBF24); animation: dbtSlide 1.15s ease-in-out infinite; }}
    .sync-meta {{ font-size: 11.5px; color: rgba(255,255,255,0.72); font-weight: 600; white-space: nowrap; }}
    .data-grid-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 8px; padding: 6px 8px; border-bottom: 1px solid var(--border); font-size: 12px; font-family: var(--mono); }}
    @keyframes dbtSpin {{ to {{ transform: rotate(360deg); }} }}
    @keyframes dbtSlide {{ 0% {{ transform: translateX(-100%); }} 100% {{ transform: translateX(260%); }} }}
    @media (max-width: 640px) {{
      .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
      .sync-meta {{ display: none; }}
    }}
    @media (max-width: 580px) {{
      .shell {{ padding: 10px 10px 38px; }}
      .btn, .nav-tab, .chip, .back-nav-btn {{ min-height: 44px; }}
      input[type="text"], input[type="search"], select, textarea {{ font-size: 16px; min-height: 44px; }}
      .queue-item {{ flex-direction: column; align-items: stretch; }}
      .action-footer {{ flex-direction: column; align-items: stretch; }}
      .action-footer .btn {{ width: 100%; }}
      .modal-backdrop {{ align-items: flex-end; padding: 0; }}
      .modal-card, .loading-popup-card {{ max-width: 100%; border-radius: 20px 20px 0 0; }}
      .modal-actions {{ flex-direction: column-reverse; }}
      .modal-actions .btn {{ width: 100%; }}
    }}
  </style>
</head>
<body>
  <header class="topbar">
    <div class="brand-row" id="brandHost"></div>
    <div style="display:flex;align-items:center;gap:8px;">
      <span id="syncMeta" class="sync-meta"></span>
      <div id="projHost"></div>
      <button type="button" id="syncBtn" class="back-nav-btn" title="Re-sync workspace metadata from dbt Cloud">↻ Sync</button>
    </div>
  </header>
  <main id="appRoot" class="shell"></main>
  <div id="modalRoot"></div>
  <div id="loadingRoot"></div>
  <div id="toastRoot"></div>

  <script>
  (function() {{
    const STATE = {serialized};

    // Instant client-side UI state (0ms navigation, filtering & staging without MCP roundtrips)
    let activeTab = Number(STATE.active_tab || 1);
    if (activeTab < 1 || activeTab > 4) activeTab = 1;
    let confirmModal = STATE.pending_confirmation || null;

    // Tab 1: Action Center & Runs client-side state
    let runStatusFilter = String(STATE.initial_run_filter || "");
    let runJobFilter = "";
    let runSearchText = "";
    let selectedRunId = String(STATE.selected_run_id || "");
    let showTriggerPanel = false;
    let triggerJobId = String(STATE.selected_job_id || "");
    let triggerStepsOverride = "";

    // Tab 2: Unified Models, Health & Lineage client-side state
    let catalogViewMode = String(STATE.catalog_view_mode || "list");
    let catalogKindFilter = String(STATE.initial_kind_filter || "all"); // all | mart | model | source | exposure | attention
    let catalogSearchText = "";
    let focusUniqueId = String(STATE.focus_unique_id || "");
    let catalogSubtab = String(STATE.catalog_subtab || "overview"); // overview | lineage | sql
    let codeMode = "compiled";
    let isMobileInit = (window.innerWidth || 800) < 580;
    let lineageOrient = String(STATE.lineage_orientation || (isMobileInit ? "td" : "lr"));

    // Tab 3: Semantic Metrics client-side state
    const mvInit = STATE.metrics_view || {{}};
    let metricSearchText = "";
    let metricDisplayMode = "bars"; // bars | table | sql
    let draftMetric = String(mvInit.selected_metric || ((mvInit.metrics && mvInit.metrics[0] && mvInit.metrics[0].name) || ""));
    let draftGrain = String(mvInit.grain || "MONTH");
    let draftGroupDim = String(mvInit.group_dim || "");
    let draftWhereDim = String(mvInit.where_dim || "");
    let draftWhereVal = String(mvInit.where_val || "");

    // Tab 4: SQL & Macros client-side state
    const svInit = STATE.sql_view || {{}};
    let sqlEditorText = String(svInit.generated_sql || "");
    let nlPromptText = String(svInit.nl_prompt || "");
    let pickerSearchText = "";
    let pickerKind = "mart"; // mart | source | macro

    // In-Screen Refresh Popup & MCP Permission Latch state (no top-right loading spinner)
    let isSubmitting = false;
    let loadingPopupDismissed = false;
    let submitLabel = "Refreshing dbt Cloud";
    let submitDetail = "Running dbt Cloud MCP tool — Canvas will refresh automatically when ready.";
    let submitStartMs = 0;
    let permissionHandled = false;
    let submitTimer = null;

    function el(tag, attrs, children) {{
      const node = document.createElement(tag);
      if (attrs) {{
        Object.entries(attrs).forEach(([k, v]) => {{
          if (v === null || v === undefined || v === false) return;
          if (k === "className") node.className = String(v);
          else if (k === "text") node.textContent = String(v);
          else if (k === "style") node.setAttribute("style", String(v));
          else if (k === "disabled" && v === true) node.disabled = true;
          else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2).toLowerCase(), v);
          else node.setAttribute(k, String(v));
        }});
      }}
      (children || []).forEach(ch => {{
        if (ch === null || ch === undefined || ch === false) return;
        node.appendChild(typeof ch === "string" ? document.createTextNode(ch) : ch);
      }});
      return node;
    }}

    function makeDbtSvg() {{
      const ns = "http://www.w3.org/2000/svg";
      const svg = document.createElementNS(ns, "svg");
      svg.setAttribute("viewBox", "0 0 24 24");
      svg.setAttribute("width", "18");
      svg.setAttribute("height", "18");
      svg.setAttribute("fill", "#FF694B");
      const p = document.createElementNS(ns, "path");
      p.setAttribute("d", "M19.4 3L12 10.4 4.6 3 2 5.6l7.4 7.4L2 20.4 4.6 23l7.4-7.4 7.4 7.4 2.6-2.6-7.4-7.4L22 5.6 19.4 3z");
      svg.appendChild(p);
      return svg;
    }}

    function makeRefreshSvg() {{
      const ns = "http://www.w3.org/2000/svg";
      const svg = document.createElementNS(ns, "svg");
      svg.setAttribute("viewBox", "0 0 24 24");
      svg.setAttribute("class", "refresh-svg");
      svg.setAttribute("fill", "none");
      svg.setAttribute("stroke", "currentColor");
      svg.setAttribute("stroke-width", "2.3");
      svg.setAttribute("stroke-linecap", "round");
      svg.setAttribute("stroke-linejoin", "round");
      const p1 = document.createElementNS(ns, "path");
      p1.setAttribute("d", "M21 12a9 9 0 0 0-15.4-6.4L3 8");
      const p2 = document.createElementNS(ns, "path");
      p2.setAttribute("d", "M3 3v5h5");
      const p3 = document.createElementNS(ns, "path");
      p3.setAttribute("d", "M3 12a9 9 0 0 0 15.4 6.4L21 16");
      const p4 = document.createElementNS(ns, "path");
      p4.setAttribute("d", "M16 16h5v5");
      svg.appendChild(p1);
      svg.appendChild(p2);
      svg.appendChild(p3);
      svg.appendChild(p4);
      return svg;
    }}

    function showToast(msg) {{
      const host = document.getElementById("toastRoot");
      host.replaceChildren(el("div", {{ className: "toast", text: msg }}));
      setTimeout(() => {{ host.replaceChildren(); }}, 2300);
    }}

    function statusPillClass(st) {{
      const s = String(st || "").toLowerCase();
      if (s.includes("err") || s.includes("fail") || s.includes("crit") || s.includes("stale")) return "pill pill-crit";
      if (s.includes("run") || s.includes("queue") || s.includes("warn") || s.includes("start")) return "pill pill-warn";
      if (s.includes("ok") || s.includes("succ") || s.includes("health") || s.includes("pass") || s.includes("fresh") || s.includes("governed")) return "pill pill-ok";
      return "pill pill-neutral";
    }}

    function isFullscreen() {{
      const sw = (window.screen && window.screen.availWidth) ? window.screen.availWidth : 0;
      const w = window.innerWidth || 0;
      return sw >= 768 && w >= Math.min(sw * 0.80, sw - 140);
    }}

    function renderLoadingPopup() {{
      const host = document.getElementById("loadingRoot");
      host.replaceChildren();
      if (!isSubmitting || loadingPopupDismissed) return;

      const sec = Math.max(0, Math.floor((Date.now() - submitStartMs) / 1000));
      const subtitleText = permissionHandled
        ? "Permission handled — finishing dbt Cloud request & refreshing Canvas..."
        : submitDetail;

      let permNotice = null;
      if (!permissionHandled && (isFullscreen() || sec >= 3)) {{
        const msg = isFullscreen()
          ? "Full-Screen Canvas active: if Gemini prompts 'Agent needs your permission' in Chat, exit Full Screen and click Allow."
          : "If Gemini prompts 'Agent needs your permission' in the left Chat panel, click Allow to continue.";
        permNotice = el("div", {{ className: "banner banner-warn", style: "font-size:12px;padding:9px 11px;" }}, [
          el("span", {{ text: msg }})
        ]);
      }}

      host.appendChild(el("div", {{ className: "modal-backdrop", style: "z-index:130;" }}, [
        el("div", {{ className: "loading-popup-card", role: "status", "aria-live": "polite" }}, [
          el("div", {{ style: "display:flex;align-items:flex-start;justify-content:space-between;gap:12px;" }}, [
            el("div", {{ style: "display:flex;align-items:center;gap:12px;min-width:0;" }}, [
              el("div", {{ className: "refresh-icon-wrap" }}, [makeRefreshSvg()]),
              el("div", {{ style: "min-width:0;" }}, [
                el("div", {{ style: "font-size:15px;font-weight:800;color:var(--text);", text: submitLabel }}),
                el("div", {{ style: "font-size:12px;color:var(--muted);font-weight:600;margin-top:1px;", text: "Refreshing Canvas · " + sec + "s elapsed" }})
              ])
            ]),
            el("button", {{
              type: "button",
              className: "btn btn-sm",
              title: "Hide popup while refresh finishes in background",
              text: "Hide",
              onClick: () => {{
                isSubmitting = false;
                loadingPopupDismissed = true;
                if (submitTimer) clearInterval(submitTimer);
                renderLoadingPopup();
              }}
            }})
          ]),
          el("div", {{ style: "font-size:12.8px;color:var(--secondary);", text: subtitleText }}),
          el("div", {{ className: "popup-progress-track" }}, [
            el("div", {{ className: "popup-progress-bar" }})
          ]),
          permNotice
        ])
      ]));
    }}

    function syncActivity() {{
      const syncMeta = document.getElementById("syncMeta");
      if (syncMeta) {{
        syncMeta.textContent = STATE.last_synced ? ("Synced " + STATE.last_synced) : "";
      }}
      renderLoadingPopup();
    }}

    window.addEventListener("blur", () => {{
      if (isSubmitting && (Date.now() - submitStartMs > 350)) {{
        permissionHandled = true;
        syncActivity();
      }}
    }});
    window.addEventListener("resize", () => {{ if (isSubmitting) syncActivity(); }});

    function dispatchAction(actionName, data, label, detail) {{
      isSubmitting = true;
      loadingPopupDismissed = false;
      submitLabel = label || "Refreshing dbt Cloud";
      submitDetail = detail || "Fetching live data from dbt Cloud MCP & refreshing this workbench...";
      submitStartMs = Date.now();
      permissionHandled = false;
      if (submitTimer) clearInterval(submitTimer);
      syncActivity();
      submitTimer = setInterval(syncActivity, 1000);
      const payload = Object.assign({{ project_id: STATE.active_project_id || "", active_tab: activeTab }}, data || {{}});
      try {{
        window.parent.postMessage({{ type: "a2ui_action", action: actionName, data: payload }}, "*");
      }} catch (e) {{}}
    }}

    // Helper: Open staged Confirmation Modal in 0ms client-side
    function openRetryModal(runObj) {{
      confirmModal = {{
        title: "Confirm Safe Retry — Run #" + runObj.id,
        warning: "dbt Cloud will queue a retry run resuming from the point of failure.",
        rows: [
          {{ label: "Job", value: runObj.job_name + (runObj.job_id ? " (#" + runObj.job_id + ")" : "") }},
          {{ label: "Failed Run ID", value: "#" + runObj.id }},
          {{ label: "MCP Tool", value: "retry_job_run" }}
        ],
        payload: {{ operation: "retry_run", run_id: runObj.id, job_id: runObj.job_id || "", confirmed_in_gui: true, execute: true }}
      }};
      renderModal();
    }}

    function openCancelModal(runObj) {{
      confirmModal = {{
        title: "Cancel Active Run #" + runObj.id,
        warning: "Sends an immediate cancellation signal to this running/queued dbt Cloud job.",
        rows: [
          {{ label: "Job", value: runObj.job_name }},
          {{ label: "Run ID", value: "#" + runObj.id }},
          {{ label: "MCP Tool", value: "cancel_job_run" }}
        ],
        payload: {{ operation: "cancel_run", run_id: runObj.id, confirmed_in_gui: true, execute: true }}
      }};
      renderModal();
    }}

    function openTriggerModal(jobId, stepsStr, reasonLabel) {{
      const jobs = Array.isArray(STATE.jobs) ? STATE.jobs : [];
      const chosenId = String(jobId || (jobs[0] && jobs[0].id) || "");
      const jobObj = jobs.find(j => String(j.id) === chosenId);
      if (!chosenId) {{
        showToast("No dbt Cloud jobs discovered to trigger.");
        return;
      }}
      confirmModal = {{
        title: "Confirm dbt Cloud Job Trigger",
        warning: stepsStr ? "Overrides default job steps for this single run only." : "Runs the job's configured default build steps.",
        rows: [
          {{ label: "Target Job", value: (jobObj ? jobObj.name : "Job") + " (#" + chosenId + ")" }},
          {{ label: "Steps Override", value: stepsStr || "(Job default steps)" }},
          {{ label: "Cause", value: reasonLabel || "Triggered via Gemini Enterprise Workbench" }}
        ],
        payload: {{
          operation: "trigger_job",
          job_id: chosenId,
          steps_override: stepsStr || "",
          cause: reasonLabel || "Triggered via Gemini Enterprise Workbench",
          confirmed_in_gui: true,
          execute: true
        }}
      }};
      renderModal();
    }}

    function jumpToModelInExplorer(uid, subtab) {{
      focusUniqueId = String(uid || "");
      catalogViewMode = "detail";
      catalogSubtab = subtab || "overview";
      activeTab = 2;
      render();
    }}

    function openInSqlRunner(snippet, titleMsg) {{
      sqlEditorText = "SELECT *\\nFROM " + snippet + "\\nLIMIT 25";
      activeTab = 4;
      render();
      showToast(titleMsg || ("Loaded " + snippet + " into SQL Runner"));
    }}

    function allUnifiedResources() {{
      const models = Array.isArray(STATE.models) ? STATE.models : [];
      const sources = Array.isArray(STATE.sources) ? STATE.sources : [];
      const exposures = Array.isArray(STATE.exposures) ? STATE.exposures : [];
      return [...models, ...sources, ...exposures];
    }}

    function renderHeader() {{
      const brandHost = document.getElementById("brandHost");
      const allRes = allUnifiedResources();
      const leftItems = [];
      if (activeTab === 2 && catalogViewMode === "detail" && allRes.length > 0) {{
        leftItems.push(el("button", {{
          type: "button",
          className: "back-nav-btn",
          text: "← All Resources (" + allRes.length + ")",
          onClick: () => {{ catalogViewMode = "list"; render(); }}
        }}));
      }} else {{
        leftItems.push(makeDbtSvg());
      }}
      leftItems.push(el("span", {{ className: "brand-title", text: "dbt Cloud Workbench" }}));
      brandHost.replaceChildren(...leftItems);

      const projHost = document.getElementById("projHost");
      projHost.replaceChildren();
      const projs = Array.isArray(STATE.projects) ? STATE.projects : [];
      if (projs.length > 0) {{
        const sel = el("select", {{ className: "proj-select", title: "Switch dbt Cloud Project", onChange: ev => {{
          dispatchAction("dbt_switch_project", {{ project_id: ev.target.value, tab: activeTab, confirmed_in_gui: false }}, "Switching dbt Cloud Project", "Prefetching jobs, runs, models, source freshness & metrics for the selected project...");
        }} }});
        projs.forEach(p => {{
          const opt = el("option", {{ value: p.id, text: p.name }});
          if (String(p.id) === String(STATE.active_project_id)) opt.selected = true;
          sel.appendChild(opt);
        }});
        projHost.appendChild(sel);
      }}

      const syncBtn = document.getElementById("syncBtn");
      syncBtn.onclick = () => dispatchAction("dbt_refresh_workspace", {{ tab: activeTab, confirmed_in_gui: false }}, "Refreshing dbt Cloud Workspace", "Re-syncing latest runs, models, source freshness & governed metrics...");
      syncActivity();
    }}

    // ==========================================================================
    // TAB 1: ACTION CENTER & JOB RUNS (KPIs + Needs Attention + 0ms Run Filter)
    // ==========================================================================
    function renderTab1ActionCenter(container) {{
      const jobs = Array.isArray(STATE.jobs) ? STATE.jobs : [];
      const runs = Array.isArray(STATE.runs) ? STATE.runs : [];
      const sources = Array.isArray(STATE.sources) ? STATE.sources : [];
      const models = Array.isArray(STATE.models) ? STATE.models : [];
      const marts = Array.isArray(STATE.marts) ? STATE.marts : [];
      const metrics = (STATE.metrics_view && Array.isArray(STATE.metrics_view.metrics)) ? STATE.metrics_view.metrics : [];
      const diags = STATE.run_diagnostics || {{}};

      const failedRuns = runs.filter(r => r.status_bucket === "error");
      const activeRuns = runs.filter(r => r.status_bucket === "running");
      const alertSources = sources.filter(s => s.status_bucket === "error" || s.status_bucket === "warn");

      // 1. Project KPI Summary Strip (1-click 0ms jump/filter)
      container.appendChild(el("div", {{ className: "kpi-grid" }}, [
        el("button", {{ type: "button", className: "kpi-card", onClick: () => {{ runStatusFilter = failedRuns.length ? "error" : ""; render(); }} }}, [
          el("span", {{ className: "kpi-val", style: failedRuns.length ? "color:var(--crit-text);" : "color:var(--ok-text);", text: String(failedRuns.length) }}),
          el("span", {{ className: "kpi-lbl", text: failedRuns.length === 1 ? "Failed Run" : "Failed Runs" }})
        ]),
        el("button", {{ type: "button", className: "kpi-card", onClick: () => {{
          catalogKindFilter = alertSources.length ? "attention" : "source";
          catalogViewMode = "list";
          activeTab = 2;
          render();
        }} }}, [
          el("span", {{ className: "kpi-val", style: alertSources.length ? "color:var(--warn-text);" : "color:var(--ok-text);", text: alertSources.length ? (alertSources.length + " Alert") : (sources.length + " OK") }}),
          el("span", {{ className: "kpi-lbl", text: "Source Freshness" }})
        ]),
        el("button", {{ type: "button", className: "kpi-card", onClick: () => {{ catalogKindFilter = "mart"; catalogViewMode = "list"; activeTab = 2; render(); }} }}, [
          el("span", {{ className: "kpi-val", text: marts.length + " / " + models.length }}),
          el("span", {{ className: "kpi-lbl", text: "Marts / Models" }})
        ]),
        el("button", {{ type: "button", className: "kpi-card", onClick: () => {{ activeTab = 3; render(); }} }}, [
          el("span", {{ className: "kpi-val", text: String(metrics.length) }}),
          el("span", {{ className: "kpi-lbl", text: "Governed Metrics" }})
        ])
      ]));

      // 2. "Needs Review & Action" Triage Card (surfaces immediate actionable items at top)
      if (failedRuns.length > 0 || activeRuns.length > 0 || alertSources.length > 0) {{
        const triageItems = [];
        failedRuns.slice(0, 3).forEach(fr => {{
          const d = diags[fr.id] || {{}};
          const errFirst = d.error_text ? d.error_text.split("\\n").find(l => l.trim()) || "" : "";
          triageItems.push(el("div", {{ className: "queue-item", onClick: () => {{ selectedRunId = fr.id; render(); }} }}, [
            el("div", {{ style: "min-width:0;flex:1;" }}, [
              el("div", {{ style: "display:flex;align-items:center;gap:7px;flex-wrap:wrap;" }}, [
                el("span", {{ className: "pill pill-crit", text: "FAILED RUN #" + fr.id }}),
                el("span", {{ style: "font-weight:800;", text: fr.job_name }})
              ]),
              errFirst ? el("div", {{ style: "font-size:11.8px;color:var(--crit-text);font-family:var(--mono);margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;", text: errFirst.slice(0, 110) }}) : null
            ]),
            el("div", {{ style: "display:flex;gap:6px;flex-wrap:wrap;" }}, [
              el("button", {{ type: "button", className: "btn btn-sm btn-primary", text: "⚡ Safe Retry →", onClick: ev => {{ ev.stopPropagation(); openRetryModal(fr); }} }}),
              d.failed_model_hint ? el("button", {{ type: "button", className: "btn btn-sm", text: "▶ Rebuild +" + d.failed_model_hint, onClick: ev => {{
                ev.stopPropagation();
                openTriggerModal(fr.job_id, "dbt build --select +" + d.failed_model_hint + "+", "Selective rebuild after Run #" + fr.id);
              }} }}) : null
            ])
          ]));
        }});

        activeRuns.slice(0, 2).forEach(ar => {{
          triageItems.push(el("div", {{ className: "queue-item", onClick: () => {{ selectedRunId = ar.id; render(); }} }}, [
            el("div", null, [
              el("span", {{ className: "pill pill-warn", text: ar.status.toUpperCase() + " · #" + ar.id }}),
              el("span", {{ style: "font-weight:700;margin-left:8px;", text: ar.job_name }})
            ]),
            el("button", {{ type: "button", className: "btn btn-sm", text: "⏹ Cancel Run", onClick: ev => {{ ev.stopPropagation(); openCancelModal(ar); }} }})
          ]));
        }});

        alertSources.slice(0, 2).forEach(asrc => {{
          triageItems.push(el("div", {{ className: "queue-item", onClick: () => jumpToModelInExplorer(asrc.unique_id, "overview") }}, [
            el("div", null, [
              el("span", {{ className: statusPillClass(asrc.freshness), text: "SOURCE · " + asrc.freshness }}),
              el("span", {{ style: "font-weight:700;font-family:var(--mono);margin-left:8px;", text: asrc.name }})
            ]),
            el("button", {{ type: "button", className: "btn btn-sm", text: "Inspect Source →", onClick: ev => {{ ev.stopPropagation(); jumpToModelInExplorer(asrc.unique_id, "overview"); }} }})
          ]));
        }});

        container.appendChild(el("div", {{ className: "card card-triage" }}, [
          el("div", {{ className: "row-between" }}, [
            el("div", {{ style: "font-weight:800;font-size:14.5px;", text: "Needs Review & Action (" + triageItems.length + ")" }}),
            el("button", {{ type: "button", className: "btn btn-sm btn-navy", text: showTriggerPanel ? "Hide Job Launcher" : "▶ Launch a dbt Job", onClick: () => {{ showTriggerPanel = !showTriggerPanel; render(); }} }})
          ]),
          el("div", {{ className: "queue-list" }}, triageItems)
        ]));
      }} else {{
        container.appendChild(el("div", {{ className: "card" }}, [
          el("div", {{ className: "row-between" }}, [
            el("div", null, [
              el("span", {{ className: "pill pill-ok", text: "All Clear" }}),
              el("div", {{ style: "font-weight:800;font-size:14.5px;margin-top:4px;", text: "No failed runs or stale source alerts in " + (STATE.active_project_name || "dbt Cloud") }})
            ]),
            el("button", {{ type: "button", className: "btn btn-navy", text: showTriggerPanel ? "Hide Job Launcher" : "▶ Trigger dbt Job", onClick: () => {{ showTriggerPanel = !showTriggerPanel; render(); }} }})
          ])
        ]));
      }}

      // 3. Collapsible Job Trigger Card (with 1-click preset step chips)
      if (showTriggerPanel && jobs.length > 0) {{
        const jobSel = el("select", {{ onChange: ev => {{ triggerJobId = ev.target.value; }} }}, jobs.map(j => {{
          const o = el("option", {{ value: j.id, text: j.name + " (#" + j.id + ")" }});
          if (String(j.id) === String(triggerJobId || jobs[0].id)) o.selected = true;
          return o;
        }}));
        const stepsInp = el("input", {{
          type: "text",
          value: triggerStepsOverride,
          placeholder: "Optional step override (leave blank for default job steps, or click a preset below)",
          onInput: ev => {{ triggerStepsOverride = ev.target.value; }}
        }});
        const presets = [
          ["Default Job Steps", ""],
          ["Build Modified (+state:modified+)", "dbt build --select state:modified+"],
          ["Run Tests Only", "dbt test"],
          ["Check Source Freshness", "dbt source freshness"]
        ];
        container.appendChild(el("div", {{ className: "card" }}, [
          el("div", {{ className: "row-between" }}, [
            el("div", {{ style: "font-weight:800;font-size:14.5px;", text: "Trigger dbt Cloud Job Run" }}),
            el("button", {{ type: "button", className: "btn btn-sm", text: "✕ Close", onClick: () => {{ showTriggerPanel = false; render(); }} }})
          ]),
          jobSel,
          stepsInp,
          el("div", {{ className: "chip-bar" }}, presets.map(([lbl, cmd]) =>
            el("button", {{ type: "button", className: "chip" + (triggerStepsOverride === cmd ? " active" : ""), text: lbl, onClick: () => {{
              triggerStepsOverride = cmd;
              render();
            }} }})
          )),
          el("div", {{ className: "action-footer" }}, [
            el("button", {{ type: "button", className: "btn btn-primary", text: "Review & Trigger Job →", onClick: () => {{
              openTriggerModal(jobSel.value, stepsInp.value.trim(), "Triggered via Gemini Enterprise Workbench");
            }} }})
          ])
        ]));
      }}

      // 4. Instant 0ms Filterable Recent Runs Explorer + Selected Run Inspector
      const filteredRuns = runs.filter(r => {{
        if (runStatusFilter && r.status_bucket !== runStatusFilter) return false;
        if (runJobFilter && String(r.job_id) !== String(runJobFilter)) return false;
        if (runSearchText) {{
          const q = runSearchText.toLowerCase();
          const hay = (r.id + " " + r.job_name + " " + r.status + " " + (r.git_branch || "") + " " + (r.trigger_cause || "")).toLowerCase();
          if (!hay.includes(q)) return false;
        }}
        return true;
      }});

      const curRun = runs.find(r => String(r.id) === String(selectedRunId)) || filteredRuns[0] || null;
      if (curRun && String(curRun.id) !== String(selectedRunId)) selectedRunId = String(curRun.id);

      const searchBox = el("input", {{
        type: "search",
        value: runSearchText,
        placeholder: "Instant filter by run #, job name, git branch, or trigger...",
        onInput: ev => {{ runSearchText = ev.target.value; renderRunListOnly(); }}
      }});
      const jobFilterSel = el("select", {{ style: "max-width:220px;", onChange: ev => {{ runJobFilter = ev.target.value; render(); }} }}, [
        el("option", {{ value: "", text: "All Jobs (" + jobs.length + ")" }}),
        ...jobs.map(j => {{
          const o = el("option", {{ value: j.id, text: j.name }});
          if (String(j.id) === String(runJobFilter)) o.selected = true;
          return o;
        }})
      ]);

      const runListHost = el("div", {{ className: "queue-list" }});
      const countLabel = el("span", {{ style: "font-size:12px;color:var(--muted);font-weight:600;" }});

      function renderRunListOnly() {{
        const listNow = runs.filter(r => {{
          if (runStatusFilter && r.status_bucket !== runStatusFilter) return false;
          if (runJobFilter && String(r.job_id) !== String(runJobFilter)) return false;
          if (runSearchText) {{
            const q = runSearchText.toLowerCase();
            const hay = (r.id + " " + r.job_name + " " + r.status + " " + (r.git_branch || "") + " " + (r.trigger_cause || "")).toLowerCase();
            if (!hay.includes(q)) return false;
          }}
          return true;
        }});
        countLabel.textContent = "Showing " + listNow.length + " of " + runs.length + " prefetched runs (0ms filter)";
        runListHost.replaceChildren(...(listNow.length ? listNow.slice(0, 14).map(r => {{
          const hasCachedDiag = Boolean(diags[r.id]);
          return el("div", {{
            className: "queue-item" + (String(r.id) === String(selectedRunId) ? " selected" : ""),
            onClick: () => {{ selectedRunId = String(r.id); render(); }}
          }}, [
            el("div", {{ style: "min-width:0;" }}, [
              el("div", {{ style: "font-weight:700;", text: r.job_name + " · Run #" + r.id }}),
              el("div", {{ style: "font-size:11.5px;color:var(--muted);font-family:var(--mono);", text: (r.created_at || "") + (r.duration ? " · " + r.duration : "") + (r.git_branch ? " · " + r.git_branch : "") }})
            ]),
            el("div", {{ style: "display:flex;align-items:center;gap:6px;" }}, [
              hasCachedDiag ? el("span", {{ className: "pill pill-neutral", style: "font-size:10.5px;", text: "Logs Ready" }}) : null,
              el("span", {{ className: statusPillClass(r.status), text: r.status.toUpperCase() }})
            ])
          ]);
        }}) : [el("div", {{ style: "font-size:12.5px;color:var(--muted);padding:8px 2px;", text: "No runs match your current filter." }})]));
      }}
      renderRunListOnly();

      const statusCounts = {{
        "": runs.length,
        error: failedRuns.length,
        running: activeRuns.length,
        success: runs.filter(r => r.status_bucket === "success").length,
        cancelled: runs.filter(r => r.status_bucket === "cancelled").length
      }};

      container.appendChild(el("div", {{ className: "card" }}, [
        el("div", {{ className: "row-between" }}, [
          el("div", {{ style: "font-weight:800;font-size:15px;", text: "Recent Job Runs" }}),
          countLabel
        ]),
        el("div", {{ className: "chip-bar" }}, [
          ["", "All (" + statusCounts[""] + ")"],
          ["error", "Failed (" + statusCounts.error + ")"],
          ["running", "Active (" + statusCounts.running + ")"],
          ["success", "Succeeded (" + statusCounts.success + ")"],
          ["cancelled", "Cancelled (" + statusCounts.cancelled + ")"]
        ].map(([st, lbl]) =>
          el("button", {{ type: "button", className: "chip" + (runStatusFilter === st ? " active" : ""), text: lbl, onClick: () => {{ runStatusFilter = st; render(); }} }})
        )),
        el("div", {{ style: "display:flex;gap:8px;flex-wrap:wrap;" }}, [
          el("div", {{ style: "flex:1;min-width:200px;" }}, [searchBox]),
          jobs.length > 1 ? jobFilterSel : null
        ]),
        runListHost
      ]));

      // 5. Selected Run Inspector (instant for pre-hydrated runs; Deep-Dive button for full artifacts & docs)
      if (curRun) {{
        const diag = diags[curRun.id] || null;
        const isFailed = curRun.status_bucket === "error";
        const isActive = curRun.status_bucket === "running";
        const steps = (diag && Array.isArray(diag.steps)) ? diag.steps : [];
        const artifacts = (diag && Array.isArray(diag.artifacts)) ? diag.artifacts : [];
        const docsHits = (diag && Array.isArray(diag.docs_hits)) ? diag.docs_hits : [];

        container.appendChild(el("div", {{ className: "card" }}, [
          el("div", {{ className: "row-between" }}, [
            el("div", null, [
              el("span", {{ className: statusPillClass(curRun.status), text: curRun.status.toUpperCase() }}),
              el("h3", {{ style: "font-size:15px;font-weight:800;margin-top:4px;", text: "Run #" + curRun.id + " — " + curRun.job_name }}),
              el("div", {{ style: "font-size:12px;color:var(--muted);font-family:var(--mono);", text: (curRun.created_at || "") + (curRun.duration ? " · Duration: " + curRun.duration : "") + ((diag && diag.git_sha) ? " · SHA " + diag.git_sha : "") }})
            ]),
            el("div", {{ style: "display:flex;gap:6px;flex-wrap:wrap;" }}, [
              isFailed ? el("button", {{ type: "button", className: "btn btn-primary", text: "⚡ Safe Retry Run →", onClick: () => openRetryModal(curRun) }}) : null,
              isActive ? el("button", {{ type: "button", className: "btn", text: "⏹ Cancel Run →", onClick: () => openCancelModal(curRun) }}) : null,
              curRun.job_id ? el("button", {{ type: "button", className: "btn", text: "▶ Re-Trigger Job", onClick: () => openTriggerModal(curRun.job_id, "", "Re-triggered from Run #" + curRun.id) }}) : null,
              (!diag || !diag.deep_dive_loaded) ? el("button", {{
                type: "button",
                className: "btn btn-navy",
                text: diag ? "🔍 Deep-Dive Artifacts & Docs" : "🔍 Load Step Logs & Diagnostics",
                onClick: () => dispatchAction("dbt_deep_dive_run", {{ run_id: curRun.id, job_id: curRun.job_id, with_docs: true, confirmed_in_gui: false }}, "Deep-Diving Run #" + curRun.id, "Fetching step logs, build artifacts & dbt Docs troubleshooting...")
              }}) : null
            ])
          ]),
          (diag && diag.error_text)
            ? el("pre", {{ className: "code-box", text: diag.error_text }})
            : (diag ? el("div", {{ style: "font-size:12.5px;color:var(--secondary);", text: "No step errors reported for Run #" + curRun.id + "." }})
                    : el("div", {{ style: "font-size:12.5px;color:var(--muted);", text: "Summary metadata is loaded above. Click 'Load Step Logs & Diagnostics' only if you want step-by-step logs, artifacts, and dbt Docs troubleshooting for this run." }})),
          steps.length > 0 ? el("div", {{ style: "display:flex;flex-direction:column;gap:4px;" }}, [
            el("div", {{ style: "font-size:12px;font-weight:700;color:var(--secondary);", text: "Execution Steps (" + steps.length + ")" }}),
            ...steps.map(s => el("div", {{ className: "row-between", style: "padding:5px 8px;background:var(--subtle);border-radius:7px;font-size:12px;" }}, [
              el("span", {{ style: "font-family:var(--mono);font-weight:600;", text: s.name }}),
              el("div", {{ style: "display:flex;align-items:center;gap:8px;" }}, [
                s.duration ? el("span", {{ style: "color:var(--muted);font-family:var(--mono);", text: s.duration }}) : null,
                el("span", {{ className: statusPillClass(s.status), text: s.status || "done" }})
              ])
            ]))
          ]) : null,
          artifacts.length > 0 ? el("div", {{ className: "chip-bar" }}, artifacts.map(a =>
            el("span", {{ className: "pill pill-neutral", style: "font-family:var(--mono);", text: a }})
          )) : null,
          ...docsHits.map(d => el("div", {{ style: "padding:9px 11px;background:var(--subtle);border-radius:9px;border:1px solid var(--border);" }}, [
            el("div", {{ style: "font-weight:700;font-size:12.5px;", text: "dbt Docs Troubleshooting: " + d.title }}),
            el("div", {{ style: "font-size:12px;color:var(--secondary);margin-top:2px;", text: d.snippet }})
          ]))
        ]));
      }}
    }}

    // ==========================================================================
    // TAB 2: UNIFIED MODELS, HEALTH, SOURCES & LINEAGE (0ms Catalog + Deep Dive)
    // ==========================================================================
    function renderTab2CatalogAndLineage(container) {{
      const models = Array.isArray(STATE.models) ? STATE.models : [];
      const marts = Array.isArray(STATE.marts) ? STATE.marts : [];
      const sources = Array.isArray(STATE.sources) ? STATE.sources : [];
      const exposures = Array.isArray(STATE.exposures) ? STATE.exposures : [];
      const deepDives = STATE.model_deep_dives || {{}};
      const jobs = Array.isArray(STATE.jobs) ? STATE.jobs : [];
      const defaultJobId = String(STATE.selected_job_id || (jobs[0] && jobs[0].id) || "");
      const allRes = [...models, ...sources, ...exposures];

      if (catalogViewMode === "detail" && focusUniqueId) {{
        const item = allRes.find(r => r.unique_id === focusUniqueId || r.name === focusUniqueId) || {{
          unique_id: focusUniqueId,
          name: focusUniqueId.split(".").pop(),
          kind: focusUniqueId.split(".")[0] || "model",
          ref_snippet: "{{{{ ref('" + focusUniqueId.split(".").pop() + "') }}}}"
        }};
        const dd = deepDives[item.unique_id] || deepDives[focusUniqueId] || null;

        // Detail Top Summary + Immediate Actions + Deep-Dive Trigger
        container.appendChild(el("div", {{ className: "card" }}, [
          el("div", {{ className: "row-between" }}, [
            el("button", {{ type: "button", className: "btn btn-sm", text: "← All Resources (" + allRes.length + ")", onClick: () => {{ catalogViewMode = "list"; render(); }} }}),
            el("div", {{ style: "display:flex;gap:6px;flex-wrap:wrap;" }}, [
              item.ref_snippet ? el("button", {{ type: "button", className: "btn btn-sm", text: "🧪 Query in SQL Runner", onClick: () => openInSqlRunner(item.ref_snippet) }}) : null,
              (item.kind === "mart" || item.kind === "model") && defaultJobId ? el("button", {{
                type: "button",
                className: "btn btn-sm btn-primary",
                text: "▶ Rebuild +" + item.name + "+",
                onClick: () => openTriggerModal(defaultJobId, "dbt build --select +" + item.name + "+", "Selective build for " + item.name)
              }}) : null,
              (!dd || !dd.has_perf) ? el("button", {{
                type: "button",
                className: "btn btn-sm btn-navy",
                text: dd ? "🔬 Load Run Performance History" : "🔬 Deep-Dive Lineage, SQL & Health",
                onClick: () => dispatchAction("dbt_deep_dive_model", {{ unique_id: item.unique_id, subtab: catalogSubtab, orientation: lineageOrient, confirmed_in_gui: false }}, "Inspecting " + item.name, "Fetching lineage, compiled SQL, column schema & run performance history...")
              }}) : null
            ])
          ]),
          el("div", null, [
            el("div", {{ style: "display:flex;gap:6px;align-items:center;flex-wrap:wrap;" }}, [
              el("span", {{ className: statusPillClass(item.status_badge || item.kind), text: String(item.status_badge || item.kind).toUpperCase() }}),
              (dd && dd.materialization) || item.materialization ? el("span", {{ className: "pill pill-neutral", text: "Materialized: " + ((dd && dd.materialization) || item.materialization) }}) : null,
              dd ? el("span", {{ className: "pill pill-ok", text: "Deep-Dive Loaded" }}) : null
            ]),
            el("h2", {{ style: "font-size:17px;font-weight:800;margin-top:5px;font-family:var(--mono);", text: item.name }}),
            el("div", {{ style: "font-size:12px;color:var(--muted);font-family:var(--mono);", text: item.unique_id }}),
            ((dd && dd.description) || item.description) ? el("p", {{ style: "margin-top:6px;color:var(--secondary);", text: (dd && dd.description) || item.description }}) : null
          ]),
          el("div", {{ className: "seg-bar", style: "padding-top:4px;border-top:1px solid var(--border);" }}, [
            ["overview", "Overview & Health"],
            ["lineage", "Lineage Graph" + (dd ? " (" + ((dd.upstream || []).length + (dd.downstream || []).length) + ")" : "")],
            ["sql", "Columns & SQL" + (dd && dd.columns ? " (" + dd.columns.length + ")" : "")]
          ].map(([k, lbl]) =>
            el("button", {{ type: "button", className: "chip" + (catalogSubtab === k ? " active" : ""), text: lbl, onClick: () => {{ catalogSubtab = k; render(); }} }})
          ))
        ]));

        if (!dd) {{
          container.appendChild(el("div", {{ className: "card" }}, [
            el("div", {{ style: "font-weight:800;", text: "Catalog Metadata Ready — Run Deep Dive for Full Lineage, SQL & Build History" }}),
            el("div", {{ style: "font-size:12.8px;color:var(--secondary);", text: "To keep browsing instant, column-level schemas, compiled warehouse SQL, DAG edges, and historical run durations are fetched on demand." }}),
            item.ref_snippet ? el("pre", {{ className: "code-box", text: "-- dbt Reference Snippet\\nSELECT *\\nFROM " + item.ref_snippet + "\\nLIMIT 25" }}) : null,
            el("div", {{ className: "action-footer" }}, [
              el("button", {{
                type: "button",
                className: "btn btn-primary",
                text: "🔬 Run Deep Dive on " + item.name + " →",
                onClick: () => dispatchAction("dbt_deep_dive_model", {{ unique_id: item.unique_id, subtab: catalogSubtab, orientation: lineageOrient, confirmed_in_gui: false }}, "Deep-Diving " + item.name, "Fetching upstream/downstream lineage, compiled SQL, columns & health...")
              }})
            ])
          ]));
          return;
        }}

        if (catalogSubtab === "overview") {{
          const hEntries = dd.health_kv ? Object.entries(dd.health_kv) : [];
          const perf = Array.isArray(dd.performance) ? dd.performance : [];
          const maxSec = Math.max(1, ...perf.map(p => Number(p.execution_seconds) || 0));
          container.appendChild(el("div", {{ className: "card" }}, [
            el("div", {{ style: "font-weight:800;font-size:14.5px;", text: "Health, Freshness & Execution Trends" }}),
            ...(hEntries.length ? hEntries.map(([k, v]) => el("div", {{ className: "row-between", style: "padding:5px 0;border-bottom:1px solid var(--border);font-size:12.5px;" }}, [
              el("span", {{ style: "color:var(--muted);font-weight:600;", text: k }}),
              el("span", {{ style: "font-family:var(--mono);", text: String(v) }})
            ])) : [
              dd.health_text
                ? el("pre", {{ className: "code-box", text: dd.health_text }})
                : el("div", {{ style: "font-size:12.5px;color:var(--secondary);", text: "Schema: " + (dd.database ? dd.database + "." : "") + (dd.schema || "default") + " · Upstream: " + (dd.upstream || []).length + " · Downstream: " + (dd.downstream || []).length }})
            ]),
            perf.length > 0 ? el("div", {{ style: "margin-top:8px;display:flex;flex-direction:column;gap:5px;" }}, [
              el("div", {{ style: "font-size:12px;font-weight:700;color:var(--secondary);", text: "Recent Execution Duration Trend (" + perf.length + " runs)" }}),
              ...perf.map(p => el("div", {{ className: "bar-row" }}, [
                el("span", {{ className: statusPillClass(p.status), text: p.status }}),
                el("div", {{ className: "bar-track" }}, [el("div", {{ className: "bar-fill", style: "width:" + Math.max(5, Math.round(((Number(p.execution_seconds) || 0) / maxSec) * 100)) + "%;" }})]),
                el("span", {{ style: "font-family:var(--mono);font-size:11.5px;", text: p.execution_seconds + "s" }})
              ]))
            ]) : null
          ]));
        }} else if (catalogSubtab === "lineage") {{
          const isTD = lineageOrient === "td";
          const up = Array.isArray(dd.upstream) ? dd.upstream : [];
          const down = Array.isArray(dd.downstream) ? dd.downstream : [];
          const arrowTxt = isTD ? "↓" : "→";
          const makeNode = (n, isFocus) => el("div", {{
            className: "lin-node" + (isFocus ? " focus" : ""),
            onClick: () => {{
              if (isFocus) return;
              if (deepDives[n.unique_id]) {{
                focusUniqueId = n.unique_id;
                render();
              }} else {{
                dispatchAction("dbt_deep_dive_model", {{ unique_id: n.unique_id, subtab: "lineage", orientation: lineageOrient, confirmed_in_gui: false }}, "Tracing Lineage: " + n.name, "Fetching dependency graph & node details from dbt Cloud...");
              }}
            }}
          }}, [
            el("div", {{ className: "row-between" }}, [
              el("span", {{ className: "pill pill-neutral", style: "font-size:10px;", text: String(n.resource_type || "model").toUpperCase() }}),
              (!isFocus && deepDives[n.unique_id]) ? el("span", {{ style: "font-size:10px;color:var(--ok-text);font-weight:700;", text: "Cached" }}) : null
            ]),
            el("div", {{ style: "font-weight:700;font-family:var(--mono);font-size:12px;margin-top:3px;", text: n.name || n.unique_id }})
          ]);

          container.appendChild(el("div", {{ className: "card" }}, [
            el("div", {{ className: "row-between" }}, [
              el("div", {{ style: "font-weight:800;", text: "Upstream & Downstream Dependency Graph" }}),
              el("button", {{ type: "button", className: "btn btn-sm", text: isTD ? "⇅ Top-Down" : "⇄ Left-to-Right", onClick: () => {{
                lineageOrient = isTD ? "lr" : "td";
                render();
              }} }})
            ]),
            el("div", {{ className: "lineage-grid" }}, [
              el("div", {{ className: isTD ? "lin-flow-td" : "lin-flow-lr" }}, [
                el("div", {{ className: "lin-col" }}, [
                  el("div", {{ style: "font-size:11px;font-weight:800;color:var(--muted);text-transform:uppercase;", text: "Upstream (" + up.length + ")" }}),
                  ...(up.length ? up.map(n => makeNode(n, false)) : [el("div", {{ style: "font-size:12px;color:var(--muted);", text: "No upstream parents" }})])
                ]),
                el("div", {{ style: "display:flex;align-items:center;justify-content:center;font-weight:900;color:#64748B;font-size:16px;", text: arrowTxt }}),
                el("div", {{ className: "lin-col" }}, [
                  el("div", {{ style: "font-size:11px;font-weight:800;color:#B9381E;text-transform:uppercase;", text: "Selected Resource" }}),
                  makeNode({{ unique_id: dd.unique_id, name: dd.name, resource_type: dd.resource_type }}, true)
                ]),
                el("div", {{ style: "display:flex;align-items:center;justify-content:center;font-weight:900;color:#64748B;font-size:16px;", text: arrowTxt }}),
                el("div", {{ className: "lin-col" }}, [
                  el("div", {{ style: "font-size:11px;font-weight:800;color:var(--muted);text-transform:uppercase;", text: "Downstream (" + down.length + ")" }}),
                  ...(down.length ? down.map(n => makeNode(n, false)) : [el("div", {{ style: "font-size:12px;color:var(--muted);", text: "Leaf node" }})])
                ])
              ])
            ])
          ]));
        }} else {{
          const cols = Array.isArray(dd.columns) ? dd.columns : [];
          const sqlStr = codeMode === "compiled" ? (dd.compiled_sql || dd.raw_sql) : (dd.raw_sql || dd.compiled_sql);
          container.appendChild(el("div", {{ className: "card" }}, [
            el("div", {{ className: "row-between" }}, [
              el("div", {{ style: "font-weight:800;", text: "Columns (" + cols.length + ") & SQL Definition" }}),
              (dd.compiled_sql || dd.raw_sql) ? el("div", {{ style: "display:flex;gap:6px;" }}, [
                el("button", {{ type: "button", className: "btn btn-sm" + (codeMode === "compiled" ? " btn-navy" : ""), text: "Compiled SQL", onClick: () => {{ codeMode = "compiled"; render(); }} }}),
                el("button", {{ type: "button", className: "btn btn-sm" + (codeMode === "raw" ? " btn-navy" : ""), text: "Raw Jinja", onClick: () => {{ codeMode = "raw"; render(); }} }})
              ]) : null
            ]),
            ...cols.slice(0, 20).map(c => el("div", {{ className: "row-between", style: "padding:4px 0;border-bottom:1px solid var(--border);font-size:12px;" }}, [
              el("div", null, [
                el("span", {{ style: "font-family:var(--mono);font-weight:700;", text: c.name }}),
                c.description ? el("span", {{ style: "color:var(--muted);margin-left:8px;", text: c.description }}) : null
              ]),
              el("span", {{ className: "pill pill-neutral", text: c.type }})
            ])),
            sqlStr ? el("pre", {{ className: "code-box", text: sqlStr }}) : (dd.fallback_text ? el("pre", {{ className: "code-box", text: dd.fallback_text }}) : null)
          ]));
        }}
        return;
      }}

      // Catalog Master List View (100% 0ms instant search & resource filtering)
      const listHost = el("div", {{ className: "queue-list" }});
      const countSpan = el("span", {{ style: "font-size:12px;color:var(--muted);font-weight:600;" }});
      const searchInp = el("input", {{
        type: "search",
        value: catalogSearchText,
        placeholder: "Instant 0ms filter across marts, models, sources & exposures (or click AI Search for semantic match)...",
        onInput: ev => {{ catalogSearchText = ev.target.value; renderCatalogItemsOnly(); }}
      }});

      function renderCatalogItemsOnly() {{
        const filtered = allRes.filter(item => {{
          if (catalogKindFilter === "mart" && item.kind !== "mart") return false;
          if (catalogKindFilter === "model" && item.kind !== "model" && item.kind !== "mart") return false;
          if (catalogKindFilter === "source" && item.kind !== "source") return false;
          if (catalogKindFilter === "exposure" && item.kind !== "exposure") return false;
          if (catalogKindFilter === "attention" && item.status_bucket !== "error" && item.status_bucket !== "warn") return false;
          if (catalogSearchText) {{
            const q = catalogSearchText.toLowerCase();
            const hay = (item.name + " " + item.unique_id + " " + (item.description || "") + " " + (item.status_badge || "")).toLowerCase();
            if (!hay.includes(q)) return false;
          }}
          return true;
        }});
        countSpan.textContent = "Showing " + filtered.length + " of " + allRes.length + " resources (0ms filter)";
        listHost.replaceChildren(...(filtered.length ? filtered.slice(0, 25).map(item => {{
          const isCached = Boolean(deepDives[item.unique_id]);
          return el("div", {{
            className: "queue-item",
            onClick: () => {{ focusUniqueId = item.unique_id; catalogViewMode = "detail"; render(); }}
          }}, [
            el("div", {{ style: "min-width:0;flex:1;" }}, [
              el("div", {{ style: "display:flex;align-items:center;gap:6px;flex-wrap:wrap;" }}, [
                el("span", {{ className: statusPillClass(item.status_badge || item.kind), text: String(item.status_badge || item.kind).toUpperCase() }}),
                el("span", {{ style: "font-weight:800;font-family:var(--mono);", text: item.name }}),
                isCached ? el("span", {{ className: "pill pill-neutral", style: "font-size:10.5px;", text: "Lineage & SQL Ready" }}) : null
              ]),
              item.description ? el("div", {{ style: "font-size:12px;color:var(--secondary);margin-top:2px;", text: item.description }}) : el("div", {{ style: "font-size:11.5px;color:var(--muted);font-family:var(--mono);", text: item.unique_id }})
            ]),
            el("div", {{ style: "display:flex;gap:6px;align-items:center;" }}, [
              item.ref_snippet ? el("button", {{
                type: "button",
                className: "btn btn-sm",
                text: "SQL",
                title: "Open in SQL Runner",
                onClick: ev => {{ ev.stopPropagation(); openInSqlRunner(item.ref_snippet); }}
              }}) : null,
              el("span", {{ style: "font-weight:700;color:var(--primary);font-size:12px;", text: "Inspect →" }})
            ])
          ]);
        }}) : [el("div", {{ style: "font-size:12.5px;color:var(--muted);padding:8px 2px;", text: "No resources match this local filter. Try 'AI Semantic Search' for broader discovery." }})]));
      }}
      renderCatalogItemsOnly();

      const attnCount = sources.filter(s => s.status_bucket === "error" || s.status_bucket === "warn").length;
      container.appendChild(el("div", {{ className: "card" }}, [
        el("div", {{ className: "row-between" }}, [
          el("div", {{ style: "font-weight:800;font-size:15.5px;", text: "Models, Sources, Freshness & Lineage Explorer" }}),
          countSpan
        ]),
        el("div", {{ className: "chip-bar" }}, [
          ["all", "All (" + allRes.length + ")"],
          ["mart", "Marts (" + marts.length + ")"],
          ["model", "All Models (" + models.length + ")"],
          ["source", "Raw Sources (" + sources.length + ")"],
          ["exposure", "BI Exposures (" + exposures.length + ")"],
          ...(attnCount > 0 ? [["attention", "Needs Attention (" + attnCount + ")"]] : [])
        ].map(([k, lbl]) =>
          el("button", {{ type: "button", className: "chip" + (catalogKindFilter === k ? " active" : ""), text: lbl, onClick: () => {{ catalogKindFilter = k; render(); }} }})
        )),
        el("div", {{ style: "display:flex;gap:8px;" }}, [
          searchInp,
          el("button", {{
            type: "button",
            className: "btn btn-navy",
            title: "Run dbt MCP get_related_models semantic search",
            text: "🧠 AI Search",
            onClick: () => {{
              if (!searchInp.value.trim()) {{ showToast("Enter a business topic first for AI Semantic Search."); return; }}
              dispatchAction("dbt_deep_dive_model", {{ search_query: searchInp.value.trim(), confirmed_in_gui: false }}, "AI Semantic Model Search", "Finding related dbt models & hydrating lineage for '" + searchInp.value.trim() + "'...");
            }}
          }})
        ]),
        listHost
      ]));
    }}

    // ==========================================================================
    // TAB 3: SEMANTIC METRIC STUDIO (0ms Metric Search + On-Demand Slice Query)
    // ==========================================================================
    function renderTab3Metrics(container) {{
      const mv = STATE.metrics_view || {{}};
      const metrics = Array.isArray(mv.metrics) ? mv.metrics : [];
      const saved = Array.isArray(mv.saved_queries) ? mv.saved_queries : [];
      const dims = Array.isArray(mv.dimensions) ? mv.dimensions : [];
      const rows = Array.isArray(mv.rows) ? mv.rows : [];

      const activeMetricObj = metrics.find(m => m.name === draftMetric) || metrics[0] || null;
      const isSliceDirty = (
        draftMetric !== String(mv.selected_metric || "") ||
        draftGrain !== String(mv.grain || "MONTH") ||
        draftGroupDim !== String(mv.group_dim || "") ||
        draftWhereDim !== String(mv.where_dim || "") ||
        draftWhereVal !== String(mv.where_val || "")
      );

      const sliceCard = el("div", {{ className: "card" }}, [
        el("div", {{ className: "row-between" }}, [
          el("div", null, [
            el("span", {{ className: "pill pill-neutral", text: "dbt Semantic Layer · MetricFlow" }}),
            el("h2", {{ style: "font-size:16.5px;font-weight:800;margin-top:4px;", text: activeMetricObj ? activeMetricObj.label : "Governed Metrics" }}),
            activeMetricObj && activeMetricObj.description ? el("div", {{ style: "font-size:12px;color:var(--secondary);", text: activeMetricObj.description }}) : null
          ]),
          el("div", {{ className: "chip-bar" }}, [
            ["bars", "Visual Bars"],
            ["table", "Data Rows"],
            ["sql", "Compiled SQL"]
          ].map(([m, lbl]) =>
            el("button", {{ type: "button", className: "chip" + (metricDisplayMode === m ? " active" : ""), text: lbl, onClick: () => {{ metricDisplayMode = m; render(); }} }})
          ))
        ])
      ]);

      if (metrics.length > 0) {{
        const metricSel = el("select", {{ onChange: ev => {{ draftMetric = ev.target.value; render(); }} }}, metrics.map(m => {{
          const o = el("option", {{ value: m.name, text: m.label + " (" + m.name + ")" }});
          if (m.name === draftMetric) o.selected = true;
          return o;
        }}));
        const grainSel = el("select", {{ onChange: ev => {{ draftGrain = ev.target.value; render(); }} }}, ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"].map(g => {{
          const o = el("option", {{ value: g, text: "Grain: " + g }});
          if (g === draftGrain) o.selected = true;
          return o;
        }}));
        const dimSel = el("select", {{ onChange: ev => {{ draftGroupDim = ev.target.value; render(); }} }}, [
          el("option", {{ value: "", text: "Group By: (None)" }}),
          ...dims.filter(d => d.name !== "metric_time").map(d => {{
            const o = el("option", {{ value: d.name, text: "By: " + d.name }});
            if (d.name === draftGroupDim) o.selected = true;
            return o;
          }})
        ]);

        sliceCard.appendChild(el("div", {{ style: "display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;" }}, [
          metricSel, grainSel, dimSel
        ]));
        sliceCard.appendChild(el("div", {{ className: "action-footer" }}, [
          isSliceDirty ? el("span", {{ style: "font-size:12px;color:var(--warn-text);font-weight:600;", text: "Selection changed — click Run Metric Slice to query warehouse" }}) : null,
          el("button", {{
            type: "button",
            className: "btn btn-primary",
            text: "▶ Run Metric Slice →",
            onClick: () => dispatchAction("dbt_metrics_query", {{
              metric: draftMetric,
              grain: draftGrain,
              group_dim: draftGroupDim,
              where_dim: draftWhereDim,
              where_val: draftWhereVal,
              confirmed_in_gui: false
            }}, "Querying Semantic Layer", "Compiling & running MetricFlow slice for " + draftMetric + " (" + draftGrain + ")...")
          }})
        ]));
      }} else {{
        sliceCard.appendChild(el("div", {{ className: "banner banner-warn", text: mv.error || "No Semantic Layer metrics were returned for this project." }}));
      }}
      container.appendChild(sliceCard);

      // Metric Results View (0ms toggle between Bars, Table, and Compiled SQL)
      if (metricDisplayMode === "sql") {{
        container.appendChild(el("div", {{ className: "card" }}, [
          el("div", {{ style: "font-weight:800;", text: "Compiled MetricFlow SQL (" + (mv.selected_metric || "metric") + ")" }}),
          el("pre", {{ className: "code-box", text: mv.compiled_sql || "-- Run a metric slice to inspect compiled MetricFlow SQL." }})
        ]));
      }} else if (rows.length > 0 && metricDisplayMode === "table") {{
        const cols = Object.keys(rows[0]);
        container.appendChild(el("div", {{ className: "card" }}, [
          el("div", {{ style: "font-weight:800;", text: "Query Rows (" + rows.length + " rows · " + (mv.selected_metric || "") + ")" }}),
          el("div", {{ className: "data-grid-row", style: "font-weight:800;background:var(--subtle);" }}, cols.map(c => el("span", {{ text: c }}))),
          ...rows.slice(0, 20).map(r => el("div", {{ className: "data-grid-row" }}, cols.map(c => el("span", {{ text: String(r[c] ?? "") }}))))
        ]));
      }} else if (rows.length > 0) {{
        const cols = Object.keys(rows[0]);
        const numCol = cols.find(c => typeof rows[0][c] === "number" || (!isNaN(parseFloat(rows[0][c])) && !c.toLowerCase().includes("time") && !c.toLowerCase().includes("date"))) || cols[cols.length - 1];
        const labelCol = cols.find(c => c !== numCol) || cols[0];
        const maxVal = Math.max(1, ...rows.map(r => Math.abs(parseFloat(r[numCol]) || 0)));
        container.appendChild(el("div", {{ className: "card" }}, [
          el("div", {{ style: "font-weight:800;font-size:14px;", text: (mv.selected_metric || "Metric") + " · " + (mv.grain || "MONTH") + " (" + rows.length + " rows)" }}),
          ...rows.slice(0, 14).map(r => {{
            const rawVal = parseFloat(r[numCol]);
            const pct = isNaN(rawVal) ? 15 : Math.max(4, Math.round((Math.abs(rawVal) / maxVal) * 100));
            const lbl = String(r[labelCol] ?? "Total").slice(0, 28);
            const dispVal = isNaN(rawVal) ? String(r[numCol] ?? "") : rawVal.toLocaleString(undefined, {{ maximumFractionDigits: 2 }});
            return el("div", {{ className: "bar-row" }}, [
              el("span", {{ style: "width:140px;font-family:var(--mono);font-size:11.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;", text: lbl }}),
              el("div", {{ className: "bar-track" }}, [el("div", {{ className: "bar-fill", style: "width:" + pct + "%;" }})]),
              el("span", {{ style: "min-width:84px;text-align:right;font-family:var(--mono);font-weight:700;", text: dispVal }})
            ]);
          }})
        ]));
      }} else if (mv.raw_preview) {{
        container.appendChild(el("div", {{ className: "card" }}, [
          el("pre", {{ className: "code-box", text: mv.raw_preview }})
        ]));
      }}

      // Instant 0ms Searchable Metric & Saved Query Catalog
      if (metrics.length > 1 || saved.length > 0) {{
        const catList = el("div", {{ className: "queue-list" }});
        const mSearch = el("input", {{
          type: "search",
          value: metricSearchText,
          placeholder: "Instant filter across " + metrics.length + " governed metrics & " + saved.length + " saved queries...",
          onInput: ev => {{ metricSearchText = ev.target.value; syncMetricCatalog(); }}
        }});
        function syncMetricCatalog() {{
          const q = metricSearchText.toLowerCase();
          const fMet = metrics.filter(m => !q || (m.name + " " + m.label + " " + (m.description || "")).toLowerCase().includes(q));
          catList.replaceChildren(...fMet.slice(0, 15).map(m => el("div", {{
            className: "queue-item" + (m.name === draftMetric ? " selected" : ""),
            onClick: () => {{ draftMetric = m.name; render(); }}
          }}, [
            el("div", null, [
              el("div", {{ style: "font-weight:700;", text: m.label + " (" + m.name + ")" }}),
              m.description ? el("div", {{ style: "font-size:12px;color:var(--muted);", text: m.description }}) : null
            ]),
            el("span", {{ className: "pill pill-neutral", text: m.type }})
          ])));
        }}
        syncMetricCatalog();
        container.appendChild(el("div", {{ className: "card" }}, [
          el("div", {{ style: "font-weight:800;", text: "Governed Metric Catalog (" + metrics.length + ")" }}),
          mSearch,
          catList
        ]));
      }}
    }}

    // ==========================================================================
    // TAB 4: GOVERNED SQL RUNNER & MACROS (0ms Snippet Builder + Guarded Execute)
    // ==========================================================================
    function renderTab4SqlAndMacros(container) {{
      const sv = STATE.sql_view || {{}};
      const marts = Array.isArray(STATE.marts) ? STATE.marts : [];
      const sources = Array.isArray(STATE.sources) ? STATE.sources : [];
      const macros = Array.isArray(STATE.macros) ? STATE.macros : [];
      const qRows = Array.isArray(sv.query_rows) ? sv.query_rows : [];

      const nlInp = el("input", {{
        type: "text",
        value: nlPromptText,
        placeholder: "Optional AI prompt: ask in plain English to generate dbt Jinja SQL (uses text_to_sql)...",
        onInput: ev => {{ nlPromptText = ev.target.value; }}
      }});
      const sqlArea = el("textarea", {{
        className: "code-input",
        rows: "6",
        onInput: ev => {{ sqlEditorText = ev.target.value; }}
      }});
      sqlArea.value = sqlEditorText;

      container.appendChild(el("div", {{ className: "card" }}, [
        el("div", {{ className: "row-between" }}, [
          el("div", null, [
            el("span", {{ className: "pill pill-neutral", text: "Governed SQL & Jinja Runner" }}),
            el("h2", {{ style: "font-size:16px;font-weight:800;margin-top:4px;", text: "Warehouse SQL & NL-to-dbt-SQL" }})
          ])
        ]),
        el("div", {{ style: "display:flex;gap:8px;" }}, [
          nlInp,
          el("button", {{ type: "button", className: "btn btn-navy", text: "✨ Generate SQL", onClick: () => {{
            if (!nlInp.value.trim()) {{ showToast("Type a natural-language question first."); return; }}
            dispatchAction("dbt_sql_generate", {{ nl_prompt: nlInp.value.trim(), confirmed_in_gui: false }}, "Generating dbt SQL", "Running dbt Cloud AI text_to_sql against your governed models...");
          }} }})
        ]),
        sqlArea,
        el("div", {{ className: "action-footer" }}, [
          el("button", {{ type: "button", className: "btn btn-primary", text: "▶ Review & Execute Warehouse SQL →", onClick: () => {{
            if (!sqlArea.value.trim()) {{ showToast("SQL query cannot be empty."); return; }}
            confirmModal = {{
              title: "Confirm Warehouse SQL Execution",
              warning: "Executes this dbt Jinja / SQL query against your connected data warehouse.",
              rows: [
                {{ label: "dbt Project", value: STATE.active_project_name || String(STATE.active_project_id || "") }},
                {{ label: "SQL Query", value: sqlArea.value.slice(0, 260) }}
              ],
              payload: {{ operation: "execute_sql", sql: sqlArea.value, confirmed_in_gui: true, execute: true }}
            }};
            renderModal();
          }} }})
        ])
      ]));

      if (qRows.length > 0 || sv.query_text) {{
        const cols = qRows.length > 0 ? Object.keys(qRows[0]) : [];
        container.appendChild(el("div", {{ className: "card" }}, [
          el("div", {{ style: "font-weight:800;", text: "Warehouse Query Results" + (qRows.length ? " (" + qRows.length + " rows)" : "") }}),
          qRows.length > 0
            ? el("div", null, [
                el("div", {{ className: "data-grid-row", style: "font-weight:800;background:var(--subtle);" }}, cols.map(c => el("span", {{ text: c }}))),
                ...qRows.slice(0, 20).map(r => el("div", {{ className: "data-grid-row" }}, cols.map(c => el("span", {{ text: String(r[c] ?? "") }}))))
              ])
            : el("pre", {{ className: "code-box", text: sv.query_text }})
        ]));
      }}

      // Instant 0ms Reference & Macro Snippet Picker
      const pickerHost = el("div", {{ className: "queue-list" }});
      const pSearch = el("input", {{
        type: "search",
        value: pickerSearchText,
        placeholder: "Instant filter across prefetched marts, sources, and macros...",
        onInput: ev => {{ pickerSearchText = ev.target.value; syncPicker(); }}
      }});

      function syncPicker() {{
        const q = pickerSearchText.toLowerCase();
        const pool = pickerKind === "mart" ? marts : (pickerKind === "source" ? sources : macros);
        const filtered = pool.filter(it => !q || (it.name + " " + (it.description || "") + " " + (it.package || "")).toLowerCase().includes(q));
        pickerHost.replaceChildren(...(filtered.length ? filtered.slice(0, 14).map(it => {{
          const snip = it.ref_snippet || it.snippet || it.name;
          return el("div", {{ className: "queue-item", onClick: () => {{
            if (pickerKind === "macro") {{
              sqlEditorText = (sqlEditorText ? sqlEditorText + "\\n" : "") + snip;
            }} else {{
              sqlEditorText = "SELECT *\\nFROM " + snip + "\\nLIMIT 25";
            }}
            sqlArea.value = sqlEditorText;
            showToast("Inserted " + snip + " (0ms)");
          }} }}, [
            el("div", null, [
              el("div", {{ style: "font-weight:700;font-family:var(--mono);", text: it.name }}),
              el("div", {{ style: "font-size:12px;color:var(--muted);", text: it.description || it.package || it.unique_id || "" }})
            ]),
            el("div", {{ style: "display:flex;gap:6px;align-items:center;" }}, [
              pickerKind !== "macro" ? el("button", {{ type: "button", className: "btn btn-sm", text: "Count(*)", onClick: ev => {{
                ev.stopPropagation();
                sqlEditorText = "SELECT COUNT(*) AS row_count\\nFROM " + snip;
                sqlArea.value = sqlEditorText;
                showToast("Loaded row count query for " + it.name);
              }} }}) : null,
              el("span", {{ className: "pill pill-neutral", style: "font-family:var(--mono);", text: snip }})
            ])
          ]);
        }}) : [el("div", {{ style: "font-size:12.5px;color:var(--muted);", text: "No matching items." }})]));
      }}
      syncPicker();

      container.appendChild(el("div", {{ className: "card" }}, [
        el("div", {{ style: "font-weight:800;", text: "Instant Reference & Macro Picker (Click to Insert)" }}),
        el("div", {{ className: "chip-bar" }}, [
          ["mart", "Mart Models (" + marts.length + ")"],
          ["source", "Raw Sources (" + sources.length + ")"],
          ["macro", "dbt Macros (" + macros.length + ")"]
        ].map(([k, lbl]) =>
          el("button", {{ type: "button", className: "chip" + (pickerKind === k ? " active" : ""), text: lbl, onClick: () => {{ pickerKind = k; render(); }} }})
        )),
        pSearch,
        pickerHost
      ]));
    }}

    // ==========================================================================
    // TWO-STEP IN-SCREEN CONFIRMATION MODAL (`confirmed_in_gui: true`)
    // ==========================================================================
    function renderModal() {{
      const root = document.getElementById("modalRoot");
      root.replaceChildren();
      if (!confirmModal) return;
      const rowsWrap = el("div", {{ style: "padding:10px 12px;background:var(--subtle);border:1px solid var(--border);border-radius:10px;display:flex;flex-direction:column;gap:7px;" }},
        (confirmModal.rows || []).map(r => el("div", null, [
          el("div", {{ style: "font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;", text: r.label }}),
          el("div", {{ style: "font-weight:600;font-family:var(--mono);font-size:12px;word-break:break-word;", text: r.value }})
        ]))
      );
      const backBtn = el("button", {{
        type: "button",
        className: "btn",
        disabled: isSubmitting,
        text: "← Back to Edit",
        onClick: () => {{ if (!isSubmitting) {{ confirmModal = null; renderModal(); }} }}
      }});
      const okBtn = el("button", {{
        type: "button",
        className: "btn btn-primary",
        disabled: isSubmitting,
        text: "✓ Confirm & Execute",
        onClick: () => {{
          if (isSubmitting) return;
          const payloadToRun = confirmModal.payload;
          const execTitle = (confirmModal.title || "Executing on dbt Cloud").replace(/^Confirm\\s+/i, "Running ");
          confirmModal = null;
          renderModal();
          dispatchAction("dbt_execute", payloadToRun, execTitle, "Executing confirmed action via dbt Cloud MCP & refreshing Canvas...");
        }}
      }});

      root.appendChild(el("div", {{ className: "modal-backdrop" }}, [
        el("div", {{ className: "modal-card" }}, [
          el("div", {{ style: "font-size:16px;font-weight:800;", text: confirmModal.title || "Confirm Action" }}),
          confirmModal.warning ? el("div", {{ className: "banner banner-warn", text: confirmModal.warning }}) : null,
          rowsWrap,
          el("div", {{ className: "modal-actions", style: "display:flex;justify-content:flex-end;gap:8px;" }}, [backBtn, okBtn])
        ])
      ]));
    }}

    function render() {{
      renderHeader();
      const root = document.getElementById("appRoot");
      root.replaceChildren();
      try {{
        if (STATE.banner) {{
          const b = STATE.banner;
          root.appendChild(el("div", {{ className: "banner banner-" + (b.kind || "ok") }}, [
            el("strong", {{ text: b.title || "Notice" }}),
            b.detail ? el("div", {{ style: "margin-top:3px;", text: b.detail }}) : null
          ]));
        }}

        const runs = Array.isArray(STATE.runs) ? STATE.runs : [];
        const failedCnt = runs.filter(r => r.status_bucket === "error").length;
        const resCnt = allUnifiedResources().length;
        const metCnt = (STATE.metrics_view && Array.isArray(STATE.metrics_view.metrics)) ? STATE.metrics_view.metrics.length : 0;

        // Instant 0ms client-side tab switching! (Only calls MCP if bundle was never hydrated yet)
        const tabDefs = [
          [1, "1. Action Center & Runs", failedCnt ? (failedCnt + "!") : String(runs.length), failedCnt > 0],
          [2, "2. Models, Health & Lineage", String(resCnt), false],
          [3, "3. Semantic Metrics", String(metCnt), false],
          [4, "4. SQL & Macros", "SQL", false]
        ];
        root.appendChild(el("nav", {{ className: "nav-tabs" }}, tabDefs.map(([idx, lbl, badge, isCrit]) =>
          el("button", {{
            type: "button",
            className: "nav-tab" + (activeTab === idx ? " active" : ""),
            onClick: () => {{
              if (!STATE.bundle_ready) {{
                dispatchAction("dbt_nav", {{ tab: idx, confirmed_in_gui: false }}, "Loading dbt Cloud Workspace", "Prefetching workspace runs, models, freshness & metrics...");
                return;
              }}
              activeTab = idx;
              render();
            }}
          }}, [
            el("span", {{ text: lbl }}),
            el("span", {{ className: "tab-badge" + (isCrit && activeTab !== idx ? " tab-badge-crit" : ""), text: badge }})
          ])
        )));

        if (activeTab === 1) renderTab1ActionCenter(root);
        else if (activeTab === 2) renderTab2CatalogAndLineage(root);
        else if (activeTab === 3) renderTab3Metrics(root);
        else renderTab4SqlAndMacros(root);

        renderModal();
      }} catch (err) {{
        root.appendChild(el("div", {{ className: "banner banner-crit", text: "Render error: " + (err && err.message ? err.message : String(err)) }}));
      }}
    }}

    render();
  }})();
  </script>
</body>
</html>"""


# ==============================================================================
# 8. STAGE A2UI v0.9 CANVAS PAYLOAD (DETERMINISTIC `surfaceId` + ROTATING `frame_id`)
# ==============================================================================
def stage_a2ui_canvas(
    state: dict[str, Any],
    *,
    card_title: str,
    card_description: str,
    surface_id: str = DEFAULT_SURFACE_ID,
) -> dict[str, Any]:
    out_path = _safe_tmp_path("DBT_A2UI_OUT", DEFAULT_A2UI_OUT_PATH)
    seq = int(state.get("_render_seq") or 0) + 1
    state["_render_seq"] = seq
    save_state(state)

    frame_id = f"{surface_id}_frame_{seq}"
    html_content = render_dbt_workbench_html(state)

    payload: list[dict[str, Any]] = [
        {
            "version": "v0.9",
            "createSurface": {
                "surfaceId": surface_id,
                "catalogId": GE_COMPOSITE_CATALOG_V09,
            },
        },
        {
            "version": "v0.9",
            "updateComponents": {
                "surfaceId": surface_id,
                "components": [
                    {
                        "id": "root",
                        "component": "Canvas",
                        "cardTitle": card_title,
                        "cardDescription": card_description,
                        "cardIcon": "analytics",
                        "autoOpen": True,
                        "autoFullscreen": False,
                        "children": [frame_id],
                    },
                    {
                        "id": frame_id,
                        "component": "IFrameSrcdoc",
                        "htmlContent": html_content,
                        "height": 860,
                        "title": card_title,
                    },
                ],
            },
        },
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    runs = state.get("runs") if isinstance(state.get("runs"), list) else []
    failed_runs = [r for r in runs if isinstance(r, dict) and r.get("status_bucket") == "error"]
    sources = state.get("sources") if isinstance(state.get("sources"), list) else []
    stale_sources = [s for s in sources if isinstance(s, dict) and s.get("status_bucket") in ("error", "warn")]

    return {
        "MANDATORY_FINAL_TOOL_CALL": {
            "tool": "emit_a2ui",
            "arguments": {"a2ui_json_file": str(out_path)},
            "note": "YOU MUST CALL emit_a2ui WITH THIS a2ui_json_file AS YOUR VERY LAST TOOL CALL OR THE CANVAS WILL BE BLANK!",
        },
        "a2ui_ready": True,
        "a2ui_surface_id": surface_id,
        "a2ui_frame_id": frame_id,
        "a2ui_json_file": str(out_path),
        "active_project_id": state.get("active_project_id"),
        "active_project_name": state.get("active_project_name"),
        "workspace_summary": {
            "jobs_count": len(state.get("jobs") or []),
            "recent_runs_count": len(runs),
            "failed_runs_count": len(failed_runs),
            "models_count": len(state.get("models") or []),
            "marts_count": len(state.get("marts") or []),
            "sources_count": len(sources),
            "source_alerts_count": len(stale_sources),
            "exposures_count": len(state.get("exposures") or []),
            "metrics_count": len((state.get("metrics_view") or {}).get("metrics") or []),
        },
    }


# ==============================================================================
# 9. CLI SUBCOMMAND ROUTERS (`open`, `incident`, `audit`, `lineage`, `metrics`, `sql`, `decide`)
# ==============================================================================
TAB_TITLES: dict[int, tuple[str, str]] = {
    1: ("dbt Action Center & Runs", "Triage failed builds, filter runs in 0ms, and retry or trigger jobs"),
    2: ("dbt Models, Health & Lineage", "Filter marts, models, sources & exposures in 0ms; deep-dive lineage & SQL"),
    3: ("dbt Semantic Metric Studio", "Browse governed MetricFlow metrics and run custom dimensional slices"),
    4: ("dbt SQL & Macro Workbench", "Insert governed ref/source snippets, generate AI SQL, and run queries"),
}


def _normalize_tab_index(raw_tab: int | None) -> int:
    t = int(raw_tab or 1)
    if t == 5:
        return 4  # Backwards compatibility with legacy 5-tab layout
    if t < 1 or t > 4:
        return 1
    return t


def handle_open_or_deep_dive(args: argparse.Namespace, command_mode: str = "open") -> dict[str, Any]:
    gw = DbtMcpGateway(args.connector, args.endpoint, args.mock_fixtures_json)
    state = load_state()
    state.pop("pending_confirmation", None)
    state.pop("banner", None)
    state["initial_run_filter"] = ""
    state["initial_kind_filter"] = "all"

    req_pid = validate_int_id(getattr(args, "project_id", ""), "project_id", allow_empty=True)
    prev_pid = str(state.get("active_project_id") or "")
    force_refresh = bool(getattr(args, "refresh", False))
    pid, pname, _ = resolve_active_project(gw, state, req_pid, force_refresh=force_refresh)
    if prev_pid and prev_pid != str(pid):
        force_refresh = True

    # Always ensure the workspace prefetch bundle is populated so all 4 tabs work in 0ms!
    hydrate_workspace_bundle(gw, state, project_id=pid, force_refresh=force_refresh)

    if command_mode == "incident":
        state["active_tab"] = 1
        st_filt = str(getattr(args, "status_filter", "") or "").lower()
        if st_filt in ("error", "running", "success", "cancelled"):
            state["initial_run_filter"] = st_filt
        jid = validate_int_id(getattr(args, "job_id", ""), "job_id", allow_empty=True)
        if jid:
            state["selected_job_id"] = str(jid)
        rid = validate_int_id(getattr(args, "run_id", ""), "run_id", allow_empty=True)
        if rid:
            deep_dive_run(gw, state, project_id=pid, run_id=rid, fetch_docs=bool(getattr(args, "with_docs", True)))

    elif command_mode in ("audit", "lineage"):
        state["active_tab"] = 2
        uid = getattr(args, "unique_id", "")
        q = getattr(args, "query", "")
        orient = getattr(args, "orientation", "")
        subtab = getattr(args, "subtab", "") or ("lineage" if command_mode == "lineage" else "overview")
        if uid or q:
            deep_dive_model(gw, state, project_id=pid, unique_id=uid, search_query=q, orientation=orient, subtab=subtab)
        else:
            state["catalog_view_mode"] = "detail" if command_mode == "lineage" else "list"
            state["catalog_subtab"] = subtab
            if orient in ("lr", "td"):
                state["lineage_orientation"] = orient

    elif command_mode == "metrics":
        state["active_tab"] = 3
        met = getattr(args, "metric", "")
        gr = getattr(args, "grain", "")
        gd = getattr(args, "group_dim", "__keep__")
        wd = getattr(args, "where_dim", "")
        wv = getattr(args, "where_val", "")
        if met or wd or wv or (gd and gd != "__keep__") or (gr and gr.upper() != (state.get("metrics_view") or {}).get("grain", "MONTH")):
            deep_dive_metric_slice(
                gw, state, project_id=pid, selected_metric=met, grain=gr, group_dim=gd, where_dim=wd, where_val=wv
            )

    elif command_mode == "sql":
        state["active_tab"] = 4
        nlp = getattr(args, "nl_prompt", "")
        sql_ovr = getattr(args, "sql", "")
        if nlp or sql_ovr:
            deep_dive_generate_sql(gw, state, project_id=pid, nl_prompt=nlp, sql_override=sql_ovr)

    else:
        tab = _normalize_tab_index(getattr(args, "tab", 1))
        state["active_tab"] = tab
        # Optional targeted parameters passed to `open`
        rid = validate_int_id(getattr(args, "run_id", ""), "run_id", allow_empty=True)
        if rid:
            deep_dive_run(gw, state, project_id=pid, run_id=rid, fetch_docs=bool(getattr(args, "with_docs", False)))
        if getattr(args, "unique_id", "") or getattr(args, "query", ""):
            deep_dive_model(
                gw,
                state,
                project_id=pid,
                unique_id=getattr(args, "unique_id", ""),
                search_query=getattr(args, "query", ""),
                orientation=getattr(args, "orientation", ""),
            )
        if getattr(args, "metric", ""):
            deep_dive_metric_slice(
                gw,
                state,
                project_id=pid,
                selected_metric=getattr(args, "metric", ""),
                grain=getattr(args, "grain", ""),
                group_dim=getattr(args, "group_dim", "__keep__"),
            )
        if getattr(args, "nl_prompt", "") or getattr(args, "sql", ""):
            deep_dive_generate_sql(
                gw,
                state,
                project_id=pid,
                nl_prompt=getattr(args, "nl_prompt", ""),
                sql_override=getattr(args, "sql", ""),
            )

    active_t = int(state.get("active_tab") or 1)
    title, desc = TAB_TITLES.get(active_t, ("dbt Cloud Workbench", "Interactive dbt Cloud Canvas"))
    return stage_a2ui_canvas(state, card_title="dbt Cloud Workbench", card_description=f"{pname} · {desc}")


def handle_decide_command(args: argparse.Namespace) -> dict[str, Any]:
    gw = DbtMcpGateway(args.connector, args.endpoint, args.mock_fixtures_json)
    state = load_state()
    req_pid = validate_int_id(args.project_id, "project_id", allow_empty=True)
    pid, pname, _ = resolve_active_project(gw, state, req_pid)
    hydrate_workspace_bundle(gw, state, project_id=pid, force_refresh=False)

    op = (args.operation or args.action or "").strip().lower()
    if op == "safe_retry":
        op = "retry_run"

    if not args.execute:
        rows = [{"label": "Operation", "value": op}]
        if args.run_id:
            rows.append({"label": "Run ID", "value": f"#{args.run_id}"})
        if args.job_id:
            rows.append({"label": "Job ID", "value": f"#{args.job_id}"})
        if args.steps_override:
            rows.append({"label": "Steps Override", "value": args.steps_override})
        if args.sql:
            rows.append({"label": "SQL", "value": args.sql[:240]})
        state["active_tab"] = 4 if op == "execute_sql" else 1
        state["pending_confirmation"] = {
            "title": f"Confirm dbt Cloud Action: {op}",
            "warning": "Staged from Chat — review details below and click Confirm & Execute.",
            "rows": rows,
            "payload": {
                "operation": op,
                "run_id": args.run_id,
                "job_id": args.job_id,
                "steps_override": args.steps_override,
                "git_branch": args.git_branch,
                "schema_override": args.schema_override,
                "cause": args.cause,
                "sql": args.sql,
                "confirmed_in_gui": True,
                "execute": True,
            },
        }
        return stage_a2ui_canvas(
            state,
            card_title="dbt Cloud Workbench",
            card_description=f"Confirm staged action ({op}) in {pname}",
        )

    state.pop("pending_confirmation", None)

    def _refresh_runs_after_mutation(focus_rid: str = "", focus_jid: str = "") -> None:
        raw_runs, _, _ = gw.call_soft("list_jobs_runs", {"order_by": "-created_at", "limit": 40})
        jobs = state.get("jobs") or []
        job_name_by_id = {str(j["id"]): str(j["name"]) for j in jobs if isinstance(j, dict) and "id" in j}
        norm_runs: list[dict[str, Any]] = []
        for r in extract_items(raw_runs, ("runs", "data", "items"))[:40]:
            rid_v = r.get("id") or r.get("run_id")
            try:
                rid_i = int(rid_v)
            except (TypeError, ValueError):
                continue
            r_jid = str(r.get("job_definition_id") or r.get("job_id") or "")
            st_human = str(r.get("status_humanized") or r.get("status") or "unknown").lower()
            norm_runs.append({
                "id": str(rid_i),
                "job_id": r_jid,
                "job_name": job_name_by_id.get(r_jid) or str((r.get("job") or {}).get("name") or f"Job {r_jid or 'Run'}"),
                "status": st_human,
                "status_bucket": _normalize_run_status_bucket(st_human),
                "duration": str(r.get("duration_humanized") or r.get("duration") or ""),
                "created_at": str(r.get("created_at") or r.get("finished_at") or "")[:19],
                "git_branch": str(r.get("git_branch") or ""),
                "trigger_cause": mask_pii(
                    str(r.get("trigger", {}).get("cause") if isinstance(r.get("trigger"), dict) else (r.get("cause") or ""))[:90]
                ),
            })
        if norm_runs:
            state["runs"] = norm_runs
        if focus_rid:
            state["selected_run_id"] = focus_rid
        elif norm_runs:
            state["selected_run_id"] = norm_runs[0]["id"]
        if focus_jid:
            state["selected_job_id"] = focus_jid
        state["last_synced"] = _now_utc_short()

    if op == "retry_run":
        rid = validate_int_id(args.run_id, "run_id", allow_empty=False)
        assert rid is not None
        resp, _ = gw.call("retry_job_run", {"run_id": rid})
        new_rid = str((resp.get("data") or resp).get("id") or "") if isinstance(resp, dict) else ""
        state["active_tab"] = 1
        _refresh_runs_after_mutation(focus_rid=new_rid or str(rid))
        state["banner"] = {
            "kind": "ok",
            "title": f"Safe Retry Queued for Run #{rid}" + (f" → New Run #{new_rid}" if new_rid else ""),
            "detail": "dbt Cloud queued a retry build resuming from the point of failure.",
        }
        return stage_a2ui_canvas(state, card_title="dbt Cloud Workbench", card_description=state["banner"]["title"])

    if op == "cancel_run":
        rid = validate_int_id(args.run_id, "run_id", allow_empty=False)
        assert rid is not None
        gw.call("cancel_job_run", {"run_id": rid})
        state["active_tab"] = 1
        _refresh_runs_after_mutation(focus_rid=str(rid))
        state["banner"] = {
            "kind": "warn",
            "title": f"Cancelled Run #{rid}",
            "detail": "Cancellation signal sent to dbt Cloud.",
        }
        return stage_a2ui_canvas(state, card_title="dbt Cloud Workbench", card_description=state["banner"]["title"])

    if op == "trigger_job":
        jid = validate_int_id(args.job_id, "job_id", allow_empty=False)
        assert jid is not None
        t_args: dict[str, Any] = {
            "job_id": jid,
            "cause": (args.cause or "Triggered via Gemini Enterprise A2UI Workbench")[:160],
        }
        if args.git_branch:
            t_args["git_branch"] = re.sub(r"[^A-Za-z0-9_./-]", "", args.git_branch)[:100]
        if args.schema_override:
            t_args["schema_override"] = re.sub(r"[^A-Za-z0-9_]", "", args.schema_override)[:80]
        if args.steps_override:
            steps = [s.strip() for s in args.steps_override.split("&&") if s.strip()]
            if steps:
                t_args["steps_override"] = steps[:6]
        resp, _ = gw.call("trigger_job_run", t_args)
        new_rid = str((resp.get("data") or resp).get("id") or "") if isinstance(resp, dict) else ""
        state["active_tab"] = 1
        _refresh_runs_after_mutation(focus_rid=new_rid, focus_jid=str(jid))
        state["banner"] = {
            "kind": "ok",
            "title": f"Triggered dbt Job #{jid}" + (f" (Run #{new_rid})" if new_rid else ""),
            "detail": f"Steps: {args.steps_override}" if args.steps_override else "Queued with default job build steps.",
        }
        return stage_a2ui_canvas(state, card_title="dbt Cloud Workbench", card_description=state["banner"]["title"])

    if op == "execute_sql":
        sql_clean = (args.sql or "").strip()
        if not sql_clean:
            raise CliError("SQL query cannot be empty.")
        raw_exec, txt_exec = gw.call("execute_sql", {"project_id": pid, "sql": sql_clean[:4000]})
        rows = extract_items(raw_exec, ("rows", "data", "items", "results"))
        state["active_tab"] = 4
        sv = state.setdefault("sql_view", {})
        sv["generated_sql"] = sql_clean[:3500]
        sv["query_rows"] = rows[:50]
        sv["query_text"] = txt_exec[:2500]
        state["banner"] = {
            "kind": "ok",
            "title": "Warehouse SQL Executed",
            "detail": f"Returned {len(rows)} rows from {pname}." if rows else "Query completed on warehouse.",
        }
        return stage_a2ui_canvas(state, card_title="dbt Cloud Workbench", card_description=state["banner"]["detail"])

    raise CliError(f"Unsupported operation '{op}'.")


# ==============================================================================
# 10. ARGUMENT PARSER & ZERO-DROP ENTRY POINT
# ==============================================================================
def _make_sub_common() -> argparse.ArgumentParser:
    sc = argparse.ArgumentParser(add_help=False)
    sc.add_argument("--connector", default=argparse.SUPPRESS, help="dbt MCP dataConnector resource")
    sc.add_argument("--endpoint", default=argparse.SUPPRESS, help="dbt MCP HTTPS endpoint URL")
    sc.add_argument("--project-id", default=argparse.SUPPRESS, help="Optional numeric dbt Cloud project_id")
    sc.add_argument("--refresh", action="store_true", default=argparse.SUPPRESS, help="Force full workspace re-sync")
    sc.add_argument("--mock-fixtures-json", default=argparse.SUPPRESS, help="Internal offline smoke-test fixture path")
    return sc


def build_parser() -> argparse.ArgumentParser:
    top_common = argparse.ArgumentParser(add_help=False)
    top_common.add_argument("--connector", default="", help="dbt MCP dataConnector resource (persisted once passed)")
    top_common.add_argument("--endpoint", default="", help="dbt MCP HTTPS endpoint URL (persisted once passed)")
    top_common.add_argument("--project-id", default="", help="Optional numeric dbt Cloud project_id")
    top_common.add_argument("--refresh", action="store_true", help="Force full parallel workspace re-sync")
    top_common.add_argument("--mock-fixtures-json", default="", help="Internal offline smoke-test fixture path")

    sub_common = _make_sub_common()
    parser = argparse.ArgumentParser(description="Prefetch-First dbt Cloud A2UI Canvas CLI", parents=[top_common])
    sub = parser.add_subparsers(dest="subcommand")

    p_open = sub.add_parser("open", parents=[sub_common], help="Prefetch workspace bundle & open dbt Workbench (tab 1-4)")
    p_open.add_argument("--tab", type=int, default=1)
    p_open.add_argument("--metric", default="")
    p_open.add_argument("--grain", default="")
    p_open.add_argument("--group-dim", default="__keep__")
    p_open.add_argument("--unique-id", default="")
    p_open.add_argument("--query", default="")
    p_open.add_argument("--orientation", default="")
    p_open.add_argument("--job-id", default="")
    p_open.add_argument("--run-id", default="")
    p_open.add_argument("--status-filter", default="")
    p_open.add_argument("--with-docs", action="store_true")
    p_open.add_argument("--nl-prompt", default="")
    p_open.add_argument("--sql", default="")

    p_inc = sub.add_parser("incident", parents=[sub_common], help="Tab 1 Deep Dive: Job Run logs, artifacts & dbt Docs")
    p_inc.add_argument("--job-id", default="")
    p_inc.add_argument("--run-id", default="")
    p_inc.add_argument("--status-filter", default="")
    p_inc.add_argument("--with-docs", action="store_true")

    p_aud = sub.add_parser("audit", parents=[sub_common], help="Tab 2 Deep Dive: Model health, sources & performance")
    p_aud.add_argument("--unique-id", default="")
    p_aud.add_argument("--subtab", default="overview")
    p_aud.add_argument("--scenario", default="")

    p_lin = sub.add_parser("lineage", parents=[sub_common], help="Tab 2 Deep Dive: Model lineage, columns & SQL")
    p_lin.add_argument("--unique-id", default="")
    p_lin.add_argument("--query", default="")
    p_lin.add_argument("--subtab", default="lineage")
    p_lin.add_argument("--orientation", default="lr", choices=["lr", "td"])

    p_met = sub.add_parser("metrics", parents=[sub_common], help="Tab 3 Deep Dive: Custom Semantic Layer metric slice")
    p_met.add_argument("--metric", default="")
    p_met.add_argument("--grain", default="MONTH")
    p_met.add_argument("--group-dim", default="")
    p_met.add_argument("--where-dim", default="")
    p_met.add_argument("--where-val", default="")

    p_sql = sub.add_parser("sql", parents=[sub_common], help="Tab 4 Deep Dive: AI NL-to-dbt-SQL generation")
    p_sql.add_argument("--nl-prompt", default="")
    p_sql.add_argument("--sql", default="")
    p_sql.add_argument("--unique-id", default="")

    p_dec = sub.add_parser("decide", parents=[sub_common], help="Stage or execute mutating dbt action")
    p_dec.add_argument("--operation", default="")
    p_dec.add_argument("--action", default="")
    p_dec.add_argument("--run-id", default="")
    p_dec.add_argument("--job-id", default="")
    p_dec.add_argument("--steps-override", default="")
    p_dec.add_argument("--git-branch", default="")
    p_dec.add_argument("--schema-override", default="")
    p_dec.add_argument("--cause", default="")
    p_dec.add_argument("--sql", default="")
    p_dec.add_argument("--execute", action="store_true")

    return parser


def main() -> int:
    parser = build_parser()
    args, _ = parser.parse_known_args()
    if getattr(args, "connector", "") or getattr(args, "endpoint", ""):
        discover_dbt_mcp_connection(args.connector, args.endpoint)

    try:
        cmd = args.subcommand or "open"
        if cmd in ("open", "incident", "audit", "lineage", "metrics", "sql"):
            res = handle_open_or_deep_dive(args, command_mode=cmd)
        elif cmd == "decide":
            res = handle_decide_command(args)
        else:
            res = handle_open_or_deep_dive(args, command_mode="open")
        print(json.dumps(res, indent=2))
        return 0
    except Exception as exc:
        fallback_state = load_state()
        fallback_state.setdefault("active_tab", 1)
        fallback_state["banner"] = {
            "kind": "warn",
            "title": "dbt Cloud MCP Connection / Setup Notice",
            "detail": mask_pii(str(exc))[:420],
        }
        info = stage_a2ui_canvas(
            fallback_state,
            card_title="dbt Cloud Workbench",
            card_description="Setup or connection notice — open Canvas for details",
        )
        print(json.dumps({"status": "warning", "error": mask_pii(str(exc)), **info}, indent=2))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
