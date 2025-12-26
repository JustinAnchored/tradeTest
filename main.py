import asyncio
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment, DictLoader, select_autoescape
from supabase import create_client


# =============================================================================
# CONFIG (edit these at the top)
# =============================================================================

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://yqxfrwxqytmjjkyezdvi.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxeGZyd3hxeXRtampreWV6ZHZpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Mjc1OTE3NzksImV4cCI6MjA0MzE2Nzc3OX0.AaAZKtvXvAyfLCpLOOIaRKO8KntH70p9J0KJo3jBDpU")

# WMS / Fulcrum API
FULCRUM_HOST = os.getenv("FULCRUM_HOST", "rootsfulfillment.shoppingcartfulfillment.com")
FULCRUM_AUTH = os.getenv("FULCRUM_AUTH", "Basic am1vbm5pZzpBbmNob3JlZDEyMzgh")  # e.g. "Bearer xxx" or "xxxxx" as required


# IMPORTANT: This is what replaces your old Authorization header.
# Use your browser cookie string from curl -b '...'
# Example: "ace.settings=...; dstToken=...; dstCloudComputer=..."
FULCRUM_COOKIE = os.getenv("FULCRUM_COOKIE", "ace.settings=%7B%22navbar-fixed%22%3A1%2C%22sidebar-fixed%22%3A1%2C%22sidebar-collapsed%22%3A-1%7D; dstIntegratedLabelType=1; dstPrintOrderChecked=1; dstCloudComputer=641378; dstLabelPrinterFormat_74392833=PDF; dstLabelPrinterFormat_74451316=ZPL; ajs_anonymous_id=%225c037d72-7ca4-48f8-98ac-01245b4f9414%22; dstLabelPrinter=74392833; dstLabelPrinterFormat=PDF; dstIntegratedLabelPrinter=74392833; dstPackingListPrinter=74392833; dstPickTicketPrinter=74392833; dstToken=8a22ce4a68e3a9369132cfd89fa4a3ff")

# Owners to iterate (dstOwnerList)
OWNER_IDS = ["33423","27","98580","31","566813","59921","641632","524862","1004425","295644","309210","581507","609057","150705","296540","296517","296546","288234","488296","513406","447199","214228","437199","106234","279259","28","296426","132850","638486","518270","181418","223858","51","32","29","260886","489011","46495","33524","10","181324","56","265694","258737","363440","617057","84524","460759","91040","513422","451243","9","55","517974","225472","260976","623746","66652","50471","167781"]


# Pagination
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))  # length=100
SLEEP_BETWEEN_PAGES_SEC = float(os.getenv("SLEEP_BETWEEN_PAGES_SEC", "0.15"))

# Reliability
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))
RETRY_BASE_SLEEP_SEC = float(os.getenv("RETRY_BASE_SLEEP_SEC", "1.0"))

# Start behavior
START_ON_STARTUP = os.getenv("START_ON_STARTUP", "false").lower() == "true"

# Supabase tables
TBL_CONTACTS = "wms_contacts"
TBL_STATE = "wms_job_state"

# =============================================================================


STATUS_TEMPLATE = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <title>WMS Contacts Import Status</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; }
      .card { border: 1px solid #ddd; border-radius: 10px; padding: 16px; margin-bottom: 16px; }
      .row { display: flex; gap: 16px; flex-wrap: wrap; }
      .pill { display:inline-block; padding: 4px 10px; border-radius: 999px; background:#f3f3f3; border:1px solid #e5e5e5; }
      .good { background:#eaffea; border-color:#b7e7b7; }
      .bad { background:#ffecec; border-color:#f0b5b5; }
      .muted { color: #666; }
      code { background:#f6f6f6; padding:2px 6px; border-radius:6px; }
      input { padding: 6px 10px; border:1px solid #ccc; border-radius:8px; }
      button { padding: 8px 12px; border:1px solid #ccc; border-radius:10px; cursor:pointer; background:#fafafa; }
      button:hover { background:#f2f2f2; }
      .actions form { display:inline-block; margin-right: 8px; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
    </style>
  </head>
  <body>
    <h2>WMS Contacts Import Status</h2>

    <div class="card">
      <div class="row">
        <div><span class="muted">Worker task:</span>
          {% if worker_running %}
            <span class="pill good">RUNNING</span>
          {% else %}
            <span class="pill bad">NOT RUNNING</span>
          {% endif %}
        </div>

        <div><span class="muted">DB status:</span>
          {% if state.status == 'running' %}
            <span class="pill good">{{ state.status }}</span>
          {% elif state.status == 'idle' %}
            <span class="pill">{{ state.status }}</span>
          {% else %}
            <span class="pill bad">{{ state.status }}</span>
          {% endif %}
        </div>

        <div><span class="muted">Heartbeat:</span>
          {% if heartbeat_stale %}
            <span class="pill bad">{{ state.last_heartbeat_at or 'null' }}</span>
          {% else %}
            <span class="pill good">{{ state.last_heartbeat_at or 'null' }}</span>
          {% endif %}
        </div>
      </div>

      <p class="muted" style="margin-top:10px;">
        Owner cursor: <code>{{ state.cursor_owner or 'null' }}</code>
        Start offset: <code>{{ state.cursor_start or 0 }}</code>
        Page size: <code>{{ page_size }}</code>
      </p>

      <p class="muted">
        Processed: <span class="mono">{{ state.pages_processed }}</span> pages,
        <span class="mono">{{ state.orders_processed }}</span> rows
      </p>

      <p class="muted">Last message: <span class="mono">{{ state.last_message or '' }}</span></p>
      {% if state.last_error %}
        <p><b>Error:</b> <span class="mono">{{ state.last_error }}</span></p>
      {% endif %}
    </div>

    <div class="card actions">
      <h3>Controls</h3>

      <form method="get" action="/start">
        <button type="submit">Start / Resume</button>
      </form>

      <form method="get" action="/stop">
        <button type="submit">Stop</button>
      </form>

      <form method="get" action="/tick">
        <button type="submit">Tick (process 1 page)</button>
      </form>

      <form method="get" action="/reset">
        <button type="submit">Reset cursor (owner 1, start=0)</button>
      </form>
    </div>

    <div class="card">
      <h3>Owner list</h3>
      <p class="mono">{{ owners }}</p>
      <p class="muted">Importer runs owner-by-owner, paging using start/length until it gets fewer than length rows.</p>
    </div>
  </body>
</html>
""".strip()


jinja_env = Environment(
    loader=DictLoader({"status.html": STATUS_TEMPLATE}),
    autoescape=select_autoescape(["html", "xml"]),
)

app = FastAPI()
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

_worker_task: Optional[asyncio.Task] = None
_worker_lock = asyncio.Lock()


# =============================================================================
# Helpers
# =============================================================================

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

EMAIL_RE = re.compile(r"mailto:([^\"'>\s]+)", re.IGNORECASE)
ID_RE = re.compile(r"[?&]id=(\d+)", re.IGNORECASE)

def parse_email(html: str) -> Optional[str]:
    if not html:
        return None
    m = EMAIL_RE.search(html)
    if not m:
        return None
    return m.group(1).strip().lower()

def parse_company_id(html: str) -> Optional[int]:
    if not html:
        return None
    m = ID_RE.search(html)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


async def sb_get_state() -> Dict[str, Any]:
    resp = supabase.table(TBL_STATE).select("*").eq("id", 1).execute()
    if resp.data and len(resp.data) > 0:
        return resp.data[0]

    # Ensure row exists
    supabase.table(TBL_STATE).upsert({"id": 1, "status": "idle", "cursor_start": 0}).execute()
    resp2 = supabase.table(TBL_STATE).select("*").eq("id", 1).execute()
    return resp2.data[0]


async def sb_update_state(patch: Dict[str, Any]) -> None:
    patch = dict(patch)
    patch["updated_at"] = now_utc_iso()
    try:
        supabase.table(TBL_STATE).update(patch).eq("id", 1).execute()
    except Exception as e:
        print("Failed to update state:", e)


def datatables_base_params() -> Dict[str, str]:
    """
    Minimal-ish DataTables params. Your server may want many of these;
    this set is typically enough for dstContactSearch.
    """
    params = {
        "dstObjectType": "dstContactSearch",
        "cmd": "Search",
        "dstJson": "1",
        "dstSearchCriteria": "",
        "dstSearchSel": "1",
        "dstContactTypeList": "2",
        "dstActive": "1",
        "draw": "1",
        "dstSearch": "",
        "dstSearchSel": "1",
        "dstSearchCriteria": "",
        "search[value]": "",
        "search[regex]": "false",
        "order[0][column]": "2",
        "order[0][dir]": "asc",
    }

    # Columns 0..11 (like your curl)
    for i in range(12):
        params[f"columns[{i}][data]"] = str(i)
        params[f"columns[{i}][name]"] = ""
        params[f"columns[{i}][searchable]"] = "true"
        params[f"columns[{i}][orderable]"] = "true" if i != 0 else "false"
        params[f"columns[{i}][search][value]"] = ""
        params[f"columns[{i}][search][regex]"] = "false"

    return params


async def http_fetch_contacts(owner_id: str, start: int, length: int) -> Dict[str, Any]:
    url = f"{FULCRUM_BASE}{FULCRUM_PATH}"

    params = datatables_base_params()
    params["dstOwnerList"] = owner_id
    params["start"] = str(start)
    params["length"] = str(length)

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{FULCRUM_BASE}/fulcrum/dstContainer.php?dstObjectType=dstContactSearch&dstContactTypeList=2",
        "User-Agent": "Mozilla/5.0",
        "Connection": "keep-alive",
        "Cookie": FULCRUM_COOKIE,
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            timeout = httpx.Timeout(HTTP_TIMEOUT_SEC)
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.get(url, params=params, headers=headers)
            if r.status_code == 429:
                await asyncio.sleep(min(RETRY_BASE_SLEEP_SEC * (2 ** (attempt - 1)), 60))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            await asyncio.sleep(min(RETRY_BASE_SLEEP_SEC * (2 ** (attempt - 1)), 60))

    raise RuntimeError(f"Fetch failed owner={owner_id} start={start} length={length}: {last_exc}")


def row_to_contact(owner_id: str, row: List[Any]) -> Dict[str, Any]:
    """
    Data is like: payload["data"] = [ [col0, col1, ... col11], ... ]
    We'll extract:
      - company_id from col0 (Select link)
      - company_name from col1
      - contact_type from col2
      - contact_name from col4
      - email from col10 (mailto)
    Store the full row array into raw_columns.
    """
    col0 = _safe_text(row[0]) if len(row) > 0 else None
    col1 = _safe_text(row[1]) if len(row) > 1 else None
    col2 = _safe_text(row[2]) if len(row) > 2 else None
    col4 = _safe_text(row[4]) if len(row) > 4 else None
    col10 = _safe_text(row[10]) if len(row) > 10 else None

    company_id = parse_company_id(col0 or "")
    email = parse_email(col10 or "")

    # Primary key strategy:
    # Prefer owner|company_id (stable)
    # Fallback to owner|email if company_id missing
    # Fallback to owner|hash-like timestamp if both missing (rare)
    if company_id is not None:
        contact_uid = f"{owner_id}|{company_id}"
    elif email:
        contact_uid = f"{owner_id}|{email}"
    else:
        contact_uid = f"{owner_id}|unknown|{int(datetime.now().timestamp()*1000)}"

    return {
        "contact_uid": contact_uid,
        "owner_id": owner_id,
        "company_id": company_id,
        "company_name": col1,
        "contact_type": col2,
        "contact_name": col4,
        "email": email,
        "raw_columns": row,
        "ingested_at": now_utc_iso(),
    }


async def upsert_contacts(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    supabase.table(TBL_CONTACTS).upsert(rows, on_conflict="contact_uid").execute()


def next_owner(current_owner: Optional[str]) -> Optional[str]:
    if not OWNER_IDS:
        return None
    if not current_owner:
        return OWNER_IDS[0]
    try:
        idx = OWNER_IDS.index(current_owner)
    except ValueError:
        return OWNER_IDS[0]
    nxt = idx + 1
    if nxt >= len(OWNER_IDS):
        return None
    return OWNER_IDS[nxt]


async def process_one_page(state: Dict[str, Any]) -> Dict[str, Any]:
    owner = state.get("cursor_owner") or (OWNER_IDS[0] if OWNER_IDS else None)
    if not owner:
        raise RuntimeError("OWNER_IDS is empty. Set OWNER_IDS env var or edit OWNER_IDS in main.py")

    start = int(state.get("cursor_start") or 0)
    length = PAGE_SIZE

    await sb_update_state({
        "status": "running",
        "last_heartbeat_at": now_utc_iso(),
        "last_message": f"Fetching owner={owner} start={start} length={length}",
        "last_error": None,
    })

    payload = await http_fetch_contacts(owner, start, length)
    data = payload.get("data") or []

    # Convert + upsert
    contacts = [row_to_contact(owner, r) for r in data]
    await upsert_contacts(contacts)

    pages_processed = int(state.get("pages_processed") or 0) + 1
    rows_processed = int(state.get("orders_processed") or 0) + len(contacts)

    # Decide next cursor
    if len(data) < length:
        # Owner finished -> move to next owner, reset start
        nxt = next_owner(owner)
        if nxt is None:
            await sb_update_state({
                "status": "idle",
                "cursor_owner": owner,
                "cursor_start": start,  # last start
                "pages_processed": pages_processed,
                "orders_processed": rows_processed,
                "last_heartbeat_at": now_utc_iso(),
                "last_message": f"Completed all owners. Last owner={owner} returned {len(data)} (<{length}).",
            })
            return {"done": True, "message": "Completed all owners"}

        await sb_update_state({
            "cursor_owner": nxt,
            "cursor_start": 0,
            "pages_processed": pages_processed,
            "orders_processed": rows_processed,
            "last_heartbeat_at": now_utc_iso(),
            "last_message": f"Owner {owner} finished (returned {len(data)}). Moving to owner={nxt} start=0.",
        })
        return {"done": False, "owner_finished": True, "owner": owner, "next_owner": nxt, "returned": len(data)}

    # More pages for this owner
    new_start = start + len(data)  # safest (works even if server returns <length sometimes)
    await sb_update_state({
        "cursor_owner": owner,
        "cursor_start": new_start,
        "pages_processed": pages_processed,
        "orders_processed": rows_processed,
        "last_heartbeat_at": now_utc_iso(),
        "last_message": f"Upserted {len(data)} rows for owner={owner}. Next start={new_start}.",
    })
    return {"done": False, "owner": owner, "start": start, "returned": len(data), "next_start": new_start}


async def worker_loop() -> None:
    while True:
        state = await sb_get_state()
        status = (state.get("status") or "idle").lower()

        if status == "stopping":
            await sb_update_state({
                "status": "idle",
                "last_message": "Stopped by user",
                "last_heartbeat_at": now_utc_iso(),
            })
            return

        if status != "running":
            return

        try:
            result = await process_one_page(state)
            if result.get("done"):
                return
        except Exception as e:
            await sb_update_state({
                "status": "error",
                "last_error": str(e),
                "last_message": "Worker stopped due to error",
                "last_heartbeat_at": now_utc_iso(),
            })
            return

        await asyncio.sleep(SLEEP_BETWEEN_PAGES_SEC)


async def ensure_worker_running() -> None:
    global _worker_task
    async with _worker_lock:
        if _worker_task and not _worker_task.done():
            return
        _worker_task = asyncio.create_task(worker_loop())


# =============================================================================
# Routes
# =============================================================================

@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    state = await sb_get_state()

    worker_running = _worker_task is not None and not _worker_task.done()

    hb = state.get("last_heartbeat_at")
    heartbeat_stale = False
    if hb and state.get("status") == "running":
        try:
            hb_dt = datetime.fromisoformat(hb.replace("Z", "+00:00"))
            heartbeat_stale = (datetime.now(timezone.utc) - hb_dt).total_seconds() > 300
        except Exception:
            heartbeat_stale = False

    html = jinja_env.get_template("status.html").render(
        request=request,
        state=state,
        worker_running=worker_running,
        heartbeat_stale=heartbeat_stale,
        owners=OWNER_IDS,
        page_size=PAGE_SIZE,
    )
    return HTMLResponse(html)


@app.get("/start")
async def start():
    state = await sb_get_state()

    # If no cursor_owner set, initialize to first owner
    cursor_owner = state.get("cursor_owner") or (OWNER_IDS[0] if OWNER_IDS else None)
    if not cursor_owner:
        return JSONResponse({"ok": False, "error": "OWNER_IDS is empty"}, status_code=400)

    # Keep existing cursor_start if present, else 0
    cursor_start = int(state.get("cursor_start") or 0)

    await sb_update_state({
        "status": "running",
        "cursor_owner": cursor_owner,
        "cursor_start": cursor_start,
        "last_error": None,
        "last_message": f"Started/resumed at owner={cursor_owner} start={cursor_start}",
        "last_heartbeat_at": now_utc_iso(),
    })

    await ensure_worker_running()
    return {"ok": True, "message": "Worker started", "cursor_owner": cursor_owner, "cursor_start": cursor_start}


@app.get("/stop")
async def stop():
    await sb_update_state({
        "status": "stopping",
        "last_message": "Stop requested",
        "last_heartbeat_at": now_utc_iso(),
    })
    return {"ok": True, "message": "Stop requested"}


@app.get("/reset")
async def reset():
    if not OWNER_IDS:
        return JSONResponse({"ok": False, "error": "OWNER_IDS is empty"}, status_code=400)

    await sb_update_state({
        "status": "idle",
        "cursor_owner": OWNER_IDS[0],
        "cursor_start": 0,
        "pages_processed": 0,
        "orders_processed": 0,
        "last_error": None,
        "last_message": f"Reset cursor to owner={OWNER_IDS[0]} start=0",
        "last_heartbeat_at": now_utc_iso(),
    })
    return {"ok": True, "message": "Reset complete", "cursor_owner": OWNER_IDS[0], "cursor_start": 0}


@app.get("/tick")
async def tick():
    """
    Process exactly one page (no background loop needed).
    """
    state = await sb_get_state()

    # Ensure initialized
    if not state.get("cursor_owner"):
        if not OWNER_IDS:
            return JSONResponse({"ok": False, "error": "OWNER_IDS is empty"}, status_code=400)
        await sb_update_state({"cursor_owner": OWNER_IDS[0], "cursor_start": 0})

    # Temporarily set running so UI is consistent
    if (state.get("status") or "").lower() != "running":
        await sb_update_state({"status": "running", "last_message": "Tick invoked; setting status=running"})

    state = await sb_get_state()

    try:
        result = await process_one_page(state)
        return {"ok": True, "result": result}
    except Exception as e:
        await sb_update_state({
            "status": "error",
            "last_error": str(e),
            "last_message": "Tick failed",
            "last_heartbeat_at": now_utc_iso(),
        })
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.on_event("startup")
async def on_startup():
    if not START_ON_STARTUP:
        return
    state = await sb_get_state()
    if (state.get("status") or "").lower() == "running":
        await ensure_worker_running()
