import asyncio
import os
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment, DictLoader, select_autoescape

# Supabase (supabase-py)
from supabase import create_client


# =============================================================================
# CONFIG (edit these at the top)
# =============================================================================

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://yqxfrwxqytmjjkyezdvi.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxeGZyd3hxeXRtampreWV6ZHZpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Mjc1OTE3NzksImV4cCI6MjA0MzE2Nzc3OX0.AaAZKtvXvAyfLCpLOOIaRKO8KntH70p9J0KJo3jBDpU")

# WMS / Fulcrum API
FULCRUM_HOST = os.getenv("FULCRUM_HOST", "rootsfulfillment.shoppingcartfulfillment.com")
FULCRUM_AUTH = os.getenv("FULCRUM_AUTH", "Basic am1vbm5pZzpBbmNob3JlZDEyMzgh")  # e.g. "Bearer xxx" or "xxxxx" as required

# Import behavior
START_ON_STARTUP = os.getenv("START_ON_STARTUP", "true").lower() == "true"

# Throttling / reliability
SLEEP_BETWEEN_PAGES_SEC = float(os.getenv("SLEEP_BETWEEN_PAGES_SEC", "0.15"))
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))
RETRY_BASE_SLEEP_SEC = float(os.getenv("RETRY_BASE_SLEEP_SEC", "1.0"))

# Batch sizes
UPSERT_ORDERS_BATCH_SIZE = int(os.getenv("UPSERT_ORDERS_BATCH_SIZE", "250"))

# Table names (match SQL above)
TBL_ORDERS = "wms_orders"
TBL_ROLLUP = "wms_customer_rollup"
TBL_STATE = "wms_job_state"

# =============================================================================


STATUS_TEMPLATE = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <title>WMS Import Status</title>
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
    <h2>WMS Import Status</h2>

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
        Cursor: <code>{{ state.cursor_date or 'null' }}</code> page <code>{{ state.cursor_page }}</code>
        &nbsp;|&nbsp; Range: <code>{{ state.run_start_date or 'null' }}</code> → <code>{{ state.run_end_date or 'null' }}</code>
      </p>

      <p class="muted">
        Processed: <span class="mono">{{ state.pages_processed }}</span> pages,
        <span class="mono">{{ state.orders_processed }}</span> orders
      </p>

      <p class="muted">Last message: <span class="mono">{{ state.last_message or '' }}</span></p>
      {% if state.last_error %}
        <p><b>Error:</b> <span class="mono">{{ state.last_error }}</span></p>
      {% endif %}
    </div>

    <div class="card actions">
      <h3>Controls</h3>

      <form method="get" action="/start">
        <label>start_date (YYYY-MM-DD):</label>
        <input name="start_date" value="{{ default_start }}">
        <label>end_date (YYYY-MM-DD):</label>
        <input name="end_date" value="{{ default_end }}">
        <button type="submit">Start / Resume</button>
      </form>

      <form method="get" action="/stop">
        <button type="submit">Stop</button>
      </form>

      <form method="get" action="/tick">
        <button type="submit">Tick (process 1 page)</button>
      </form>

      <form method="get" action="/reset">
        <button type="submit">Reset cursor to start_date</button>
      </form>
    </div>

    <div class="card">
      <h3>Tips</h3>
      <ul>
        <li>If heartbeat stops updating while status is <code>running</code>, the importer is stuck or dead.</li>
        <li>Use <code>/start</code> to resume; it reads checkpoint from <code>wms_job_state</code>.</li>
        <li>For DigitalOcean, run a single uvicorn worker (avoid multiple background importers).</li>
      </ul>
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

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _fmt_fulcrum_date(d: date) -> str:
    # API expects MM/DD/YYYY
    return d.strftime("%m/%d/%Y")


def _safe_lower_email(email: Optional[str]) -> Optional[str]:
    if not email:
        return None
    return email.strip().lower() or None


def _make_customer_uid(owner: str, email: Optional[str]) -> Optional[str]:
    email_l = _safe_lower_email(email)
    if not email_l:
        return None
    return f"{owner.strip()}|{email_l}"


def _make_order_uid(owner: str, sales_order_id: str) -> str:
    return f"{owner.strip()}|{str(sales_order_id).strip()}"


def _parse_last_updated_on(s: Optional[str]) -> Tuple[Optional[datetime], Optional[str]]:
    """
    Fulcrum sample: "2025-12-24 03:22:50"
    Timezone is not specified. We store the raw string too.
    We interpret as UTC to keep timestamptz consistent.
    """
    if not s:
        return None, None
    raw = s
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt, raw
    except Exception:
        return None, raw


def _chunked(lst: List[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


async def _supabase_get_state() -> Dict[str, Any]:
    try:
        resp = supabase.table(TBL_STATE).select("*").eq("id", 1).execute()
        if resp.data and len(resp.data) > 0:
            return resp.data[0]
    except Exception:
        pass

    # Ensure it exists
    supabase.table(TBL_STATE).upsert({"id": 1, "status": "idle"}).execute()
    resp2 = supabase.table(TBL_STATE).select("*").eq("id", 1).execute()
    return resp2.data[0]


async def _supabase_update_state(patch: Dict[str, Any]) -> None:
    patch = dict(patch)
    patch["updated_at"] = _now_utc().isoformat()
    try:
        supabase.table(TBL_STATE).update(patch).eq("id", 1).execute()
    except Exception as e:
        # Last resort: don't crash the worker due to status update
        print("Failed to update state:", e)


async def _http_get_salesorders(day: date, page: int) -> Dict[str, Any]:
    """
    Fetch one page for a single day window: from_date = day, to_date = day + 1
    """
    from_date = _fmt_fulcrum_date(day)
    to_date = _fmt_fulcrum_date(day + timedelta(days=1))

    url = f"https://{FULCRUM_HOST}/api.v2/salesorder/"
    headers = {"Content-Type": "application/json", "Authorization": FULCRUM_AUTH}
    params = {"from_date": from_date, "to_date": to_date, "page": str(page)}

    last_exc: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            timeout = httpx.Timeout(HTTP_TIMEOUT_SEC)
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.get(url, headers=headers, params=params)
            if r.status_code == 429:
                # Rate limited: backoff
                sleep_s = RETRY_BASE_SLEEP_SEC * (2 ** (attempt - 1))
                await asyncio.sleep(min(sleep_s, 60))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            sleep_s = RETRY_BASE_SLEEP_SEC * (2 ** (attempt - 1))
            await asyncio.sleep(min(sleep_s, 60))

    raise RuntimeError(f"Failed fetching {url} day={day} page={page}: {last_exc}")


def _order_to_row(o: Dict[str, Any]) -> Dict[str, Any]:
    owner = (o.get("owner") or "").strip()
    sales_order_id = str(o.get("sales_order_id") or "").strip()
    order_uid = _make_order_uid(owner, sales_order_id)

    email = o.get("email_address")
    customer_uid = _make_customer_uid(owner, email)

    last_updated_on_dt, last_updated_on_raw = _parse_last_updated_on(o.get("last_updated_on"))

    row = {
        "order_uid": order_uid,
        "owner": owner,
        "sales_order_id": int(sales_order_id) if sales_order_id.isdigit() else None,
        "customer_uid": customer_uid,
        "email": _safe_lower_email(email),
        "customer_name": o.get("customer_name"),
        "phone_number": o.get("phone_number"),

        "order_number": o.get("order_number"),
        "merchant_order_number": o.get("merchant_order_number"),
        "warehouse": o.get("warehouse"),

        "amount_due": o.get("amount_due"),
        "tax": o.get("tax"),
        "shipping": o.get("shipping"),

        "status": o.get("status"),
        "tags": o.get("tags"),

        "shipping_provider": o.get("shipping_provider"),
        "shipping_provider_service": o.get("shipping_provider_service"),

        "last_updated_on": last_updated_on_dt.isoformat() if last_updated_on_dt else None,
        "last_updated_on_raw": last_updated_on_raw,

        "shipping_address_json": o.get("shipping_address"),
        "billing_address_json": o.get("billing_address"),

        # "raw_json" minimized per your request: just line items
        "lineitems_json": o.get("order_details") or [],

        "ingested_at": _now_utc().isoformat(),
    }

    return row


async def _upsert_orders(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    for batch in _chunked(rows, UPSERT_ORDERS_BATCH_SIZE):
        # Upsert by primary key order_uid (idempotent)
        supabase.table(TBL_ORDERS).upsert(batch, on_conflict="order_uid").execute()


async def _refresh_rollups_for_customer_uids(customer_uids: List[str]) -> None:
    if not customer_uids:
        return

    # De-dupe and keep reasonable size
    uids = sorted(set([u for u in customer_uids if u]))

    # Compute rollups in Postgres (idempotent, avoids double counting)
    resp = supabase.rpc("compute_customer_rollups", {"uids": uids}).execute()
    rollup_rows = resp.data or []

    # Upsert rollups
    # Keep updated_at current
    now_iso = _now_utc().isoformat()
    for r in rollup_rows:
        r["updated_at"] = now_iso

    if rollup_rows:
        supabase.table(TBL_ROLLUP).upsert(rollup_rows, on_conflict="customer_uid").execute()


async def _process_one_page(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process exactly one page for the current cursor_date.
    If the page is empty -> advance to next day and reset page=1.
    """
    cursor_date_raw = state.get("cursor_date")
    cursor_page = int(state.get("cursor_page") or 1)

    if not cursor_date_raw:
        # If no cursor_date yet, initialize from run_start_date
        run_start = state.get("run_start_date")
        if not run_start:
            raise RuntimeError("No cursor_date and no run_start_date. Use /start?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD")
        cursor_date = _parse_ymd(run_start)
        cursor_page = 1
    else:
        cursor_date = _parse_ymd(cursor_date_raw)

    run_end_raw = state.get("run_end_date")
    if not run_end_raw:
        raise RuntimeError("No run_end_date set. Use /start?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD")
    run_end = _parse_ymd(run_end_raw)

    if cursor_date > run_end:
        # Done
        await _supabase_update_state({
            "status": "idle",
            "last_message": f"Completed: cursor_date {cursor_date} past run_end_date {run_end}",
            "last_heartbeat_at": _now_utc().isoformat(),
        })
        return {"done": True, "message": "Completed"}

    # Fetch and upsert
    payload = await _http_get_salesorders(cursor_date, cursor_page)
    sales_orders = payload.get("sales_orders") or []

    # Heartbeat before heavy work
    await _supabase_update_state({
        "last_heartbeat_at": _now_utc().isoformat(),
        "last_message": f"Fetched day={cursor_date} page={cursor_page} orders={len(sales_orders)}",
        "last_error": None,
        "status": state.get("status") or "running",
    })

    if not sales_orders:
        # Advance to next day
        next_day = cursor_date + timedelta(days=1)
        await _supabase_update_state({
            "cursor_date": next_day.isoformat(),
            "cursor_page": 1,
            "pages_processed": int(state.get("pages_processed") or 0) + 1,
            "last_heartbeat_at": _now_utc().isoformat(),
            "last_message": f"No orders on day={cursor_date} page={cursor_page}; advancing to {next_day}",
        })
        return {"done": False, "advanced_day": True, "day": str(cursor_date), "page": cursor_page}

    rows = [_order_to_row(o) for o in sales_orders]
    await _upsert_orders(rows)

    affected_customer_uids = [r.get("customer_uid") for r in rows if r.get("customer_uid")]
    await _refresh_rollups_for_customer_uids(affected_customer_uids)

    # Update state for next page
    await _supabase_update_state({
        "cursor_date": cursor_date.isoformat(),
        "cursor_page": cursor_page + 1,
        "pages_processed": int(state.get("pages_processed") or 0) + 1,
        "orders_processed": int(state.get("orders_processed") or 0) + len(rows),
        "last_heartbeat_at": _now_utc().isoformat(),
        "last_message": f"Upserted {len(rows)} orders for day={cursor_date} page={cursor_page}",
    })

    return {"done": False, "advanced_day": False, "day": str(cursor_date), "page": cursor_page, "orders": len(rows)}


async def _worker_run_until_done() -> None:
    """
    Background loop: keeps processing pages until:
      - status becomes stopping/idle/error
      - cursor_date passes run_end_date
    Checkpointing is in wms_job_state so it can resume after restart.
    """
    while True:
        state = await _supabase_get_state()
        status = (state.get("status") or "idle").lower()

        if status == "stopping":
            await _supabase_update_state({
                "status": "idle",
                "last_message": "Stopped by user",
                "last_heartbeat_at": _now_utc().isoformat(),
            })
            return

        if status != "running":
            # Only run when explicitly set to running
            return

        try:
            result = await _process_one_page(state)
            if result.get("done"):
                return
        except Exception as e:
            await _supabase_update_state({
                "status": "error",
                "last_error": str(e),
                "last_message": "Worker stopped due to error",
                "last_heartbeat_at": _now_utc().isoformat(),
            })
            return

        await asyncio.sleep(SLEEP_BETWEEN_PAGES_SEC)


async def _ensure_worker_running() -> None:
    global _worker_task
    async with _worker_lock:
        if _worker_task and not _worker_task.done():
            return
        _worker_task = asyncio.create_task(_worker_run_until_done())


# =============================================================================
# Routes
# =============================================================================

@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
async def status_page(request: Request):
    state = await _supabase_get_state()

    # Worker task status
    worker_running = _worker_task is not None and not _worker_task.done()

    # Heartbeat staleness
    hb = state.get("last_heartbeat_at")
    heartbeat_stale = False
    if hb:
        try:
            hb_dt = datetime.fromisoformat(hb.replace("Z", "+00:00"))
            heartbeat_stale = (_now_utc() - hb_dt) > timedelta(minutes=5) and (state.get("status") == "running")
        except Exception:
            heartbeat_stale = False

    # Defaults for form
    today = date.today()
    default_start = (today - timedelta(days=1)).isoformat()
    default_end = today.isoformat()

    html = jinja_env.get_template("status.html").render(
        request=request,
        state=state,
        worker_running=worker_running,
        heartbeat_stale=heartbeat_stale,
        default_start=default_start,
        default_end=default_end,
    )
    return HTMLResponse(html)


@app.get("/start")
async def start(start_date: str, end_date: str):
    """
    Start (or resume) importing over a date range inclusive.
    Uses checkpoint fields cursor_date/cursor_page.
    """
    s = _parse_ymd(start_date)
    e = _parse_ymd(end_date)
    if e < s:
        return JSONResponse({"ok": False, "error": "end_date must be >= start_date"}, status_code=400)

    # Initialize cursor if missing or out of range
    state = await _supabase_get_state()
    cursor_date_raw = state.get("cursor_date")
    cursor_page = int(state.get("cursor_page") or 1)

    if not cursor_date_raw:
        cursor_date = s
        cursor_page = 1
    else:
        cursor_date = _parse_ymd(cursor_date_raw)
        # If existing cursor is outside the new range, reset to start
        if cursor_date < s or cursor_date > e:
            cursor_date = s
            cursor_page = 1

    await _supabase_update_state({
        "status": "running",
        "run_start_date": s.isoformat(),
        "run_end_date": e.isoformat(),
        "cursor_date": cursor_date.isoformat(),
        "cursor_page": cursor_page,
        "last_error": None,
        "last_message": f"Started: {s} → {e} at cursor {cursor_date} page {cursor_page}",
        "last_heartbeat_at": _now_utc().isoformat(),
    })

    await _ensure_worker_running()
    return {"ok": True, "message": "Worker started", "run_start_date": start_date, "run_end_date": end_date}


@app.get("/stop")
async def stop():
    await _supabase_update_state({
        "status": "stopping",
        "last_message": "Stop requested",
        "last_heartbeat_at": _now_utc().isoformat(),
    })
    return {"ok": True, "message": "Stop requested"}


@app.get("/reset")
async def reset():
    """
    Reset cursor_date/cursor_page back to run_start_date (keeps the range).
    """
    state = await _supabase_get_state()
    run_start = state.get("run_start_date")
    run_end = state.get("run_end_date")
    if not run_start or not run_end:
        return JSONResponse({"ok": False, "error": "No run_start_date/run_end_date set. Use /start first."}, status_code=400)

    await _supabase_update_state({
        "cursor_date": run_start,
        "cursor_page": 1,
        "pages_processed": 0,
        "orders_processed": 0,
        "last_error": None,
        "last_message": f"Reset cursor to {run_start} page 1",
        "last_heartbeat_at": _now_utc().isoformat(),
    })
    return {"ok": True, "message": "Reset complete", "cursor_date": run_start, "cursor_page": 1}


@app.get("/tick")
async def tick():
    """
    Process exactly one page (useful for manual step-through or external cron hitting /tick).
    """
    state = await _supabase_get_state()
    try:
        # Temporarily treat as running for tick
        if (state.get("status") or "").lower() != "running":
            await _supabase_update_state({"status": "running", "last_message": "Tick invoked; setting status=running"})
            state = await _supabase_get_state()

        result = await _process_one_page(state)
        return {"ok": True, "result": result}
    except Exception as e:
        await _supabase_update_state({
            "status": "error",
            "last_error": str(e),
            "last_message": "Tick failed",
            "last_heartbeat_at": _now_utc().isoformat(),
        })
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.on_event("startup")
async def on_startup():
    # Auto-resume if configured and DB status says running
    if not START_ON_STARTUP:
        return
    state = await _supabase_get_state()
    if (state.get("status") or "").lower() == "running":
        await _ensure_worker_running()
