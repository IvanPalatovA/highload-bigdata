import os
import sys
import time
import json
import gzip
import zlib
import signal
import threading
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests

RUNNING = True

def handle_sig(signum, frame):
    global RUNNING
    RUNNING = False

signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)

MODE = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("COLLECTOR_MODE", "record")
SOURCE = os.environ.get("SOURCE", "github")
RAW_FILTER = os.environ.get("SOURCE_FILTER", "CreateEvent,DeleteEvent,ReleaseEvent")
SOURCE_FILTER = set(x.strip() for x in RAW_FILTER.split(",") if x.strip())

RAW_DIR = os.environ.get("COLLECTOR_RAW_DIR", "/data/raw")
OUT_DIR = os.environ.get("COLLECTOR_OUT_DIR", os.path.join(RAW_DIR, "history" if MODE == "backfill" else "stream"))
STATE_DIR = os.path.join(RAW_DIR, "state")

RAW_LIMIT_GB = float(os.environ.get("COLLECTOR_RAW_LIMIT_GB", "20"))
METRICS_PORT = int(os.environ.get("COLLECTOR_METRICS_PORT", "9101"))
USER_AGENT = os.environ.get("COLLECTOR_USER_AGENT", "mephi-bigdata-collector-v93/1.0 (palatov.vanya@gmail.com)")
POLL_SECONDS = int(os.environ.get("COLLECTOR_POLL_SECONDS", "60"))

HIST_FROM = os.environ.get("COLLECTOR_HISTORY_FROM", "")
HIST_TO = os.environ.get("COLLECTOR_HISTORY_TO", "")
MAX_EVENTS_PER_HOUR = int(os.environ.get("COLLECTOR_MAX_EVENTS_PER_HOUR", "0"))

METRICS = {
    "events_total": 0,
    "errors_http": 0,
    "last_success_ts": 0
}
METRICS_LOCK = threading.Lock()

def log_json(level: str, msg: str, **kwargs):
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    record = {"ts": now_iso, "level": level, "msg": msg}
    record.update(kwargs)
    print(json.dumps(record, ensure_ascii=False), flush=True)

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        accept_header = self.headers.get("Accept", "")
        is_browser = "text/html" in accept_header and "text/plain" not in accept_header
        show_html = self.path == "/" or (self.path.startswith("/metrics") and is_browser and "format=raw" not in self.path)

        with METRICS_LOCK:
            ev_count = METRICS["events_total"]
            err_count = METRICS["errors_http"]
            last_ts = METRICS["last_success_ts"]

        last_dt_str = datetime.fromtimestamp(last_ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if last_ts else "Ожидание первого опроса"

        h_files = 0
        s_files = 0
        try:
            import glob
            h_files = len(glob.glob(os.path.join(RAW_DIR, "history", "dt=*", "*.jsonl.gz")))
            s_files = len(glob.glob(os.path.join(RAW_DIR, "stream", "dt=*", "hour=*", "*.jsonl.gz")))
        except Exception:
            pass

        if show_html:
            html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Collector Metrics & Dashboard — Variant 93</title>
    <style>
        :root {{
            --bg: #0f172a;
            --surface: #1e293b;
            --border: #334155;
            --text: #f8fafc;
            --text-muted: #94a3b8;
            --primary: #38bdf8;
            --success: #22c55e;
            --danger: #ef4444;
            --warn: #f59e0b;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: var(--bg);
            color: var(--text);
            padding: 32px 16px;
            display: flex;
            justify-content: center;
        }}
        .container {{ max-width: 860px; width: 100%; }}
        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 24px;
            padding-bottom: 16px;
            border-bottom: 1px solid var(--border);
        }}
        .title h1 {{
            font-size: 24px;
            font-weight: 700;
            color: #fff;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .title p {{ color: var(--text-muted); font-size: 14px; margin-top: 4px; }}
        .badge {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            background: rgba(34, 197, 94, 0.15);
            color: var(--success);
            border: 1px solid rgba(34, 197, 94, 0.3);
            padding: 6px 14px;
            border-radius: 9999px;
            font-size: 13px;
            font-weight: 600;
        }}
        .pulse {{
            width: 8px;
            height: 8px;
            background: var(--success);
            border-radius: 50%;
            box-shadow: 0 0 8px var(--success);
            animation: pulse 2s infinite;
        }}
        @keyframes pulse {{
            0% {{ opacity: 1; transform: scale(1); }}
            50% {{ opacity: 0.4; transform: scale(1.2); }}
            100% {{ opacity: 1; transform: scale(1); }}
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}
        .card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
        }}
        .card-label {{
            font-size: 12px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }}
        .card-val {{
            font-size: 30px;
            font-weight: 700;
            color: #fff;
        }}
        .card-sub {{ font-size: 12px; color: var(--text-muted); margin-top: 6px; }}
        .info-table {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
            margin-bottom: 24px;
        }}
        .info-row {{
            display: flex;
            justify-content: space-between;
            padding: 12px 20px;
            border-bottom: 1px solid var(--border);
            font-size: 14px;
        }}
        .info-row:last-child {{ border-bottom: none; }}
        .info-key {{ color: var(--text-muted); }}
        .info-val {{ font-weight: 500; font-family: ui-monospace, monospace; color: #e2e8f0; }}
        .raw-box {{
            background: #020617;
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 16px;
        }}
        .raw-box pre {{
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
            font-size: 13px;
            color: #38bdf8;
            overflow-x: auto;
            line-height: 1.5;
        }}
        .btn {{
            background: var(--border);
            color: var(--text);
            border: none;
            padding: 6px 12px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 12px;
            text-decoration: none;
            transition: all 0.2s;
        }}
        .btn:hover {{ background: #475569; }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="title">
                <h1>⚡ Big Data Platform — Collector</h1>
                <p>Служба сбора данных: Вариант №93 (GH Archive)</p>
            </div>
            <div class="badge">
                <span class="pulse"></span> Служба активна
            </div>
        </header>

        <div class="grid">
            <div class="card">
                <div class="card-label">Собрано событий</div>
                <div class="card-val" style="color: var(--primary);">{ev_count:,}</div>
                <div class="card-sub">В слое сырых данных</div>
            </div>
            <div class="card">
                <div class="card-label">Ошибок сети / HTTP</div>
                <div class="card-val" style="color: {'var(--success)' if err_count == 0 else 'var(--danger)'};">{err_count}</div>
                <div class="card-sub">Сбои обращений к источнику</div>
            </div>
            <div class="card">
                <div class="card-label">Исторический срез</div>
                <div class="card-val" style="color: #a855f7;">{h_files} / 168</div>
                <div class="card-sub">7 суток истории (backfill 100%)</div>
            </div>
            <div class="card">
                <div class="card-label">Поток времени (stream)</div>
                <div class="card-val" style="color: var(--warn);">{s_files}</div>
                <div class="card-sub">Часовых файлов в stream</div>
            </div>
        </div>

        <div class="info-table">
            <div class="info-row">
                <span class="info-key">Режим службы (COLLECTOR_MODE)</span>
                <span class="info-val">{MODE} (фоновый демон)</span>
            </div>
            <div class="info-row">
                <span class="info-key">Источник данных (SOURCE)</span>
                <span class="info-val">{SOURCE} (https://data.gharchive.org/)</span>
            </div>
            <div class="info-row">
                <span class="info-key">Фильтр событий (SOURCE_FILTER)</span>
                <span class="info-val">{RAW_FILTER}</span>
            </div>
            <div class="info-row">
                <span class="info-key">Время последнего опроса</span>
                <span class="info-val">{last_dt_str}</span>
            </div>
        </div>

        <div class="raw-box">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                <span style="font-size: 13px; color: var(--text-muted); font-weight: 600;">Prometheus OpenMetrics Format (/metrics)</span>
                <a href="/metrics?format=raw" class="btn">Открыть сырой вывод</a>
            </div>
            <pre># HELP collector_events_total Total number of events processed matching filter
# TYPE collector_events_total counter
collector_events_total{{mode="{MODE}"}} {ev_count}

# HELP collector_errors_total Total number of HTTP errors when requesting source
# TYPE collector_errors_total counter
collector_errors_total{{mode="{MODE}",kind="http"}} {err_count}

# HELP collector_last_success_timestamp_seconds Timestamp of last successful poll
# TYPE collector_last_success_timestamp_seconds gauge
collector_last_success_timestamp_seconds {last_ts}</pre>
        </div>
    </div>
</body>
</html>"""
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            lines = [
                "# HELP collector_events_total Total number of events processed matching filter",
                "# TYPE collector_events_total counter",
                f'collector_events_total{{mode="{MODE}"}} {ev_count}',
                "",
                "# HELP collector_errors_total Total number of HTTP errors when requesting source",
                "# TYPE collector_errors_total counter",
                f'collector_errors_total{{mode="{MODE}",kind="http"}} {err_count}',
                "",
                "# HELP collector_last_success_timestamp_seconds Timestamp of last successful poll",
                "# TYPE collector_last_success_timestamp_seconds gauge",
                f'collector_last_success_timestamp_seconds {last_ts}',
                ""
            ]
            body = "\n".join(lines).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format, *args):
        pass

def start_metrics_server():
    server = HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

def load_state(state_file: str) -> dict:
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"completed_hours": [], "seen_keys_list": []}

def save_state(state_file: str, state: dict):
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    tmp = state_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, state_file)

def enforce_rotation():
    limit_bytes = int(RAW_LIMIT_GB * (1024 ** 3))
    files = []
    total_size = 0
    for sub in ("stream", "history"):
        d = os.path.join(RAW_DIR, sub)
        if not os.path.exists(d):
            continue
        for root, _, filenames in os.walk(d):
            for fn in filenames:
                if fn.endswith(".jsonl.gz"):
                    fp = os.path.join(root, fn)
                    try:
                        st = os.stat(fp)
                        total_size += st.st_size
                        files.append((st.st_mtime, st.st_size, fp))
                    except OSError:
                        pass
    if total_size <= limit_bytes:
        return
    files.sort(key=lambda x: x[0])
    removed = 0
    for _, sz, fp in files:
        if total_size <= limit_bytes:
            break
        try:
            os.remove(fp)
            total_size -= sz
            removed += 1
        except OSError:
            pass
    if removed > 0:
        log_json("info", "rotated", files=removed)

def normalize_event_time(ts_str: str) -> str:
    if not ts_str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    if ts_str.endswith("Z"):
        base = ts_str[:-1]
        if "." not in base:
            return base + ".000Z"
        return ts_str
    return ts_str

def transform_event(raw_ev: dict, received_at_iso: str) -> dict:
    actor = raw_ev.get("actor") or {}
    repo = raw_ev.get("repo") or {}
    org = raw_ev.get("org") or {}
    payload = raw_ev.get("payload") or {}
    return {
        "event_key": str(raw_ev.get("id", "")),
        "event_time": normalize_event_time(raw_ev.get("created_at", "")),
        "received_at": received_at_iso,
        "source": SOURCE,
        "id": str(raw_ev.get("id", "")),
        "type": str(raw_ev.get("type", "")),
        "public": bool(raw_ev.get("public", True)),
        "actor_id": int(actor.get("id") or 0),
        "actor_login": str(actor.get("login") or ""),
        "repo_id": int(repo.get("id") or 0),
        "repo_name": str(repo.get("name") or ""),
        "org_id": int(org.get("id")) if org.get("id") is not None else None,
        "org_login": str(org.get("login")) if org.get("login") is not None else None,
        "ref_type": str(payload.get("ref_type")) if payload.get("ref_type") is not None else None,
        "ref": str(payload.get("ref")) if payload.get("ref") is not None else None,
        "master_branch": str(payload.get("master_branch")) if payload.get("master_branch") is not None else None,
        "description": str(payload.get("description")) if payload.get("description") is not None else None,
        "pusher_type": str(payload.get("pusher_type")) if payload.get("pusher_type") is not None else None
    }

def fetch_and_process_hour(dt_str: str, hour_int: int, is_history: bool, seen_keys: set) -> bool:
    url = f"https://data.gharchive.org/{dt_str}-{hour_int}.json.gz"
    headers = {"User-Agent": USER_AGENT}
    backoff = 2
    max_attempts = 5

    for attempt in range(1, max_attempts + 1):
        if not RUNNING:
            return False
        try:
            resp = requests.get(url, headers=headers, stream=True, timeout=30)
            if resp.status_code == 404:
                log_json("warn", "http", status=404, url=url, pause=0)
                return False
            if resp.status_code == 429 or resp.status_code >= 500:
                with METRICS_LOCK:
                    METRICS["errors_http"] += 1
                log_json("warn", "http", status=resp.status_code, pause=backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                log_json("info", "retry", attempt=attempt + 1)
                continue
            resp.raise_for_status()

            now_utc = datetime.now(timezone.utc)
            received_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            if is_history:
                target_dir = os.path.join(OUT_DIR, f"dt={dt_str}")
                final_name = f"gharchive-{dt_str}-{hour_int:02d}.jsonl.gz"
            else:
                recv_dt = now_utc.strftime("%Y-%m-%d")
                recv_hr = now_utc.strftime("%H")
                target_dir = os.path.join(OUT_DIR, f"dt={recv_dt}", f"hour={recv_hr}")
                final_name = f"gharchive-{dt_str}-{hour_int:02d}.jsonl.gz"

            os.makedirs(target_dir, exist_ok=True)
            tmp_path = os.path.join(target_dir, f".tmp-{final_name}")
            final_path = os.path.join(target_dir, final_name)

            decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
            buf = b""
            got = 0
            kept = 0
            raw_bytes = 0

            with gzip.open(tmp_path, "wt", encoding="utf-8") as gz_out:
                for chunk in resp.iter_content(chunk_size=65536):
                    if not RUNNING:
                        break
                    if not chunk:
                        continue
                    data = decompressor.decompress(chunk)
                    buf += data
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        if not line.strip():
                            continue
                        got += 1
                        raw_bytes += len(line) + 1
                        try:
                            ev = json.loads(line.decode("utf-8", errors="replace"))
                        except Exception:
                            continue
                        ev_type = ev.get("type", "")
                        if SOURCE_FILTER and ev_type not in SOURCE_FILTER:
                            continue
                        ev_id = str(ev.get("id", ""))
                        if not ev_id or ev_id in seen_keys:
                            continue
                        seen_keys.add(ev_id)
                        out_obj = transform_event(ev, received_iso)
                        gz_out.write(json.dumps(out_obj, ensure_ascii=False) + "\n")
                        kept += 1
                    if MAX_EVENTS_PER_HOUR > 0 and got >= MAX_EVENTS_PER_HOUR:
                        break
            resp.close()

            if not RUNNING:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                return False

            os.replace(tmp_path, final_path)
            comp_bytes = os.path.getsize(final_path)

            with METRICS_LOCK:
                METRICS["events_total"] += kept
                METRICS["last_success_ts"] = int(time.time())

            log_json("info", "poll", archive=f"{dt_str}-{hour_int}", got=got, kept=kept, raw_bytes=raw_bytes, compressed_bytes=comp_bytes)
            log_json("info", "closed", file=final_path, lines=kept)
            enforce_rotation()
            return True

        except requests.RequestException as e:
            with METRICS_LOCK:
                METRICS["errors_http"] += 1
            log_json("warn", "http", status=0, error=str(e), pause=backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            if attempt < max_attempts:
                log_json("info", "retry", attempt=attempt + 1)

    return False

def run_backfill():
    state_file = os.path.join(STATE_DIR, "backfill_state.json")
    state = load_state(state_file)
    completed = set(state.get("completed_hours", []))

    if HIST_FROM and HIST_TO:
        start_dt = datetime.strptime(HIST_FROM, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(HIST_TO, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        now_d = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = now_d - timedelta(days=8)
        end_dt = now_d - timedelta(days=2)

    cur = start_dt
    while cur <= end_dt and RUNNING:
        dt_str = cur.strftime("%Y-%m-%d")
        seen_keys = set()
        for h in range(24):
            if not RUNNING:
                break
            h_key = f"{dt_str}-{h}"
            if h_key in completed:
                continue
            ok = fetch_and_process_hour(dt_str, h, is_history=True, seen_keys=seen_keys)
            if ok:
                completed.add(h_key)
                state["completed_hours"] = sorted(list(completed))[-2000:]
                save_state(state_file, state)
        cur += timedelta(days=1)

def run_record():
    state_file = os.path.join(STATE_DIR, "record_state.json")
    state = load_state(state_file)
    completed = set(state.get("completed_hours", []))
    seen_keys = set(state.get("seen_keys_list", []))

    while RUNNING:
        now_utc = datetime.now(timezone.utc)
        for offset in range(6, 1, -1):
            if not RUNNING:
                break
            target_time = now_utc - timedelta(hours=offset)
            dt_str = target_time.strftime("%Y-%m-%d")
            h_int = target_time.hour
            h_key = f"{dt_str}-{h_int}"
            if h_key in completed:
                continue
            ok = fetch_and_process_hour(dt_str, h_int, is_history=False, seen_keys=seen_keys)
            if ok:
                completed.add(h_key)
                state["completed_hours"] = sorted(list(completed))[-500:]
                state["seen_keys_list"] = list(seen_keys)[-50000:]
                save_state(state_file, state)

        try:
            r = requests.head("https://data.gharchive.org/", headers={"User-Agent": USER_AGENT}, timeout=10)
            if r.status_code >= 400:
                with METRICS_LOCK:
                    METRICS["errors_http"] += 1
                log_json("warn", "http", status=r.status_code, pause=5)
            else:
                with METRICS_LOCK:
                    METRICS["last_success_ts"] = int(time.time())
        except requests.RequestException as e:
            with METRICS_LOCK:
                METRICS["errors_http"] += 1
            log_json("warn", "http", status=0, error=str(e), pause=5)

        for _ in range(POLL_SECONDS):
            if not RUNNING:
                break
            time.sleep(1)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)
    start_metrics_server()
    log_json("info", "start", mode=MODE, source=SOURCE, filter=RAW_FILTER)
    if MODE == "backfill":
        run_backfill()
    else:
        run_record()

if __name__ == "__main__":
    main()
