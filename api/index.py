import os
import json
import asyncio
from typing import Optional, Dict, Any
from starlette.applications import Starlette
from starlette.responses import JSONResponse, HTMLResponse, PlainTextResponse
from starlette.routing import Route
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from telethon import TelegramClient
from telethon.sessions import StringSession

def _safe_int(val: Any) -> int:
    try:
        return int(str(val).strip())
    except Exception:
        return 0

CFG = {
    "api_id": _safe_int(os.environ.get("TG_API_ID", "0")),
    "api_hash": os.environ.get("TG_API_HASH", "").strip(),
    "session_string": os.environ.get("TG_SESSION_STRING", "").strip(),
    "api_key": os.environ.get("TG_API_KEY", "").strip()
}

_client: Optional[TelegramClient] = None
_loop = None

async def get_tg(custom_cfg: Optional[Dict[str, Any]] = None) -> TelegramClient:
    """Stateless, multi-bot safe MTProto connection (receive_updates=False prevents session lock)."""
    global _client, _loop
    c = custom_cfg or CFG
    if not c.get("api_id") or not c.get("session_string"):
        raise RuntimeError("Missing TG_API_ID, TG_API_HASH, or TG_SESSION_STRING in Vercel Environment Variables")

    cur_loop = asyncio.get_running_loop()
    if _client is None or _loop != cur_loop:
        _loop = cur_loop
        _client = TelegramClient(
            StringSession(c["session_string"]),
            int(c["api_id"]),
            c["api_hash"],
            receive_updates=False,
            request_retries=5,
            connection_retries=5,
            entity_cache_limit=5
        )
    if not _client.is_connected():
        await _client.connect()
    return _client

def check_auth(request: Request) -> bool:
    key = CFG["api_key"]
    if not key:
        return True
    auth = request.headers.get("authorization", "")
    bearer = auth.replace("Bearer ", "").strip() if auth.startswith("Bearer ") else None
    provided = request.headers.get("x-api-key") or request.query_params.get("key") or bearer
    return provided == key

def compact_json(data: Any, status: int = 200, cache_sec: int = 0) -> JSONResponse:
    headers = {"Cache-Control": f"private, max-age={cache_sec}"} if cache_sec > 0 else {}
    return JSONResponse(data, status_code=status, headers=headers)

def format_meta(msg) -> Dict[str, Any]:
    f = None
    if msg.file:
        f = {
            "n": msg.file.name or f"file_{msg.id}{msg.file.ext or ''}",
            "b": msg.file.size or 0,
            "mb": round((msg.file.size or 0) / 1048576, 2),
            "m": msg.file.mime_type
        }
    return {
        "id": msg.id,
        "t": (msg.raw_text or "")[:120],
        "f": f
    }

# =========================================================
# HANDLERS (0 BYTES VERCEL STORAGE / 0 BYTES FILE BANDWIDTH)
# =========================================================
async def handle_health(request: Request):
    env_ready = bool(CFG["api_id"] and CFG["api_hash"] and CFG["session_string"])
    return compact_json({
        "ok": 1,
        "engine": "TG-Cloud V10 Final (Zero-Vercel-Load)",
        "env_configured": env_ready,
        "vercel_storage_used": "0 Bytes",
        "vercel_file_bandwidth": "0 Bytes (Direct Client <-> Telegram MTProto)",
        "routes": ["/", "/health", "/api/ticket", "/files", "/kv/{key}", "/delete/{id}", "/sdk/tgcloud.py"]
    })

async def handle_ticket(request: Request):
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized API Key"}, 401)
    if not CFG["api_id"] or not CFG["session_string"]:
        return compact_json({
            "ok": 0,
            "err": "Set TG_API_ID, TG_API_HASH, and TG_SESSION_STRING in Vercel Project Settings -> Environment Variables, then Redeploy."
        }, 400)
    return compact_json({
        "ok": 1,
        "id": CFG["api_id"],
        "hash": CFG["api_hash"],
        "s": CFG["session_string"]
    }, cache_sec=86400)

async def handle_sdk(request: Request):
    for p in (
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "tgcloud.py"),
        os.path.join(os.path.dirname(__file__), "tgcloud.py"),
        "tgcloud.py"
    ):
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return PlainTextResponse(f.read(), headers={"Cache-Control": "public, max-age=31536000"})
    return PlainTextResponse("# Upload tgcloud.py to repository root\n", status_code=404)

async def handle_files(request: Request):
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    try:
        tg = await get_tg()
        limit = min(_safe_int(request.query_params.get("limit", 25)) or 25, 100)
        q = request.query_params.get("q") or request.query_params.get("search") or None
        items = [format_meta(m) async for m in tg.iter_messages("me", limit=limit, search=q)]
        return compact_json({"ok": 1, "count": len(items), "items": items})
    except Exception as e:
        return compact_json({"ok": 0, "err": str(e)}, 500)

async def handle_kv(request: Request, k: str):
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    try:
        tg = await get_tg()
        if not k:
            keys = {}
            async for m in tg.iter_messages("me", limit=50, search="#KV_"):
                if m.raw_text and m.raw_text.startswith("#KV_"):
                    parts = m.raw_text.split("\n", 1)
                    key_name = parts[0].replace("#KV_", "").strip()
                    val_raw = parts[1].strip() if len(parts) > 1 else ""
                    if key_name not in keys:
                        try:
                            keys[key_name] = json.loads(val_raw)
                        except Exception:
                            keys[key_name] = val_raw
            return compact_json({"ok": 1, "count": len(keys), "data": keys})

        tag = f"#KV_{k}"
        if request.method == "GET":
            async for m in tg.iter_messages("me", limit=5, search=tag):
                if m.raw_text and m.raw_text.startswith(tag):
                    raw = m.raw_text[len(tag):].strip()
                    try:
                        val = json.loads(raw)
                    except Exception:
                        val = raw
                    return compact_json({"ok": 1, "k": k, "v": val, "id": m.id})
            return compact_json({"ok": 0, "err": "not_found"}, 404)

        if request.method == "DELETE":
            ids = [m.id async for m in tg.iter_messages("me", limit=5, search=tag) if m.raw_text and m.raw_text.startswith(tag)]
            if ids:
                await tg.delete_messages("me", ids)
            return compact_json({"ok": 1, "del": k})

        body = await request.json()
        val_obj = body.get("v", body.get("value", body))
        val_str = json.dumps(val_obj, separators=(",", ":"), ensure_ascii=False)
        ids = [m.id async for m in tg.iter_messages("me", limit=5, search=tag) if m.raw_text and m.raw_text.startswith(tag)]
        if ids:
            await tg.delete_messages("me", ids)
        msg = await tg.send_message("me", f"{tag}\n{val_str}")
        return compact_json({"ok": 1, "k": k, "id": msg.id})
    except Exception as e:
        return compact_json({"ok": 0, "err": str(e)}, 500)

async def handle_delete(request: Request, mid: int):
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    try:
        tg = await get_tg()
        await tg.delete_messages("me", [mid])
        return compact_json({"ok": 1, "del": mid})
    except Exception as e:
        return compact_json({"ok": 0, "err": str(e)}, 500)

# =========================================================
# 100% CLIENT-SIDE DIRECT MTPROTO WEBSOCKET DASHBOARD UI
# =========================================================
async def handle_dashboard(request: Request):
    return HTMLResponse("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TG-CLOUD V10 MAX // ZERO-VERCEL-LOAD</title>
<script src="https://cdn.jsdelivr.net/npm/telegram@2.22.2/browser/telegram.min.js"></script>
<style>
:root{--bg:#040406;--card:rgba(14,16,22,0.94);--bd:rgba(255,255,255,0.1);--ac:#fff;--gr:#00ff9d;--rd:#ff4d4d;--tx:#e2e8f0;--mt:#94a3b8}
*{box-sizing:border-box;font-family:'JetBrains Mono',monospace}
body{background:var(--bg);color:var(--tx);margin:0;padding:14px}
.wrap{max-width:960px;margin:0 auto}
.box{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:14px;margin-bottom:12px}
h2{margin:0 0 8px;color:var(--ac);font-size:.88rem;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px}
input,button{width:100%;padding:9px 10px;margin-top:6px;background:rgba(0,0,0,0.65);border:1px solid var(--bd);color:var(--tx);border-radius:6px;font-size:.8rem}
button{background:var(--ac);color:#000;font-weight:700;cursor:pointer;border:none;transition:.15s}
button:hover{opacity:.86}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
@media(max-width:680px){.grid{grid-template-columns:1fr}}
.row{display:flex;justify-content:space-between;align-items:center;padding:9px 4px;border-bottom:1px solid rgba(255,255,255,0.06);font-size:.78rem;gap:8px}
.tag{color:var(--gr);font-size:.72rem}
.pill{padding:4px 10px;border-radius:99px;font-size:.72rem;background:rgba(255,255,255,0.06);cursor:pointer;border:1px solid var(--bd)}
a{color:var(--ac);text-decoration:none;margin-left:8px;font-weight:700;cursor:pointer}
.log{font-size:.75rem;color:var(--gr);margin-top:8px;word-break:break-all}
</style>
</head>
<body>
<div class="wrap">
  <div class="box">
    <h2>
      <span>⚡ TG-CLOUD V10 MAX // 100% DIRECT MTPROTO</span>
      <span id="st" style="color:var(--gr);font-size:.74rem">0 BYTES VERCEL LOAD</span>
    </h2>
    <div style="display:flex;gap:8px">
      <input type="password" id="key" placeholder="Optional API Key (if set)">
      <button onclick="reconnectMT()" style="width:160px">CONNECT TG</button>
    </div>
    <div id="manualCfg" style="display:none;margin-top:8px;border-top:1px solid var(--bd);padding-top:8px">
      <div style="font-size:.74rem;color:#ffb800;margin-bottom:4px">⚠️ Vercel Env Vars not set yet? Paste below to connect directly from browser (saved in browser localStorage):</div>
      <div style="display:flex;gap:6px">
        <input type="number" id="m_id" placeholder="TG_API_ID">
        <input type="text" id="m_hash" placeholder="TG_API_HASH">
      </div>
      <input type="text" id="m_sess" placeholder="TG_SESSION_STRING">
      <button onclick="saveManualCfg()" style="background:var(--gr)">SAVE IN BROWSER & CONNECT DIRECTLY</button>
    </div>
  </div>

  <div class="grid">
    <div class="box">
      <h2>📤 4X PARALLEL DIRECT UPLOAD (UNLIMITED GB)</h2>
      <input type="file" id="files" multiple>
      <input type="text" id="utag" value="#storage" placeholder="Tag (#storage, #backup, #bot)">
      <button onclick="uploadDirect()">UPLOAD DIRECTLY TO TELEGRAM</button>
      <div id="ulog" class="log"></div>
    </div>

    <div class="box">
      <h2>🗄️ DIRECT CLOUD KV DATABASE (0 SQLITE)</h2>
      <div style="display:flex;gap:6px">
        <input type="text" id="kvk" placeholder="Key (e.g. config)">
        <input type="text" id="kvv" placeholder='Value / JSON (e.g. {"pro":true})'>
      </div>
      <div style="display:flex;gap:6px">
        <button onclick="kvSet()">SAVE DIRECT</button>
        <button onclick="kvGet()" style="background:var(--mt)">READ DIRECT</button>
      </div>
      <div id="klog" class="log"></div>
    </div>
  </div>

  <div class="box">
    <h2>🗂️ SAVED MESSAGES EXPLORER (100% DIRECT WEBSOCKET)</h2>
    <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">
      <span class="pill" onclick="setFilter('')">All</span>
      <span class="pill" onclick="setFilter('#storage')">📦 #storage</span>
      <span class="pill" onclick="setFilter('#remote')">🌐 #remote</span>
      <span class="pill" onclick="setFilter('#KV_')">🗄️ #KV_</span>
    </div>
    <div style="display:flex;gap:8px">
      <input type="text" id="q" placeholder="Search filename or #tag directly on Telegram..." onkeyup="if(event.key==='Enter')loadDirect()">
      <button onclick="loadDirect()" style="width:140px">REFRESH</button>
    </div>
    <div id="list" style="margin-top:10px"></div>
  </div>
</div>

<script>
let tgClient = null;
const getKey = () => document.getElementById('key').value.trim();

function saveManualCfg() {
  const id = Number(document.getElementById('m_id').value.trim());
  const hash = document.getElementById('m_hash').value.trim();
  const s = document.getElementById('m_sess').value.trim();
  if (!id || !hash || !s) return alert('Fill all 3 fields');
  localStorage.setItem('tg_cloud_tk', JSON.stringify({ok: 1, id, hash, s}));
  document.getElementById('manualCfg').style.display = 'none';
  tgClient = null;
  loadDirect();
}

async function getClient() {
  if (tgClient && tgClient.connected) return tgClient;
  const st = document.getElementById('st');
  st.innerText = 'CONNECTING DIRECT MTPROTO...';

  let tk = JSON.parse(localStorage.getItem('tg_cloud_tk') || 'null');
  if (!tk || !tk.s) {
    const r = await fetch('/api/ticket?key=' + encodeURIComponent(getKey()));
    tk = await r.json();
    if (!tk.ok) {
      document.getElementById('manualCfg').style.display = 'block';
      throw new Error(tk.err || 'Env Vars missing');
    }
    localStorage.setItem('tg_cloud_tk', JSON.stringify(tk));
  }

  const { TelegramClient } = window.telegram;
  const { StringSession } = window.telegram.sessions;
  tgClient = new TelegramClient(new StringSession(tk.s), Number(tk.id), tk.hash, {
    connectionRetries: 15,
    useWSS: true
  });
  await tgClient.connect();
  st.innerText = '🟢 DIRECT TELEGRAM DC CONNECTED (0 VERCEL DATA)';
  return tgClient;
}

async function reconnectMT() {
  localStorage.removeItem('tg_cloud_tk');
  tgClient = null;
  await loadDirect();
}

async function uploadDirect() {
  const fl = document.getElementById('files').files;
  if (!fl.length) return alert('Select file(s)');
  const log = document.getElementById('ulog');
  const tag = document.getElementById('utag').value || '#storage';
  try {
    const c = await getClient();
    for (let i = 0; i < fl.length; i++) {
      const f = fl[i];
      const t0 = Date.now();
      await c.sendFile('me', {
        file: f,
        caption: `${tag} ${f.name}`,
        forceDocument: true,
        workers: 4,
        progressCallback: (p) => {
          const mb = (f.size / 1048576).toFixed(2);
          const sec = Math.max((Date.now() - t0) / 1000, 0.1);
          const spd = (((f.size * p) / 1048576) / sec).toFixed(2);
          log.innerText = `⚡ [${i+1}/${fl.length}] ${f.name} (${mb}MB): ${(p*100).toFixed(1)}% — ${spd} MB/s (0 Vercel Load)`;
        }
      });
    }
    log.innerText = '✅ 100% Lossless Upload Complete!';
    document.getElementById('files').value = '';
    await loadDirect();
  } catch (e) {
    log.innerText = '❌ ' + e.message;
  }
}

async function downloadDirect(msgId, name) {
  const log = document.getElementById('ulog');
  try {
    const c = await getClient();
    const msgs = await c.getMessages('me', { ids: [Number(msgId)] });
    if (!msgs || !msgs[0]) return alert('File not found');
    const t0 = Date.now();
    const buf = await c.downloadMedia(msgs[0], {
      workers: 4,
      progressCallback: (dl, tot) => {
        const pct = tot ? ((Number(dl) / Number(tot)) * 100).toFixed(1) : '...';
        const spd = ((Number(dl) / 1048576) / Math.max((Date.now() - t0)/1000, 0.1)).toFixed(2);
        log.innerText = `⚡ Downloading ${name}: ${pct}% — ${spd} MB/s (0 Vercel Load)`;
      }
    });
    const url = URL.createObjectURL(new Blob([buf]));
    const a = document.createElement('a');
    a.href = url; a.download = name; a.click();
    URL.revokeObjectURL(url);
    log.innerText = `✅ Downloaded ${name}!`;
  } catch (e) {
    log.innerText = '❌ ' + e.message;
  }
}

async function kvSet() {
  const k = document.getElementById('kvk').value.trim();
  const v = document.getElementById('kvv').value.trim();
  if (!k) return;
  const c = await getClient();
  const tag = `#KV_${k}`;
  const old = await c.getMessages('me', { search: tag, limit: 5 });
  const delIds = old.filter(m => m.message && m.message.startsWith(tag)).map(m => m.id);
  if (delIds.length) await c.deleteMessages('me', delIds, { revoke: true });
  const msg = await c.sendMessage('me', { message: `${tag}\n${v}` });
  document.getElementById('klog').innerText = `✅ Saved ${k} (Msg #${msg.id} | 0 Vercel Calls)`;
  await loadDirect();
}

async function kvGet() {
  const k = document.getElementById('kvk').value.trim();
  if (!k) return;
  const c = await getClient();
  const tag = `#KV_${k}`;
  const msgs = await c.getMessages('me', { search: tag, limit: 5 });
  const found = msgs.find(m => m.message && m.message.startsWith(tag));
  document.getElementById('klog').innerText = found ? found.message.slice(tag.length).trim() : 'Not found';
}

async function delDirect(id) {
  const c = await getClient();
  await c.deleteMessages('me', [Number(id)], { revoke: true });
  await loadDirect();
}

function setFilter(q) {
  document.getElementById('q').value = q;
  loadDirect();
}

async function loadDirect() {
  const list = document.getElementById('list');
  const q = document.getElementById('q').value.trim();
  try {
    const c = await getClient();
    const msgs = await c.getMessages('me', { limit: 25, search: q || undefined });
    list.innerHTML = msgs.map(m => {
      let fname = null, mb = 0;
      if (m.media && m.media.document) {
        const doc = m.media.document;
        mb = (Number(doc.size || 0) / 1048576).toFixed(2);
        const attr = (doc.attributes || []).find(a => a.fileName);
        fname = attr ? attr.fileName : `file_${m.id}.bin`;
      }
      const label = fname || (m.message || '').slice(0, 80) || '[Media]';
      const safeName = (fname || 'file.bin').replace(/'/g, "\\'");
      return `<div class="row">
        <div style="word-break:break-all">
          <strong>#${m.id}</strong> ${label}
          <span class="tag">${fname ? '(' + mb + ' MB)' : '[MSG]'}</span>
        </div>
        <div style="white-space:nowrap">
          ${fname ? `<a onclick="downloadDirect(${m.id},'${safeName}')" style="color:var(--gr)">[⚡DIRECT DL]</a>` : ''}
          <a onclick="delDirect(${m.id})" style="color:var(--rd)">[X]</a>
        </div>
      </div>`;
    }).join('');
  } catch (e) {
    list.innerHTML = `<div class="log">❌ ${e.message}</div>`;
  }
}
loadDirect();
</script>
</body>
</html>""")

# =========================================================
# UNIVERSAL CATCH-ALL ROUTER (NEVER RETURNS 404 NOT FOUND)
# =========================================================
async def master_router(request: Request):
    raw_path = (
        request.query_params.get("__path")
        or request.headers.get("x-matched-path")
        or request.url.path
        or "/"
    ).strip()

    # Clean Vercel internal prefixes if present
    for prefix in ("/api/index.py", "/api/index"):
        if raw_path.startswith(prefix):
            raw_path = raw_path[len(prefix):]
            break

    path = "/" + raw_path.strip("/")

    if path in ("/", "/api"):
        return await handle_dashboard(request)
    if path in ("/health", "/stats", "/api/health", "/api/stats"):
        return await handle_health(request)
    if path in ("/ticket", "/api/ticket"):
        return await handle_ticket(request)
    if path.endswith("/tgcloud.py") or path in ("/sdk", "/api/sdk"):
        return await handle_sdk(request)
    if path in ("/files", "/api/files"):
        return await handle_files(request)
    if path == "/kv" or path.startswith("/kv/") or path.startswith("/api/kv"):
        k = path.split("/kv")[-1].strip("/")
        return await handle_kv(request, k)
    if path.startswith("/delete/") or path.startswith("/api/delete/"):
        mid = _safe_int(path.split("/delete/")[-1].strip("/"))
        return await handle_delete(request, mid)

    # Default fallback -> serve Dashboard so no URL ever returns Not Found
    return await handle_dashboard(request)

routes = [
    Route("/", master_router, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]),
    Route("/{any_path:path}", master_router, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]),
]

app = Starlette(
    routes=routes,
    middleware=[Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])]
)
