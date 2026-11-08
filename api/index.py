import os
import re
import json
import random
import asyncio
from typing import Optional, Dict, Any
import httpx
from starlette.applications import Starlette
from starlette.responses import JSONResponse, HTMLResponse, PlainTextResponse, RedirectResponse
from starlette.routing import Route
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeFilename

def _safe_int(val: Any) -> int:
    try:
        return int(str(val).strip())
    except Exception:
        return 0

CFG = {
    "api_id": _safe_int(os.environ.get("TG_API_ID", "0")),
    "api_hash": os.environ.get("TG_API_HASH", "").strip(),
    "session_string": os.environ.get("TG_SESSION_STRING", "").strip(),
    "api_key": os.environ.get("TG_API_KEY", "").strip(),
    "bot_token": os.environ.get("TG_BOT_TOKEN", "").strip()
}

_client: Optional[TelegramClient] = None
_loop = None
_cached_tunnel: Dict[str, Any] = {}

async def get_tg() -> TelegramClient:
    """Stateless, multi-bot safe MTProto connection (receive_updates=False prevents session locks)."""
    global _client, _loop
    if not CFG["api_id"] or not CFG["session_string"]:
        raise RuntimeError("Missing TG_API_ID, TG_API_HASH, or TG_SESSION_STRING in Vercel Environment Variables")

    cur_loop = asyncio.get_running_loop()
    if _client is None or _loop != cur_loop:
        _loop = cur_loop
        _client = TelegramClient(
            StringSession(CFG["session_string"]),
            int(CFG["api_id"]),
            CFG["api_hash"],
            receive_updates=False,
            request_retries=5,
            connection_retries=5,
            entity_cache_limit=10
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

def compact_json(data: Any, status: int = 200, edge_cache: int = 0) -> JSONResponse:
    headers = {}
    if edge_cache > 0:
        headers["Cache-Control"] = f"public, s-maxage={edge_cache}, max-age={edge_cache}, stale-while-revalidate=600"
    return JSONResponse(data, status_code=status, headers=headers)

def format_meta(msg) -> Dict[str, Any]:
    f = None
    is_manifest = bool(msg.raw_text and msg.raw_text.startswith("#CHUNK_MANIFEST"))
    if msg.file:
        fname = msg.file.name or f"file_{msg.id}{msg.file.ext or ''}"
        f = {
            "n": fname,
            "b": msg.file.size or 0,
            "mb": round((msg.file.size or 0) / 1048576, 2),
            "m": msg.file.mime_type
        }
    elif is_manifest:
        try:
            meta = json.loads(msg.raw_text.split("\n", 1)[1])
            sz = int(meta.get("size", 0))
            f = {
                "n": meta.get("name", f"bundle_{msg.id}.bin"),
                "b": sz,
                "mb": round(sz / 1048576, 2),
                "m": "application/octet-stream",
                "chunked": True,
                "parts": len(meta.get("parts", []))
            }
        except Exception:
            pass
    return {
        "id": msg.id,
        "t": (msg.raw_text or "").split("\n")[0][:120],
        "f": f
    }

# =========================================================
# 1. AUTO-PROVISIONED DIRECT TELEGRAM TUNNEL (0 VERCEL DATA)
# =========================================================
async def _ensure_direct_tunnel(auto_create: bool = False) -> Dict[str, Any]:
    """
    Retrieves or auto-creates a personal Telegram Bot tunnel via @BotFather
    so browsers, curl, and webhooks can upload/download directly to api.telegram.org
    while Telethon copies pointers server-side into Saved Messages ('me')!
    """
    global _cached_tunnel
    if _cached_tunnel.get("bot_token") and _cached_tunnel.get("user_id"):
        return _cached_tunnel

    tg = await get_tg()
    me = await tg.get_me()
    user_id = me.id
    bot_token = CFG["bot_token"]
    bot_user = ""

    # Check Saved Messages KV for an existing auto-provisioned bot token
    if not bot_token:
        async for m in tg.iter_messages("me", limit=5, search="#KV__SYS_TUNNEL"):
            if m.raw_text and m.raw_text.startswith("#KV__SYS_TUNNEL"):
                try:
                    d = json.loads(m.raw_text.split("\n", 1)[1])
                    bot_token = d.get("bot_token", "")
                    bot_user = d.get("bot_user", "")
                    break
                except Exception:
                    pass

    # Auto-create via @BotFather if requested and not yet present
    if not bot_token and auto_create:
        suffix = f"{user_id % 10000}_{random.randint(100, 999)}"
        new_username = f"tgcloud_{suffix}_bot"
        async with tg.conversation("@BotFather", timeout=25) as conv:
            await conv.send_message("/newbot")
            await conv.get_response()
            await conv.send_message("TG Cloud Personal Tunnel")
            await conv.get_response()
            await conv.send_message(new_username)
            resp = await conv.get_response()
            match = re.search(r"(\d+:[A-Za-z0-9_-]{30,})", resp.raw_text or "")
            if match:
                bot_token = match.group(1)
                bot_user = new_username
                await tg.send_message(f"@{bot_user}", "/start")
                val_str = json.dumps({"bot_token": bot_token, "bot_user": bot_user})
                await tg.send_message("me", f"#KV__SYS_TUNNEL\n{val_str}")

    if bot_token and not bot_user:
        async with httpx.AsyncClient(timeout=10.0) as http:
            r = await http.get(f"https://api.telegram.org/bot{bot_token}/getMe")
            if r.status_code == 200:
                bot_user = r.json().get("result", {}).get("username", "")
                if bot_user:
                    try:
                        await tg.send_message(f"@{bot_user}", "/start")
                    except Exception:
                        pass

    _cached_tunnel = {
        "user_id": user_id,
        "bot_token": bot_token,
        "bot_user": bot_user
    }
    return _cached_tunnel

async def handle_ticket(request: Request):
    """GET /api/ticket -> Returns MTProto & Direct Tunnel credentials (<300 Bytes)."""
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    if not CFG["api_id"] or not CFG["session_string"]:
        return compact_json({"ok": 0, "err": "Missing Vercel Env Vars"}, 400)

    provision = request.query_params.get("provision") == "1"
    try:
        tunnel = await _ensure_direct_tunnel(auto_create=provision)
    except Exception:
        tunnel = {"user_id": 0, "bot_token": CFG["bot_token"], "bot_user": ""}

    return compact_json({
        "ok": 1,
        "id": CFG["api_id"],
        "hash": CFG["api_hash"],
        "s": CFG["session_string"],
        "uid": tunnel.get("user_id", 0),
        "bt": tunnel.get("bot_token", ""),
        "bu": tunnel.get("bot_user", "")
    }, edge_cache=3600 if not provision and not CFG["api_key"] else 0)

async def handle_sync_pointer(request: Request):
    """
    POST /api/sync
    After Browser/Curl uploads a file directly to api.telegram.org (0 Bytes on Vercel),
    this endpoint copies the 64-byte media pointer inside Telegram Data Center straight
    into Saved Messages ('me') and deletes the temp bot message!
    """
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    try:
        body = await request.json()
        tg = await get_tg()
        tunnel = await _ensure_direct_tunnel(auto_create=False)
        bot_user = body.get("bot_user") or tunnel.get("bot_user")
        tag = str(body.get("tag", "#storage")).strip()
        fname = str(body.get("filename", "file.bin")).strip()

        # Multi-part large file manifest (>45MB browser upload)
        if body.get("parts"):
            manifest = {
                "name": fname,
                "size": body.get("total_size", 0),
                "tag": tag,
                "parts": body["parts"]
            }
            m = await tg.send_message("me", f"#CHUNK_MANIFEST {tag} {fname}\n" + json.dumps(manifest, separators=(",", ":")))
            return compact_json({"ok": 1, "id": m.id, "name": fname, "vercel_bytes": 0})

        # Single file pointer copy inside Telegram DC (0 Bytes through Vercel!)
        bot_msg_id = int(body["bot_msg_id"])
        if bot_user:
            msg = await tg.get_messages(f"@{bot_user}", ids=bot_msg_id)
            if msg and msg.media:
                saved = await tg.send_file(
                    "me",
                    file=msg.media,
                    caption=f"{tag} {fname}".strip(),
                    force_document=True,
                    attributes=[DocumentAttributeFilename(file_name=fname)]
                )
                await tg.delete_messages(f"@{bot_user}", [bot_msg_id])
                return compact_json({"ok": 1, "item": format_meta(saved), "vercel_bytes": 0})

        return compact_json({"ok": 0, "err": "Media pointer not found"}, 404)
    except Exception as e:
        return compact_json({"ok": 0, "err": str(e)}, 500)

async def handle_direct_dl_redirect(request: Request, mid: int):
    """
    GET /dl/{id}
    Copies the Saved Messages file pointer server-side to the tunnel bot (0 Bytes on Vercel)
    and issues an HTTP 302 Redirect directly to https://api.telegram.org/file/bot...
    """
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    try:
        tg = await get_tg()
        tunnel = await _ensure_direct_tunnel(auto_create=True)
        bot_token = tunnel.get("bot_token")
        bot_user = tunnel.get("bot_user")
        if not bot_token or not bot_user:
            return compact_json({"ok": 0, "err": "Direct tunnel not initialized. Open Dashboard and click Auto-Enable Direct Tunnel."}, 400)

        msg = await tg.get_messages("me", ids=mid)
        if not msg:
            return compact_json({"ok": 0, "err": "File not found"}, 404)

        # If chunked manifest, return JSON manifest of direct part file_ids
        if msg.raw_text and msg.raw_text.startswith("#CHUNK_MANIFEST"):
            meta = json.loads(msg.raw_text.split("\n", 1)[1])
            return compact_json({"ok": 1, "chunked": True, "manifest": meta, "bot_token": bot_token})

        if not msg.file:
            return compact_json({"ok": 0, "err": "Message has no file"}, 400)

        if (msg.file.size or 0) > 20 * 1024 * 1024:
            return compact_json({
                "ok": 0,
                "err": "File > 20MB (Telegram Bot CDN limit). Use 'python tgcloud.py dl " + str(mid) + "' for 4x Parallel Direct MTProto download (up to 4GB)!"
            }, 400)

        # Server-side pointer copy to bot -> get file_path -> 302 Redirect to Telegram CDN!
        fwd = await tg.send_file(f"@{bot_user}", file=msg.media)
        async with httpx.AsyncClient(timeout=10.0) as http:
            # Fetch updates or getChat/forward via Bot API to resolve file_path
            r = await http.post(
                f"https://api.telegram.org/bot{bot_token}/ copyMessage".replace(" ", ""),
                json={"chat_id": tunnel["user_id"], "from_chat_id": tunnel["user_id"], "message_id": fwd.id}
            )
            await tg.delete_messages(f"@{bot_user}", [fwd.id])
            if r.status_code == 200 and r.json().get("ok"):
                copied_id = r.json()["result"]["message_id"]
                # Forward to self to read document.file_id from Bot API response
                r2 = await http.post(
                    f"https://api.telegram.org/bot{bot_token}/forwardMessage",
                    json={"chat_id": tunnel["user_id"], "from_chat_id": tunnel["user_id"], "message_id": copied_id}
                )
                await http.post(f"https://api.telegram.org/bot{bot_token}/deleteMessage", json={"chat_id": tunnel["user_id"], "message_id": copied_id})
                if r2.status_code == 200 and r2.json().get("ok"):
                    res_obj = r2.json()["result"]
                    f_id = (
                        res_obj.get("document", {}).get("file_id")
                        or (res_obj.get("photo", [{}])[-1].get("file_id") if res_obj.get("photo") else None)
                        or res_obj.get("video", {}).get("file_id")
                        or res_obj.get("audio", {}).get("file_id")
                    )
                    await http.post(f"https://api.telegram.org/bot{bot_token}/deleteMessage", json={"chat_id": tunnel["user_id"], "message_id": res_obj["message_id"]})
                    if f_id:
                        r3 = await http.get(f"https://api.telegram.org/bot{bot_token}/getFile", params={"file_id": f_id})
                        fpath = r3.json().get("result", {}).get("file_path")
                        if fpath:
                            cdn_url = f"https://api.telegram.org/file/bot{bot_token}/{fpath}"
                            if request.query_params.get("json") == "1":
                                return compact_json({"ok": 1, "cdn_url": cdn_url})
                            return RedirectResponse(url=cdn_url, status_code=302)

        return compact_json({"ok": 0, "err": "Use tgcloud.py SDK for this file type"}, 400)
    except Exception as e:
        return compact_json({"ok": 0, "err": str(e)}, 500)

# =========================================================
# 2. METADATA, KV DATABASE & SDK ENDPOINTS
# =========================================================
async def handle_health(request: Request):
    env_ready = bool(CFG["api_id"] and CFG["api_hash"] and CFG["session_string"])
    return compact_json({
        "ok": 1,
        "engine": "TG-Cloud V12 Final Hard-Brick Edition",
        "env_configured": env_ready,
        "vercel_file_transfer": "0.00 Bytes (Hard-Bricked 8KB Firewall + Direct Telegram Bypass)",
        "multi_bot_safe": True
    }, edge_cache=3600)

async def handle_sdk(request: Request):
    for p in (
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "tgcloud.py"),
        os.path.join(os.path.dirname(__file__), "tgcloud.py"),
        "tgcloud.py"
    ):
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return PlainTextResponse(f.read(), headers={"Cache-Control": "public, s-maxage=31536000, max-age=31536000, immutable"})
    return PlainTextResponse("# tgcloud.py not found\n", status_code=404)

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
                    if key_name.startswith("_SYS_"):
                        continue
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
# 3. ZERO-VERCEL-BANDWIDTH DIRECT UPLOAD/DOWNLOAD DASHBOARD
# =========================================================
async def handle_dashboard(request: Request):
    return HTMLResponse("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TG-CLOUD V12 // HARD-BRICK ZERO-VERCEL</title>
<style>
:root{--bg:#040406;--card:rgba(14,16,22,0.95);--bd:rgba(255,255,255,0.1);--ac:#fff;--gr:#00ff9d;--rd:#ff4d4d;--tx:#e2e8f0;--mt:#94a3b8}
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
      <span>🛡️ TG-CLOUD V12 // HARD-BRICK ZERO-VERCEL ENGINE</span>
      <span id="tStatus" style="color:var(--gr);font-size:.74rem">0.00 BYTES VERCEL TRANSFER</span>
    </h2>
    <div style="display:flex;gap:8px">
      <input type="password" id="key" placeholder="Optional API Key (if configured)">
      <button onclick="initTunnel(true)" style="width:240px;background:var(--gr)">⚡ AUTO-LINK DIRECT TUNNEL</button>
    </div>
  </div>

  <div class="grid">
    <div class="box">
      <h2>📤 DIRECT BROWSER -> TELEGRAM UPLOAD (0 VERCEL DATA)</h2>
      <input type="file" id="files" multiple>
      <input type="text" id="utag" value="#storage" placeholder="Tag (#storage, #temp, #bot)">
      <button onclick="directUpload()">UPLOAD DIRECTLY TO SAVED MESSAGES</button>
      <div id="ulog" class="log"></div>
    </div>

    <div class="box">
      <h2>🗄️ CLOUD KV DATABASE (0 SQLITE)</h2>
      <div style="display:flex;gap:6px">
        <input type="text" id="kvk" placeholder="Key (e.g. app_config)">
        <input type="text" id="kvv" placeholder='Value / JSON (e.g. {"pro":true})'>
      </div>
      <div style="display:flex;gap:6px">
        <button onclick="kvSet()">SAVE KEY</button>
        <button onclick="kvGet()" style="background:var(--mt)">READ KEY</button>
      </div>
      <div id="klog" class="log"></div>
    </div>
  </div>

  <div class="box">
    <h2>🗂️ SAVED MESSAGES EXPLORER</h2>
    <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">
      <span class="pill" onclick="setFilter('')">All</span>
      <span class="pill" onclick="setFilter('#storage')">📦 #storage</span>
      <span class="pill" onclick="setFilter('#TEMP')">⏳ #TEMP</span>
      <span class="pill" onclick="setFilter('#remote')">🌐 #remote</span>
      <span class="pill" onclick="setFilter('#KV_')">🗄️ #KV_</span>
    </div>
    <div style="display:flex;gap:8px">
      <input type="text" id="q" placeholder="Search filename or #tag..." onkeyup="if(event.key==='Enter')loadFiles()">
      <button onclick="loadFiles()" style="width:140px">REFRESH</button>
    </div>
    <div id="list" style="margin-top:10px"></div>
  </div>
</div>

<script>
const getKey = () => document.getElementById('key').value.trim();
let tunnel = null;

async function initTunnel(forceProvision = false) {
  const st = document.getElementById('tStatus');
  if (!forceProvision) {
    const cached = JSON.parse(localStorage.getItem('tg_v12_tunnel') || 'null');
    if (cached && cached.bt && cached.uid) {
      tunnel = cached;
      st.innerText = `🟢 DIRECT TUNNEL READY (@${tunnel.bu} | 0 VERCEL DATA)`;
      return tunnel;
    }
  }
  st.innerText = '⏳ LINKING DIRECT TELEGRAM TUNNEL...';
  const r = await fetch(`/api/ticket?provision=${forceProvision ? '1' : '0'}&key=${encodeURIComponent(getKey())}`);
  const d = await r.json();
  if (d.ok && d.bt) {
    tunnel = d;
    localStorage.setItem('tg_v12_tunnel', JSON.stringify(d));
    st.innerText = `🟢 DIRECT TUNNEL READY (@${d.bu} | 0 VERCEL DATA)`;
  } else {
    st.innerText = '⚡ CLICK "AUTO-LINK DIRECT TUNNEL" FOR BROWSER UPLOADS';
  }
  return tunnel;
}

// Uploads 100% directly from Browser -> api.telegram.org (0 Bytes on Vercel!)
async function directUpload() {
  const fl = document.getElementById('files').files;
  if (!fl.length) return alert('Select file(s) first!');
  const log = document.getElementById('ulog');
  const tag = document.getElementById('utag').value || '#storage';

  const tk = await initTunnel(true);
  if (!tk || !tk.bt) return alert('Could not initialize direct tunnel');

  for (let i = 0; i < fl.length; i++) {
    const f = fl[i];
    const t0 = Date.now();
    const CHUNK = 19 * 1024 * 1024; // 19 MB Direct Bot API chunks

    if (f.size <= 45 * 1024 * 1024) {
      log.innerText = `⚡ Uploading ${f.name} directly to Telegram Servers (0 Vercel Bytes)...`;
      const fd = new FormData();
      fd.append('chat_id', tk.uid);
      fd.append('document', f, f.name);
      const r = await fetch(`https://api.telegram.org/bot${tk.bt}/sendDocument`, { method: 'POST', body: fd });
      const d = await r.json();
      if (!d.ok) { log.innerText = '❌ ' + d.description; return; }

      // Copy 64-byte pointer into Saved Messages ('me')
      await fetch(`/api/sync?key=${encodeURIComponent(getKey())}`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ bot_msg_id: d.result.message_id, bot_user: tk.bu, filename: f.name, tag })
      });
    } else {
      const totalParts = Math.ceil(f.size / CHUNK);
      const parts = new Array(totalParts);
      let done = 0, next = 0;

      async function worker() {
        while (next < totalParts) {
          const idx = next++;
          const blob = f.slice(idx * CHUNK, Math.min((idx + 1) * CHUNK, f.size));
          const fd = new FormData();
          fd.append('chat_id', tk.uid);
          fd.append('document', blob, `${f.name}.part${idx}`);
          for (let retry = 0; retry < 10; retry++) {
            try {
              const r = await fetch(`https://api.telegram.org/bot${tk.bt}/sendDocument`, { method: 'POST', body: fd });
              const d = await r.json();
              if (d.ok && d.result.document) {
                parts[idx] = { p: idx, fid: d.result.document.file_id, mid: d.result.message_id, s: blob.size };
                done++;
                const spd = (((done * CHUNK) / 1048576) / Math.max((Date.now() - t0)/1000, 0.1)).toFixed(2);
                log.innerText = `⚡ Direct Upload ${f.name}: ${((done/totalParts)*100).toFixed(1)}% (${spd} MB/s | 0 Vercel Load)...`;
                break;
              }
            } catch (e) {}
            await new Promise(res => setTimeout(res, 1500));
          }
        }
      }
      await Promise.all(Array.from({length: Math.min(4, totalParts)}, () => worker()));
      await fetch(`/api/sync?key=${encodeURIComponent(getKey())}`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ filename: f.name, total_size: f.size, tag, parts })
      });
    }
  }
  log.innerText = '✅ Uploaded Directly to Saved Messages (0.00 Bytes Vercel Bandwidth Used)!';
  document.getElementById('files').value = '';
  loadFiles();
}

async function directDownload(id, name, mb) {
  const log = document.getElementById('ulog');
  if (mb > 20) {
    const cmd = `python tgcloud.py dl ${id} "${name}"`;
    navigator.clipboard.writeText(cmd);
    log.innerText = `📋 Copied 4x Parallel Direct MTProto Command (>20MB): ${cmd}`;
    return;
  }
  log.innerText = `⚡ Resolving Direct Telegram CDN Link for ${name} (0 Vercel Transfer)...`;
  window.open(`/dl/${id}?key=${encodeURIComponent(getKey())}`, '_blank');
}

async function kvSet() {
  const k = document.getElementById('kvk').value.trim();
  const v = document.getElementById('kvv').value.trim();
  if (!k) return;
  let parsed = v;
  try { parsed = JSON.parse(v); } catch(e) {}
  const r = await fetch(`/kv/${encodeURIComponent(k)}?key=${encodeURIComponent(getKey())}`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({v: parsed})
  });
  document.getElementById('klog').innerText = JSON.stringify(await r.json());
  loadFiles();
}

async function kvGet() {
  const k = document.getElementById('kvk').value.trim();
  if (!k) return;
  const r = await fetch(`/kv/${encodeURIComponent(k)}?key=${encodeURIComponent(getKey())}`);
  document.getElementById('klog').innerText = JSON.stringify(await r.json());
}

async function delItem(id) {
  await fetch(`/delete/${id}?key=${encodeURIComponent(getKey())}`, { method: 'DELETE' });
  loadFiles();
}

function setFilter(q) {
  document.getElementById('q').value = q;
  loadFiles();
}

async function loadFiles() {
  const list = document.getElementById('list');
  const q = document.getElementById('q').value.trim();
  try {
    const r = await fetch(`/files?limit=30&q=${encodeURIComponent(q)}&key=${encodeURIComponent(getKey())}`);
    const d = await r.json();
    if (!d.ok) {
      list.innerHTML = `<div class="log" style="color:var(--rd)">❌ ${d.err}</div>`;
      return;
    }
    list.innerHTML = (d.items || []).map(i => {
      const f = i.f;
      const safeName = f ? f.n.replace(/'/g, "\\'") : 'file.bin';
      return `<div class="row">
        <div style="word-break:break-all">
          <strong>#${i.id}</strong> ${f ? f.n : i.t}
          <span class="tag">${f ? '(' + f.mb + ' MB)' : '[MSG]'}</span>
        </div>
        <div style="white-space:nowrap">
          ${f ? `<a onclick="directDownload(${i.id},'${safeName}',${f.mb})" style="color:var(--gr)">[⚡DIRECT DL]</a>` : ''}
          <a onclick="delItem(${i.id})" style="color:var(--rd)">[X]</a>
        </div>
      </div>`;
    }).join('') || '<div class="log">No items found.</div>';
  } catch (e) {
    list.innerHTML = `<div class="log" style="color:var(--rd)">❌ ${e.message}</div>`;
  }
}
initTunnel(false);
loadFiles();
</script>
</body>
</html>""")

# =========================================================
# 4. MASTER ROUTER + HARD-BRICK 8KB ASGI FIREWALL
# =========================================================
async def master_router(request: Request):
    raw_path = (
        request.query_params.get("__path")
        or request.headers.get("x-matched-path")
        or request.url.path
        or "/"
    ).strip()

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
    if path in ("/sync", "/api/sync"):
        return await handle_sync_pointer(request)
    if path.endswith("/tgcloud.py") or path in ("/sdk", "/api/sdk"):
        return await handle_sdk(request)
    if path in ("/files", "/api/files"):
        return await handle_files(request)
    if path.startswith("/dl/") or path.startswith("/api/dl/"):
        parts = [p for p in path.split("/") if p]
        mid = _safe_int(parts[-1] if parts[-1].isdigit() else parts[1])
        return await handle_direct_dl_redirect(request, mid)
    if path == "/kv" or path.startswith("/kv/") or path.startswith("/api/kv"):
        k = path.split("/kv")[-1].strip("/")
        return await handle_kv(request, k)
    if path.startswith("/delete/") or path.startswith("/api/delete/"):
        mid = _safe_int(path.split("/delete/")[-1].strip("/"))
        return await handle_delete(request, mid)

    return await handle_dashboard(request)

_starlette = Starlette(
    routes=[
        Route("/", master_router, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]),
        Route("/{any_path:path}", master_router, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]),
    ],
    middleware=[Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])]
)

async def app(scope, receive, send):
    """
    HARD-BRICK ZERO-VERCEL-LOAD FIREWALL:
    Physically rejects any HTTP request body > 8 KB at the ASGI boundary before reading a single byte!
    Guarantees Vercel Fast Data Transfer / Origin Transfer can NEVER be used for files.
    """
    if scope["type"] == "http":
        headers = dict(scope.get("headers") or [])
        cl = int(headers.get(b"content-length", b"0") or b"0")
        if cl > 8192:
            resp = JSONResponse({
                "ok": 0,
                "err": "HARD-BRICK FIREWALL: File bytes (>8KB) are blocked on Vercel to guarantee 0% bandwidth usage. Use tgcloud.py SDK or Direct Browser Tunnel (api.telegram.org)!"
            }, status_code=413)
            await resp(scope, receive, send)
            return
    await _starlette(scope, receive, send)
