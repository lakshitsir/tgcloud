import os
import io
import gc
import json
import time
import random
import asyncio
from typing import Optional, Dict, Any
from starlette.applications import Starlette
from starlette.responses import JSONResponse, HTMLResponse, PlainTextResponse, StreamingResponse
from starlette.routing import Route
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.upload import SaveBigFilePartRequest
from telethon.tl.types import InputFileBig, DocumentAttributeFilename

PART_SIZE = 512 * 1024  # 512 KB MTProto Part Size

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
            request_retries=10,
            connection_retries=10,
            retry_delay=1,
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
        fname = msg.file.name or f"file_{msg.id}{msg.file.ext or ''}"
        f = {
            "n": fname,
            "b": msg.file.size or 0,
            "mb": round((msg.file.size or 0) / 1048576, 2),
            "m": msg.file.mime_type,
            "dl": f"/dl/{msg.id}/{fname}",
            "chunk": f"/chunk/{msg.id}?offset=0&length=65536"
        }
    return {
        "id": msg.id,
        "t": (msg.raw_text or "")[:120],
        "f": f
    }

# =========================================================
# 1. CORE METADATA, TICKET & SDK ENDPOINTS
# =========================================================
async def handle_health(request: Request):
    env_ready = bool(CFG["api_id"] and CFG["api_hash"] and CFG["session_string"])
    return compact_json({
        "ok": 1,
        "engine": "TG-Cloud V10.1 Final (Zero-Error Native Engine)",
        "env_configured": env_ready,
        "vercel_storage_used": "0 Bytes",
        "multi_bot_safe": True
    })

async def handle_ticket(request: Request):
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized API Key"}, 401)
    if not CFG["api_id"] or not CFG["session_string"]:
        return compact_json({
            "ok": 0,
            "err": "Set TG_API_ID, TG_API_HASH, and TG_SESSION_STRING in Vercel Environment Variables, then Redeploy."
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
# 2. STATELESS PARALLEL MTPROTO CHUNK UPLOADER & STREAMER
# =========================================================
async def handle_single_upload(request: Request, fname: str):
    """For small files (<3.5MB) or raw JSON/text."""
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    try:
        tg = await get_tg()
        tag = request.query_params.get("tag", "#storage")
        raw_bytes = await request.body()
        bio = io.BytesIO(raw_bytes)
        bio.name = fname
        msg = await tg.send_file(
            "me",
            file=bio,
            caption=f"{tag} {fname}".strip(),
            force_document=True,
            attributes=[DocumentAttributeFilename(file_name=fname)]
        )
        bio.close()
        del raw_bytes
        gc.collect()
        return compact_json({"ok": 1, "item": format_meta(msg)})
    except Exception as e:
        return compact_json({"ok": 0, "err": str(e)}, 500)

async def handle_up_part(request: Request):
    """
    Receives a 2MB slice (up to 4 x 512KB MTProto parts) from the Browser UI and pushes
    all 512KB parts concurrently to Telegram via SaveBigFilePartRequest with 0 disk usage!
    """
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    try:
        tg = await get_tg()
        file_id = int(request.query_params["file_id"])
        base_part = int(request.query_params["base_part"])
        total_parts = int(request.query_params["total_parts"])
        raw_slice = await request.body()

        tasks = []
        sub_idx = 0
        for offset in range(0, len(raw_slice), PART_SIZE):
            part_bytes = raw_slice[offset:offset + PART_SIZE]
            p_num = base_part + sub_idx
            tasks.append(
                tg(SaveBigFilePartRequest(
                    file_id=file_id,
                    file_part=p_num,
                    file_total_parts=total_parts,
                    bytes=part_bytes
                ))
            )
            sub_idx += 1

        await asyncio.gather(*tasks)
        del raw_slice
        gc.collect()
        return compact_json({"ok": 1, "parts_saved": sub_idx})
    except Exception as e:
        return compact_json({"ok": 0, "err": str(e)}, 500)

async def handle_up_commit(request: Request):
    """Finalizes a multi-part MTProto upload in Saved Messages."""
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    try:
        tg = await get_tg()
        body = await request.json()
        file_id = int(body["file_id"])
        total_parts = int(body["total_parts"])
        fname = str(body.get("filename", "file.bin"))
        tag = str(body.get("tag", "#storage"))

        input_file = InputFileBig(id=file_id, parts=total_parts, name=fname)
        msg = await tg.send_file(
            "me",
            file=input_file,
            caption=f"{tag} {fname}".strip(),
            force_document=True,
            attributes=[DocumentAttributeFilename(file_name=fname)]
        )
        return compact_json({"ok": 1, "item": format_meta(msg)})
    except Exception as e:
        return compact_json({"ok": 0, "err": str(e)}, 500)

async def handle_chunk_or_dl(request: Request, mid: int):
    """4KB-aligned selective chunk reader (/chunk/{id} & /dl/{id})."""
    if not check_auth(request):
        return compact_json({"ok": 0, "err": "Unauthorized"}, 401)
    try:
        tg = await get_tg()
        msg = await tg.get_messages("me", ids=mid)
        if not msg or not msg.file:
            return compact_json({"ok": 0, "err": "File not found"}, 404)

        total_size = msg.file.size or 0
        fname = msg.file.name or f"file_{mid}.bin"
        offset = _safe_int(request.query_params.get("offset", 0))
        length = _safe_int(request.query_params.get("length", min(total_size - offset, 2097152)))
        length = min(length, total_size - offset, 3145728)

        aligned_offset = (offset // 4096) * 4096
        skip = offset - aligned_offset
        needed = length + skip

        buf = bytearray()
        async for chunk in tg.iter_download(msg.media, offset=aligned_offset, request_size=PART_SIZE):
            buf.extend(chunk)
            if len(buf) >= needed:
                break

        data = bytes(buf[skip:skip + length])
        buf.clear()
        gc.collect()

        if request.query_params.get("as") == "text":
            return PlainTextResponse(data.decode("utf-8", errors="replace"))

        headers = {
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Content-Length": str(len(data)),
            "Content-Range": f"bytes {offset}-{offset + len(data) - 1}/{total_size}",
            "X-Total-Size": str(total_size)
        }
        return StreamingResponse(io.BytesIO(data), media_type=msg.file.mime_type or "application/octet-stream", headers=headers)
    except Exception as e:
        return compact_json({"ok": 0, "err": str(e)}, 500)

# =========================================================
# 3. ZERO-DEPENDENCY NATIVE DASHBOARD UI (NO BROKEN CDN!)
# =========================================================
async def handle_dashboard(request: Request):
    return HTMLResponse("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TG-CLOUD V10.1 MAX // ZERO-ERROR</title>
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
      <span>⚡ TG-CLOUD V10.1 MAX // NATIVE PARALLEL ENGINE</span>
      <span id="st" style="color:var(--gr);font-size:.74rem">ONLINE (0 DISK)</span>
    </h2>
    <input type="password" id="key" placeholder="Optional API Key (leave empty if none)">
  </div>

  <div class="grid">
    <div class="box">
      <h2>📤 PARALLEL CHUNK UPLOAD (UNLIMITED SIZE)</h2>
      <input type="file" id="files" multiple>
      <input type="text" id="utag" value="#storage" placeholder="Tag (#storage, #backup, #bot)">
      <button onclick="uploadFiles()" id="upBtn">UPLOAD LOSSLESS TO TELEGRAM</button>
      <div id="ulog" class="log"></div>
    </div>

    <div class="box">
      <h2>🗄️ CLOUD KV DATABASE (0 SQLITE)</h2>
      <div style="display:flex;gap:6px">
        <input type="text" id="kvk" placeholder="Key (e.g. config)">
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
      <span class="pill" onclick="setFilter('#AI_MODEL')">🧠 #AI_MODEL</span>
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

async function uploadFiles() {
  const fl = document.getElementById('files').files;
  if (!fl.length) return alert('Select file(s) first!');
  const log = document.getElementById('ulog');
  const tag = document.getElementById('utag').value || '#storage';

  for (let idx = 0; idx < fl.length; idx++) {
    const f = fl[idx];
    const t0 = Date.now();

    // Small file (<3MB) -> Single fast request
    if (f.size <= 3 * 1024 * 1024) {
      log.innerText = `⚡ Uploading ${f.name} (${(f.size/1048576).toFixed(2)} MB)...`;
      const r = await fetch(`/up/${encodeURIComponent(f.name)}?tag=${encodeURIComponent(tag)}&key=${encodeURIComponent(getKey())}`, {
        method: 'PUT',
        body: f
      });
      const d = await r.json();
      if (!d.ok) { log.innerText = '❌ ' + d.err; return; }
      continue;
    }

    // Large file (>3MB to 2GB) -> Slice into 2MB (4 x 512KB) chunks & upload in parallel!
    const PART_BYTES = 512 * 1024;
    const SLICE_BYTES = 4 * PART_BYTES; // 2 MB per HTTP call
    const totalParts = Math.ceil(f.size / PART_BYTES);
    const totalSlices = Math.ceil(f.size / SLICE_BYTES);
    const fileId = Math.floor(Math.random() * 9000000000000000) + 1000000000000;

    let completedSlices = 0;
    let nextSlice = 0;
    const workers = 3;

    async function sliceWorker() {
      while (nextSlice < totalSlices) {
        const sIdx = nextSlice++;
        const startByte = sIdx * SLICE_BYTES;
        const endByte = Math.min(startByte + SLICE_BYTES, f.size);
        const blob = f.slice(startByte, endByte);
        const basePart = sIdx * 4;

        for (let retry = 0; retry < 10; retry++) {
          try {
            const r = await fetch(`/api/up-part?file_id=${fileId}&base_part=${basePart}&total_parts=${totalParts}&key=${encodeURIComponent(getKey())}`, {
              method: 'POST',
              body: blob
            });
            const d = await r.json();
            if (d.ok) break;
          } catch (e) {}
          await new Promise(res => setTimeout(res, 1500));
        }
        completedSlices++;
        const pct = ((completedSlices / totalSlices) * 100).toFixed(1);
        const sec = Math.max((Date.now() - t0) / 1000, 0.1);
        const spd = (((completedSlices * SLICE_BYTES) / 1048576) / sec).toFixed(2);
        log.innerText = `⚡ Uploading ${f.name}: ${pct}% (${spd} MB/s)...`;
      }
    }

    await Promise.all(Array.from({length: Math.min(workers, totalSlices)}, () => sliceWorker()));

    log.innerText = `⏳ Finalizing ${f.name} in Saved Messages...`;
    const cr = await fetch(`/api/up-commit?key=${encodeURIComponent(getKey())}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ file_id: fileId, total_parts: totalParts, filename: f.name, tag })
    });
    const cd = await cr.json();
    if (!cd.ok) { log.innerText = '❌ ' + cd.err; return; }
  }

  log.innerText = '✅ 100% Lossless Upload Complete!';
  document.getElementById('files').value = '';
  loadFiles();
}

async function fastDownload(id, name, totalBytes) {
  const log = document.getElementById('ulog');
  if (totalBytes <= 3 * 1024 * 1024) {
    window.open(`/dl/${id}/${encodeURIComponent(name)}?key=${encodeURIComponent(getKey())}`, '_blank');
    return;
  }

  // Parallel 2MB Chunked Downloader for large files
  const CHUNK = 2 * 1024 * 1024;
  const count = Math.ceil(totalBytes / CHUNK);
  const buffers = new Array(count);
  let done = 0, next = 0;
  const t0 = Date.now();

  async function dlWorker() {
    while (next < count) {
      const i = next++;
      const off = i * CHUNK;
      const len = Math.min(CHUNK, totalBytes - off);
      for (let retry = 0; retry < 10; retry++) {
        try {
          const r = await fetch(`/chunk/${id}?offset=${off}&length=${len}&key=${encodeURIComponent(getKey())}`);
          if (r.ok) {
            buffers[i] = await r.arrayBuffer();
            break;
          }
        } catch (e) {}
        await new Promise(res => setTimeout(res, 1500));
      }
      done++;
      const pct = ((done / count) * 100).toFixed(1);
      const spd = (((done * CHUNK) / 1048576) / Math.max((Date.now() - t0)/1000, 0.1)).toFixed(2);
      log.innerText = `⚡ Downloading ${name}: ${pct}% (${spd} MB/s)...`;
    }
  }

  await Promise.all(Array.from({length: Math.min(4, count)}, () => dlWorker()));
  const blob = new Blob(buffers);
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = name; a.click();
  URL.revokeObjectURL(url);
  log.innerText = `✅ Downloaded ${name}!`;
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
  const d = await r.json();
  document.getElementById('klog').innerText = JSON.stringify(d);
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
      list.innerHTML = `<div class="log" style="color:var(--rd)">❌ ${d.err || 'Error loading files'}</div>`;
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
          ${f ? `<a onclick="fastDownload(${i.id},'${safeName}',${f.b})" style="color:var(--gr)">[⚡FAST DL]</a>` : ''}
          ${f ? `<a href="/chunk/${i.id}?offset=0&length=4096&as=text&key=${encodeURIComponent(getKey())}" target="_blank" style="color:var(--mt)">[PEEK]</a>` : ''}
          <a onclick="delItem(${i.id})" style="color:var(--rd)">[X]</a>
        </div>
      </div>`;
    }).join('') || '<div class="log">No items found.</div>';
  } catch (e) {
    list.innerHTML = `<div class="log" style="color:var(--rd)">❌ ${e.message}</div>`;
  }
}
loadFiles();
</script>
</body>
</html>""")

# =========================================================
# 4. UNIVERSAL CATCH-ALL ROUTER
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
    if path.endswith("/tgcloud.py") or path in ("/sdk", "/api/sdk"):
        return await handle_sdk(request)
    if path in ("/files", "/api/files"):
        return await handle_files(request)
    if path == "/api/up-part":
        return await handle_up_part(request)
    if path == "/api/up-commit":
        return await handle_up_commit(request)
    if path.startswith("/up/"):
        fname = path.split("/up/", 1)[1].strip("/") or "file.bin"
        return await handle_single_upload(request, fname)
    if path.startswith("/chunk/") or path.startswith("/dl/"):
        parts = [p for p in path.split("/") if p]
        mid = _safe_int(parts[1]) if len(parts) > 1 else 0
        return await handle_chunk_or_dl(request, mid)
    if path == "/kv" or path.startswith("/kv/") or path.startswith("/api/kv"):
        k = path.split("/kv")[-1].strip("/")
        return await handle_kv(request, k)
    if path.startswith("/delete/") or path.startswith("/api/delete/"):
        mid = _safe_int(path.split("/delete/")[-1].strip("/"))
        return await handle_delete(request, mid)

    return await handle_dashboard(request)

routes = [
    Route("/", master_router, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]),
    Route("/{any_path:path}", master_router, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]),
]

app = Starlette(
    routes=routes,
    middleware=[Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])]
    )
        
