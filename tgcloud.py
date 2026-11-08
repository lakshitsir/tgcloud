#!/usr/bin/env python3
"""
TG-Cloud V10 MAX // Universal Zero-Vercel-Load Direct MTProto SDK & CLI
- 100% Direct Bot/Device <-> Telegram Data Center (0 Bytes Vercel Bandwidth/Storage).
- 4x Parallel Async Workers (512KB MTProto Chunks) + 15x Anti-Breakable Auto-Retry.
- Multi-Bot Safe: Uses receive_updates=False so 100+ bots can run simultaneously.
"""
import os
import sys
import io
import gc
import time
import json
import random
import hashlib
import asyncio
from typing import Optional, Dict, Any, Union
import httpx
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import InputFileBig, InputFile, DocumentAttributeFilename

PART_SIZE = 512 * 1024  # 512 KB Max MTProto Chunk
PARALLEL_WORKERS = 4    # 4x Concurrent Workers (Strict 2 MB RAM Buffer Cap)

VERCEL_URL = os.environ.get("TG_VERCEL_URL", "https://tgclodd.vercel.app").rstrip("/")
API_KEY = os.environ.get("TG_API_KEY", "")
LOCAL_CFG = os.path.expanduser("~/.tg_cloud_ticket.json")
TERMUX_CFG = os.path.expanduser("~/tg_cloud_config.json")

_cached_creds: Optional[Dict[str, Any]] = None

def configure(
    vercel_url: Optional[str] = None,
    api_key: Optional[str] = None,
    api_id: Optional[int] = None,
    api_hash: Optional[str] = None,
    session_string: Optional[str] = None
):
    global VERCEL_URL, API_KEY, _cached_creds
    if vercel_url:
        VERCEL_URL = vercel_url.rstrip("/")
    if api_key is not None:
        API_KEY = api_key
    if api_id and api_hash and session_string:
        _cached_creds = {"id": int(api_id), "hash": api_hash, "s": session_string}

def _get_creds() -> Dict[str, Any]:
    global _cached_creds
    if _cached_creds:
        return _cached_creds

    if os.environ.get("TG_API_ID") and os.environ.get("TG_SESSION_STRING"):
        _cached_creds = {
            "id": int(os.environ["TG_API_ID"]),
            "hash": os.environ["TG_API_HASH"],
            "s": os.environ["TG_SESSION_STRING"]
        }
        return _cached_creds

    for path in (TERMUX_CFG, LOCAL_CFG):
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    d = json.load(f)
                s = d.get("s") or d.get("session_string")
                aid = d.get("id") or d.get("api_id")
                ahash = d.get("hash") or d.get("api_hash")
                if s and aid and ahash:
                    _cached_creds = {"id": int(aid), "hash": ahash, "s": s}
                    return _cached_creds
            except Exception:
                pass

    headers = {"x-api-key": API_KEY} if API_KEY else {}
    r = httpx.get(f"{VERCEL_URL}/api/ticket", headers=headers, timeout=15.0)
    r.raise_for_status()
    tk = r.json()
    if not tk.get("ok"):
        raise RuntimeError(tk.get("err", "Failed to fetch ticket"))
    _cached_creds = {"id": int(tk["id"]), "hash": tk["hash"], "s": tk["s"]}
    try:
        with open(LOCAL_CFG, "w", encoding="utf-8") as f:
            json.dump(_cached_creds, f)
    except Exception:
        pass
    return _cached_creds

def _make_client() -> TelegramClient:
    c = _get_creds()
    return TelegramClient(
        StringSession(c["s"]),
        int(c["id"]),
        c["hash"],
        receive_updates=False,
        request_retries=15,
        connection_retries=15,
        retry_delay=1,
        auto_reconnect=True,
        entity_cache_limit=10
    )

async def _upload_async(source: Union[str, bytes, bytearray, io.IOBase], filename: str, tag: str, caption: str) -> Dict[str, Any]:
    client = _make_client()
    await client.connect()
    t0 = time.time()

    try:
        if isinstance(source, (bytes, bytearray)):
            file_size = len(source)
            stream_obj = io.BytesIO(source)
            close_stream = True
        elif isinstance(source, str):
            file_size = os.path.getsize(source)
            stream_obj = open(source, "rb")
            close_stream = True
        else:
            source.seek(0, 2)
            file_size = source.tell()
            source.seek(0)
            stream_obj = source
            close_stream = False

        total_parts = max(1, (file_size + PART_SIZE - 1) // PART_SIZE)
        file_id = random.randint(-(2**63), 2**63 - 1)
        is_big = file_size > 10 * 1024 * 1024
        md5_hasher = hashlib.md5() if not is_big else None

        queue: asyncio.Queue = asyncio.Queue(maxsize=PARALLEL_WORKERS)
        worker_errors = []

        async def worker():
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    break
                part_idx, chunk_bytes = item
                try:
                    for attempt in range(15):
                        try:
                            if not client.is_connected():
                                await client.connect()
                            if is_big:
                                await client(SaveBigFilePartRequest(
                                    file_id=file_id,
                                    file_part=part_idx,
                                    file_total_parts=total_parts,
                                    bytes=chunk_bytes
                                ))
                            else:
                                await client(SaveFilePartRequest(
                                    file_id=file_id,
                                    file_part=part_idx,
                                    bytes=chunk_bytes
                                ))
                            break
                        except FloodWaitError as fw:
                            await asyncio.sleep(fw.seconds + 1)
                        except Exception as e:
                            if attempt == 14:
                                raise RuntimeError(f"Chunk #{part_idx} failed after 15 retries: {e}")
                            await asyncio.sleep(min(1.5 * (attempt + 1), 8))
                except Exception as e:
                    worker_errors.append(e)
                finally:
                    del chunk_bytes
                    queue.task_done()

        num_w = min(PARALLEL_WORKERS, total_parts)
        workers = [asyncio.create_task(worker()) for _ in range(num_w)]

        try:
            for part_idx in range(total_parts):
                if worker_errors:
                    raise worker_errors[0]
                chunk = stream_obj.read(PART_SIZE)
                if not chunk:
                    break
                if md5_hasher is not None:
                    md5_hasher.update(chunk)
                await queue.put((part_idx, chunk))
                del chunk

            for _ in range(num_w):
                await queue.put(None)
            await queue.join()

            if worker_errors:
                raise worker_errors[0]

            input_file = (
                InputFileBig(id=file_id, parts=total_parts, name=filename)
                if is_big
                else InputFile(id=file_id, parts=total_parts, name=filename, md5_checksum=md5_hasher.hexdigest())
            )

            msg = await client.send_file(
                "me",
                file=input_file,
                caption=f"{tag} {caption}".strip() or filename,
                force_document=True,
                attributes=[DocumentAttributeFilename(file_name=filename)]
            )
            elapsed = round(max(time.time() - t0, 0.05), 2)
            size_mb = round(file_size / 1048576, 2)
            return {
                "ok": True,
                "id": msg.id,
                "name": filename,
                "bytes": file_size,
                "mb": size_mb,
                "mbps": round(size_mb / elapsed, 2),
                "vercel_load": 0
            }
        finally:
            if close_stream:
                stream_obj.close()
            for w in workers:
                if not w.done():
                    w.cancel()
    finally:
        await client.disconnect()
        gc.collect()

async def _pipe_url_async(url: str, filename: Optional[str], tag: str, caption: str) -> Dict[str, Any]:
    client = _make_client()
    await client.connect()
    t0 = time.time()

    try:
        fname = filename or url.split("/")[-1].split("?")[0] or "remote.bin"
        async with httpx.AsyncClient(follow_redirects=True, timeout=25.0) as http:
            head = await http.get(url, headers={"Range": "bytes=0-0"})
            total_size = (
                int(head.headers["content-range"].split("/")[-1])
                if "content-range" in head.headers
                else int(head.headers.get("content-length", 0))
            )

        if total_size <= 0:
            async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as http:
                r = await http.get(url)
                r.raise_for_status()
                return await _upload_async(r.content, fname, tag, caption)

        total_parts = max(1, (total_size + PART_SIZE - 1) // PART_SIZE)
        file_id = random.randint(-(2**63), 2**63 - 1)
        is_big = total_size > 10 * 1024 * 1024
        md5_hasher = hashlib.md5() if not is_big else None

        queue: asyncio.Queue = asyncio.Queue(maxsize=PARALLEL_WORKERS)
        worker_errors = []

        async def worker():
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    break
                part_idx, chunk_bytes = item
                try:
                    for attempt in range(15):
                        try:
                            if not client.is_connected():
                                await client.connect()
                            if is_big:
                                await client(SaveBigFilePartRequest(file_id=file_id, file_part=part_idx, file_total_parts=total_parts, bytes=chunk_bytes))
                            else:
                                await client(SaveFilePartRequest(file_id=file_id, file_part=part_idx, bytes=chunk_bytes))
                            break
                        except FloodWaitError as fw:
                            await asyncio.sleep(fw.seconds + 1)
                        except Exception as e:
                            if attempt == 14:
                                raise e
                            await asyncio.sleep(min(1.5 * (attempt + 1), 8))
                except Exception as e:
                    worker_errors.append(e)
                finally:
                    del chunk_bytes
                    queue.task_done()

        num_w = min(PARALLEL_WORKERS, total_parts)
        workers = [asyncio.create_task(worker()) for _ in range(num_w)]

        offset = 0
        part_idx = 0
        buf = bytearray()
        async with httpx.AsyncClient(follow_redirects=True, timeout=45.0) as http:
            while offset < total_size:
                for retry in range(15):
                    try:
                        async with http.stream("GET", url, headers={"Range": f"bytes={offset}-"}) as resp:
                            resp.raise_for_status()
                            async for piece in resp.aiter_bytes(chunk_size=262144):
                                buf.extend(piece)
                                offset += len(piece)
                                while len(buf) >= PART_SIZE or (offset >= total_size and len(buf) > 0):
                                    take = min(PART_SIZE, len(buf))
                                    chunk_bytes = bytes(buf[:take])
                                    del buf[:take]
                                    if md5_hasher is not None:
                                        md5_hasher.update(chunk_bytes)
                                    await queue.put((part_idx, chunk_bytes))
                                    part_idx += 1
                        break
                    except Exception as e:
                        if retry == 14:
                            raise RuntimeError(f"URL Pipe failed after 15 retries at {offset}: {e}")
                        await asyncio.sleep(2)

        for _ in range(num_w):
            await queue.put(None)
        await queue.join()
        if worker_errors:
            raise worker_errors[0]

        input_file = (
            InputFileBig(id=file_id, parts=total_parts, name=fname)
            if is_big
            else InputFile(id=file_id, parts=total_parts, name=fname, md5_checksum=md5_hasher.hexdigest())
        )
        msg = await client.send_file("me", file=input_file, caption=f"{tag} {caption}".strip() or fname, force_document=True, attributes=[DocumentAttributeFilename(file_name=fname)])
        elapsed = round(max(time.time() - t0, 0.05), 2)
        size_mb = round(total_size / 1048576, 2)
        return {"ok": True, "id": msg.id, "name": fname, "mb": size_mb, "mbps": round(size_mb / elapsed, 2), "vercel_load": 0}
    finally:
        await client.disconnect()
        gc.collect()

async def _download_async(msg_id: int, output_path: Optional[str] = None, offset: int = 0, length: Optional[int] = None) -> Union[str, bytes]:
    client = _make_client()
    await client.connect()
    try:
        msg = await client.get_messages("me", ids=int(msg_id))
        if not msg or not msg.file:
            raise FileNotFoundError(f"Message #{msg_id} has no file")

        total_size = msg.file.size or 0
        fname = output_path or msg.file.name or f"tg_{msg_id}.bin"
        target_len = min(length, total_size - offset) if length else (total_size - offset)

        if output_path is None and length is not None:
            aligned_off = (offset // 4096) * 4096
            skip = offset - aligned_off
            buf = bytearray()
            async for chunk in client.iter_download(msg.media, offset=aligned_off, request_size=PART_SIZE):
                buf.extend(chunk)
                if len(buf) >= skip + target_len:
                    break
            return bytes(buf[skip:skip + target_len])

        cur_pos = os.path.getsize(fname) if (os.path.exists(fname) and offset == 0) else offset
        mode = "ab" if cur_pos > 0 and offset == 0 else "wb"

        with open(fname, mode) as f:
            while cur_pos < total_size:
                aligned_off = (cur_pos // 4096) * 4096
                skip = cur_pos - aligned_off
                for retry in range(15):
                    try:
                        if not client.is_connected():
                            await client.connect()
                        async for chunk in client.iter_download(msg.media, offset=aligned_off, request_size=PART_SIZE):
                            if skip > 0:
                                chunk = chunk[skip:]
                                skip = 0
                            f.write(chunk)
                            cur_pos += len(chunk)
                            del chunk
                            if cur_pos >= total_size:
                                break
                        break
                    except FloodWaitError as fw:
                        await asyncio.sleep(fw.seconds + 1)
                    except Exception as e:
                        if retry == 14:
                            raise RuntimeError(f"Download failed at {cur_pos}: {e}")
                        await asyncio.sleep(2)
        return os.path.abspath(fname)
    finally:
        await client.disconnect()
        gc.collect()

async def _kv_set_async(key: str, value: Any) -> Dict[str, Any]:
    client = _make_client()
    await client.connect()
    try:
        tag = f"#KV_{key}"
        val_str = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        old_ids = [m.id async for m in client.iter_messages("me", limit=5, search=tag) if m.raw_text and m.raw_text.startswith(tag)]
        if old_ids:
            await client.delete_messages("me", old_ids)
        msg = await client.send_message("me", f"{tag}\n{val_str}")
        return {"ok": True, "k": key, "id": msg.id, "vercel_load": 0}
    finally:
        await client.disconnect()

async def _kv_get_async(key: str, default: Any = None) -> Any:
    client = _make_client()
    await client.connect()
    try:
        tag = f"#KV_{key}"
        async for m in client.iter_messages("me", limit=5, search=tag):
            if m.raw_text and m.raw_text.startswith(tag):
                raw = m.raw_text[len(tag):].strip()
                try:
                    return json.loads(raw)
                except Exception:
                    return raw
        return default
    finally:
        await client.disconnect()

async def _list_async(search: str = "", limit: int = 25) -> Dict[str, Any]:
    client = _make_client()
    await client.connect()
    try:
        items = []
        async for m in client.iter_messages("me", limit=limit, search=search or None):
            items.append({
                "id": m.id,
                "t": (m.raw_text or "")[:120],
                "name": m.file.name if m.file else None,
                "mb": round((m.file.size or 0) / 1048576, 2) if m.file else 0
            })
        return {"ok": True, "items": items, "vercel_load": 0}
    finally:
        await client.disconnect()

async def _delete_async(msg_id: int) -> Dict[str, Any]:
    client = _make_client()
    await client.connect()
    try:
        await client.delete_messages("me", [int(msg_id)])
        return {"ok": True, "del": int(msg_id), "vercel_load": 0}
    finally:
        await client.disconnect()

def _run(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()

def upload(file_path_or_bytes: Union[str, bytes, bytearray, io.IOBase], filename: Optional[str] = None, tag: str = "#storage", caption: str = "") -> Dict[str, Any]:
    if not filename:
        filename = os.path.basename(file_path_or_bytes) if isinstance(file_path_or_bytes, str) else "buffer.bin"
    return _run(_upload_async(file_path_or_bytes, filename, tag, caption))

def pipe_url(url: str, filename: Optional[str] = None, tag: str = "#remote", caption: str = "") -> Dict[str, Any]:
    return _run(_pipe_url_async(url, filename, tag, caption))

def download(msg_id: int, output_path: Optional[str] = None) -> str:
    return _run(_download_async(msg_id, output_path=output_path))

def read_chunk(msg_id: int, offset: int = 0, length: int = 65536, as_text: bool = False) -> Union[bytes, str]:
    raw = _run(_download_async(msg_id, output_path=None, offset=offset, length=length))
    return raw.decode("utf-8", errors="replace") if as_text else raw

def save_json(filename: str, data: Any, tag: str = "#json") -> Dict[str, Any]:
    raw_bytes = (json.dumps(data, separators=(",", ":"), ensure_ascii=False) if isinstance(data, (dict, list)) else str(data)).encode("utf-8")
    return upload(raw_bytes, filename=filename, tag=tag)

def read_raw(msg_id: int, as_json: bool = False) -> Any:
    text = read_chunk(msg_id, offset=0, length=2 * 1024 * 1024, as_text=True)
    return json.loads(text) if as_json else text

def kv_set(key: str, value: Any) -> Dict[str, Any]:
    return _run(_kv_set_async(key, value))

def kv_get(key: str, default: Any = None) -> Any:
    return _run(_kv_get_async(key, default))

def list_files(search: str = "", limit: int = 25) -> Dict[str, Any]:
    return _run(_list_async(search, limit))

def delete(msg_id: int) -> Dict[str, Any]:
    return _run(_delete_async(msg_id))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tgcloud.py [up <file> | dl <id> | pipe <url> | chunk <id> <off> <len> | ls [query] | del <id>]")
        sys.exit(1)
    c = sys.argv[1].lower()
    if c == "up":
        print(json.dumps(upload(sys.argv[2], tag=sys.argv[3] if len(sys.argv) > 3 else "#storage"), indent=2))
    elif c == "dl":
        print(json.dumps({"ok": True, "path": download(int(sys.argv[2]), sys.argv[3] if len(sys.argv) > 3 else None)}, indent=2))
    elif c == "pipe":
        print(json.dumps(pipe_url(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None), indent=2))
    elif c == "chunk":
        print(read_chunk(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), as_text=True))
    elif c == "ls":
        print(json.dumps(list_files(sys.argv[2] if len(sys.argv) > 2 else ""), indent=2))
    elif c == "del":
        print(json.dumps(delete(int(sys.argv[2])), indent=2))
