"""
Telegram -> VLC streaming bot
Architecture:
  - Telegram webhook (HTTP) wakes the app and delivers new messages.
    This is what lets Render's free tier spin the service up on demand.
  - Pyrogram (MTProto client, logged in as the bot) does the actual file
    download/streaming, which bypasses the 20MB limit of the classic
    Bot API's getFile() call. Regular accounts can stream files up to 2GB.

Env vars required (set these in Render's dashboard, never hardcode them):
  API_ID        - from https://my.telegram.org
  API_HASH      - from https://my.telegram.org
  BOT_TOKEN     - from @BotFather
  WEBHOOK_URL   - your Render service's public URL, e.g. https://yourapp.onrender.com
                  (no trailing slash)
  PORT          - provided automatically by Render, do not set manually
"""

import os
import asyncio
from aiohttp import web
import aiohttp
from pyrogram import Client

# ==================== CONFIGURATION (from environment) ====================
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
WEBHOOK_URL = os.environ["WEBHOOK_URL"].rstrip("/")
PORT = int(os.environ.get("PORT", 8080))
# ============================================================================

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"  # token in the path so randoms can't hit your webhook

# msg_id -> file_id, in-memory only (fine for personal/occasional use;
# resets whenever the free service spins down and restarts)
file_database = {}

# Pyrogram client, logged in as the bot via MTProto.
# in_memory=True avoids writing a session file to Render's ephemeral disk.
pyro_app = Client(
    "bot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
)

http_session: aiohttp.ClientSession = None


async def send_message(chat_id, text):
    async with http_session.post(
        f"{TELEGRAM_API}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
    ) as resp:
        return await resp.json()


async def set_webhook():
    url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
    async with http_session.post(f"{TELEGRAM_API}/setWebhook", json={"url": url}) as resp:
        data = await resp.json()
        print("setWebhook response:", data)


async def webhook_handler(request):
    """Receives updates pushed by Telegram. This inbound HTTP request is
    what wakes a sleeping Render free instance."""
    update = await request.json()
    message = update.get("message") or update.get("channel_post")
    if not message:
        return web.Response(text="ok")

    chat_id = message["chat"]["id"]
    msg_id = str(message["message_id"])

    media = message.get("video") or message.get("document")

    if not media:
        text = message.get("text", "")
        if text == "/start":
            await send_message(
                chat_id,
                "👋 Welcome! Send or forward any video or document (up to 2GB) "
                "and I'll give you a direct VLC stream link.",
            )
        else:
            await send_message(chat_id, "❌ Please send a valid video file or document.")
        return web.Response(text="ok")

    file_id = media["file_id"]
    file_database[msg_id] = {
        "file_id": file_id,
        "file_size": media.get("file_size", 0),
        "mime_type": media.get("mime_type", "video/mp4"),
    }

    stream_url = f"{WEBHOOK_URL}/stream/{msg_id}"
    reply_text = (
        f"✅ *File Link Generated!*\n\n"
        f"🔗 *VLC Link:*\n`{stream_url}`\n\n"
        f"👉 Copy the link, open VLC (Ctrl+N on Desktop), paste it, and stream!"
    )
    await send_message(chat_id, reply_text)
    return web.Response(text="ok")


async def stream_handler(request):
    """Streams the file to VLC chunk by chunk via Pyrogram/MTProto,
    without ever loading the whole file into memory."""
    msg_id = request.match_info.get("msg_id")
    if msg_id not in file_database:
        return web.Response(text="File Link Expired or Not Found", status=404)

    entry = file_database[msg_id]
    file_id = entry["file_id"]
    file_size = entry.get("file_size") or 0
    mime_type = entry.get("mime_type") or "video/mp4"

    CHUNK_SIZE = 1024 * 1024  # Pyrogram streams in fixed 1MiB chunks

    range_header = request.headers.get("Range")
    start = 0
    end = file_size - 1 if file_size else None

    if range_header and file_size:
        # Expected form: "bytes=START-END" (END is optional)
        try:
            units, rng = range_header.split("=")
            range_start, range_end = rng.split("-")
            start = int(range_start)
            end = int(range_end) if range_end else file_size - 1
        except (ValueError, IndexError):
            start, end = 0, file_size - 1

    status = 206 if range_header and file_size else 200
    headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
    }
    if file_size:
        content_length = end - start + 1
        headers["Content-Length"] = str(content_length)
        if status == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    response = web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)

    start_chunk = start // CHUNK_SIZE
    skip_in_first_chunk = start - (start_chunk * CHUNK_SIZE)
    bytes_remaining = (end - start + 1) if file_size else None

    try:
        first = True
        async for chunk in pyro_app.stream_media(file_id, offset=start_chunk):
            if first:
                chunk = chunk[skip_in_first_chunk:]
                first = False

            if bytes_remaining is not None:
                if bytes_remaining <= 0:
                    break
                if len(chunk) > bytes_remaining:
                    chunk = chunk[:bytes_remaining]
                bytes_remaining -= len(chunk)

            await response.write(chunk)
    except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
        # The client (VLC) closed or seeked away mid-stream - not an error worth logging loudly
        pass

    return response


async def health_handler(request):
    """Simple endpoint so Render sees a live HTTP service."""
    return web.Response(text="Bot is running")


async def on_startup(app):
    global http_session
    http_session = aiohttp.ClientSession()
    await pyro_app.start()
    await set_webhook()
    print("Startup complete.")


async def on_cleanup(app):
    await pyro_app.stop()
    await http_session.close()


def main():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    app.router.add_get("/stream/{msg_id}", stream_handler)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
