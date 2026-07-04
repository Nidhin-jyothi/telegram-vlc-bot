"""
Telegram -> VLC streaming bot (VPS version)

Simpler than the Render version: since this runs on a real always-on server,
Pyrogram can just stay connected directly - no webhook, no spin-down
workarounds needed.

Env vars required (put these in a .env file or export them - see systemd
service setup, never hardcode them in this file):
  API_ID    - from https://my.telegram.org
  API_HASH  - from https://my.telegram.org
  BOT_TOKEN - from @BotFather
  PUBLIC_IP - your VM's public IP, e.g. 123.45.67.89
  PORT      - defaults to 8080
"""

import os
import asyncio
from aiohttp import web
from pyrogram import Client, filters

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
PUBLIC_IP = os.environ["PUBLIC_IP"]
PORT = int(os.environ.get("PORT", 8080))

CHUNK_SIZE = 1024 * 1024  # Pyrogram streams in fixed 1MiB chunks

app = Client(
    "bot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)


@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    await message.reply_text(
        "👋 Welcome! Send or forward any video or document (up to 2GB) "
        "and I'll give you a direct VLC stream link."
    )


@app.on_message(filters.video | filters.document)
async def handle_media(client, message):
    stream_url = f"http://{PUBLIC_IP}:{PORT}/stream/{message.chat.id}/{message.id}"
    await message.reply_text(
        f"✅ *File Link Generated!*\n\n"
        f"🔗 *VLC Link:*\n`{stream_url}`\n\n"
        f"👉 Copy the link, open VLC (Ctrl+N on Desktop), paste it, and stream!",
        parse_mode="Markdown",
    )


async def stream_handler(request):
    """Streams the file to VLC chunk by chunk via Pyrogram/MTProto,
    without loading the whole file into memory."""
    try:
        chat_id = int(request.match_info["chat_id"])
        msg_id = int(request.match_info["msg_id"])
    except (KeyError, ValueError):
        return web.Response(text="Invalid link", status=400)

    message = await app.get_messages(chat_id, msg_id)
    media = message.video or message.document if message else None
    if not media:
        return web.Response(text="File Link Expired or Not Found", status=404)

    file_size = media.file_size or 0
    mime_type = getattr(media, "mime_type", None) or "video/mp4"

    range_header = request.headers.get("Range")
    start = 0
    end = file_size - 1 if file_size else None

    if range_header and file_size:
        try:
            _, rng = range_header.split("=")
            range_start, range_end = rng.split("-")
            start = int(range_start)
            end = int(range_end) if range_end else file_size - 1
        except (ValueError, IndexError):
            start, end = 0, file_size - 1

    status = 206 if range_header and file_size else 200
    headers = {"Content-Type": mime_type, "Accept-Ranges": "bytes"}
    if file_size:
        headers["Content-Length"] = str(end - start + 1)
        if status == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    response = web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)

    start_chunk = start // CHUNK_SIZE
    skip_in_first_chunk = start - (start_chunk * CHUNK_SIZE)
    bytes_remaining = (end - start + 1) if file_size else None

    try:
        first = True
        async for chunk in app.stream_media(message, offset=start_chunk):
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
        pass  # client closed/seeked - not worth logging loudly

    return response


async def health_handler(request):
    return web.Response(text="Bot is running")


async def main():
    await app.start()
    print("Pyrogram client started.")

    web_app = web.Application()
    web_app.router.add_get("/", health_handler)
    web_app.router.add_get("/stream/{chat_id}/{msg_id}", stream_handler)

    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"Stream server running on port {PORT}")

    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
