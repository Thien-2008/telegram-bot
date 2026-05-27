# v11.6 - VIP Channel Membership Check
import os
import asyncio
import random
import string
import logging
import threading
import time
import requests
from flask import Flask
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes
)
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO)

TOKEN      = os.environ.get("TOKEN", "")
ADMIN_ID   = os.environ.get("ADMIN_ID", "0")
ADMIN_ID   = int(ADMIN_ID) if ADMIN_ID.isdigit() else 0
MONGO_URI  = os.environ.get("MONGO_URI", "")
CHANNEL_ID = os.environ.get("CHANNEL_ID", "")
CHANNEL_ID = int(CHANNEL_ID) if CHANNEL_ID else 0
CHANNEL_LINK = "https://t.me/+rmpfiQeaToAyYzhl"
PORT = int(os.environ.get("PORT", 8080))

client     = AsyncIOMotorClient(MONGO_URI)
db         = client["botdb"]
albums_col = db["albums"]

flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    return "Bot is running", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT)

def make_link(key):
    return "https://t.me/khotailieu_A1_bot?start=" + key

def gen_key(length=8):
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))

def force_delete_webhook():
    for attempt in range(5):
        try:
            url = f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true"
            r = requests.get(url, timeout=10)
            result = r.json()
            logging.info(f"deleteWebhook attempt {attempt+1}: {result}")
            if result.get("ok"):
                time.sleep(5)
                return True
        except Exception as e:
            logging.error(f"deleteWebhook error: {e}")
        time.sleep(3)
    return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args

    if not args:
        await update.message.reply_text(
            "\U0001f44b Ch\u00e0o b\u1ea1n\n\n"
            "V\u00e0o k\u00eanh b\u00ean d\u01b0\u1edbi \u0111\u1ec3 ch\u1ecdn n\u1ed9i dung mu\u1ed1n xem nh\u00e9\n"
            "\U0001f449 " + CHANNEL_LINK
        )
        return

    key = args[0]

    try:
        album = await albums_col.find_one({"key": key})
    except Exception as e:
        logging.error(f"DB error: {e}")
        await update.message.reply_text(
            "\u26a0 L\u1ed7i k\u1ebft n\u1ed1i, th\u1eed l\u1ea1i sau m\u1ed9t ch\u00fat nh\u00e9"
        )
        return

    if not album or not album.get("items"):
        await update.message.reply_text(
            "\U0001f614 Link n\u00e0y kh\u00f4ng c\u00f2n n\u1eefa r\u1ed3i\n\n"
            "V\u00e0o k\u00eanh \u0111\u1ec3 l\u1ea5y link m\u1edbi nh\u00e9\n"
            "\U0001f449 " + CHANNEL_LINK
        )
        return

    # Check VIP
    if update.effective_user.id != ADMIN_ID:
        try:
            member = await context.bot.get_chat_member(
                chat_id=CHANNEL_ID,
                user_id=update.effective_user.id
            )
            allowed = ["member", "administrator", "creator", "restricted"]
            if member.status not in allowed:
                await update.message.reply_text(
                    "\U0001f512 N\u1ed9i dung n\u00e0y d\u00e0nh ri\u00eang cho th\u00e0nh vi\u00ean VIP\n\n"
                    "Li\u00ean h\u1ec7 admin \u0111\u1ec3 \u0111\u01b0\u1ee3c h\u1ed7 tr\u1ee3 nh\u00e9\n"
                    "\U0001f449 " + CHANNEL_LINK
                )
                return
        except Exception as e:
            logging.error(f"Membership check error: {e}")
            await update.message.reply_text(
                "\u26a0 X\u00e1c minh quy\u1ec1n truy c\u1eadp th\u1ea5t b\u1ea1i\n"
                "Th\u1eed l\u1ea1i sau \u00edt ph\u00fat nh\u00e9"
            )
            return

    # Send content
    chat_id  = update.effective_chat.id
    sent_ids = []

    for item in album["items"]:
        try:
            if item["type"] == "video":
                msg = await context.bot.send_video(
                    chat_id=chat_id,
                    video=item["file_id"],
                    protect_content=True
                )
            elif item["type"] == "photo":
                msg = await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=item["file_id"],
                    protect_content=True
                )
            sent_ids.append(msg.message_id)
        except Exception as e:
            logging.error(f"Send error: {e}")

    notice = await update.message.reply_text(
        "\u23f3 N\u1ed9i dung s\u1ebd t\u1ef1 x\u00f3a sau 20 ph\u00fat\n"
        "Xem nhanh l\u00ean nh\u00e9"
    )
    sent_ids.append(notice.message_id)
    asyncio.create_task(delete_after(context, chat_id, sent_ids, 1200))


async def delete_after(context, chat_id, message_ids, delay):
    await asyncio.sleep(delay)
    for mid in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception as e:
            logging.error(f"Delete error: {e}")
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "\u23f0 N\u1ed9i dung \u0111\u00e3 h\u1ebft h\u1ea1n r\u1ed3i\n\n"
                "V\u00e0o k\u00eanh \u0111\u1ec3 l\u1ea5y link m\u1edbi nh\u00e9\n"
                "\U0001f449 " + CHANNEL_LINK
            )
        )
    except Exception as e:
        logging.error(f"Notify error: {e}")


async def new_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    key = gen_key()
    await albums_col.insert_one({"key": key, "items": []})
    context.user_data["current_key"] = key
    await update.message.reply_text(
        "\U0001f4c1 Album m\u1edbi: " + key + "\n\n"
        "Forward video/\u1ea3nh v\u00e0o\n"
        "G\u00f5 /done khi xong"
    )


async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    key = context.user_data.get("current_key")
    if not key:
        await update.message.reply_text("\u26a0 Ch\u01b0a t\u1ea1o album, g\u00f5 /new_album tr\u01b0\u1edbc nh\u00e9")
        return
    album = await albums_col.find_one({"key": key})
    count = len(album.get("items", [])) if album else 0
    if count == 0:
        await update.message.reply_text("\u26a0 Album \u0111ang tr\u1ed1ng, forward file v\u00e0o tr\u01b0\u1edbc nh\u00e9")
        return
    link = make_link(key)
    await update.message.reply_text(
        "\u2705 Xong, album c\u00f3 " + str(count) + " file\n\n"
        "\U0001f517 Link chia s\u1ebb\n" + link
    )
    context.user_data.pop("current_key", None)


async def list_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        albums = await albums_col.find().to_list(length=100)
    except Exception as e:
        await update.message.reply_text("\u26a0 L\u1ed7i k\u1ebft n\u1ed1i DB")
        return
    if not albums:
        await update.message.reply_text("Ch\u01b0a c\u00f3 album n\u00e0o")
        return
    text = "\U0001f4cb Danh s\u00e1ch album\n\n"
    for album in albums:
        key   = album["key"]
        count = len(album.get("items", []))
        text += "\u2022 " + key + " \u2014 " + str(count) + " file\n"
        text += "  " + make_link(key) + "\n\n"
    await update.message.reply_text(text)


async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("D\u00f9ng: /del_album <key>")
        return
    key    = context.args[0]
    result = await albums_col.delete_one({"key": key})
    if result.deleted_count:
        await update.message.reply_text("\U0001f5d1 \u0110\u00e3 x\u00f3a album " + key)
    else:
        await update.message.reply_text("\u274c Kh\u00f4ng t\u00ecm th\u1ea5y")


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    key = context.user_data.get("current_key")
    if not key:
        return
    item = None
    if update.message.video:
        item = {"type": "video", "file_id": update.message.video.file_id}
    elif update.message.photo:
        item = {"type": "photo", "file_id": update.message.photo[-1].file_id}
    if not item:
        return
    await albums_col.update_one({"key": key}, {"$push": {"items": item}})
    album = await albums_col.find_one({"key": key})
    count = len(album.get("items", []))
    await update.message.reply_text(
        "\u2705 \u0110\u00e3 l\u01b0u \u2014 album " + key + " c\u00f3 " + str(count) + " file"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        "\U0001f4d6 H\u01b0\u1edbng d\u1eabn\n\n"
        "1. /new_album \u2014 T\u1ea1o album m\u1edbi\n"
        "2. Forward video/\u1ea3nh v\u00e0o bot\n"
        "3. /done \u2014 L\u1ea5y link chia s\u1ebb\n"
        "4. /list \u2014 Xem t\u1ea5t c\u1ea3 album\n"
        "5. /del_album <key> \u2014 X\u00f3a album\n\n"
        "N\u1ed9i dung t\u1ef1 x\u00f3a sau 20 ph\u00fat"
    )


def run_bot():
    while True:
        try:
            force_delete_webhook()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            app = Application.builder().token(TOKEN).build()
            app.add_handler(CommandHandler("start",     start))
            app.add_handler(CommandHandler("new_album", new_album))
            app.add_handler(CommandHandler("done",      done))
            app.add_handler(CommandHandler("list",      list_albums))
            app.add_handler(CommandHandler("del_album", delete_album))
            app.add_handler(CommandHandler("help",      help_cmd))
            app.add_handler(MessageHandler(filters.VIDEO | filters.PHOTO, handle_media))
            logging.info("Bot started")
            app.run_polling(drop_pending_updates=True)
        except Exception as e:
            logging.error(f"Bot crashed: {e} — restarting in 10s")
            time.sleep(10)


def main():
    t = threading.Thread(target=run_flask)
    t.daemon = True
    t.start()
    run_bot()

if __name__ == "__main__":
    main()
