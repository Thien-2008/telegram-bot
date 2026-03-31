# v10.1
import os
import asyncio
import random
import string
import logging
import threading
from flask import Flask
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes
)
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO)

TOKEN = os.environ.get("TOKEN", "")
ADMIN_ID = os.environ.get("ADMIN_ID", "0")
ADMIN_ID = int(ADMIN_ID) if ADMIN_ID.isdigit() else 0
MONGO_URI = os.environ.get("MONGO_URI", "")
CHANNEL_LINK = "https://t.me/+rmpfiQeaToAyYzhl"
PORT = int(os.environ.get("PORT", 8080))

client = AsyncIOMotorClient(MONGO_URI)
db = client["botdb"]
albums_col = db["albums"]

flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    return "Bot is running!", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT)

def make_link(key):
    return "https://t.me/khotailieu_A1_bot?start=" + key

def gen_key(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "Ủa Ní ơi! 👋\n\n"
            "Ní chưa chọn link video nào hết á 😄\n\n"
            "Vào kênh bên dưới để chọn video muốn xem nhé:\n"
            "👉 " + CHANNEL_LINK
        )
        return
    key = args[0]
    try:
        album = await albums_col.find_one({"key": key})
    except Exception as e:
        logging.error(f"DB error: {e}")
        await update.message.reply_text("Lỗi kết nối, thử lại sau nhé Ní!")
        return
    if not album or not album.get("items"):
        await update.message.reply_text(
            "Ní ơi, link này không còn nữa gòi 😢\n\n"
            "Vào kênh để lấy link mới nhé:\n"
            "👉 " + CHANNEL_LINK
        )
        return
    chat_id = update.effective_chat.id
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
    await update.message.reply_text(
        "Xem nhanh lên Ní ơi! ⏳\n"
        "Nội dung sẽ tự xóa sau 20 phút đó nhe! 🔥"
    )
    asyncio.create_task(delete_after(context, chat_id, sent_ids, 1200))

async def delete_after(context, chat_id, message_ids, delay):
    await asyncio.sleep(delay)
    for mid in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception as e:
            logging.error(f"Delete error: {e}")

async def new_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    key = gen_key()
    await albums_col.insert_one({"key": key, "items": []})
    context.user_data["current_key"] = key
    await update.message.reply_text(
        "📁 Album mới: " + key + "\n\n"
        "Forward video/ảnh vào!\n"
        "Gõ /done khi xong."
    )

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    key = context.user_data.get("current_key")
    if not key:
        await update.message.reply_text("⚠️ Chưa tạo album! Gõ /new_album trước.")
        return
    album = await albums_col.find_one({"key": key})
    count = len(album.get("items", [])) if album else 0
    if count == 0:
        await update.message.reply_text("⚠️ Album trống! Forward video/ảnh vào trước.")
        return
    link = make_link(key)
    await update.message.reply_text(
        "✅ Xong! Album có " + str(count) + " file!\n\n"
        "🔗 Link chia sẻ:\n" + link
    )
    context.user_data.pop("current_key", None)

async def list_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        albums = await albums_col.find().to_list(length=100)
    except Exception as e:
        await update.message.reply_text("Lỗi kết nối DB!")
        return
    if not albums:
        await update.message.reply_text("Chưa có album nào!")
        return
    text = "📋 Danh sách album:\n\n"
    for album in albums:
        key = album["key"]
        count = len(album.get("items", []))
        text += "• " + key + " — " + str(count) + " file\n"
        text += "  " + make_link(key) + "\n\n"
    await update.message.reply_text(text)

async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dùng: /del_album <key>")
        return
    key = context.args[0]
    result = await albums_col.delete_one({"key": key})
    if result.deleted_count:
        await update.message.reply_text("🗑 Đã xóa album " + key + "!")
    else:
        await update.message.reply_text("❌ Không tìm thấy!")

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
        "✅ Đã lưu! Album " + key + " có " + str(count) + " file."
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        "📖 Hướng dẫn sử dụng bot:\n\n"
        "1. /new_album — Tạo album mới\n"
        "2. Forward video/ảnh vào bot\n"
        "3. /done — Lấy link chia sẻ\n"
        "4. /list — Xem tất cả album\n"
        "5. /del_album <key> — Xóa album\n\n"
        "Video tự xóa sau 20 phút!"
    )

async def post_init(application):
    await application.bot.delete_webhook(drop_pending_updates=True)
    logging.info("Bot started!")

def main():
    t = threading.Thread(target=run_flask)
    t.daemon = True
    t.start()

    # ✅ Bỏ .connect_timeout/.read_timeout/.write_timeout (không tương thích v20.7)
    app = Application.builder()\
        .token(TOKEN)\
        .post_init(post_init)\
        .build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("new_album", new_album))
    app.add_handler(CommandHandler("done", done))
    app.add_handler(CommandHandler("list", list_albums))
    app.add_handler(CommandHandler("del_album", delete_album))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(MessageHandler(filters.VIDEO | filters.PHOTO, handle_media))
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
