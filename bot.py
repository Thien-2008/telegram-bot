import os
import asyncio
import random
import string
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes
)

logging.basicConfig(level=logging.INFO)

TOKEN = os.environ.get("TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
albums = {}
current_album = {}

def gen_key(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("⚠️ Vui lòng bấm đúng link!")
        return

    key = args[0]
    if key not in albums or not albums[key]:
        await update.message.reply_text("❌ Nội dung không tồn tại!")
        return

    chat_id = update.effective_chat.id
    sent_ids = []

    for item in albums[key]:
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
    current_album["key"] = key
    albums[key] = []
    await update.message.reply_text(
        f"📁 Album mới: `{key}`\n\n"
        f"Forward video/ảnh vào đây!\n"
        f"Gõ /done khi xong.",
        parse_mode="Markdown"
    )

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    key = current_album.get("key")
    if not key:
        await update.message.reply_text("⚠️ Chưa tạo album! Gõ /new_album trước.")
        return
    count = len(albums.get(key, []))
    if count == 0:
        await update.message.reply_text("⚠️ Album trống! Forward video/ảnh vào trước.")
        return
    bot_info = await context.bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={key}"
    await update.message.reply_text(
        f"✅ Album `{key}` có {count} file!\n\n"
        f"🔗 Link chia sẻ:\n{link}",
        parse_mode="Markdown"
    )
    current_album.clear()

async def list_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not albums:
        await update.message.reply_text("Chưa có album nào!")
        return
    text = "📋 Danh sách album:\n\n"
    bot_info = await context.bot.get_me()
    for key, items in albums.items():
        text += f"• `{key}` — {len(items)} file\n"
        text += f"  https://t.me/{bot_info.username}?start={key}\n\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dùng: /del_album <key>")
        return
    key = context.args[0]
    if key in albums:
        del albums[key]
        await update.message.reply_text(f"🗑 Đã xóa album `{key}`!", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Không tìm thấy album!")

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    key = current_album.get("key")
    if not key:
        return
    if update.message.video:
        albums[key].append({
            "type": "video",
            "file_id": update.message.video.file_id
        })
    elif update.message.photo:
        albums[key].append({
            "type": "photo",
            "file_id": update.message.photo[-1].file_id
        })
    else:
        return
    count = len(albums[key])
    await update.message.reply_text(f"✅ Đã lưu! Album `{key}` có {count} file.", parse_mode="Markdown")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        "📖 Hướng dẫn:\n\n"
        "/new_album — Tạo album mới\n"
        "/done — Hoàn thành & lấy link\n"
        "/list — Xem danh sách album\n"
        "/del_album <key> — Xóa album\n\n"
        "Sau /new_album, forward video/ảnh vào là tự lưu!"
    )

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("new_album", new_album))
    app.add_handler(CommandHandler("done", done))
    app.add_handler(CommandHandler("list", list_albums))
    app.add_handler(CommandHandler("del_album", delete_album))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(MessageHandler(filters.VIDEO | filters.PHOTO, handle_media))
    app.run_polling()

if __name__ == "__main__":
    main()
