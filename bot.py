import os
import asyncio
import logging
import time
import requests
import secrets
import string
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update, InputMediaPhoto, InputMediaVideo
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters
)

# ==================== LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ==================== ENV VARS ====================
TOKEN        = os.environ.get("TOKEN")
ADMIN_ID     = int(os.environ.get("ADMIN_ID", "0"))
MONGO_URI    = os.environ.get("MONGO_URI")
CHANNEL_ID   = os.environ.get("CHANNEL_ID")   # ID kênh VIP (dạng -100xxxxxxxxx)
ADMIN_CONTACT = os.environ.get("ADMIN_CONTACT", "")
BOT_USERNAME  = os.environ.get("BOT_USERNAME", "")  # không có @

DELETE_AFTER = 10 * 60  # 10 phút (giây)

# ==================== RATE LIMIT ====================
user_last_request = {}
RATE_LIMIT_SECONDS = 5

def is_rate_limited(user_id: int) -> bool:
    now = time.time()
    last = user_last_request.get(user_id, 0)
    if now - last < RATE_LIMIT_SECONDS:
        return True
    user_last_request[user_id] = now
    return False

# ==================== HELPERS ====================
def get_col(context: ContextTypes.DEFAULT_TYPE):
    return context.application.bot_data["albums_col"]

def get_banned(context: ContextTypes.DEFAULT_TYPE):
    return context.application.bot_data["banned_col"]

def make_key() -> str:
    chars = string.ascii_letters + string.digits
    return ''.join(secrets.choice(chars) for _ in range(8))

def make_link(key: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={key}"

def force_delete_webhook():
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true"
        requests.get(url, timeout=10)
    except Exception:
        pass

# ==================== JOB: XÓA TIN SAU 10P ====================
async def delete_messages(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    chat_id = data["chat_id"]
    for msg_id in data["message_ids"]:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass

# ==================== USER: /start ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    user_id = user.id

    # Rate limit
    if is_rate_limited(user_id):
        return

    # Kiểm tra bị ban
    banned_col = get_banned(context)
    if await banned_col.find_one({"user_id": user_id}):
        await update.message.reply_text(
            "🚫 Bạn đã bị chặn khỏi hệ thống.",
            protect_content=True
        )
        return

    args = context.args

    # Không có key → hướng dẫn mua VIP
    if not args:
        await update.message.reply_text(
            "👑 <b>Kênh VIP độc quyền</b>\n\n"
            "Nội dung chỉ dành cho thành viên đã đăng ký.\n\n"
            f"💎 Liên hệ admin để mua quyền truy cập:\n<b>{ADMIN_CONTACT}</b>",
            parse_mode="HTML",
            protect_content=True
        )
        return

    key        = args[0]
    albums_col = get_col(context)

    # Lấy album từ DB
    try:
        album = await albums_col.find_one({"key": key})
    except Exception:
        await update.message.reply_text(
            "⚡ <b>Hệ thống đang bận</b>\n\nVui lòng thử lại sau ít phút.",
            parse_mode="HTML",
            protect_content=True
        )
        return

    if not album:
        await update.message.reply_text(
            "🔱 <b>Không tìm thấy nội dung</b>\n\n"
            "Link này đã hết hạn hoặc không tồn tại.\n\n"
            "↩️ Vào kênh VIP và bấm lại link để xem nhé!",
            parse_mode="HTML",
            protect_content=True
        )
        return

    # Kiểm tra member kênh VIP
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        if member.status in ("left", "kicked"):
            await update.message.reply_text(
                "🔐 <b>Bạn chưa có quyền truy cập</b>\n\n"
                "Nội dung chỉ dành cho thành viên kênh VIP.\n\n"
                f"💎 Liên hệ admin để đăng ký:\n<b>{ADMIN_CONTACT}</b>",
                parse_mode="HTML",
                protect_content=True
            )
            return
    except Exception:
        pass  # Nếu không check được thì cho qua

    # Thông báo admin có user xem nội dung
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"👁 User xem nội dung\n\n"
                f"• ID: <code>{user_id}</code>\n"
                f"• Tên: {user.full_name}\n"
                f"• Album: <code>{key}</code>"
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass

    # Gửi nội dung (protect_content=True cho tất cả)
    items        = album.get("items", [])
    sent_msg_ids = []

    if len(items) == 1:
        item = items[0]
        if item["type"] == "video":
            msg = await context.bot.send_video(
                chat_id=user_id,
                video=item["file_id"],
                protect_content=True
            )
        else:
            msg = await context.bot.send_photo(
                chat_id=user_id,
                photo=item["file_id"],
                protect_content=True
            )
        sent_msg_ids.append(msg.message_id)

    else:
        media = []
        for item in items:
            if item["type"] == "video":
                media.append(InputMediaVideo(media=item["file_id"]))
            else:
                media.append(InputMediaPhoto(media=item["file_id"]))

        msgs = await context.bot.send_media_group(
            chat_id=user_id,
            media=media,
            protect_content=True
        )
        sent_msg_ids = [m.message_id for m in msgs]

    # Thông báo 10 phút tự xóa
    notice = await update.message.reply_text(
        "✨ <b>Đây là nội dung của bạn!</b>\n\n"
        "⏳ Nội dung sẽ tự xóa sau <b>10 phút</b>.\n"
        "Muốn xem lại, vào kênh VIP bấm lại link nhé 👑",
        parse_mode="HTML",
        protect_content=True
    )
    sent_msg_ids.append(notice.message_id)

    # Đặt job xóa sau 10 phút
    context.application.job_queue.run_once(
        delete_messages,
        DELETE_AFTER,
        data={"chat_id": user_id, "message_ids": sent_msg_ids},
        name=f"del_{user_id}_{key}"
    )

# ==================== ADMIN: /new ====================
async def new_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if context.user_data.get("current_key"):
        await update.message.reply_text(
            "🔱 <b>Đang có album chưa hoàn thành!</b>\n\n"
            "Gõ /done để lấy link album hiện tại trước nhé.",
            parse_mode="HTML"
        )
        return

    key        = make_key()
    albums_col = get_col(context)
    await albums_col.insert_one({
        "key":        key,
        "items":      [],
        "created_at": datetime.utcnow()
    })
    context.user_data["current_key"] = key

    await update.message.reply_text(
        "💎 <b>Album mới đã sẵn sàng!</b>\n\n"
        f"Key: <code>{key}</code>\n\n"
        "Forward video/ảnh vào đây, gõ /done khi xong.",
        parse_mode="HTML"
    )

# ==================== ADMIN: /done ====================
async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    key = context.user_data.get("current_key")
    if not key:
        await update.message.reply_text("Chưa có album nào đang upload.")
        return

    context.user_data.pop("current_key", None)

    await update.message.reply_text(
        "👑 <b>Hoàn tất!</b>\n\n"
        f"🔗 Link chia sẻ:\n<code>{make_link(key)}</code>",
        parse_mode="HTML"
    )

# ==================== ADMIN: /list ====================
async def list_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_col(context)
    try:
        albums = await albums_col.find(
            {}, {"key": 1, "items": 1}
        ).to_list(length=50)
    except Exception:
        await update.message.reply_text("❌ Lỗi DB!")
        return

    if not albums:
        await update.message.reply_text("💎 Chưa có album nào.")
        return

    text = "👑 <b>Danh sách album</b>\n\n"
    for a in albums:
        text += f"• <code>{a['key']}</code> — {len(a.get('items', []))} file\n"

    await update.message.reply_text(text, parse_mode="HTML")

# ==================== ADMIN: /check ====================
async def check_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Dùng: /check &lt;key&gt;", parse_mode="HTML")
        return

    key        = context.args[0]
    albums_col = get_col(context)
    album      = await albums_col.find_one({"key": key})

    if not album:
        await update.message.reply_text(
            f"💎 Không tìm thấy album <code>{key}</code>",
            parse_mode="HTML"
        )
        return

    items  = album.get("items", [])
    videos = sum(1 for i in items if i["type"] == "video")
    photos = sum(1 for i in items if i["type"] == "photo")

    await update.message.reply_text(
        f"👑 <b>Album:</b> <code>{key}</code>\n\n"
        f"💎 Tổng: <b>{len(items)}</b> file\n"
        f"🎬 Video: <b>{videos}</b>\n"
        f"🖼 Ảnh: <b>{photos}</b>\n\n"
        f"🔗 <code>{make_link(key)}</code>",
        parse_mode="HTML"
    )

# ==================== ADMIN: /del ====================
async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Dùng: /del &lt;key&gt;", parse_mode="HTML")
        return

    key        = context.args[0]
    albums_col = get_col(context)
    result     = await albums_col.delete_one({"key": key})

    if result.deleted_count == 0:
        await update.message.reply_text(
            f"💎 Không tìm thấy album <code>{key}</code>",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"✨ Đã xóa album <code>{key}</code>",
            parse_mode="HTML"
        )

# ==================== ADMIN: /clean ====================
async def clean_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_col(context)
    cutoff     = datetime.utcnow() - timedelta(days=7)

    result = await albums_col.delete_many({
        "$or": [
            {"items": []},
            {"created_at": {"$lt": cutoff}}
        ]
    })

    await update.message.reply_text(
        f"✨ Đã dọn <b>{result.deleted_count}</b> album cũ.",
        parse_mode="HTML"
    )

# ==================== ADMIN: /status ====================
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_col(context)
    try:
        total = await albums_col.count_documents({})
        await update.message.reply_text(
            "✨ <b>Hệ thống hoạt động bình thường</b>\n\n"
            f"👑 Tổng album: <b>{total}</b>\n"
            "⚡ DB: Connected",
            parse_mode="HTML"
        )
    except Exception:
        await update.message.reply_text(
            "⚡ <b>Lỗi kết nối DB!</b>",
            parse_mode="HTML"
        )

# ==================== ADMIN: /ban ====================
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Dùng: /ban &lt;user_id&gt;", parse_mode="HTML")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("User ID phải là số.")
        return

    banned_col = get_banned(context)
    await banned_col.update_one(
        {"user_id": target_id},
        {"$set": {"user_id": target_id, "banned_at": datetime.utcnow()}},
        upsert=True
    )
    await update.message.reply_text(
        f"✅ Đã ban user <code>{target_id}</code>",
        parse_mode="HTML"
    )

# ==================== ADMIN: /unban ====================
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Dùng: /unban &lt;user_id&gt;", parse_mode="HTML")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("User ID phải là số.")
        return

    banned_col = get_banned(context)
    result     = await banned_col.delete_one({"user_id": target_id})

    if result.deleted_count:
        await update.message.reply_text(
            f"✅ Đã unban user <code>{target_id}</code>",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text("Không tìm thấy user này trong danh sách ban.")

# ==================== ADMIN: /help ====================
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    await update.message.reply_text(
        "👑 <b>Hướng dẫn Admin</b>\n\n"
        "1. /new — Tạo album mới\n"
        "   Sau khi gõ → forward ảnh/video vào bot\n"
        "   Xong thì gõ /done để lấy link\n\n"
        "2. /done — Kết thúc upload, bot trả về deep link\n"
        "   Dán link vào bài đăng kênh VIP\n\n"
        "3. /list — Xem toàn bộ album đang có\n"
        "   Hiển thị key + số file mỗi album\n\n"
        "4. /check &lt;key&gt; — Xem chi tiết 1 album\n"
        "   VD: /check abc123\n\n"
        "5. /del &lt;key&gt; — Xóa album\n"
        "   VD: /del abc123\n"
        "   ⚠️ Không khôi phục được!\n\n"
        "6. /clean — Xóa album trống + cũ hơn 7 ngày\n\n"
        "7. /ban &lt;user_id&gt; — Chặn user\n"
        "8. /unban &lt;user_id&gt; — Bỏ chặn user\n\n"
        "9. /status — Kiểm tra bot + DB",
        parse_mode="HTML"
    )

# ==================== ADMIN: NHẬN MEDIA ====================
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    key = context.user_data.get("current_key")
    if not key:
        return

    albums_col = get_col(context)

    if update.message.video:
        file_id   = update.message.video.file_id
        file_type = "video"
    elif update.message.photo:
        file_id   = update.message.photo[-1].file_id
        file_type = "photo"
    else:
        return

    await albums_col.update_one(
        {"key": key},
        {"$push": {"items": {"type": file_type, "file_id": file_id}}}
    )
    await update.message.reply_text("✅ Đã thêm file vào album.")

# ==================== RUN BOT ====================
def run_bot():
    while True:
        try:
            force_delete_webhook()

            async def main_bot():
                client = AsyncIOMotorClient(
                    MONGO_URI,
                    serverSelectionTimeoutMS=5000
                )
                db         = client["botdb"]
                albums_col = db["albums"]
                banned_col = db["banned"]

                app = Application.builder().token(TOKEN).build()
                app.bot_data["albums_col"] = albums_col
                app.bot_data["banned_col"] = banned_col

                # User
                app.add_handler(CommandHandler("start", start))

                # Admin
                app.add_handler(CommandHandler(["new_album", "new"], new_album))
                app.add_handler(CommandHandler("done",   done))
                app.add_handler(CommandHandler("list",   list_albums))
                app.add_handler(CommandHandler("check",  check_album))
                app.add_handler(CommandHandler(["del_album", "del"], delete_album))
                app.add_handler(CommandHandler("clean",  clean_albums))
                app.add_handler(CommandHandler("status", status_cmd))
                app.add_handler(CommandHandler("ban",    ban_user))
                app.add_handler(CommandHandler("unban",  unban_user))
                app.add_handler(CommandHandler("help",   help_cmd))
                app.add_handler(MessageHandler(
                    filters.VIDEO | filters.PHOTO, handle_media
                ))

                logging.info("✅ Bot started!")
                await app.run_polling(drop_pending_updates=True)

            asyncio.run(main_bot())

        except Exception as e:
            logging.error(f"Bot crashed: {e} — restarting in 10s...")
            time.sleep(10)

if __name__ == "__main__":
    run_bot()
