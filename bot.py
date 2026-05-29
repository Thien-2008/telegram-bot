import os
import asyncio
import logging
import time
import secrets
import string
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web
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
TOKEN         = os.environ.get("TOKEN")
ADMIN_ID      = int(os.environ.get("ADMIN_ID", "0"))
MONGO_URI     = os.environ.get("MONGO_URI")
CHANNEL_ID    = os.environ.get("CHANNEL_ID")
ADMIN_CONTACT = os.environ.get("ADMIN_CONTACT", "")
BOT_USERNAME  = os.environ.get("BOT_USERNAME", "")
LOG_GROUP_ID  = os.environ.get("LOG_GROUP_ID")
PORT          = int(os.environ.get("PORT", 8080))
DELETE_AFTER  = 10 * 60

# ==================== IN-MEMORY TRACKING ====================
# {user_id: [timestamps]}
request_log:      dict = defaultdict(list)
invalid_attempts: dict = defaultdict(int)
rate_hit_count:   dict = defaultdict(int)
manual_start_count: dict = defaultdict(int)
nonmember_attempts: dict = defaultdict(int)

RATE_LIMIT_SEC        = 5
SPAM_AUTO_BAN         = 10   # requests trong 60s
INVALID_WARN_THRESH   = 3
INVALID_AUTO_BAN      = 5
RATE_WARN_THRESH      = 3
MANUAL_WARN_THRESH    = 3
NONMEMBER_WARN_THRESH = 3

# ==================== HELPERS ====================
def get_col(ctx):     return ctx.application.bot_data["albums_col"]
def get_banned(ctx):  return ctx.application.bot_data["banned_col"]
def get_jobs(ctx):    return ctx.application.bot_data["jobs_col"]
def get_warns(ctx):   return ctx.application.bot_data["warns_col"]

def make_key() -> str:
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))

def make_link(key: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={key}"

def now_str() -> str:
    return datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M")

# ==================== DB RETRY ====================
async def db_retry(op, retries=3):
    last_err = None
    for i in range(retries):
        try:
            return await op()
        except Exception as e:
            last_err = e
            logging.error(f"DB retry {i+1}/{retries}: {e}")
            await asyncio.sleep(1)
    raise last_err

# ==================== LOG GROUP ====================
async def send_log(app: Application, text: str):
    if not LOG_GROUP_ID:
        return
    try:
        await app.bot.send_message(
            chat_id=LOG_GROUP_ID,
            text=text,
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Log group error: {e}")

async def log_access(app, user):
    pass  # gọi riêng trong start sau khi xác nhận

async def log_ban(app, target_id, target_name, reason, ban_type="Thủ công"):
    await send_log(app,
        f"Ban người dùng\n"
        f"ID: <code>{target_id}</code>\n"
        f"Tên: {target_name}\n"
        f"Lý do: {reason}\n"
        f"Loại: {ban_type}\n"
        f"Thời gian: {now_str()}"
    )

async def log_unban(app, target_id):
    await send_log(app,
        f"Hủy ban người dùng\n"
        f"ID: <code>{target_id}</code>\n"
        f"Thời gian: {now_str()}"
    )

async def log_warning(app, user, behavior: str, count: int):
    username = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Cảnh báo hành vi bất thường\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {user.full_name}\n"
        f"Username: {username}\n"
        f"Hành vi: {behavior}\n"
        f"Số lần: {count}\n"
        f"Thời gian: {now_str()}"
    )

async def log_auto_ban(app, user, reason: str):
    username = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Tự động ban\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {user.full_name}\n"
        f"Username: {username}\n"
        f"Lý do: {reason}\n"
        f"Thời gian: {now_str()}"
    )

async def log_view(app, user, key: str):
    username = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Truy cập nội dung\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {user.full_name}\n"
        f"Username: {username}\n"
        f"Tệp truy cập: <code>{key}</code>\n"
        f"Thời gian: {now_str()}"
    )

# ==================== AUTO BAN HELPER ====================
async def do_auto_ban(app: Application, user, reason: str):
    banned_col = app.bot_data["banned_col"]
    warns_col  = app.bot_data["warns_col"]
    await banned_col.update_one(
        {"user_id": user.id},
        {"$set": {
            "user_id":   user.id,
            "name":      user.full_name,
            "reason":    reason,
            "ban_type":  "Tự động",
            "banned_at": datetime.now(timezone.utc)
        }},
        upsert=True
    )
    await warns_col.delete_many({"user_id": user.id})
    await log_auto_ban(app, user, reason)

# ==================== RATE + SPAM CHECK ====================
async def check_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Trả về True nếu user bị chặn (đã xử lý), False nếu cho qua.
    """
    user    = update.effective_user
    user_id = user.id
    app     = context.application
    now     = time.time()

    # Chặn bot account
    if user.is_bot:
        await do_auto_ban(app, user, "Phát hiện tài khoản bot")
        return True

    # Spam detection: > SPAM_AUTO_BAN requests trong 60s
    request_log[user_id] = [t for t in request_log[user_id] if now - t < 60]
    request_log[user_id].append(now)
    if len(request_log[user_id]) > SPAM_AUTO_BAN:
        await do_auto_ban(app, user, "Lạm dụng hệ thống")
        await update.message.reply_text(
            "Quyền truy cập của bạn đã bị thu hồi tự động.\n"
            f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}",
            protect_content=True
        )
        return True

    # Rate limit: < RATE_LIMIT_SEC giữa các request
    last_req = request_log[user_id][-2] if len(request_log[user_id]) >= 2 else 0
    if now - last_req < RATE_LIMIT_SEC:
        rate_hit_count[user_id] += 1
        if rate_hit_count[user_id] >= RATE_WARN_THRESH:
            await log_warning(app, user, "Bị rate limit nhiều lần", rate_hit_count[user_id])
            rate_hit_count[user_id] = 0
        return True

    return False

# ==================== AIOHTTP KEEP-ALIVE ====================
async def health_handler(request):
    return web.Response(text="ok")

async def start_web_server():
    webapp = web.Application()
    webapp.router.add_get("/", health_handler)
    runner = web.AppRunner(webapp)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"Web server running on port {PORT}")

# ==================== EXPIRE WORKER ====================
async def expire_worker(application: Application):
    jobs_col = application.bot_data["jobs_col"]
    while True:
        try:
            now    = datetime.now(timezone.utc)
            cursor = jobs_col.find({"expire_at": {"$lte": now}, "done": False})
            async for job in cursor:
                for msg_id in job.get("message_ids", []):
                    try:
                        await application.bot.delete_message(
                            chat_id=job["chat_id"],
                            message_id=msg_id
                        )
                    except Exception:
                        pass
                await jobs_col.update_one(
                    {"_id": job["_id"]},
                    {"$set": {"done": True}}
                )
        except Exception as e:
            logging.error(f"Expire worker error: {e}")
        await asyncio.sleep(60)

# ==================== USER: /start ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    user_id = user.id
    app     = context.application

    # Check ban
    banned_col = get_banned(context)
    try:
        is_banned = await db_retry(lambda: banned_col.find_one({"user_id": user_id}))
    except Exception:
        is_banned = False

    if is_banned:
        await update.message.reply_text(
            "Quyền truy cập của bạn đã bị thu hồi.\n"
            f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}",
            protect_content=True
        )
        return

    # Spam + rate check
    if await check_user(update, context):
        return

    args = context.args

    # Không có args → người lạ hoặc gõ thủ công
    if not args:
        manual_start_count[user_id] += 1
        if manual_start_count[user_id] >= MANUAL_WARN_THRESH:
            await log_warning(app, user, "Gõ /start thủ công nhiều lần", manual_start_count[user_id])
            manual_start_count[user_id] = 0
        await update.message.reply_text(
            "Hệ thống nội dung độc quyền.\n\n"
            "Bạn hiện chưa có quyền truy cập.\n"
            f"Vui lòng liên hệ quản trị viên: {ADMIN_CONTACT}",
            protect_content=True
        )
        return

    key        = args[0]
    albums_col = get_col(context)

    try:
        album = await db_retry(lambda: albums_col.find_one({"key": key}))
    except Exception:
        await update.message.reply_text(
            "Hệ thống đang xử lý dữ liệu ngầm. Vui lòng thử lại sau vài phút.",
            protect_content=True
        )
        return

    if not album:
        invalid_attempts[user_id] += 1
        count = invalid_attempts[user_id]
        if count >= INVALID_WARN_THRESH:
            await log_warning(app, user, "Bấm link không hợp lệ nhiều lần", count)
        if count >= INVALID_AUTO_BAN:
            await do_auto_ban(app, user, "Cố tình dò link")
            await update.message.reply_text(
                "Quyền truy cập của bạn đã bị thu hồi tự động.\n"
                f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}",
                protect_content=True
            )
            return
        await update.message.reply_text(
            "Dữ liệu không tồn tại hoặc phiên chia sẻ đã hết hạn.",
            protect_content=True
        )
        return

    # Reset invalid counter khi link hợp lệ
    invalid_attempts[user_id] = 0

    # Check member kênh VIP
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        if member.status in ("left", "kicked"):
            nonmember_attempts[user_id] += 1
            count = nonmember_attempts[user_id]
            if count >= NONMEMBER_WARN_THRESH:
                await log_warning(app, user, "Truy cập trái phép nhiều lần", count)
                nonmember_attempts[user_id] = 0
            await update.message.reply_text(
                "Xác thực không thành công.\n\n"
                "Nội dung được bảo mật và chỉ hiển thị với thành viên kênh nội bộ.\n"
                f"Đăng ký quyền truy cập: {ADMIN_CONTACT}",
                protect_content=True
            )
            return
    except Exception:
        pass

    nonmember_attempts[user_id] = 0

    # Log truy cập vào nhóm
    await log_view(app, user, key)

    # Gửi nội dung
    items        = album.get("items", [])
    sent_msg_ids = []

    if len(items) == 1:
        item = items[0]
        if item["type"] == "video":
            msg = await context.bot.send_video(
                chat_id=user_id, video=item["file_id"], protect_content=True
            )
        else:
            msg = await context.bot.send_photo(
                chat_id=user_id, photo=item["file_id"], protect_content=True
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
            chat_id=user_id, media=media, protect_content=True
        )
        sent_msg_ids = [m.message_id for m in msgs]

    # Lưu job xóa vào MongoDB
    jobs_col = get_jobs(context)
    await jobs_col.insert_one({
        "chat_id":     user_id,
        "message_ids": sent_msg_ids,
        "expire_at":   datetime.now(timezone.utc) + timedelta(seconds=DELETE_AFTER),
        "done":        False
    })

# ==================== ADMIN: /new ====================
async def new_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if context.user_data.get("current_key"):
        await update.message.reply_text(
            "Cảnh báo: Một tiến trình đóng gói đang diễn ra.\n"
            "Vui lòng nhập /done để kết thúc tác vụ hiện tại."
        )
        return

    key        = make_key()
    albums_col = get_col(context)
    await albums_col.insert_one({
        "key":        key,
        "items":      [],
        "created_at": datetime.now(timezone.utc)
    })
    context.user_data["current_key"] = key

    await update.message.reply_text(
        f"Khởi tạo lưu trữ mới.\n"
        f"ID: <code>{key}</code>\n\n"
        f"Vui lòng forward tệp tin vào đây. Nhập /done khi hoàn tất.",
        parse_mode="HTML"
    )

# ==================== ADMIN: /done ====================
async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    key = context.user_data.get("current_key")
    if not key:
        await update.message.reply_text("Không có tiến trình nào đang diễn ra.")
        return

    context.user_data.pop("current_key", None)
    await update.message.reply_text(
        f"Đóng gói hoàn tất.\n"
        f"Liên kết truy cập:\n<code>{make_link(key)}</code>",
        parse_mode="HTML"
    )

# ==================== ADMIN: /list ====================
async def list_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_col(context)
    try:
        albums = await db_retry(
            lambda: albums_col.find({}, {"key": 1, "items": 1}).to_list(length=50)
        )
    except Exception:
        await update.message.reply_text("Cảnh báo: Mất kết nối cơ sở dữ liệu.")
        return

    if not albums:
        await update.message.reply_text("Không có tệp dữ liệu nào trong hệ thống.")
        return

    text = "Danh sách lưu trữ:\n\n"
    for a in albums:
        text += f"<code>{a['key']}</code> — {len(a.get('items', []))} tệp\n"
    await update.message.reply_text(text, parse_mode="HTML")

# ==================== ADMIN: /check ====================
async def check_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Cú pháp: /check &lt;ID&gt;", parse_mode="HTML")
        return

    key        = context.args[0]
    albums_col = get_col(context)
    album      = await db_retry(lambda: albums_col.find_one({"key": key}))

    if not album:
        await update.message.reply_text(
            f"Không tìm thấy ID: <code>{key}</code>", parse_mode="HTML"
        )
        return

    items  = album.get("items", [])
    videos = sum(1 for i in items if i["type"] == "video")
    photos = sum(1 for i in items if i["type"] == "photo")

    await update.message.reply_text(
        f"Chi tiết: <code>{key}</code>\n"
        f"Tổng số tệp: {len(items)}\n"
        f"Video: {videos} | Ảnh: {photos}\n"
        f"Liên kết: <code>{make_link(key)}</code>",
        parse_mode="HTML"
    )

# ==================== ADMIN: /del ====================
async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Cú pháp: /del &lt;ID&gt;", parse_mode="HTML")
        return

    key        = context.args[0]
    albums_col = get_col(context)
    result     = await albums_col.delete_one({"key": key})

    if result.deleted_count == 0:
        await update.message.reply_text(
            f"Không tìm thấy ID: <code>{key}</code>", parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"Đã hủy vĩnh viễn tệp dữ liệu <code>{key}</code>.", parse_mode="HTML"
        )

# ==================== ADMIN: /clean ====================
async def clean_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_col(context)
    cutoff     = datetime.now(timezone.utc) - timedelta(days=7)
    result     = await albums_col.delete_many({
        "$or": [{"items": []}, {"created_at": {"$lt": cutoff}}]
    })
    await update.message.reply_text(
        f"Đã dọn dẹp bộ nhớ: {result.deleted_count} tệp dữ liệu rỗng/quá hạn."
    )

# ==================== ADMIN: /ban ====================
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text(
            "Cú pháp: /ban &lt;ID&gt; &lt;lý do&gt;\n\n"
            "Lý do gợi ý:\n"
            "share — Chia sẻ nội dung ra ngoài\n"
            "spam — Spam hệ thống\n"
            "fake — Tài khoản giả mạo\n"
            "expired — Hết hạn đăng ký\n"
            "other — Lý do khác",
            parse_mode="HTML"
        )
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID không hợp lệ.")
        return

    reason     = " ".join(context.args[1:]) if len(context.args) > 1 else "Không rõ lý do"
    banned_col = get_banned(context)

    # Lấy tên user nếu có thể
    try:
        chat = await context.bot.get_chat(target_id)
        name = chat.full_name
    except Exception:
        name = "Không rõ"

    await banned_col.update_one(
        {"user_id": target_id},
        {"$set": {
            "user_id":   target_id,
            "name":      name,
            "reason":    reason,
            "ban_type":  "Thủ công",
            "banned_at": datetime.now(timezone.utc)
        }},
        upsert=True
    )

    await update.message.reply_text(
        f"Đã đưa ID <code>{target_id}</code> vào danh sách đen.\n"
        f"Lý do: {reason}",
        parse_mode="HTML"
    )
    await log_ban(context.application, target_id, name, reason)

# ==================== ADMIN: /unban ====================
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Cú pháp: /unban &lt;ID&gt;", parse_mode="HTML")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID không hợp lệ.")
        return

    banned_col = get_banned(context)
    result     = await banned_col.delete_one({"user_id": target_id})

    if result.deleted_count:
        await update.message.reply_text(
            f"Đã khôi phục quyền cho ID <code>{target_id}</code>.", parse_mode="HTML"
        )
        await log_unban(context.application, target_id)
    else:
        await update.message.reply_text("Không tìm thấy ID trong danh sách đen.")

# ==================== ADMIN: /status ====================
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_col(context)
    banned_col = get_banned(context)
    try:
        total_albums = await albums_col.count_documents({})
        total_banned = await banned_col.count_documents({})
        await update.message.reply_text(
            f"Trạng thái hệ thống:\n\n"
            f"Phân vùng dữ liệu: {total_albums} tệp\n"
            f"Danh sách đen: {total_banned} tài khoản\n"
            f"Cơ sở dữ liệu: Kết nối ổn định"
        )
    except Exception:
        await update.message.reply_text("Cảnh báo: Mất kết nối cơ sở dữ liệu.")

# ==================== ADMIN: /help ====================
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    await update.message.reply_text(
        "Hướng dẫn vận hành:\n\n"
        "/new — Khởi tạo phân vùng dữ liệu mới\n"
        "/done — Đóng gói và xuất liên kết truy cập\n"
        "/list — Liệt kê toàn bộ dữ liệu\n"
        "/check &lt;ID&gt; — Xem chi tiết một phân vùng\n"
        "/del &lt;ID&gt; — Xóa vĩnh viễn dữ liệu\n"
        "/clean — Dọn dẹp dữ liệu quá hạn hoặc rỗng\n"
        "/ban &lt;ID&gt; &lt;lý do&gt; — Chặn người dùng\n"
        "/unban &lt;ID&gt; — Hủy chặn người dùng\n"
        "/status — Kiểm tra tình trạng máy chủ",
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
        file_id, file_type = update.message.video.file_id, "video"
    elif update.message.photo:
        file_id, file_type = update.message.photo[-1].file_id, "photo"
    else:
        return

    await albums_col.update_one(
        {"key": key},
        {"$push": {"items": {"type": file_type, "file_id": file_id}}}
    )
    await update.message.reply_text("Tệp đã được thêm vào phân vùng.")

# ==================== SETUP DB ====================
async def setup_db(application: Application):
    client = AsyncIOMotorClient(
        MONGO_URI,
        maxPoolSize=10,
        minPoolSize=0,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=10000,
        retryWrites=True
    )
    await client.admin.command("ping")
    db = client["botdb"]

    application.bot_data["albums_col"] = db["albums"]
    application.bot_data["banned_col"] = db["banned"]
    application.bot_data["jobs_col"]   = db["jobs"]
    application.bot_data["warns_col"]  = db["warns"]

    logging.info("DB connected!")

# ==================== RUN BOT ====================
async def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",  start))
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

    async with app:
        await setup_db(app)
        await start_web_server()
        await app.start()
        asyncio.create_task(expire_worker(app))
        logging.info("Bot started!")
        await app.updater.start_polling(drop_pending_updates=True)
        await asyncio.Event().wait()  # chạy mãi mãi

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logging.error(f"Bot crashed: {e} — restarting in 10s...")
            time.sleep(10)
