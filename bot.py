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
request_log:         dict = defaultdict(list)
invalid_attempts:    dict = defaultdict(int)
rate_hit_count:      dict = defaultdict(int)
manual_start_count:  dict = defaultdict(int)
nonmember_attempts:  dict = defaultdict(int)

RATE_LIMIT_SEC        = 5
SPAM_AUTO_BAN         = 10
INVALID_WARN_THRESH   = 3
INVALID_AUTO_BAN      = 5
RATE_WARN_THRESH      = 3
MANUAL_WARN_THRESH    = 3
NONMEMBER_WARN_THRESH = 3

# ==================== HELPERS ====================
def get_albums(ctx): return ctx.application.bot_data["albums_col"]
def get_banned(ctx): return ctx.application.bot_data["banned_col"]
def get_jobs(ctx):   return ctx.application.bot_data["jobs_col"]

def make_key() -> str:
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))

def make_link(key: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={key}"

def now_str() -> str:
    return datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M")

async def user_reply(update: Update, text: str, **kwargs):
    return await update.message.reply_text(
        text, protect_content=True, **kwargs
    )

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

# ==================== LOG ====================
async def send_log(app: Application, text: str):
    if not LOG_GROUP_ID:
        return
    try:
        await app.bot.send_message(
            chat_id=LOG_GROUP_ID, text=text, parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Log error: {e}")

async def log_view(app, user, key: str):
    uname = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Truy cập nội dung\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {user.full_name}\n"
        f"Username: {uname}\n"
        f"Album: <code>{key}</code>\n"
        f"Thời gian: {now_str()}"
    )

async def log_warning(app, user, behavior: str, count: int):
    uname = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Cảnh báo hành vi bất thường\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {user.full_name}\n"
        f"Username: {uname}\n"
        f"Hành vi: {behavior}\n"
        f"Số lần: {count}\n"
        f"Thời gian: {now_str()}"
    )

async def log_ban(app, target_id, name, reason, ban_type="Thủ công"):
    await send_log(app,
        f"Ban người dùng\n"
        f"ID: <code>{target_id}</code>\n"
        f"Tên: {name}\n"
        f"Lý do: {reason}\n"
        f"Loại: {ban_type}\n"
        f"Thời gian: {now_str()}"
    )

async def log_unban(app, target_id):
    await send_log(app,
        f"Hủy ban\n"
        f"ID: <code>{target_id}</code>\n"
        f"Thời gian: {now_str()}"
    )

# ==================== AUTO BAN ====================
async def do_auto_ban(app: Application, user, reason: str):
    banned_col = app.bot_data["banned_col"]
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
    await log_ban(app, user.id, user.full_name, reason, "Tự động")

# ==================== SPAM + RATE CHECK ====================
async def check_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user    = update.effective_user
    user_id = user.id
    app     = context.application
    now     = time.time()

    if user.is_bot:
        await do_auto_ban(app, user, "Phát hiện tài khoản bot")
        return True

    request_log[user_id] = [t for t in request_log[user_id] if now - t < 60]
    request_log[user_id].append(now)

    if len(request_log[user_id]) > SPAM_AUTO_BAN:
        await do_auto_ban(app, user, "Lạm dụng hệ thống")
        await user_reply(update,
            "Quyền truy cập của bạn đã bị thu hồi tự động.\n"
            f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}"
        )
        return True

    prev = request_log[user_id][-2] if len(request_log[user_id]) >= 2 else 0
    if now - prev < RATE_LIMIT_SEC:
        rate_hit_count[user_id] += 1
        if rate_hit_count[user_id] >= RATE_WARN_THRESH:
            await log_warning(app, user, "Bị rate limit nhiều lần", rate_hit_count[user_id])
            rate_hit_count[user_id] = 0
        return True

    return False

# ==================== CHECK VIP ====================
async def is_vip_member(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status not in ("left", "kicked")
    except Exception:
        return False

# ==================== KEEP-ALIVE ====================
async def health_handler(request):
    return web.Response(text="ok")

async def start_web_server():
    webapp = web.Application()
    webapp.router.add_get("/", health_handler)
    runner = web.AppRunner(webapp)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web server on port {PORT}")

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
                            chat_id=job["chat_id"], message_id=msg_id
                        )
                    except Exception:
                        pass
                await jobs_col.update_one(
                    {"_id": job["_id"]}, {"$set": {"done": True}}
                )
        except Exception as e:
            logging.error(f"Expire worker: {e}")
        await asyncio.sleep(60)
        # ==================== USER: /start ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    user_id = user.id
    app     = context.application

    banned_col = get_banned(context)
    try:
        is_banned = await db_retry(lambda: banned_col.find_one({"user_id": user_id}))
    except Exception:
        is_banned = False

    if is_banned:
        await user_reply(update,
            "Quyền truy cập của bạn đã bị thu hồi.\n"
            f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}"
        )
        return

    if await check_user(update, context):
        return

    args = context.args

    if not args:
        manual_start_count[user_id] += 1
        if manual_start_count[user_id] >= MANUAL_WARN_THRESH:
            await log_warning(app, user, "Gõ /start thủ công nhiều lần", manual_start_count[user_id])
            manual_start_count[user_id] = 0

        vip = await is_vip_member(context, user_id)
        if vip:
            await user_reply(update,
                "Bạn đã là thành viên kênh VIP.\n\n"
                "Vào kênh và bấm vào link trong bài đăng để xem nội dung."
            )
        else:
            await user_reply(update,
                "Hệ thống nội dung độc quyền.\n\n"
                "Bạn hiện chưa có quyền truy cập.\n"
                f"Vui lòng liên hệ quản trị viên: {ADMIN_CONTACT}"
            )
        return

    key        = args[0]
    albums_col = get_albums(context)

    try:
        album = await db_retry(lambda: albums_col.find_one({"key": key}))
    except Exception:
        await user_reply(update,
            "Hệ thống đang xử lý dữ liệu. Vui lòng thử lại sau vài phút."
        )
        return

    if not album:
        invalid_attempts[user_id] += 1
        count = invalid_attempts[user_id]
        if count >= INVALID_WARN_THRESH:
            await log_warning(app, user, "Bấm link không hợp lệ nhiều lần", count)
        if count >= INVALID_AUTO_BAN:
            await do_auto_ban(app, user, "Cố tình dò link")
            await user_reply(update,
                "Quyền truy cập của bạn đã bị thu hồi tự động.\n"
                f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}"
            )
            return
        await user_reply(update,
            "Dữ liệu không tồn tại hoặc phiên chia sẻ đã hết hạn."
        )
        return

    invalid_attempts[user_id] = 0

    vip = await is_vip_member(context, user_id)
    if not vip:
        nonmember_attempts[user_id] += 1
        count = nonmember_attempts[user_id]
        if count >= NONMEMBER_WARN_THRESH:
            await log_warning(app, user, "Truy cập trái phép nhiều lần", count)
            nonmember_attempts[user_id] = 0
        await user_reply(update,
            "Xác thực không thành công.\n\n"
            "Nội dung được bảo mật và chỉ hiển thị với thành viên kênh nội bộ.\n"
            f"Đăng ký quyền truy cập: {ADMIN_CONTACT}"
        )
        return

    nonmember_attempts[user_id] = 0
    await log_view(app, user, key)

    items = album.get("items", [])
    if not items:
        await user_reply(update,
            "Dữ liệu không tồn tại hoặc phiên chia sẻ đã hết hạn."
        )
        return

    sent_msg_ids = []

    if len(items) == 1:
        item = items[0]
        try:
            if item["type"] == "video":
                msg = await context.bot.send_video(
                    chat_id=user_id, video=item["file_id"], protect_content=True
                )
            else:
                msg = await context.bot.send_photo(
                    chat_id=user_id, photo=item["file_id"], protect_content=True
                )
            sent_msg_ids.append(msg.message_id)
        except Exception as e:
            logging.error(f"Send single error: {e}")
            await user_reply(update,
                "Hệ thống đang xử lý dữ liệu. Vui lòng thử lại sau vài phút."
            )
            return
    else:
        for i in range(0, len(items), 10):
            batch = items[i:i+10]
            media = []
            for item in batch:
                if item["type"] == "video":
                    media.append(InputMediaVideo(media=item["file_id"]))
                else:
                    media.append(InputMediaPhoto(media=item["file_id"]))
            try:
                msgs = await context.bot.send_media_group(
                    chat_id=user_id, media=media, protect_content=True
                )
                sent_msg_ids.extend([m.message_id for m in msgs])
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.error(f"Send media group error: {e}")
                await user_reply(update,
                    "Hệ thống đang xử lý dữ liệu. Vui lòng thử lại sau vài phút."
                )
                return

    if sent_msg_ids:
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
            "Đang có album chưa hoàn thành.\n"
            "Gõ /done để lấy link trước."
        )
        return

    key        = make_key()
    albums_col = get_albums(context)
    await albums_col.insert_one({
        "key":        key,
        "items":      [],
        "created_at": datetime.now(timezone.utc)
    })
    context.user_data["current_key"] = key

    await update.message.reply_text(
        f"Album mới đã tạo.\n"
        f"Key: <code>{key}</code>\n\n"
        f"Forward ảnh hoặc video vào đây.\n"
        f"Gõ /done khi xong.",
        parse_mode="HTML"
    )

# ==================== ADMIN: NHẬN MEDIA ====================
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    key = context.user_data.get("current_key")
    if not key:
        return

    albums_col = get_albums(context)

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

    album = await albums_col.find_one({"key": key}, {"items": 1})
    count = len(album.get("items", [])) if album else 0

    await update.message.reply_text(
        f"Đã nhận {file_type} — album có {count} file.\n"
        f"Gõ /done khi xong."
    )

# ==================== ADMIN: /done ====================
async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    key = context.user_data.get("current_key")
    if not key:
        await update.message.reply_text("Không có album nào đang tạo.")
        return

    albums_col = get_albums(context)
    album      = await albums_col.find_one({"key": key})

    if not album or not album.get("items"):
        await update.message.reply_text(
            "Album chưa có file nào.\n"
            "Hãy forward ảnh hoặc video vào trước."
        )
        return

    context.user_data.pop("current_key", None)
    count = len(album.get("items", []))

    await update.message.reply_text(
        f"Hoàn tất. Album có {count} file.\nLink chia sẻ:"
    )
    await update.message.reply_text(make_link(key))

# ==================== ADMIN: /list ====================
async def list_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_albums(context)
    try:
        albums = await db_retry(
            lambda: albums_col.find(
                {}, {"key": 1, "items": 1}
            ).sort("created_at", -1).to_list(length=50)
        )
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return

    if not albums:
        await update.message.reply_text("Chưa có album nào.")
        return

    text = f"Danh sách album ({len(albums)}):\n\n"
    for i, a in enumerate(albums, 1):
        text += f"{i}. <code>{a['key']}</code> — {len(a.get('items', []))} file\n"

    await update.message.reply_text(text, parse_mode="HTML")

# ==================== ADMIN: /detail ====================
async def detail_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_albums(context)
    try:
        albums = await db_retry(
            lambda: albums_col.find(
                {}, {"key": 1, "items": 1}
            ).sort("created_at", -1).to_list(length=50)
        )
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return

    if not albums:
        await update.message.reply_text("Chưa có album nào.")
        return

    for i, a in enumerate(albums, 1):
        key   = a["key"]
        count = len(a.get("items", []))
        await update.message.reply_text(
            f"{i}\nSố file: {count}\nLink truy cập:"
        )
        await update.message.reply_text(make_link(key))
        await asyncio.sleep(0.3)
        # ==================== ADMIN: /check ====================
async def check_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text(
            "Dùng: /check <key>\nKey lấy từ /list hoặc /done."
        )
        return

    key        = context.args[0]
    albums_col = get_albums(context)

    try:
        album = await db_retry(lambda: albums_col.find_one({"key": key}))
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return

    if not album:
        await update.message.reply_text(
            f"Không tìm thấy album với key {key}.\n"
            f"Dùng /list để xem danh sách key."
        )
        return

    items  = album.get("items", [])
    videos = sum(1 for i in items if i["type"] == "video")
    photos = sum(1 for i in items if i["type"] == "photo")

    await update.message.reply_text(
        f"Album: {key}\n"
        f"Tổng: {len(items)} file\n"
        f"Video: {videos} | Ảnh: {photos}\n"
        f"Link:"
    )
    await update.message.reply_text(make_link(key))

# ==================== ADMIN: /del ====================
async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Dùng: /del <key>")
        return

    key        = context.args[0]
    albums_col = get_albums(context)

    try:
        result = await albums_col.delete_one({"key": key})
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return

    if result.deleted_count == 0:
        await update.message.reply_text(f"Không tìm thấy album với key {key}.")
    else:
        await update.message.reply_text(f"Đã xóa album {key}.")

# ==================== ADMIN: /clean ====================
async def clean_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_albums(context)
    cutoff     = datetime.now(timezone.utc) - timedelta(days=7)

    try:
        result = await albums_col.delete_many({
            "$or": [
                {"items": []},
                {"created_at": {"$lt": cutoff}}
            ]
        })
        await update.message.reply_text(
            f"Đã xóa {result.deleted_count} album trống hoặc cũ hơn 7 ngày."
        )
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")

# ==================== ADMIN: /ban ====================
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text(
            "Dùng: /ban <ID> <lý do>\n\n"
            "Lý do gợi ý:\n"
            "share — Chia sẻ nội dung ra ngoài\n"
            "spam — Spam hệ thống\n"
            "fake — Tài khoản giả mạo\n"
            "expired — Hết hạn đăng ký\n"
            "other — Lý do khác"
        )
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID không hợp lệ, phải là số.")
        return

    reason     = " ".join(context.args[1:]) if len(context.args) > 1 else "Không rõ lý do"
    banned_col = get_banned(context)

    try:
        chat = await context.bot.get_chat(target_id)
        name = chat.full_name
    except Exception:
        name = "Không rõ"

    try:
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
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return

    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                "Quyền truy cập của bạn đã bị thu hồi.\n"
                f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}"
            ),
            protect_content=True
        )
    except Exception:
        pass

    await update.message.reply_text(
        f"Đã ban {target_id} ({name}).\nLý do: {reason}"
    )
    await log_ban(context.application, target_id, name, reason)

# ==================== ADMIN: /unban ====================
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Dùng: /unban <ID>")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID không hợp lệ, phải là số.")
        return

    banned_col = get_banned(context)

    try:
        result = await banned_col.delete_one({"user_id": target_id})
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return

    if result.deleted_count:
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text=(
                    "Quyền truy cập của bạn đã được khôi phục.\n"
                    "Vào kênh VIP và bấm link trong bài đăng để xem nội dung."
                ),
                protect_content=True
            )
        except Exception:
            pass
        await update.message.reply_text(f"Đã gỡ ban {target_id}.")
        await log_unban(context.application, target_id)
    else:
        await update.message.reply_text("Không tìm thấy ID này trong danh sách ban.")

# ==================== ADMIN: /status ====================
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    albums_col = get_albums(context)
    banned_col = get_banned(context)
    jobs_col   = get_jobs(context)

    try:
        total_albums = await albums_col.count_documents({})
        total_banned = await banned_col.count_documents({})
        pending_jobs = await jobs_col.count_documents({"done": False})
        await update.message.reply_text(
            f"Trạng thái hệ thống:\n\n"
            f"Album: {total_albums}\n"
            f"Danh sách ban: {total_banned} tài khoản\n"
            f"Job chờ xóa: {pending_jobs}\n"
            f"Cơ sở dữ liệu: Kết nối ổn định"
        )
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")

# ==================== ADMIN: /help ====================
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    await update.message.reply_text(
        "Hướng dẫn sử dụng:\n\n"
        "/new — Tạo album mới, forward ảnh/video vào, gõ /done khi xong\n"
        "/done — Lấy link chia sẻ\n"
        "/list — Danh sách tất cả album\n"
        "/detail — Danh sách album kèm link đầy đủ\n"
        "/check <key> — Xem thông tin 1 album\n"
        "/del <key> — Xóa album\n"
        "/clean — Xóa album trống hoặc cũ hơn 7 ngày\n"
        "/ban <ID> <lý do> — Chặn người dùng\n"
        "/unban <ID> — Gỡ chặn người dùng\n"
        "/status — Trạng thái hệ thống\n\n"
        "Lưu ý: key là mã album, lấy từ /list hoặc /done."
    )

# ==================== SETUP DB ====================
async def setup_db(application: Application):
    client = AsyncIOMotorClient(
        MONGO_URI,
        maxPoolSize=10,
        minPoolSize=0,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=20000,
        retryWrites=True,
        tls=True,
        tlsAllowInvalidCertificates=True
    )
    await client.admin.command("ping")
    db = client["botdb"]

    albums_col = db["albums"]
    jobs_col   = db["jobs"]
    banned_col = db["banned"]

    await albums_col.create_index("key", unique=True)
    await jobs_col.create_index([("expire_at", 1), ("done", 1)])
    await banned_col.create_index("user_id", unique=True)

    application.bot_data["albums_col"] = albums_col
    application.bot_data["banned_col"] = banned_col
    application.bot_data["jobs_col"]   = jobs_col

    logging.info("DB connected + indexes created!")

# ==================== MAIN ====================
async def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler(["new_album", "new"], new_album))
    app.add_handler(CommandHandler("done",   done))
    app.add_handler(CommandHandler("list",   list_albums))
    app.add_handler(CommandHandler("detail", detail_albums))
    app.add_handler(CommandHandler("check",  check_album))
    app.add_handler(CommandHandler(["del_album", "del"], delete_album))
    app.add_handler(CommandHandler("clean",  clean_albums))
    app.add_handler(CommandHandler("ban",    ban_user))
    app.add_handler(CommandHandler("unban",  unban_user))
    app.add_handler(CommandHandler("status", status_cmd))
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
        await asyncio.Event().wait()

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logging.error(f"Bot crashed: {e} — restarting in 10s...")
            time.sleep(10)
