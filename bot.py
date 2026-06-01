import os
import asyncio
import logging
import time
import secrets
import string
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web
from telegram import Update, InputMediaPhoto, InputMediaVideo, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackQueryHandler
)
from telegram.error import Forbidden, BadRequest, TelegramError

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

# ==================== KIỂM TRA ENV ====================
def check_env():
    missing = []
    for var in ["TOKEN", "ADMIN_ID", "MONGO_URI", "CHANNEL_ID", "BOT_USERNAME"]:
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        logging.critical(f"Thiếu biến môi trường: {', '.join(missing)}")
        exit(1)

# ==================== IN-MEMORY TRACKING ====================
request_log:          dict = defaultdict(list)
invalid_attempts:     dict = defaultdict(int)
rate_hit_count:       dict = defaultdict(int)
manual_start_count:   dict = defaultdict(int)
nonmember_attempts:   dict = defaultdict(int)
warn_count:           dict = defaultdict(int)

RATE_LIMIT_SEC        = 5
SPAM_WARN_THRESH      = 5
SPAM_TEMP_BAN         = 8
SPAM_PERM_BAN         = 60
INVALID_WARN_THRESH   = 3
INVALID_AUTO_BAN      = 5
RATE_WARN_THRESH      = 3
MANUAL_WARN_THRESH    = 3
NONMEMBER_WARN_THRESH = 3

# ==================== LÝ DO BAN ====================
LY_DO_GỢI_Ý = {
    "quay-roi":  "Quấy rối thành viên",
    "chia-se":   "Chia sẻ nội dung ra ngoài",
    "spam":      "Spam tin nhắn",
    "gia-mao":   "Tài khoản giả mạo",
    "het-han":   "Hết hạn đăng ký",
    "ban-lai":   "Bán lại quyền truy cập",
    "vi-pham":   "Vi phạm quy định",
    "abuse":     "Hành vi phá hoại",
    "da-nghi":   "Tài khoản đáng ngờ",
    "nhieu-tk":  "Dùng nhiều tài khoản",
    "hoan-tien": "Đòi hoàn tiền/bùng tiền",
}

# ==================== PARSE THỜI HẠN BAN ====================
def parse_duration(text: str):
    """Trả về timedelta hoặc None nếu là ban vĩnh viễn."""
    match = re.match(r'^(\d+)(g|ng)$', text.lower())
    if not match:
        return None
    value = int(match.group(1))
    unit  = match.group(2)
    if unit == "g":
        return timedelta(hours=value)
    elif unit == "ng":
        return timedelta(days=value)
    return None

# ==================== HELPERS ====================
def get_albums(ctx): return ctx.application.bot_data["albums_col"]
def get_banned(ctx): return ctx.application.bot_data["banned_col"]
def get_jobs(ctx):   return ctx.application.bot_data["jobs_col"]

def make_key() -> str:
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))

def make_link(key: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={key}"

def now_str() -> str:
    return datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M")

def sanitize_log(text: str, max_len: int = 500) -> str:
    """Cắt ngắn và làm sạch text trước khi gửi log."""
    text = text.replace("<", "&lt;").replace(">", "&gt;")
    if len(text) > max_len:
        text = text[:max_len] + "..."
    return text

async def user_reply(update: Update, text: str, **kwargs):
    return await update.message.reply_text(
        text, protect_content=True, **kwargs
    )

async def auto_delete_msg(bot, chat_id: int, message_id: int, delay: int):
    """Tự động xóa tin nhắn sau delay giây."""
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def user_reply_temp(update: Update, context, text: str,
                           delay: int = 120, **kwargs):
    """Gửi tin nhắn rồi tự xóa sau delay giây."""
    msg = await update.message.reply_text(
        text, protect_content=True, **kwargs
    )
    asyncio.create_task(auto_delete_msg(
        context.bot,
        update.effective_chat.id,
        msg.message_id,
        delay
    ))
    return msg

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
_log_buffer:    list = []
_log_lock = asyncio.Lock() if False else None  # khởi tạo sau

async def send_log(app: Application, text: str, reply_markup=None):
    if not LOG_GROUP_ID:
        return
    text = sanitize_log(text)
    try:
        await app.bot.send_message(
            chat_id=LOG_GROUP_ID,
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup
        )
    except Exception as e:
        logging.error(f"Log error: {e}")

async def log_view(app, user, key: str):
    uname = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Truy cập nội dung\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {sanitize_log(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Album: <code>{key}</code>\n"
        f"Thời gian: {now_str()}"
    )

async def log_warning(app, user, behavior: str, count: int):
    uname = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Cảnh báo hành vi bất thường\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {sanitize_log(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Hành vi: {sanitize_log(behavior)}\n"
        f"Số lần: {count}\n"
        f"Thời gian: {now_str()}"
    )

async def log_ban(app, target_id, name, reason, ban_type="Thủ công", show_unban_btn=False):
    keyboard = None
    if show_unban_btn:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Gỡ ban ngay", callback_data=f"unban_{target_id}")
        ]])
    await send_log(app,
        f"Ban người dùng\n"
        f"ID: <code>{target_id}</code>\n"
        f"Tên: {sanitize_log(str(name))}\n"
        f"Lý do: {sanitize_log(reason)}\n"
        f"Loại: {ban_type}\n"
        f"Thời gian: {now_str()}",
        reply_markup=keyboard
    )

async def log_unban(app, target_id):
    await send_log(app,
        f"Hủy ban\n"
        f"ID: <code>{target_id}</code>\n"
        f"Thời gian: {now_str()}"
    )

async def log_suspicious(app, user, reason: str):
    uname = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Tài khoản đáng ngờ\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {sanitize_log(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Lý do: {reason}\n"
        f"Thời gian: {now_str()}"
    )

# ==================== BAN / UNBAN HELPER ====================
async def do_ban(app: Application, target_id: int, name: str,
                 reason: str, ban_type: str = "Tự động",
                 duration: timedelta = None):
    banned_col = app.bot_data["banned_col"]
    expire_at  = None
    if duration:
        expire_at = datetime.now(timezone.utc) + duration

    await banned_col.update_one(
        {"user_id": target_id},
        {"$set": {
            "user_id":   target_id,
            "name":      name,
            "reason":    reason,
            "ban_type":  ban_type,
            "expire_at": expire_at,
            "banned_at": datetime.now(timezone.utc)
        }},
        upsert=True
    )
    await log_ban(app, target_id, name, reason, ban_type,
                  show_unban_btn=(ban_type == "Tự động"))

async def do_unban(app: Application, target_id: int):
    banned_col = app.bot_data["banned_col"]
    await banned_col.delete_one({"user_id": target_id})
    await log_unban(app, target_id)

# ==================== CALLBACK: NÚT GỠ BAN ====================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_user.id != ADMIN_ID:
        await query.answer("Bạn không có quyền!")
        return

    data = query.data
    if data.startswith("unban_"):
        target_id  = int(data.split("_")[1])
        banned_col = get_banned(context)
        result     = await banned_col.delete_one({"user_id": target_id})
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
            await query.answer("Đã gỡ ban!")
            await query.edit_message_text(
                query.message.text + f"\n\n✅ Đã gỡ ban lúc {now_str()}"
            )
            await log_unban(context.application, target_id)
        else:
            await query.answer("Không tìm thấy trong danh sách ban!")

# ==================== KIỂM TRA VIP ====================
async def is_vip_member(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status not in ("left", "kicked")
    except Exception as e:
        logging.error(f"Check VIP error: {e}")
        return False  # Lỗi → từ chối, không cho qua

# ==================== SPAM + RATE CHECK ====================
async def check_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user    = update.effective_user
    user_id = user.id
    app     = context.application
    now     = time.time()

    # Chặn bot account
    if user.is_bot:
        await do_ban(app, user_id, user.full_name, "Phát hiện tài khoản bot")
        return True

    # Phát hiện tài khoản đáng ngờ
    if not user.username:
        await log_suspicious(app, user, "Không có username")
    if not user.last_name and len(user.first_name) < 2:
        await log_suspicious(app, user, "Tên quá ngắn, có thể là bot")

    # Spam detection
    request_log[user_id] = [t for t in request_log[user_id] if now - t < 60]
    request_log[user_id].append(now)
    count = len(request_log[user_id])

    if count >= SPAM_PERM_BAN:
        await do_ban(app, user_id, user.full_name, "Lạm dụng hệ thống nghiêm trọng")
        await user_reply_temp(update, context,
            "Quyền truy cập của bạn đã bị thu hồi vĩnh viễn.\n"
            f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}",
            delay=300
        )
        return True

    if count >= SPAM_TEMP_BAN:
        warn_count[user_id] += 1
        if warn_count[user_id] >= 2:
            await do_ban(app, user_id, user.full_name,
                         "Lạm dụng hệ thống nhiều lần", duration=timedelta(hours=24))
        else:
            await do_ban(app, user_id, user.full_name,
                         "Lạm dụng hệ thống", duration=timedelta(hours=1))
        await user_reply_temp(update, context,
            "Tài khoản của bạn bị tạm khóa do hành vi bất thường.\n"
            f"Liên hệ admin nếu cần hỗ trợ: {ADMIN_CONTACT}",
            delay=60
        )
        return True

    if count >= SPAM_WARN_THRESH:
        await log_warning(app, user, "Spam request nhiều lần", count)

    # Rate limit
    prev = request_log[user_id][-2] if len(request_log[user_id]) >= 2 else 0
    if now - prev < RATE_LIMIT_SEC:
        rate_hit_count[user_id] += 1
        if rate_hit_count[user_id] >= RATE_WARN_THRESH:
            await log_warning(app, user, "Bị rate limit nhiều lần", rate_hit_count[user_id])
            rate_hit_count[user_id] = 0
        return True

    return False

# ==================== KEEP-ALIVE ====================
async def health_handler(request):
    return web.Response(text="ok")

async def db_health_handler(request):
    try:
        client = request.app["mongo_client"]
        await client.admin.command("ping")
        return web.Response(text="ok - DB connected")
    except Exception as e:
        return web.Response(text=f"error - DB: {e}", status=500)

async def start_web_server(mongo_client):
    webapp = web.Application()
    webapp["mongo_client"] = mongo_client
    webapp.router.add_get("/", health_handler)
    webapp.router.add_get("/health", db_health_handler)
    runner = web.AppRunner(webapp)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web server on port {PORT}")

# ==================== EXPIRE WORKER ====================
async def expire_worker(application: Application):
    """Xóa message hết hạn sau 10 phút."""
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
                    except (Forbidden, BadRequest):
                        pass  # User block bot hoặc message đã xóa → bỏ qua
                    except Exception as e:
                        logging.error(f"Delete msg error: {e}")
                await jobs_col.update_one(
                    {"_id": job["_id"]}, {"$set": {"done": True}}
                )
        except Exception as e:
            logging.error(f"Expire worker: {e}")
        await asyncio.sleep(60)

# ==================== UNBAN WORKER ====================
async def unban_worker(application: Application):
    """Tự động gỡ ban hết hạn."""
    banned_col = application.bot_data["banned_col"]
    while True:
        try:
            now    = datetime.now(timezone.utc)
            cursor = banned_col.find({
                "expire_at": {"$lte": now, "$ne": None}
            })
            async for ban in cursor:
                target_id = ban["user_id"]
                await banned_col.delete_one({"_id": ban["_id"]})
                try:
                    await application.bot.send_message(
                        chat_id=target_id,
                        text=(
                            "Lệnh tạm khóa của bạn đã hết hạn.\n"
                            "Quyền truy cập đã được khôi phục.\n"
                            "Vào kênh VIP và bấm link để xem nội dung."
                        ),
                        protect_content=True
                    )
                except Exception:
                    pass
                await log_unban(application, target_id)
        except Exception as e:
            logging.error(f"Unban worker: {e}")
        await asyncio.sleep(60)

# ==================== RETRY GỬI MEDIA ====================
async def send_with_retry(coro_func, retries=3, delay=2):
    for i in range(retries):
        try:
            return await coro_func()
        except TelegramError as e:
            if "Too Many Requests" in str(e):
                wait = delay * (2 ** i)
                logging.warning(f"Flood control, chờ {wait}s...")
                await asyncio.sleep(wait)
            else:
                raise
    raise Exception("Gửi media thất bại sau nhiều lần thử")

# ==================== USER: /start ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    user_id = user.id
    app     = context.application

    # Kiểm tra ban
    banned_col = get_banned(context)
    try:
        ban_doc = await db_retry(lambda: banned_col.find_one({"user_id": user_id}))
    except Exception:
        ban_doc = None

    if ban_doc:
        reason    = ban_doc.get("reason", "Vi phạm quy định")
        expire_at = ban_doc.get("expire_at")
        if expire_at:
            expire_str = expire_at.strftime("%d/%m/%Y %H:%M")
            await user_reply(update,
                f"Tài khoản của bạn đang bị tạm khóa.\n"
                f"Lý do: {reason}\n"
                f"Hết hạn lúc: {expire_str}\n"
                f"Liên hệ admin: {ADMIN_CONTACT}"
            )
        else:
            await user_reply(update,
                f"Quyền truy cập của bạn đã bị thu hồi.\n"
                f"Lý do: {reason}\n"
                f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}"
            )
        return

    if await check_user(update, context):
        return

    args = context.args

    # Không có args
    if not args:
        manual_start_count[user_id] += 1
        if manual_start_count[user_id] >= MANUAL_WARN_THRESH:
            await log_warning(app, user, "Gõ /start thủ công nhiều lần",
                              manual_start_count[user_id])
            manual_start_count[user_id] = 0

        vip = await is_vip_member(context, user_id)
        if vip:
            await user_reply_temp(update, context,
                "Bạn đã là thành viên kênh VIP.\n\n"
                "Vào kênh và bấm vào link trong bài đăng để xem nội dung.",
                delay=120
            )
        else:
            await user_reply_temp(update, context,
                "Hệ thống nội dung độc quyền.\n\n"
                "Bạn hiện chưa có quyền truy cập.\n"
                f"Vui lòng liên hệ quản trị viên: {ADMIN_CONTACT}",
                delay=300
            )
        return

    key        = args[0]
    albums_col = get_albums(context)

    try:
        album = await db_retry(lambda: albums_col.find_one({"key": key}))
    except Exception:
        await user_reply_temp(update, context,
            "Hệ thống đang xử lý dữ liệu. Vui lòng thử lại sau vài phút.",
            delay=60
        )
        return

    if not album:
        invalid_attempts[user_id] += 1
        count = invalid_attempts[user_id]
        if count >= INVALID_WARN_THRESH:
            await log_warning(app, user, "Bấm link không hợp lệ nhiều lần", count)
        if count >= INVALID_AUTO_BAN:
            await do_ban(app, user_id, user.full_name, "Cố tình dò link")
            await user_reply_temp(update, context,
                "Quyền truy cập của bạn đã bị thu hồi tự động.\n"
                f"Nếu cho rằng đây là nhầm lẫn, vui lòng liên hệ: {ADMIN_CONTACT}",
                delay=300
            )
            return
        await user_reply_temp(update, context,
            "Dữ liệu không tồn tại hoặc phiên chia sẻ đã hết hạn.",
            delay=120
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
        await user_reply_temp(update, context,
            "Xác thực không thành công.\n\n"
            "Nội dung được bảo mật và chỉ hiển thị với thành viên kênh nội bộ.\n"
            f"Đăng ký quyền truy cập: {ADMIN_CONTACT}",
            delay=120
        )
        return

    nonmember_attempts[user_id] = 0
    await log_view(app, user, key)

    items = album.get("items", [])
    if not items:
        await user_reply_temp(update, context,
            "Dữ liệu không tồn tại hoặc phiên chia sẻ đã hết hạn.",
            delay=120
        )
        return

    sent_msg_ids = []

    if len(items) == 1:
        item = items[0]
        try:
            if item["type"] == "video":
                msg = await send_with_retry(lambda: context.bot.send_video(
                    chat_id=user_id, video=item["file_id"],
                    protect_content=True, has_spoiler=True
                ))
            else:
                msg = await send_with_retry(lambda: context.bot.send_photo(
                    chat_id=user_id, photo=item["file_id"],
                    protect_content=True, has_spoiler=True
                ))
            sent_msg_ids.append(msg.message_id)
        except Exception as e:
            logging.error(f"Send single error: {e}")
            await user_reply_temp(update, context,
                "Hệ thống đang xử lý dữ liệu. Vui lòng thử lại sau vài phút.",
                delay=120
            )
            return
    else:
        for i in range(0, len(items), 10):
            batch = items[i:i+10]
            media = []
            for item in batch:
                if item["type"] == "video":
                    media.append(InputMediaVideo(media=item["file_id"], has_spoiler=True))
                else:
                    media.append(InputMediaPhoto(media=item["file_id"], has_spoiler=True))
            try:
                msgs = await send_with_retry(lambda: context.bot.send_media_group(
                    chat_id=user_id, media=media, protect_content=True
                ))
                sent_msg_ids.extend([m.message_id for m in msgs])
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.error(f"Send media group error: {e}")
                await user_reply_temp(update, context,
                    "Hệ thống đang xử lý dữ liệu. Vui lòng thử lại sau vài phút.",
                    delay=120
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

# ==================== HANDLER LỆNH KHÔNG CÓ QUYỀN ====================
async def no_permission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        return
    await user_reply_temp(update, context,
        "Bạn không có quyền sử dụng lệnh này.",
        delay=120
    )

# ==================== ADMIN: /new ====================
async def new_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
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
        f"Mã: <code>{key}</code>\n\n"
        f"Forward ảnh hoặc video vào đây.\n"
        f"Gõ /done khi xong.",
        parse_mode="HTML"
    )

# ==================== ADMIN: NHẬN MEDIA ====================
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
        return

    # Forward tin nhắn → trả về ID
    if update.message.forward_from:
        fwd = update.message.forward_from
        uname = f"@{fwd.username}" if fwd.username else "Không có"
        await update.message.reply_text(
            f"Thông tin tài khoản:\n"
            f"ID: <code>{fwd.id}</code>\n"
            f"Tên: {fwd.full_name}\n"
            f"Username: {uname}",
            parse_mode="HTML"
        )
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
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
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
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
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
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
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
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
        return

    if not context.args:
        await update.message.reply_text(
            "Dùng: /check <mã>\n"
            "Mã lấy từ /list hoặc /done."
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
            f"Không tìm thấy album với mã {key}.\n"
            f"Dùng /list để xem danh sách."
        )
        return

    items  = album.get("items", [])
    videos = sum(1 for i in items if i["type"] == "video")
    photos = sum(1 for i in items if i["type"] == "photo")

    await update.message.reply_text(
        f"Album: <code>{key}</code>\n"
        f"Tổng: {len(items)} file\n"
        f"Video: {videos} | Ảnh: {photos}\n"
        f"Link:",
        parse_mode="HTML"
    )
    await update.message.reply_text(make_link(key))

# ==================== ADMIN: /del ====================
async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
        return

    if not context.args:
        await update.message.reply_text("Dùng: /del <mã>")
        return

    key        = context.args[0]
    albums_col = get_albums(context)
    jobs_col   = get_jobs(context)

    try:
        result = await albums_col.delete_one({"key": key})
        # Cascade xóa job liên quan
        await jobs_col.delete_many({"album_key": key})
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return

    if result.deleted_count == 0:
        await update.message.reply_text(f"Không tìm thấy album với mã {key}.")
    else:
        await update.message.reply_text(f"Đã xóa album {key}.")

# ==================== ADMIN: /clean ====================
async def clean_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
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

# ==================== BAN TRONG NHÓM VÀ BOT ====================
async def _do_ban_member(update: Update, context: ContextTypes.DEFAULT_TYPE,
                          target_id: int, target_name: str,
                          reason: str, duration: timedelta = None):
    """Ban khỏi nhóm + ban khỏi bot + thông báo + log."""
    app = context.application

    # Kick khỏi nhóm nếu lệnh từ nhóm
    if update.effective_chat.type in ("group", "supergroup"):
        try:
            await context.bot.ban_chat_member(
                chat_id=update.effective_chat.id,
                user_id=target_id
            )
        except Exception as e:
            await update.message.reply_text(f"Không thể kick khỏi nhóm: {e}")
            return

    # Ban khỏi bot
    await do_ban(app, target_id, target_name, reason,
                 ban_type="Thủ công", duration=duration)

    # Thông báo cho user bị ban
    try:
        expire_text = ""
        if duration:
            expire_at  = datetime.now(timezone(timedelta(hours=7))) + duration
            expire_text = f"\nHết hạn lúc: {expire_at.strftime('%d/%m/%Y %H:%M')}"
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"Bạn đã bị cấm.\n"
                f"Lý do: {reason}{expire_text}\n"
                f"Liên hệ admin: {ADMIN_CONTACT}"
            ),
            protect_content=True
        )
    except Exception:
        pass

    # Thông báo trong nhóm/chat
    expire_info = ""
    if duration:
        hours = int(duration.total_seconds() // 3600)
        days  = int(duration.total_seconds() // 86400)
        if days >= 1:
            expire_info = f" ({days} ngày)"
        else:
            expire_info = f" ({hours} giờ)"

    await update.message.reply_text(
        f"Đã cấm {target_name}{expire_info}.\nLý do: {reason}"
    )

# ==================== ADMIN: /ban ====================
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
        return

    target_id   = None
    target_name = "Không rõ"
    duration    = None
    reason      = "Vi phạm quy định"
    args        = context.args or []

    # Cách 1: Reply tin nhắn
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        target_id   = target_user.id
        target_name = target_user.full_name
        # Parse args: [thời_hạn?] [lý_do...]
        if args:
            dur = parse_duration(args[0])
            if dur:
                duration = dur
                reason   = " ".join(args[1:]) if len(args) > 1 else "Vi phạm quy định"
            else:
                reason   = " ".join(args)
    elif args:
        first = args[0]
        # Cách 2: @username
        if first.startswith("@"):
            try:
                chat        = await context.bot.get_chat(first)
                target_id   = chat.id
                target_name = chat.full_name
            except Exception:
                await update.message.reply_text("Không tìm thấy username này.")
                return
            rest = args[1:]
        # Cách 3: ID
        else:
            try:
                target_id = int(first)
                try:
                    chat        = await context.bot.get_chat(target_id)
                    target_name = chat.full_name
                except Exception:
                    pass
            except ValueError:
                await update.message.reply_text("ID không hợp lệ, phải là số.")
                return
            rest = args[1:]

        # Parse thời hạn và lý do từ phần còn lại
        if rest:
            dur = parse_duration(rest[0])
            if dur:
                duration = dur
                reason   = " ".join(rest[1:]) if len(rest) > 1 else "Vi phạm quy định"
            else:
                reason   = " ".join(rest)

    else:
        await update.message.reply_text(
            "Cách dùng lệnh /ban:\n\n"
            "1. Bấm giữ tin nhắn → Trả lời → /ban <lý do>\n"
            "2. /ban @tentaikhoan <lý do>\n"
            "3. /ban 123456789 <lý do>\n\n"
            "Ban có thời hạn:\n"
            "/ban @tentaikhoan 1g <lý do> — cấm 1 giờ\n"
            "/ban @tentaikhoan 24g <lý do> — cấm 24 giờ\n"
            "/ban @tentaikhoan 7ng <lý do> — cấm 7 ngày\n\n"
            "Lý do gợi ý:\n"
            "quay-roi — Quấy rối thành viên\n"
            "chia-se — Chia sẻ nội dung ra ngoài\n"
            "spam — Spam tin nhắn\n"
            "gia-mao — Tài khoản giả mạo\n"
            "het-han — Hết hạn đăng ký\n"
            "ban-lai — Bán lại quyền truy cập\n"
            "vi-pham — Vi phạm quy định"
        )
        return

    if not target_id:
        await update.message.reply_text("Không xác định được người dùng.")
        return

    if target_id == ADMIN_ID:
        await update.message.reply_text("Không thể ban admin.")
        return

    # Dịch lý do nếu dùng mã gợi ý
    reason = LY_DO_GỢI_Ý.get(reason, reason)

    await _do_ban_member(update, context, target_id, target_name, reason, duration)

# ==================== ADMIN: /unban ====================
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
        return

    target_id   = None
    target_name = "Không rõ"
    args        = context.args or []

    # Cách 1: Reply
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        target_id   = target_user.id
        target_name = target_user.full_name
    elif args:
        first = args[0]
        # Cách 2: @username
        if first.startswith("@"):
            try:
                chat        = await context.bot.get_chat(first)
                target_id   = chat.id
                target_name = chat.full_name
            except Exception:
                await update.message.reply_text("Không tìm thấy username này.")
                return
        # Cách 3: ID
        else:
            try:
                target_id = int(first)
                try:
                    chat        = await context.bot.get_chat(target_id)
                    target_name = chat.full_name
                except Exception:
                    pass
            except ValueError:
                await update.message.reply_text("ID không hợp lệ, phải là số.")
                return
    else:
        await update.message.reply_text(
            "Cách dùng lệnh /unban:\n\n"
            "1. Bấm giữ tin nhắn → Trả lời → /unban\n"
            "2. /unban @tentaikhoan\n"
            "3. /unban 123456789"
        )
        return

    banned_col = get_banned(context)

    try:
        result = await banned_col.delete_one({"user_id": target_id})
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return

    if result.deleted_count:
        # Bỏ lệnh cấm trong nhóm nếu lệnh từ nhóm
        if update.effective_chat.type in ("group", "supergroup"):
            try:
                await context.bot.unban_chat_member(
                    chat_id=update.effective_chat.id,
                    user_id=target_id
                )
            except Exception:
                pass

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

        await update.message.reply_text(f"Đã gỡ cấm {target_name}.")
        await log_unban(context.application, target_id)
    else:
        await update.message.reply_text("Không tìm thấy người dùng này trong danh sách cấm.")

# ==================== ADMIN: /who ====================
async def who_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
        return

    target_id = None

    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    elif context.args:
        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("ID không hợp lệ, phải là số.")
            return
    else:
        await update.message.reply_text(
            "Dùng: /who <ID>\n"
            "Hoặc bấm giữ tin nhắn → Trả lời → /who"
        )
        return

    banned_col = get_banned(context)
    ban_doc    = await banned_col.find_one({"user_id": target_id})

    try:
        chat      = await context.bot.get_chat(target_id)
        name      = chat.full_name
        username  = f"@{chat.username}" if chat.username else "Không có"
    except Exception:
        name     = "Không rõ"
        username = "Không rõ"

    if ban_doc:
        reason    = ban_doc.get("reason", "Không rõ")
        ban_type  = ban_doc.get("ban_type", "Không rõ")
        banned_at = ban_doc.get("banned_at")
        expire_at = ban_doc.get("expire_at")
        banned_str = banned_at.strftime("%d/%m/%Y %H:%M") if banned_at else "Không rõ"
        expire_str = expire_at.strftime("%d/%m/%Y %H:%M") if expire_at else "Vĩnh viễn"

        await update.message.reply_text(
            f"Thông tin tài khoản:\n"
            f"ID: <code>{target_id}</code>\n"
            f"Tên: {name}\n"
            f"Username: {username}\n\n"
            f"Trạng thái: Đang bị cấm\n"
            f"Lý do: {reason}\n"
            f"Loại ban: {ban_type}\n"
            f"Thời gian ban: {banned_str}\n"
            f"Hết hạn: {expire_str}",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"Thông tin tài khoản:\n"
            f"ID: <code>{target_id}</code>\n"
            f"Tên: {name}\n"
            f"Username: {username}\n\n"
            f"Trạng thái: Bình thường",
            parse_mode="HTML"
        )

# ==================== ADMIN: /status ====================
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
        return

    albums_col = get_albums(context)
    banned_col = get_banned(context)
    jobs_col   = get_jobs(context)

    try:
        total_albums  = await albums_col.count_documents({})
        total_banned  = await banned_col.count_documents({})
        temp_banned   = await banned_col.count_documents({"expire_at": {"$ne": None}})
        pending_jobs  = await jobs_col.count_documents({"done": False})
        await update.message.reply_text(
            f"Trạng thái hệ thống:\n\n"
            f"Album: {total_albums}\n"
            f"Tổng cấm: {total_banned} tài khoản\n"
            f"Cấm tạm thời: {temp_banned} tài khoản\n"
            f"Job chờ xóa: {pending_jobs}\n"
            f"Cơ sở dữ liệu: Kết nối ổn định"
        )
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")

# ==================== ADMIN: /help ====================
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await user_reply_temp(update, context,
            "Bạn không có quyền sử dụng lệnh này.",
            delay=120
        )
        return

    await update.message.reply_text(
        "Hướng dẫn sử dụng:\n\n"
        "— QUẢN LÝ NỘI DUNG —\n"
        "/new — Tạo album mới\n"
        "  Sau đó forward ảnh/video vào, gõ /done khi xong\n\n"
        "/done — Lấy link chia sẻ\n\n"
        "/list — Danh sách tất cả album\n\n"
        "/detail — Danh sách album kèm link đầy đủ\n\n"
        "/check <mã> — Xem thông tin 1 album\n\n"
        "/del <mã> — Xóa album\n\n"
        "/clean — Xóa album trống hoặc cũ hơn 7 ngày\n\n"
        "— QUẢN LÝ THÀNH VIÊN —\n"
        "/ban — Cấm thành viên\n"
        "  Cách 1: Bấm giữ tin nhắn → Trả lời → /ban <lý do>\n"
        "  Cách 2: /ban @tentaikhoan <lý do>\n"
        "  Cách 3: /ban 123456789 <lý do>\n"
        "  Cấm có thời hạn: thêm 1g / 24g / 7ng trước lý do\n"
        "  Ví dụ: /ban @abc 24g quay-roi\n\n"
        "/unban — Gỡ cấm thành viên\n"
        "  Cách 1: Bấm giữ tin nhắn → Trả lời → /unban\n"
        "  Cách 2: /unban @tentaikhoan\n"
        "  Cách 3: /unban 123456789\n\n"
        "/who <ID> — Xem thông tin tài khoản\n\n"
        "— HỆ THỐNG —\n"
        "/status — Trạng thái hệ thống\n\n"
        "Lý do cấm gợi ý:\n"
        "quay-roi / chia-se / spam\n"
        "gia-mao / het-han / ban-lai\n"
        "vi-pham / abuse / da-nghi\n"
        "nhieu-tk / hoan-tien"
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

    # Index tối ưu query
    await albums_col.create_index("key", unique=True)
    await jobs_col.create_index([("expire_at", 1), ("done", 1)])
    await banned_col.create_index("user_id", unique=True)
    await banned_col.create_index("expire_at")

    application.bot_data["albums_col"] = albums_col
    application.bot_data["banned_col"] = banned_col
    application.bot_data["jobs_col"]   = jobs_col
    application.bot_data["mongo_client"] = client

    logging.info("DB connected!")
    return client

# ==================== MAIN ====================
async def main():
    check_env()

    app = Application.builder().token(TOKEN).build()

    # Lệnh admin bị member gõ - phải add TRƯỚC handler admin
    for cmd in ["new","new_album","done","list","detail","check",
                "del","del_album","clean","ban","unban","who","status","help"]:
        app.add_handler(CommandHandler(cmd, no_permission))

    # User
    app.add_handler(CommandHandler("start", start))

    # Admin - nội dung
    app.add_handler(CommandHandler(["new_album", "new"], new_album))
    app.add_handler(CommandHandler("done",   done))
    app.add_handler(CommandHandler("list",   list_albums))
    app.add_handler(CommandHandler("detail", detail_albums))
    app.add_handler(CommandHandler("check",  check_album))
    app.add_handler(CommandHandler(["del_album", "del"], delete_album))
    app.add_handler(CommandHandler("clean",  clean_albums))

    # Admin - quản lý thành viên
    app.add_handler(CommandHandler("ban",    ban_user))
    app.add_handler(CommandHandler("unban",  unban_user))
    app.add_handler(CommandHandler("who",    who_user))

    # Admin - hệ thống
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("help",   help_cmd))

    # Media + forward
    app.add_handler(MessageHandler(
        filters.VIDEO | filters.PHOTO | filters.FORWARDED, handle_media
    ))

    # Callback nút gỡ ban
    app.add_handler(CallbackQueryHandler(callback_handler))

    async with app:
        mongo_client = await setup_db(app)
        await start_web_server(mongo_client)
        await app.start()
        asyncio.create_task(expire_worker(app))
        asyncio.create_task(unban_worker(app))
        logging.info("Bot started!")
        await app.updater.start_polling(drop_pending_updates=True)
        await asyncio.Event().wait()

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logging.error(f"Bot crashed: {e} — khởi động lại sau 10 giây...")
            time.sleep(10)
