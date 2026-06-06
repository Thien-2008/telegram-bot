import os
import asyncio
import logging
import time
import secrets
import string
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web
from telegram import (
    Update, InputMediaPhoto, InputMediaVideo,
    InlineKeyboardButton, InlineKeyboardMarkup, ChatPermissions
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackQueryHandler,
    ChatMemberHandler, ChatJoinRequestHandler
)
from telegram.error import Forbidden, BadRequest, TelegramError

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

TOKEN             = os.environ.get("TOKEN")
ADMIN_ID          = int(os.environ.get("ADMIN_ID", "0"))
MONGO_URI         = os.environ.get("MONGO_URI")
CHANNEL_ID        = os.environ.get("CHANNEL_ID")
GROUP_ID          = os.environ.get("GROUP_ID")
ADMIN_CONTACT     = os.environ.get("ADMIN_CONTACT", "")
BOT_USERNAME      = os.environ.get("BOT_USERNAME", "")
LOG_GROUP_ID      = os.environ.get("LOG_GROUP_ID")
PORT              = int(os.environ.get("PORT", 8080))
DELETE_AFTER      = 10 * 60
SEPAY_WEBHOOK_KEY = os.environ.get("SEPAY_WEBHOOK_KEY", "")
VIP_PRICE         = int(os.environ.get("VIP_PRICE", "119000"))
BANK_ACCOUNT      = "100887150390"
BANK_NAME         = "VietinBank"
GROUP_NAME        = os.environ.get("GROUP_NAME", "Cộng đồng")

def check_env():
    required = ["TOKEN", "ADMIN_ID", "MONGO_URI", "CHANNEL_ID",
                "BOT_USERNAME", "GROUP_ID", "SEPAY_WEBHOOK_KEY"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        logging.critical(f"Thiếu biến môi trường: {', '.join(missing)}")
        exit(1)

request_log:        dict = defaultdict(list)
invalid_attempts:   dict = defaultdict(int)
rate_hit_count:     dict = defaultdict(int)
nonmember_attempts: dict = defaultdict(int)
warn_count:         dict = defaultdict(int)
pending_kicks:      dict = {}

RATE_LIMIT_SEC        = 5
SPAM_WARN_THRESH      = 5
SPAM_TEMP_BAN         = 8
SPAM_PERM_BAN         = 60
INVALID_WARN_THRESH   = 3
INVALID_AUTO_BAN      = 5
RATE_WARN_THRESH      = 3
NONMEMBER_WARN_THRESH = 3

LY_DO = {
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

def parse_duration(text: str):
    import re as _re
    m = _re.match(r'^(\d+)(h|ng)$', text.lower())
    if not m:
        return None
    v, u = int(m.group(1)), m.group(2)
    return timedelta(hours=v) if u == "h" else timedelta(days=v)

MUTED_PERMS = ChatPermissions(
    can_send_messages=False,
    can_send_audios=False,
    can_send_documents=False,
    can_send_photos=False,
    can_send_videos=False,
    can_send_video_notes=False,
    can_send_voice_notes=False,
    can_send_polls=False,
    can_send_other_messages=False,
)
UNMUTED_PERMS = ChatPermissions(
    can_send_messages=True,
    can_send_audios=True,
    can_send_documents=True,
    can_send_photos=True,
    can_send_videos=True,
    can_send_video_notes=True,
    can_send_voice_notes=True,
    can_send_polls=True,
    can_send_other_messages=True,
)

def get_albums(ctx):   return ctx.application.bot_data["albums_col"]
def get_banned(ctx):   return ctx.application.bot_data["banned_col"]
def get_jobs(ctx):     return ctx.application.bot_data["jobs_col"]
def get_users(ctx):    return ctx.application.bot_data["users_col"]
def get_vip(ctx):      return ctx.application.bot_data["vip_col"]
def get_payments(ctx): return ctx.application.bot_data["payments_col"]
def get_demos(ctx):    return ctx.application.bot_data["demos_col"]

def make_key() -> str:
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))

def make_link(key: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={key}"

def make_ref_link(user_id: int) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"

def make_viet_qr(user_id: int) -> str:
    return (f"https://dl.vietqr.io/pay?ba={BANK_ACCOUNT}"
            f"&bn={BANK_NAME}&am={VIP_PRICE}"
            f"&tn=SEVQR%20VIP%20{user_id}")

def now_str() -> str:
    return datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M")

def sanitize(text: str, max_len: int = 500) -> str:
    text = str(text).replace("<", "&lt;").replace(">", "&gt;")
    return text[:max_len] + "..." if len(text) > max_len else text

def days_left(expire_at: datetime) -> int:
    now = datetime.now(timezone.utc)
    if expire_at.tzinfo is None:
        expire_at = expire_at.replace(tzinfo=timezone.utc)
    return max(0, (expire_at - now).days)

async def user_reply(update: Update, text: str, **kwargs):
    return await update.message.reply_text(text, protect_content=True, **kwargs)

async def auto_delete_msg(bot, chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def user_reply_temp(update: Update, context, text: str,
                           delay: int = 120, **kwargs):
    msg = await update.message.reply_text(text, protect_content=True, **kwargs)
    asyncio.create_task(auto_delete_msg(
        context.bot, update.effective_chat.id, msg.message_id, delay
    ))
    return msg

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

async def send_log(app: Application, text: str, reply_markup=None):
    if not LOG_GROUP_ID:
        return
    try:
        await app.bot.send_message(
            chat_id=LOG_GROUP_ID, text=sanitize(text),
            parse_mode="HTML", reply_markup=reply_markup
        )
    except Exception as e:
        logging.error(f"Log error: {e}")

async def log_view(app, user, key: str):
    uname = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Truy cập nội dung\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {sanitize(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Album: <code>{key}</code>\n"
        f"Thời gian: {now_str()}"
    )

async def log_warning(app, user, behavior: str, count: int):
    uname = f"@{user.username}" if user.username else "Không có"
    await send_log(app,
        f"Cảnh báo hành vi bất thường\n"
        f"ID: <code>{user.id}</code>\n"
        f"Tên: {sanitize(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Hành vi: {sanitize(behavior)}\n"
        f"Số lần: {count}\n"
        f"Thời gian: {now_str()}"
    )

async def log_ban(app, target_id, name, reason, ban_type="Thủ công", show_btn=False):
    kb = None
    if show_btn:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Gỡ ban ngay", callback_data=f"unban_{target_id}")
        ]])
    await send_log(app,
        f"Ban người dùng\n"
        f"ID: <code>{target_id}</code>\n"
        f"Tên: {sanitize(str(name))}\n"
        f"Lý do: {sanitize(reason)}\n"
        f"Loại: {ban_type}\n"
        f"Thời gian: {now_str()}",
        reply_markup=kb
    )

async def log_unban(app, target_id):
    await send_log(app,
        f"Hủy ban\nID: <code>{target_id}</code>\nThời gian: {now_str()}"
    )

async def log_payment(app, user_id, amount, total, status):
    await send_log(app,
        f"Thanh toán\n"
        f"ID: <code>{user_id}</code>\n"
        f"Lần này: {amount:,}đ\n"
        f"Tổng đã trả: {total:,}đ\n"
        f"Trạng thái: {status}\n"
        f"Thời gian: {now_str()}"
    )

async def log_vip_granted(app, user_id, name, expire_at):
    await send_log(app,
        f"Cấp VIP thành công\n"
        f"ID: <code>{user_id}</code>\n"
        f"Tên: {sanitize(str(name))}\n"
        f"Hết hạn: {expire_at.strftime('%d/%m/%Y')}\n"
        f"Thời gian: {now_str()}"
    )

async def do_ban(app, target_id, name, reason,
                 ban_type="Tự động", duration=None):
    banned_col = app.bot_data["banned_col"]
    expire_at  = datetime.now(timezone.utc) + duration if duration else None
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
                  show_btn=(ban_type == "Tự động"))

async def save_user(context, user):
    users_col = get_users(context)
    now = datetime.now(timezone.utc)
    await users_col.update_one(
        {"user_id": user.id},
        {
            "$set": {
                "user_id":   user.id,
                "username":  user.username,
                "full_name": user.full_name,
                "last_seen": now
            },
            "$setOnInsert": {
                "first_seen":      now,
                "invite_earned":   0,
                "invite_used":     0,
                "kick_count":      0,
                "rules_confirmed": False,
            }
        },
        upsert=True
    )
async def grant_vip(app: Application, user_id: int, user_name: str):
    vip_col = app.bot_data["vip_col"]
    now     = datetime.now(timezone.utc)

    doc = await vip_col.find_one({"user_id": user_id})
    if doc and doc.get("expire_at") and doc.get("active"):
        base = max(doc["expire_at"], now)
    else:
        base = now
    expire_at = base + relativedelta(months=1)

    try:
        link = await app.bot.create_chat_invite_link(
            chat_id=CHANNEL_ID,
            member_limit=1,
            expire_date=now + timedelta(hours=48),
            creates_join_request=True
        )
        invite_url = link.invite_link
    except Exception as e:
        logging.error(f"Create invite link error: {e}")
        return False

    await vip_col.update_one(
        {"user_id": user_id},
        {"$set": {
            "user_id":        user_id,
            "full_name":      user_name,
            "pending":        True,
            "invite_url":     invite_url,
            "pending_expire": expire_at,
            "notified_7d":    False,
            "notified_3d":    False,
            "notified_1d":    False,
        }},
        upsert=True
    )

    days = days_left(expire_at)
    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=(
                f"Thanh toán thành công\n\n"
                f"Gói VIP của bạn có hiệu lực đến ngày "
                f"{expire_at.strftime('%d/%m/%Y')} ({days} ngày).\n\n"
                f"Bấm link bên dưới để vào kênh VIP:\n"
                f"{invite_url}\n\n"
                f"Link chỉ dùng 1 lần, hết hạn sau 48 giờ."
            ),
            protect_content=True
        )
        await log_vip_granted(app, user_id, user_name, expire_at)
        return True
    except Exception as e:
        logging.error(f"Send VIP link error: {e}")
        return False

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    data  = query.data

    if data.startswith("unban_") and user.id == ADMIN_ID:
        target_id  = int(data.split("_")[1])
        banned_col = get_banned(context)
        result     = await banned_col.delete_one({"user_id": target_id})
        if result.deleted_count:
            try:
                await context.bot.send_message(
                    chat_id=target_id,
                    text="Quyền truy cập của bạn đã được khôi phục.",
                    protect_content=True
                )
            except Exception:
                pass
            await query.answer("Đã gỡ ban!")
            await query.edit_message_text(
                query.message.text + f"\n\nĐã gỡ ban lúc {now_str()}"
            )
            await log_unban(context.application, target_id)
        else:
            await query.answer("Không tìm thấy trong danh sách ban!")
        return

    if data.startswith("confirm_rules_"):
        target_id = int(data.split("_")[2])
        if user.id != target_id:
            await query.answer("Đây không phải nút dành cho bạn!")
            return

        users_col = get_users(context)
        await users_col.update_one(
            {"user_id": target_id},
            {"$set": {"rules_confirmed": True}},
            upsert=True
        )

        if target_id in pending_kicks:
            pending_kicks[target_id].cancel()
            del pending_kicks[target_id]

        try:
            await context.bot.restrict_chat_member(
                chat_id=GROUP_ID,
                user_id=target_id,
                permissions=UNMUTED_PERMS
            )
        except Exception:
            pass

        try:
            await query.message.delete()
        except Exception:
            pass

        await query.answer("Xác nhận thành công! Chào mừng bạn.")

        user_doc   = await users_col.find_one({"user_id": target_id})
        is_new     = not user_doc.get("rules_confirmed_before", False) if user_doc else True
        status_txt = "Lần đầu vào" if is_new else "Đã từng vào trước đó"

        await users_col.update_one(
            {"user_id": target_id},
            {"$set": {"rules_confirmed_before": True}}
        )

        await send_log(context.application,
            f"Xác nhận nội quy\n"
            f"ID: <code>{target_id}</code>\n"
            f"Tên: {sanitize(user_doc.get('full_name','') if user_doc else '')}\n"
            f"Username: {'@'+user_doc.get('username','') if user_doc and user_doc.get('username') else 'Không có'}\n"
            f"Trạng thái: {status_txt}\n"
            f"Thời gian: {now_str()}"
        )

        ref_by = user_doc.get("ref_by") if user_doc else None
        if ref_by:
            asyncio.create_task(process_referral_after_24h(
                context.application, target_id, ref_by
            ))
        return

    if data.startswith("demo_"):
        number = int(data.split("_")[1])
        asyncio.create_task(send_demo_to_user(
            context.application, user.id, user.full_name, number, query
        ))
        return

    await query.answer()

async def is_vip_member(context, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status not in ("left", "kicked")
    except Exception as e:
        logging.error(f"Check VIP error: {e}")
        return False

async def check_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user    = update.effective_user
    user_id = user.id
    app     = context.application
    now     = time.time()

    if user.is_bot:
        await do_ban(app, user_id, user.full_name, "Phát hiện tài khoản bot")
        return True

    request_log[user_id] = [t for t in request_log[user_id] if now - t < 60]
    request_log[user_id].append(now)
    count = len(request_log[user_id])

    if count >= SPAM_PERM_BAN:
        await do_ban(app, user_id, user.full_name, "Spam 60 lần trong 1 phút")
        await user_reply_temp(update, context,
            f"Quyền truy cập bị thu hồi vĩnh viễn.\n"
            f"Lý do: Spam 60 lần trong 1 phút.\n"
            f"Nếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}",
            delay=300
        )
        return True

    if count >= SPAM_TEMP_BAN:
        warn_count[user_id] += 1
        dur = timedelta(hours=24) if warn_count[user_id] >= 2 else timedelta(hours=1)
        await do_ban(app, user_id, user.full_name, "Lạm dụng hệ thống", duration=dur)
        await user_reply_temp(update, context,
            f"Tài khoản bị tạm khóa do hành vi bất thường.\n"
            f"Liên hệ admin: {ADMIN_CONTACT}", delay=60
        )
        return True

    if count >= SPAM_WARN_THRESH:
        await log_warning(app, user, "Spam request nhiều lần", count)

    prev = request_log[user_id][-2] if len(request_log[user_id]) >= 2 else 0
    if now - prev < RATE_LIMIT_SEC:
        rate_hit_count[user_id] += 1
        if rate_hit_count[user_id] >= RATE_WARN_THRESH:
            await log_warning(app, user, "Bị rate limit nhiều lần", rate_hit_count[user_id])
            rate_hit_count[user_id] = 0
        return True

    return False

async def health_handler(request):
    return web.Response(text="ok")

async def db_health_handler(request):
    try:
        client = request.app["mongo_client"]
        await client.admin.command("ping")
        return web.Response(text="ok - DB connected")
    except Exception as e:
        return web.Response(text=f"error: {e}", status=500)

async def sepay_webhook_handler(request):
    auth_header = request.headers.get("Authorization", "")
    api_key     = auth_header.replace("Apikey ", "").replace("Bearer ", "").strip()
    if SEPAY_WEBHOOK_KEY and api_key != SEPAY_WEBHOOK_KEY:
        return web.Response(
            status=401,
            text='{"success": false}',
            content_type="application/json"
        )

    try:
        data = await request.json()
    except Exception as e:
        logging.error(f"SePay parse JSON error: {e}")
        return web.Response(
            status=400,
            text='{"success": false}',
            content_type="application/json"
        )

    if data.get("transferType") != "in":
        return web.Response(text='{"success": true}', content_type="application/json")

    amount  = int(data.get("transferAmount", 0))
    content = data.get("content", "")
    ref     = data.get("referenceCode", "")

    match = re.search(r'SEVQR\s+VIP\s+(\d+)', content, re.IGNORECASE)
    if not match:
        return web.Response(text='{"success": true}', content_type="application/json")

    user_id      = int(match.group(1))
    app          = request.app["tg_app"]
    payments_col = app.bot_data["payments_col"]
    now          = datetime.now(timezone.utc)

    await payments_col.update_one(
        {"user_id": user_id},
        {
            "$inc": {"total_paid": amount},
            "$push": {
                "transactions": {
                    "amount":  amount,
                    "ref":     ref,
                    "content": content,
                    "time":    now
                }
            },
            "$setOnInsert": {"granted": False}
        },
        upsert=True
    )

    doc        = await payments_col.find_one({"user_id": user_id})
    total_paid = doc.get("total_paid", 0)
    granted    = doc.get("granted", False)

    await log_payment(app, user_id, amount, total_paid,
                      "Đang tích lũy" if total_paid < VIP_PRICE else "Đủ tiền")

    if total_paid >= VIP_PRICE and not granted:
        await payments_col.update_one(
            {"user_id": user_id}, {"$set": {"granted": True}}
        )
        users_col = app.bot_data["users_col"]
        user_doc  = await users_col.find_one({"user_id": user_id})
        user_name = user_doc.get("full_name", "Không rõ") if user_doc else "Không rõ"
        await grant_vip(app, user_id, user_name)

    elif total_paid < VIP_PRICE and not granted:
        con_thieu = VIP_PRICE - total_paid
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text=(
                    f"Đã nhận {amount:,}đ\n"
                    f"Tổng đã nhận: {total_paid:,}đ\n"
                    f"Còn thiếu: {con_thieu:,}đ\n\n"
                    f"Vui lòng chuyển thêm để hoàn tất."
                ),
                protect_content=True
            )
        except Exception:
            pass

    return web.Response(text='{"success": true}', content_type="application/json")

async def start_web_server(mongo_client, tg_app):
    webapp = web.Application()
    webapp["mongo_client"] = mongo_client
    webapp["tg_app"]       = tg_app
    webapp.router.add_get("/",               health_handler)
    webapp.router.add_get("/health",         db_health_handler)
    webapp.router.add_post("/sepay-webhook", sepay_webhook_handler)
    runner = web.AppRunner(webapp)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web server on port {PORT}")
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
                    except (Forbidden, BadRequest):
                        pass
                    except Exception as e:
                        logging.error(f"Delete msg: {e}")
                try:
                    await application.bot.send_message(
                        chat_id=job["chat_id"],
                        text="Nội dung của bạn đã hết hạn.\n"
                             "Vào kênh VIP để lấy link xem lại nhé",
                        protect_content=True
                    )
                except Exception:
                    pass
                await jobs_col.update_one(
                    {"_id": job["_id"]}, {"$set": {"done": True}}
                )
        except Exception as e:
            logging.error(f"Expire worker: {e}")
        await asyncio.sleep(60)

async def unban_worker(application: Application):
    banned_col = application.bot_data["banned_col"]
    while True:
        try:
            now    = datetime.now(timezone.utc)
            cursor = banned_col.find({"expire_at": {"$lte": now, "$ne": None}})
            async for ban in cursor:
                target_id = ban["user_id"]
                await banned_col.delete_one({"_id": ban["_id"]})
                try:
                    await application.bot.send_message(
                        chat_id=target_id,
                        text="Lệnh tạm khóa của bạn đã hết hạn.\n"
                             "Quyền truy cập đã được khôi phục.",
                        protect_content=True
                    )
                except Exception:
                    pass
                await log_unban(application, target_id)
        except Exception as e:
            logging.error(f"Unban worker: {e}")
        await asyncio.sleep(60)

async def vip_worker(application: Application):
    vip_col = application.bot_data["vip_col"]
    while True:
        try:
            now = datetime.now(timezone.utc)
            for days_before, field, msg in [
                (7, "notified_7d", "Gói VIP của bạn sẽ hết hạn sau 7 ngày."),
                (3, "notified_3d", "Gói VIP của bạn sẽ hết hạn sau 3 ngày."),
                (1, "notified_1d", "Gói VIP của bạn sẽ hết hạn vào ngày mai."),
            ]:
                cursor = vip_col.find({
                    "expire_at": {
                        "$lte": now + timedelta(days=days_before),
                        "$gt":  now + timedelta(days=days_before - 1)
                    },
                    "active": True,
                    field: {"$ne": True}
                })
                async for doc in cursor:
                    try:
                        await application.bot.send_message(
                            chat_id=doc["user_id"],
                            text=f"{msg}\n"
                                 f"Liên hệ admin để gia hạn: {ADMIN_CONTACT}",
                            protect_content=True
                        )
                        await vip_col.update_one(
                            {"_id": doc["_id"]}, {"$set": {field: True}}
                        )
                    except Exception:
                        pass

            cursor = vip_col.find({"expire_at": {"$lte": now}, "active": True})
            async for doc in cursor:
                uid = doc["user_id"]
                try:
                    await application.bot.ban_chat_member(
                        chat_id=CHANNEL_ID, user_id=uid
                    )
                    await application.bot.unban_chat_member(
                        chat_id=CHANNEL_ID, user_id=uid
                    )
                except Exception as e:
                    logging.error(f"VIP kick {uid}: {e}")
                try:
                    await application.bot.send_message(
                        chat_id=uid,
                        text=f"Gói VIP của bạn đã hết hạn.\n"
                             f"Liên hệ admin để gia hạn: {ADMIN_CONTACT}",
                        protect_content=True
                    )
                except Exception:
                    pass
                await vip_col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"active": False, "expired_at": now}}
                )
                await send_log(application,
                    f"VIP hết hạn\nID: <code>{uid}</code>\n"
                    f"Tên: {sanitize(doc.get('full_name',''))}\n"
                    f"Thời gian: {now_str()}"
                )
        except Exception as e:
            logging.error(f"VIP worker: {e}")
        await asyncio.sleep(3600)

async def process_referral_after_24h(app: Application,
                                      new_user_id: int, ref_by: int):
    await asyncio.sleep(86400)
    users_col     = app.bot_data["users_col"]
    ref_doc       = await users_col.find_one({"user_id": ref_by})
    if not ref_doc:
        return
    invite_earned = ref_doc.get("invite_earned", 0)
    if invite_earned >= 15:
        return
    await users_col.update_one(
        {"user_id": ref_by},
        {"$inc": {"invite_earned": 1}}
    )
    try:
        updated = await users_col.find_one({"user_id": ref_by})
        earned  = updated.get("invite_earned", 0)
        await app.bot.send_message(
            chat_id=ref_by,
            text=f"Người bạn giới thiệu đã ở lại nhóm đủ 24 giờ.\n"
                 f"Bạn nhận được 1 lượt xem.\n"
                 f"Tổng lượt hiện tại: {earned}/15",
            protect_content=True
        )
    except Exception:
        pass

async def send_demo_to_user(app: Application, user_id: int,
                             user_name: str, number: int, query=None):
    users_col = app.bot_data["users_col"]
    demos_col = app.bot_data["demos_col"]
    user_doc  = await users_col.find_one({"user_id": user_id})
    if not user_doc:
        if query:
            await query.answer("Không tìm thấy thông tin của bạn!")
        return
    earned   = user_doc.get("invite_earned", 0)
    used     = user_doc.get("invite_used", 0)
    luot_con = earned - used
    if luot_con <= 0:
        if query:
            await query.answer("Bạn không còn lượt xem!")
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text="Bạn không còn lượt xem.\n"
                     "Dùng /gioi_thieu để kiếm thêm lượt.",
                protect_content=True
            )
        except Exception:
            pass
        return
    demo = await demos_col.find_one({"number": number})
    if not demo or not demo.get("items"):
        if query:
            await query.answer("Không tìm thấy bộ này!")
        return
    await users_col.update_one(
        {"user_id": user_id}, {"$inc": {"invite_used": 1}}
    )
    if query:
        await query.answer(f"Đang gửi bộ #{number}...")
    items = demo.get("items", [])
    try:
        if len(items) == 1:
            item = items[0]
            if item["type"] == "video":
                await app.bot.send_video(
                    chat_id=user_id, video=item["file_id"],
                    protect_content=True, has_spoiler=True
                )
            else:
                await app.bot.send_photo(
                    chat_id=user_id, photo=item["file_id"],
                    protect_content=True, has_spoiler=True
                )
        else:
            for i in range(0, len(items), 10):
                batch = items[i:i+10]
                media = [
                    InputMediaVideo(media=it["file_id"], has_spoiler=True)
                    if it["type"] == "video"
                    else InputMediaPhoto(media=it["file_id"], has_spoiler=True)
                    for it in batch
                ]
                await app.bot.send_media_group(
                    chat_id=user_id, media=media, protect_content=True
                )
                await asyncio.sleep(0.5)
        updated  = await users_col.find_one({"user_id": user_id})
        luot_con = updated.get("invite_earned", 0) - updated.get("invite_used", 0)
        await app.bot.send_message(
            chat_id=user_id,
            text=f"Đã gửi bộ #{number}.\nLượt còn lại: {luot_con}/15",
            protect_content=True
        )
    except Exception as e:
        logging.error(f"Send demo error: {e}")

async def kick_if_not_confirmed(app: Application,
                                 chat_id, user_id, message_id):
    await asyncio.sleep(60)
    users_col = app.bot_data["users_col"]
    user_doc  = await users_col.find_one({"user_id": user_id})
    confirmed = user_doc.get("rules_confirmed", False) if user_doc else False
    if confirmed:
        return
    try:
        await app.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass
    try:
        await app.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
        await app.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
    except Exception as e:
        logging.error(f"Kick error: {e}")
    await users_col.update_one(
        {"user_id": user_id}, {"$inc": {"kick_count": 1}}, upsert=True
    )
    user_doc   = await users_col.find_one({"user_id": user_id})
    kick_count = user_doc.get("kick_count", 1) if user_doc else 1
    if kick_count >= 4:
        banned_col = app.bot_data["banned_col"]
        await banned_col.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id":   user_id,
                "name":      "Không rõ",
                "reason":    "Không xác nhận nội quy 4 lần",
                "ban_type":  "Tự động",
                "expire_at": None,
                "banned_at": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        await send_log(app,
            f"Auto ban: Không xác nhận nội quy\n"
            f"ID: <code>{user_id}</code>\n"
            f"Số lần bị kick: {kick_count}\n"
            f"Thời gian: {now_str()}"
        )
    if user_id in pending_kicks:
        del pending_kicks[user_id]

async def chat_member_updated(update: Update,
                               context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if not result:
        return
    chat_id    = str(result.chat.id)
    user       = result.new_chat_member.user
    old_status = result.old_chat_member.status
    new_status = result.new_chat_member.status
    app        = context.application

    try:
        _gid = int(GROUP_ID) if GROUP_ID else None
    except Exception:
        _gid = None

    if _gid and result.chat.id == _gid:
        if old_status in ("left", "kicked") and new_status == "member":
            banned_col = get_banned(context)
            is_banned  = await banned_col.find_one({"user_id": user.id})
            if is_banned:
                try:
                    await context.bot.ban_chat_member(
                        chat_id=_gid, user_id=user.id
                    )
                except Exception:
                    pass
                return
            try:
                await context.bot.restrict_chat_member(
                    chat_id=_gid, user_id=user.id, permissions=MUTED_PERMS
                )
            except Exception as e:
                logging.error(f"Mute error: {e}")

            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "Xác nhận nội quy",
                    callback_data=f"confirm_rules_{user.id}"
                )
            ]])
            rules_text = (
                f"Chào {user.full_name},\n\n"
                f"NỘI QUY CỘNG ĐỒNG {GROUP_NAME}\n\n"
                f"TRÁCH NHIỆM GIÁM SÁT: Nếu phát hiện thành viên khác có "
                f"hành vi làm phiền, spam hoặc vi phạm quy định, vui lòng "
                f"gửi ảnh chụp màn hình bằng chứng trực tiếp cho Admin.\n\n"
                f"TƯƠNG TÁC VĂN MINH: Không đăng tải nội dung quảng cáo, "
                f"liên kết spam hoặc gửi tin nhắn riêng làm phiền thành viên khác.\n\n"
                f"QUY TRÌNH DỊCH VỤ: Mọi giao dịch và nâng cấp quyền lợi "
                f"VIP đều phải thực hiện thông qua Bot tự động. Nếu có lỗi, "
                f"vui lòng liên hệ Admin kèm ảnh chụp màn hình.\n\n"
                f"QUYỀN QUẢN TRỊ: Quản trị viên có quyền loại bỏ thành viên "
                f"nếu phát hiện hành vi lạm dụng hoặc cố tình vi phạm.\n\n"
                f"Bằng việc xác nhận, bạn cam kết đã đọc và đồng ý với các "
                f"quy định trên.\n\n"
                f"Bạn có 60 giây để xác nhận."
            )
            try:
                msg = await context.bot.send_message(
                    chat_id=_gid, text=rules_text, reply_markup=kb
                )
                users_col = get_users(context)
                await users_col.update_one(
                    {"user_id": user.id},
                    {
                        "$set": {
                            "user_id":         user.id,
                            "username":        user.username,
                            "full_name":       user.full_name,
                            "rules_confirmed": False,
                        },
                        "$setOnInsert": {
                            "first_seen":    datetime.now(timezone.utc),
                            "invite_earned": 0,
                            "invite_used":   0,
                            "kick_count":    0,
                        }
                    },
                    upsert=True
                )
                task = asyncio.create_task(
                    kick_if_not_confirmed(app, _gid, user.id, msg.message_id)
                )
                pending_kicks[user.id] = task
            except Exception as e:
                logging.error(f"Send rules error: {e}")
        return

    if CHANNEL_ID and chat_id == str(CHANNEL_ID):
        vip_col = context.application.bot_data["vip_col"]
        now     = datetime.now(timezone.utc)
        if old_status in ("left", "kicked") and new_status == "member":
            doc       = await vip_col.find_one({"user_id": user.id})
            expire_at = doc.get("pending_expire") if doc else None
            if not expire_at:
                base      = doc.get("expire_at", now) if doc and doc.get("active") else now
                base      = max(base, now)
                expire_at = base + relativedelta(months=1)
            await vip_col.update_one(
                {"user_id": user.id},
                {"$set": {
                    "user_id":     user.id,
                    "username":    user.username,
                    "full_name":   user.full_name,
                    "joined_at":   now,
                    "expire_at":   expire_at,
                    "active":      True,
                    "pending":     False,
                    "notified_7d": False,
                    "notified_3d": False,
                    "notified_1d": False,
                }},
                upsert=True
            )
        elif old_status == "member" and new_status == "left":
            await vip_col.update_one(
                {"user_id": user.id}, {"$set": {"active": False}}
            )

async def join_request_handler(update: Update,
                                context: ContextTypes.DEFAULT_TYPE):
    req     = update.chat_join_request
    user_id = req.from_user.id
    vip_col = context.application.bot_data["vip_col"]
    if str(req.chat.id) != str(CHANNEL_ID):
        return
    doc = await vip_col.find_one({"user_id": user_id, "pending": True})
    if doc:
        try:
            await context.bot.approve_chat_join_request(
                chat_id=CHANNEL_ID, user_id=user_id
            )
        except Exception as e:
            logging.error(f"Approve join error: {e}")
    else:
        try:
            await context.bot.decline_chat_join_request(
                chat_id=CHANNEL_ID, user_id=user_id
            )
            await context.bot.send_message(
                chat_id=user_id,
                text=f"Yêu cầu vào kênh VIP bị từ chối.\n"
                     f"Vui lòng thanh toán trước.\n"
                     f"Gõ /mua để xem hướng dẫn.",
                protect_content=True
            )
        except Exception:
            pass
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    user_id = user.id
    app     = context.application

    await save_user(context, user)

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
            await user_reply_temp(update, context,
                f"Tài khoản của bạn đang bị tạm khóa.\n"
                f"Lý do: {reason}\n"
                f"Hết hạn lúc: {expire_str}\n\n"
                f"Nếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}",
                delay=300
            )
        else:
            await user_reply_temp(update, context,
                f"Quyền truy cập của bạn đã bị thu hồi.\n"
                f"Lý do: {reason}\n\n"
                f"Nếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}",
                delay=300
            )
        return

    if await check_user(update, context):
        return

    args = context.args

    if args and args[0].startswith("ref_"):
        try:
            ref_id = int(args[0].replace("ref_", ""))
            if ref_id != user_id:
                users_col = get_users(context)
                user_doc  = await users_col.find_one({"user_id": user_id})
                if not user_doc or not user_doc.get("ref_by"):
                    await users_col.update_one(
                        {"user_id": user_id},
                        {"$set": {"ref_by": ref_id}},
                        upsert=True
                    )
        except ValueError:
            pass
        context.args = []
        args = []

    if not args:
        vip = await is_vip_member(context, user_id)
        if vip:
            await user_reply_temp(update, context,
                "Bạn đã là thành viên kênh VIP.\n\n"
                "Vào kênh và bấm vào link trong bài đăng để xem nội dung.\n\n"
                "Gõ /help để xem các lệnh dành cho bạn.",
                delay=120
            )
        else:
            viet_link = make_viet_qr(user_id)
            await user_reply_temp(update, context,
                f"Chào mừng bạn đến với hệ thống nội dung VIP.\n\n"
                f"Gói VIP 1 tháng: {VIP_PRICE:,}đ\n\n"
                f"Chuyển khoản:\n"
                f"Ngân hàng: {BANK_NAME}\n"
                f"Số tài khoản: {BANK_ACCOUNT}\n"
                f"Số tiền: {VIP_PRICE:,}đ\n"
                f"Nội dung: SEVQR VIP {user_id}\n\n"
                f"Bấm link để mở app ngân hàng: {viet_link}\n\n"
                f"Hệ thống tự động cấp quyền sau khi nhận thanh toán.\n"
                f"Hỗ trợ: {ADMIN_CONTACT}\n\n"
                f"Gõ /help để xem các lệnh dành cho bạn.",
                delay=600
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
                f"Quyền truy cập bị thu hồi tự động.\n\n"
                f"Nếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}",
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
        viet_link = make_viet_qr(user_id)
        await user_reply_temp(update, context,
            f"Nội dung chỉ dành cho thành viên VIP.\n\n"
            f"Chuyển khoản:\n"
            f"Ngân hàng: {BANK_NAME}\n"
            f"Số tài khoản: {BANK_ACCOUNT}\n"
            f"Số tiền: {VIP_PRICE:,}đ\n"
            f"Nội dung: SEVQR VIP {user_id}\n\n"
            f"Bấm link để mở app ngân hàng: {viet_link}\n\n"
            f"Hỗ trợ: {ADMIN_CONTACT}",
            delay=300
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
                msg = await context.bot.send_video(
                    chat_id=user_id, video=item["file_id"],
                    protect_content=True, has_spoiler=True
                )
            else:
                msg = await context.bot.send_photo(
                    chat_id=user_id, photo=item["file_id"],
                    protect_content=True, has_spoiler=True
                )
            sent_msg_ids.append(msg.message_id)
        except Exception as e:
            logging.error(f"Send error: {e}")
            await user_reply_temp(update, context,
                "Hệ thống đang xử lý. Vui lòng thử lại sau.", delay=120
            )
            return
    else:
        for i in range(0, len(items), 10):
            batch = items[i:i+10]
            media = [
                InputMediaVideo(media=it["file_id"], has_spoiler=True)
                if it["type"] == "video"
                else InputMediaPhoto(media=it["file_id"], has_spoiler=True)
                for it in batch
            ]
            try:
                msgs = await context.bot.send_media_group(
                    chat_id=user_id, media=media, protect_content=True
                )
                sent_msg_ids.extend([m.message_id for m in msgs])
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.error(f"Send media group: {e}")
                await user_reply_temp(update, context,
                    "Hệ thống đang xử lý. Vui lòng thử lại sau.", delay=120
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

async def cmd_mua(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user      = update.effective_user
    viet_link = make_viet_qr(user.id)
    await user_reply_temp(update, context,
        f"Gói VIP 1 tháng: {VIP_PRICE:,}đ\n\n"
        f"Chuyển khoản:\n"
        f"Ngân hàng: {BANK_NAME}\n"
        f"Số tài khoản: {BANK_ACCOUNT}\n"
        f"Số tiền: {VIP_PRICE:,}đ\n"
        f"Nội dung: SEVQR VIP {user.id}\n\n"
        f"Bấm link để mở app ngân hàng: {viet_link}\n\n"
        f"Hệ thống tự động xác nhận sau khi nhận tiền.\n"
        f"Hỗ trợ: {ADMIN_CONTACT}",
        delay=600
    )

async def cmd_luot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user      = update.effective_user
    users_col = get_users(context)
    doc       = await users_col.find_one({"user_id": user.id})
    if not doc:
        await user_reply_temp(update, context,
            "Bạn chưa có thông tin trong hệ thống.\n"
            "Vào nhóm và xác nhận nội quy trước.",
            delay=120
        )
        return
    earned   = doc.get("invite_earned", 0)
    used     = doc.get("invite_used", 0)
    luot_con = earned - used
    await user_reply_temp(update, context,
        f"Lượt xem của bạn:\n\n"
        f"Đã kiếm: {earned}/15\n"
        f"Đã dùng: {used}\n"
        f"Còn lại: {luot_con}\n\n"
        f"Dùng /gioi_thieu để kiếm thêm lượt.\n"
        f"Dùng /xem để xem nội dung demo.",
        delay=120
    )

async def cmd_gioi_thieu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    ref_url = make_ref_link(user.id)
    await user_reply_temp(update, context,
        f"Link giới thiệu của bạn:\n{ref_url}\n\n"
        f"Chia sẻ link này để kiếm lượt xem.\n"
        f"Mỗi người vào nhóm và ở lại 24 giờ = +1 lượt.\n\n"
        f"Tối đa 15 lượt suốt đời. Dù mời bao nhiêu người\n"
        f"cũng chỉ kiếm được tối đa 15 lượt.\n"
        f"Dùng /luot để xem số lượt hiện tại.",
        delay=300
    )

async def cmd_xem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user      = update.effective_user
    users_col = get_users(context)
    demos_col = get_demos(context)
    doc = await users_col.find_one({"user_id": user.id})
    if not doc:
        await user_reply_temp(update, context,
            "Bạn chưa có thông tin trong hệ thống.", delay=120
        )
        return
    earned   = doc.get("invite_earned", 0)
    used     = doc.get("invite_used", 0)
    luot_con = earned - used
    if luot_con <= 0:
        await user_reply_temp(update, context,
            "Bạn không còn lượt xem.\n"
            "Dùng /gioi_thieu để kiếm thêm lượt.",
            delay=120
        )
        return
    demos = await demos_col.find(
        {}, {"number": 1, "title": 1}
    ).sort("number", 1).to_list(length=20)
    if not demos:
        await user_reply_temp(update, context,
            "Hiện chưa có bộ demo nào.", delay=120
        )
        return
    keyboard = []
    for d in demos:
        title = d.get("title") or f"Bộ {d['number']}"
        keyboard.append([InlineKeyboardButton(
            f"#{d['number']} — {title}",
            callback_data=f"demo_{d['number']}"
        )])
    await user_reply_temp(update, context,
        f"Lượt còn lại: {luot_con}/15\n\nChọn bộ muốn xem:",
        delay=300,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def cmd_help_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        return
    await user_reply_temp(update, context,
        "Hướng dẫn sử dụng:\n\n"
        "/mua — Xem thông tin gói VIP và tạo mã thanh toán\n"
        "/luot — Xem số lượt xem còn lại\n"
        "/gioi_thieu — Lấy link giới thiệu để kiếm lượt\n"
        "/xem — Dùng lượt xem nội dung demo\n"
        "/help — Hướng dẫn sử dụng\n\n"
        f"Hỗ trợ: {ADMIN_CONTACT}",
        delay=120
            )
async def new_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if context.user_data.get("current_key"):
        await update.message.reply_text(
            "Đang có album chưa hoàn thành.\nGõ /done để lấy link trước."
        )
        return
    key        = make_key()
    albums_col = get_albums(context)
    await albums_col.insert_one({
        "key": key, "items": [], "created_at": datetime.now(timezone.utc)
    })
    context.user_data["current_key"] = key
    await update.message.reply_text(
        f"Album mới đã tạo.\nMã: <code>{key}</code>\n\n"
        f"Forward ảnh hoặc video vào đây.\nGõ /done khi xong.",
        parse_mode="HTML"
    )

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if update.message.forward_from:
        fwd   = update.message.forward_from
        uname = f"@{fwd.username}" if fwd.username else "Không có"
        await update.message.reply_text(
            f"Thông tin:\nID: <code>{fwd.id}</code>\n"
            f"Tên: {fwd.full_name}\nUsername: {uname}",
            parse_mode="HTML"
        )
        return
    try:
        _gid = int(GROUP_ID) if GROUP_ID else None
    except Exception:
        _gid = None
    if _gid and update.effective_chat.id == _gid:
        caption = update.message.caption or ""
        match   = re.match(r'^#(\d+)\s*(.*)', caption.strip())
        if match:
            number    = int(match.group(1))
            title     = match.group(2).strip() or f"Bộ {number}"
            demos_col = get_demos(context)
            if update.message.video:
                file_id, file_type = update.message.video.file_id, "video"
            elif update.message.photo:
                file_id, file_type = update.message.photo[-1].file_id, "photo"
            else:
                return
            await demos_col.update_one(
                {"number": number},
                {
                    "$push":        {"items": {"type": file_type, "file_id": file_id}},
                    "$set":         {"title": title},
                    "$setOnInsert": {
                        "number":     number,
                        "created_at": datetime.now(timezone.utc)
                    }
                },
                upsert=True
            )
            demo  = await demos_col.find_one({"number": number})
            count = len(demo.get("items", [])) if demo else 1
            await update.message.reply_text(
                f"Đã lưu vào demo #{number} — {title} ({count} file)."
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
        f"Đã nhận {file_type} — album có {count} file.\nGõ /done khi xong."
    )

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
            "Album chưa có file nào.\nHãy forward ảnh hoặc video vào trước."
        )
        return
    context.user_data.pop("current_key", None)
    count = len(album.get("items", []))
    await update.message.reply_text(
        f"Hoàn tất. Album có {count} file.\nLink chia sẻ:"
    )
    await update.message.reply_text(make_link(key))

async def list_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    albums_col = get_albums(context)
    try:
        albums = await db_retry(
            lambda: albums_col.find({}, {"key": 1, "items": 1})
            .sort("created_at", -1).to_list(length=50)
        )
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return
    if not albums:
        await update.message.reply_text("Chưa có album nào.")
        return
    text = f"Danh sách album ({len(albums)}):\n\n"
    for i, a in enumerate(albums, 1):
        text += f"{i}. <code>{a['key']}</code> — {len(a.get('items',[]))} file\n"
    await update.message.reply_text(text, parse_mode="HTML")

async def detail_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    albums_col = get_albums(context)
    try:
        albums = await db_retry(
            lambda: albums_col.find({}, {"key": 1, "items": 1})
            .sort("created_at", -1).to_list(length=50)
        )
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")
        return
    if not albums:
        await update.message.reply_text("Chưa có album nào.")
        return
    for i, a in enumerate(albums, 1):
        await update.message.reply_text(
            f"{i}\nSố file: {len(a.get('items',[]))}\nLink:"
        )
        await update.message.reply_text(make_link(a["key"]))
        await asyncio.sleep(0.3)

async def check_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dùng: /check <mã>")
        return
    key        = context.args[0]
    albums_col = get_albums(context)
    album      = await albums_col.find_one({"key": key})
    if not album:
        await update.message.reply_text(f"Không tìm thấy album {key}.")
        return
    items  = album.get("items", [])
    videos = sum(1 for i in items if i["type"] == "video")
    photos = sum(1 for i in items if i["type"] == "photo")
    await update.message.reply_text(
        f"Album: <code>{key}</code>\n"
        f"Tổng: {len(items)} | Video: {videos} | Ảnh: {photos}\nLink:",
        parse_mode="HTML"
    )
    await update.message.reply_text(make_link(key))

async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dùng: /del <mã>")
        return
    key        = context.args[0]
    albums_col = get_albums(context)
    jobs_col   = get_jobs(context)
    result     = await albums_col.delete_one({"key": key})
    await jobs_col.delete_many({"album_key": key})
    if result.deleted_count == 0:
        await update.message.reply_text(f"Không tìm thấy album {key}.")
    else:
        await update.message.reply_text(f"Đã xóa album {key}.")

async def clean_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    albums_col = get_albums(context)
    cutoff     = datetime.now(timezone.utc) - timedelta(days=7)
    result     = await albums_col.delete_many({
        "$or": [{"items": []}, {"created_at": {"$lt": cutoff}}]
    })
    await update.message.reply_text(
        f"Đã xóa {result.deleted_count} album trống hoặc cũ hơn 7 ngày."
    )

async def demo_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dùng: /demo_clear <số>")
        return
    try:
        number    = int(context.args[0])
        demos_col = get_demos(context)
        result    = await demos_col.delete_one({"number": number})
        if result.deleted_count:
            await update.message.reply_text(f"Đã xóa demo #{number}.")
        else:
            await update.message.reply_text(f"Không tìm thấy demo #{number}.")
    except ValueError:
        await update.message.reply_text("Số không hợp lệ.")

async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    target_id, target_name, duration, reason = None, "Không rõ", None, "Vi phạm quy định"
    args = context.args or []
    if update.message.reply_to_message:
        u = update.message.reply_to_message.from_user
        target_id, target_name = u.id, u.full_name
        if args:
            dur = parse_duration(args[0])
            if dur:
                duration = dur
                reason   = " ".join(args[1:]) if len(args) > 1 else "Vi phạm quy định"
            else:
                reason   = " ".join(args)
    elif args:
        first = args[0]
        if first.startswith("@"):
            try:
                chat = await context.bot.get_chat(first)
                target_id, target_name = chat.id, chat.full_name
            except Exception:
                await update.message.reply_text("Không tìm thấy username này.")
                return
            rest = args[1:]
        else:
            try:
                target_id = int(first)
                try:
                    chat        = await context.bot.get_chat(target_id)
                    target_name = chat.full_name
                except Exception:
                    pass
            except ValueError:
                await update.message.reply_text("ID không hợp lệ.")
                return
            rest = args[1:]
        if rest:
            dur = parse_duration(rest[0])
            if dur:
                duration = dur
                reason   = " ".join(rest[1:]) if len(rest) > 1 else "Vi phạm quy định"
            else:
                reason   = " ".join(rest)
    else:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    "Cách dùng /ban:\n\n"
                    "1. Reply tin nhắn → /ban <lý do>\n"
                    "2. /ban @tentaikhoan <lý do>\n"
                    "3. /ban 123456789 <lý do>\n\n"
                    "Cấm có thời hạn: thêm 1h / 24h / 7ng trước lý do\n"
                    "Ví dụ: /ban @abc 24h quay-roi\n\n"
                    "Lý do gợi ý:\n"
                    "quay-roi / chia-se / spam\n"
                    "gia-mao / het-han / ban-lai\n"
                    "vi-pham / abuse / da-nghi\n"
                    "nhieu-tk / hoan-tien\n\n"
                    "Gỡ cấm: /unban @tentaikhoan hoặc /unban ID"
                )
            )
        except Exception:
            pass
        if update.effective_chat.type in ("group", "supergroup"):
            try:
                await update.message.delete()
            except Exception:
                pass
        return

    if not target_id or target_id == ADMIN_ID:
        await update.message.reply_text("Không thể ban.")
        return

    reason = LY_DO.get(reason, reason)

    banned_col = get_banned(context)
    existing   = await banned_col.find_one({"user_id": target_id})
    if existing:
        await update.message.reply_text(
            f"Tài khoản {target_name} đã bị cấm trước đó.\n"
            f"Lý do cũ: {existing.get('reason', 'Không rõ')}\n"
            f"Dùng /unban trước nếu muốn cấm lại với lý do mới."
        )
        return

    if update.effective_chat.type in ("group", "supergroup"):
        try:
            await context.bot.ban_chat_member(
                chat_id=update.effective_chat.id, user_id=target_id
            )
        except Exception as e:
            await update.message.reply_text(f"Không thể kick khỏi nhóm: {e}")
            return

    await do_ban(context.application, target_id, target_name,
                 reason, ban_type="Thủ công", duration=duration)

    try:
        expire_text = ""
        if duration:
            ea          = datetime.now(timezone(timedelta(hours=7))) + duration
            expire_text = f"\nHết hạn lúc: {ea.strftime('%d/%m/%Y %H:%M')}"
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"Bạn đã bị cấm.\nLý do: {reason}{expire_text}\n\n"
                f"Nếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}"
            ),
            protect_content=True
        )
    except Exception:
        pass

    expire_info = ""
    if duration:
        days  = int(duration.total_seconds() // 86400)
        hours = int(duration.total_seconds() // 3600)
        expire_info = f" ({days} ngày)" if days >= 1 else f" ({hours} giờ)"
    await update.message.reply_text(
        f"Đã cấm {target_name}{expire_info}.\nLý do: {reason}"
    )

async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    target_id, target_name = None, "Không rõ"
    args = context.args or []
    if update.message.reply_to_message:
        u = update.message.reply_to_message.from_user
        target_id, target_name = u.id, u.full_name
    elif args:
        first = args[0]
        if first.startswith("@"):
            try:
                chat = await context.bot.get_chat(first)
                target_id, target_name = chat.id, chat.full_name
            except Exception:
                await update.message.reply_text("Không tìm thấy username.")
                return
        else:
            try:
                target_id = int(first)
                try:
                    chat        = await context.bot.get_chat(target_id)
                    target_name = chat.full_name
                except Exception:
                    pass
            except ValueError:
                await update.message.reply_text("ID không hợp lệ.")
                return
    else:
        await update.message.reply_text(
            "Cách dùng /unban:\n\n"
            "1. Reply tin nhắn → /unban\n"
            "2. /unban @tentaikhoan\n"
            "3. /unban 123456789"
        )
        return
    banned_col = get_banned(context)
    result     = await banned_col.delete_one({"user_id": target_id})
    if result.deleted_count:
        if update.effective_chat.type in ("group", "supergroup"):
            try:
                await context.bot.unban_chat_member(
                    chat_id=update.effective_chat.id, user_id=target_id
                )
            except Exception:
                pass
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="Quyền truy cập của bạn đã được khôi phục.",
                protect_content=True
            )
        except Exception:
            pass
        await update.message.reply_text(f"Đã gỡ cấm {target_name}.")
        await log_unban(context.application, target_id)
    else:
        await update.message.reply_text("Không tìm thấy trong danh sách cấm.")

async def who_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    target_id = None
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    elif context.args:
        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("ID không hợp lệ.")
            return
    else:
        await update.message.reply_text("Dùng: /who <ID> hoặc reply tin nhắn")
        return
    banned_col = get_banned(context)
    vip_col    = get_vip(context)
    users_col  = get_users(context)
    ban_doc    = await banned_col.find_one({"user_id": target_id})
    vip_doc    = await vip_col.find_one({"user_id": target_id})
    user_doc   = await users_col.find_one({"user_id": target_id})
    try:
        chat     = await context.bot.get_chat(target_id)
        name     = chat.full_name
        username = f"@{chat.username}" if chat.username else "Không có"
    except Exception:
        name, username = "Không rõ", "Không rõ"
    text = (
        f"Thông tin:\nID: <code>{target_id}</code>\n"
        f"Tên: {name}\nUsername: {username}\n\n"
    )
    if ban_doc:
        es    = ban_doc['expire_at'].strftime('%d/%m/%Y %H:%M') if ban_doc.get('expire_at') else "Vĩnh viễn"
        text += f"Trạng thái: Đang bị cấm\nLý do: {ban_doc.get('reason','')}\nHết hạn: {es}\n\n"
    else:
        text += "Trạng thái ban: Bình thường\n\n"
    if vip_doc and vip_doc.get("active"):
        ea   = vip_doc.get("expire_at")
        d    = days_left(ea) if ea else 0
        text += f"VIP: Đang hoạt động\nHết hạn: {ea.strftime('%d/%m/%Y') if ea else ''} ({d} ngày còn lại)\n\n"
    else:
        text += "VIP: Chưa có hoặc đã hết hạn\n\n"
    if user_doc:
        earned = user_doc.get("invite_earned", 0)
        used   = user_doc.get("invite_used", 0)
        text  += f"Lượt: {earned - used} còn lại ({earned} kiếm / {used} đã dùng)"
    await update.message.reply_text(text, parse_mode="HTML")

async def extend_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dùng: /extend <ID>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID không hợp lệ.")
        return
    vip_col   = get_vip(context)
    users_col = get_users(context)
    now       = datetime.now(timezone.utc)

    user_exists      = await users_col.find_one({"user_id": target_id})
    user_in_telegram = False
    try:
        await context.bot.get_chat(target_id)
        user_in_telegram = True
    except Exception:
        pass

    if not user_exists and not user_in_telegram:
        await update.message.reply_text(
            f"Không tìm thấy người dùng với ID {target_id}.\n"
            f"Họ cần bấm /start vào bot ít nhất 1 lần trước."
        )
        return

    doc        = await vip_col.find_one({"user_id": target_id})
    if doc and doc.get("expire_at") and doc.get("active"):
        base = max(doc["expire_at"], now)
    else:
        base = now
    new_expire = base + relativedelta(months=1)
    days       = days_left(new_expire)

    await vip_col.update_one(
        {"user_id": target_id},
        {"$set": {
            "expire_at":   new_expire,
            "active":      True,
            "notified_7d": False,
            "notified_3d": False,
            "notified_1d": False,
        }},
        upsert=True
    )
    user_doc  = await users_col.find_one({"user_id": target_id})
    user_name = user_doc.get("full_name", "Không rõ") if user_doc else "Không rõ"
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"Gói VIP của bạn đã được gia hạn thành công.\n\n"
                f"Hết hạn mới: {new_expire.strftime('%d/%m/%Y')}\n"
                f"Số ngày còn lại: {days} ngày"
            ),
            protect_content=True
        )
    except Exception:
        pass
    await update.message.reply_text(
        f"Đã gia hạn VIP cho <code>{target_id}</code> ({user_name}).\n"
        f"Hết hạn mới: {new_expire.strftime('%d/%m/%Y')} ({days} ngày còn lại)",
        parse_mode="HTML"
    )

async def vip_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    vip_col = get_vip(context)
    members = await vip_col.find(
        {"active": True}, {"user_id": 1, "full_name": 1, "expire_at": 1}
    ).sort("expire_at", 1).to_list(length=50)
    if not members:
        await update.message.reply_text("Chưa có member VIP nào.")
        return
    text = f"VIP đang hoạt động ({len(members)}):\n\n"
    for i, m in enumerate(members, 1):
        ea   = m.get("expire_at")
        d    = days_left(ea) if ea else 0
        text += (
            f"{i}. <code>{m['user_id']}</code> — "
            f"{sanitize(m.get('full_name',''))} — "
            f"hết {ea.strftime('%d/%m/%Y') if ea else '?'} ({d} ngày)\n"
        )
    await update.message.reply_text(text, parse_mode="HTML")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        total_albums = await get_albums(context).count_documents({})
        total_banned = await get_banned(context).count_documents({})
        temp_banned  = await get_banned(context).count_documents({"expire_at": {"$ne": None}})
        pending_jobs = await get_jobs(context).count_documents({"done": False})
        total_vip    = await get_vip(context).count_documents({"active": True})
        total_users  = await get_users(context).count_documents({})
        total_demos  = await get_demos(context).count_documents({})
        await update.message.reply_text(
            f"Trạng thái hệ thống:\n\n"
            f"Album: {total_albums}\n"
            f"Demo: {total_demos} bộ\n"
            f"User: {total_users}\n"
            f"VIP: {total_vip}\n"
            f"Cấm: {total_banned} (tạm: {temp_banned})\n"
            f"Job chờ xóa: {pending_jobs}\n"
            f"Database: Kết nối ổn định"
        )
    except Exception:
        await update.message.reply_text("Lỗi kết nối cơ sở dữ liệu.")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        "Lệnh dành cho Admin:\n\n"
        "— NỘI DUNG —\n"
        "/new — Tạo album mới\n"
        "/done — Lấy link chia sẻ\n"
        "/list — Danh sách album\n"
        "/detail — Album kèm link\n"
        "/check <mã> — Thông tin album\n"
        "/del <mã> — Xóa album\n"
        "/clean — Xóa album cũ\n"
        "/demo_clear <số> — Xóa bộ demo\n\n"
        "— THÀNH VIÊN —\n"
        "/ban — Cấm (reply / @user / ID)\n"
        "  Thời hạn: 1h / 24h / 7ng\n"
        "/unban — Gỡ cấm\n"
        "/who <ID> — Thông tin tài khoản\n\n"
        "— VIP —\n"
        "/viplist — Danh sách VIP\n"
        "/extend <ID> — Gia hạn 1 tháng\n"
        "/addluot <ID> <số> — Thêm lượt cho member\n\n"
        "— HỆ THỐNG —\n"
        "/status — Trạng thái\n\n"
        "Lý do cấm: quay-roi / chia-se / spam\n"
        "gia-mao / het-han / ban-lai / vi-pham\n"
        "abuse / da-nghi / nhieu-tk / hoan-tien"
    )

async def cmd_add_luot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Dùng: /addluot <ID> <số lượt>")
        return
    try:
        target_id = int(context.args[0])
        so_luot   = int(context.args[1])
    except ValueError:
        await update.message.reply_text("ID và số lượt phải là số.")
        return
    users_col = get_users(context)
    await users_col.update_one(
        {"user_id": target_id},
        {"$inc": {"invite_earned": so_luot}},
        upsert=True
    )
    doc    = await users_col.find_one({"user_id": target_id})
    earned = doc.get("invite_earned", 0) if doc else so_luot
    used   = doc.get("invite_used", 0) if doc else 0
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=f"Admin đã thêm {so_luot} lượt xem cho bạn.\n"
                 f"Lượt còn lại: {earned - used}/15",
            protect_content=True
        )
    except Exception:
        pass
    await update.message.reply_text(
        f"Đã thêm {so_luot} lượt cho ID {target_id}.\n"
        f"Tổng: {earned} | Đã dùng: {used} | Còn lại: {earned - used}"
    )

async def no_permission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await user_reply_temp(update, context,
        "Bạn không có quyền sử dụng lệnh này.", delay=120
    )

async def setup_db(application: Application):
    client = AsyncIOMotorClient(
        MONGO_URI,
        maxPoolSize=10, minPoolSize=0,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=20000,
        retryWrites=True, tls=True,
        tlsAllowInvalidCertificates=True
    )
    await client.admin.command("ping")
    db   = client["botdb"]
    cols = {
        "albums_col":   db["albums"],
        "jobs_col":     db["jobs"],
        "banned_col":   db["banned"],
        "users_col":    db["users"],
        "vip_col":      db["vip_members"],
        "payments_col": db["payments"],
        "demos_col":    db["demos"],
    }
    for k, v in cols.items():
        application.bot_data[k] = v
    application.bot_data["mongo_client"] = client

    await db["albums"].create_index("key", unique=True)
    await db["jobs"].create_index([("expire_at", 1), ("done", 1)])
    await db["banned"].create_index("user_id", unique=True)
    await db["banned"].create_index("expire_at")
    await db["users"].create_index("user_id", unique=True)
    await db["vip_members"].create_index("user_id", unique=True)
    await db["vip_members"].create_index([("expire_at", 1), ("active", 1)])
    await db["payments"].create_index("user_id", unique=True)
    await db["demos"].create_index("number", unique=True)

    logging.info("DB connected + indexes created!")
    return client

async def main():
    check_env()
    app          = Application.builder().token(TOKEN).build()
    admin_filter = filters.User(user_id=ADMIN_ID)

    app.add_handler(CommandHandler("start", start))

    app.add_handler(CommandHandler(["new_album","new"], new_album, filters=admin_filter))
    app.add_handler(CommandHandler("done",       done,          filters=admin_filter))
    app.add_handler(CommandHandler("list",       list_albums,   filters=admin_filter))
    app.add_handler(CommandHandler("detail",     detail_albums, filters=admin_filter))
    app.add_handler(CommandHandler("check",      check_album,   filters=admin_filter))
    app.add_handler(CommandHandler(["del_album","del"], delete_album, filters=admin_filter))
    app.add_handler(CommandHandler("clean",      clean_albums,  filters=admin_filter))
    app.add_handler(CommandHandler("demo_clear", demo_clear,    filters=admin_filter))
    app.add_handler(CommandHandler("ban",        ban_user,      filters=admin_filter))
    app.add_handler(CommandHandler("unban",      unban_user,    filters=admin_filter))
    app.add_handler(CommandHandler("who",        who_user,      filters=admin_filter))
    app.add_handler(CommandHandler("extend",     extend_vip,    filters=admin_filter))
    app.add_handler(CommandHandler("viplist",    vip_list,      filters=admin_filter))
    app.add_handler(CommandHandler("status",     status_cmd,    filters=admin_filter))
    app.add_handler(CommandHandler("help",       help_cmd,      filters=admin_filter))
    app.add_handler(CommandHandler("addluot",    cmd_add_luot,  filters=admin_filter))

    app.add_handler(CommandHandler("mua",        cmd_mua))
    app.add_handler(CommandHandler("luot",       cmd_luot))
    app.add_handler(CommandHandler("gioi_thieu", cmd_gioi_thieu))
    app.add_handler(CommandHandler("xem",        cmd_xem))
    app.add_handler(CommandHandler("help",       cmd_help_user, filters=~admin_filter))

    app.add_handler(MessageHandler(
        (filters.VIDEO | filters.PHOTO | filters.FORWARDED) & admin_filter,
        handle_media
    ))

    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(ChatMemberHandler(
        chat_member_updated, ChatMemberHandler.CHAT_MEMBER
    ))
    app.add_handler(ChatJoinRequestHandler(join_request_handler))
    app.add_handler(MessageHandler(
        filters.COMMAND & ~admin_filter, no_permission
    ))

    async with app:
        mongo_client = await setup_db(app)
        await start_web_server(mongo_client, app)
        await app.start()
        asyncio.create_task(expire_worker(app))
        asyncio.create_task(unban_worker(app))
        asyncio.create_task(vip_worker(app))
        logging.info("Bot started!")
        await app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=[
                "message", "chat_member",
                "callback_query", "chat_join_request"
            ]
        )
        await asyncio.Event().wait()

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logging.error(f"Bot crashed: {e} — restart sau 10s...")
            time.sleep(10)
