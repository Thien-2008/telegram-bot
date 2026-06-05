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

# ==================== LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ==================== ENV VARS ====================
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
GROUP_NAME        = os.environ.get("GROUP_NAME", "Cong dong")

# ==================== KIEM TRA ENV ====================
def check_env():
    required = ["TOKEN", "ADMIN_ID", "MONGO_URI", "CHANNEL_ID",
                "BOT_USERNAME", "GROUP_ID", "SEPAY_WEBHOOK_KEY"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        logging.critical(f"Thieu bien moi truong: {', '.join(missing)}")
        exit(1)

# ==================== IN-MEMORY ====================
request_log:        dict = defaultdict(list)
invalid_attempts:   dict = defaultdict(int)
rate_hit_count:     dict = defaultdict(int)
manual_start_count: dict = defaultdict(int)
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
MANUAL_WARN_THRESH    = 3
NONMEMBER_WARN_THRESH = 3

# ==================== LY DO BAN ====================
LY_DO = {
    "quay-roi":  "Quay roi thanh vien",
    "chia-se":   "Chia se noi dung ra ngoai",
    "spam":      "Spam tin nhan",
    "gia-mao":   "Tai khoan gia mao",
    "het-han":   "Het han dang ky",
    "ban-lai":   "Ban lai quyen truy cap",
    "vi-pham":   "Vi pham quy dinh",
    "abuse":     "Hanh vi pha hoai",
    "da-nghi":   "Tai khoan dang ngo",
    "nhieu-tk":  "Dung nhieu tai khoan",
    "hoan-tien": "Doi hoan tien/bung tien",
}

# ==================== PARSE THOI HAN BAN ====================
def parse_duration(text: str):
    m = re.match(r'^(\d+)(g|ng)$', text.lower())
    if not m:
        return None
    v, u = int(m.group(1)), m.group(2)
    return timedelta(hours=v) if u == "g" else timedelta(days=v)

# ==================== PERMISSIONS ====================
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

# ==================== HELPERS ====================
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

def make_qr_url(user_id: int) -> str:
    desc = f"SEVQR VIP {user_id}"
    return (f"https://qr.sepay.vn/img?bank={BANK_NAME}"
            f"&acc={BANK_ACCOUNT}&template=compact"
            f"&amount={VIP_PRICE}&des={desc.replace(' ', '%20')}")

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
    uname = f"@{user.username}" if user.username else "Khong co"
    await send_log(app,
        f"Truy cap noi dung\n"
        f"ID: <code>{user.id}</code>\n"
        f"Ten: {sanitize(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Album: <code>{key}</code>\n"
        f"Thoi gian: {now_str()}"
    )

async def log_warning(app, user, behavior: str, count: int):
    uname = f"@{user.username}" if user.username else "Khong co"
    await send_log(app,
        f"Canh bao hanh vi bat thuong\n"
        f"ID: <code>{user.id}</code>\n"
        f"Ten: {sanitize(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Hanh vi: {sanitize(behavior)}\n"
        f"So lan: {count}\n"
        f"Thoi gian: {now_str()}"
    )

async def log_ban(app, target_id, name, reason, ban_type="Thu cong", show_btn=False):
    kb = None
    if show_btn:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Go ban ngay", callback_data=f"unban_{target_id}")
        ]])
    await send_log(app,
        f"Ban nguoi dung\n"
        f"ID: <code>{target_id}</code>\n"
        f"Ten: {sanitize(str(name))}\n"
        f"Ly do: {sanitize(reason)}\n"
        f"Loai: {ban_type}\n"
        f"Thoi gian: {now_str()}",
        reply_markup=kb
    )

async def log_unban(app, target_id):
    await send_log(app,
        f"Huy ban\nID: <code>{target_id}</code>\nThoi gian: {now_str()}"
    )

async def log_payment(app, user_id, amount, total, status):
    await send_log(app,
        f"Thanh toan\n"
        f"ID: <code>{user_id}</code>\n"
        f"Lan nay: {amount:,}d\n"
        f"Tong da tra: {total:,}d\n"
        f"Trang thai: {status}\n"
        f"Thoi gian: {now_str()}"
    )

async def log_vip_granted(app, user_id, name, expire_at):
    await send_log(app,
        f"Cap VIP thanh cong\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(str(name))}\n"
        f"Het han: {expire_at.strftime('%d/%m/%Y')}\n"
        f"Thoi gian: {now_str()}"
    )

# ==================== BAN / UNBAN HELPER ====================
async def do_ban(app, target_id, name, reason,
                 ban_type="Tu dong", duration=None):
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
                  show_btn=(ban_type == "Tu dong"))

# ==================== LUU USER ====================
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
                "first_seen":        now,
                "invite_earned":     0,
                "invite_used":       0,
                "invite_3d_count":   0,
                "invite_3d_reset":   now,
                "kick_count":        0,
                "rules_confirmed":   False,
            }
        },
        upsert=True
)
# ==================== GRANT VIP ====================
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
                f"Thanh toan thanh cong\n\n"
                f"Goi VIP co hieu luc den ngay "
                f"{expire_at.strftime('%d/%m/%Y')} ({days} ngay).\n\n"
                f"Bam link de vao kenh VIP:\n"
                f"{invite_url}\n\n"
                f"Link chi dung 1 lan, het han sau 48 gio."
            ),
            protect_content=True
        )
        await log_vip_granted(app, user_id, user_name, expire_at)
        return True
    except Exception as e:
        logging.error(f"Send VIP link error: {e}")
        return False

# ==================== CALLBACK HANDLER ====================
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
                    text="Quyen truy cap cua ban da duoc khoi phuc.",
                    protect_content=True
                )
            except Exception:
                pass
            await query.answer("Da go ban!")
            await query.edit_message_text(
                query.message.text + f"\n\nDa go ban luc {now_str()}"
            )
            await log_unban(context.application, target_id)
        else:
            await query.answer("Khong tim thay trong danh sach ban!")
        return

    if data.startswith("confirm_rules_"):
        target_id = int(data.split("_")[2])
        if user.id != target_id:
            await query.answer("Day khong phai nut danh cho ban!")
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

        await query.answer("Xac nhan thanh cong! Chao mung ban.")

        user_doc = await users_col.find_one({"user_id": target_id})
        ref_by   = user_doc.get("ref_by") if user_doc else None
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

# ==================== KIEM TRA VIP ====================
async def is_vip_member(context, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status not in ("left", "kicked")
    except Exception as e:
        logging.error(f"Check VIP error: {e}")
        return False

# ==================== SPAM + RATE CHECK ====================
async def check_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user    = update.effective_user
    user_id = user.id
    app     = context.application
    now     = time.time()

    if user.is_bot:
        await do_ban(app, user_id, user.full_name, "Phat hien tai khoan bot")
        return True

    request_log[user_id] = [t for t in request_log[user_id] if now - t < 60]
    request_log[user_id].append(now)
    count = len(request_log[user_id])

    if count >= SPAM_PERM_BAN:
        await do_ban(app, user_id, user.full_name, "Spam 60 lan trong 1 phut")
        await user_reply_temp(update, context,
            f"Quyen truy cap bi thu hoi vinh vien.\n"
            f"Ly do: Spam 60 lan trong 1 phut.\nLien he: {ADMIN_CONTACT}",
            delay=300
        )
        return True

    if count >= SPAM_TEMP_BAN:
        warn_count[user_id] += 1
        dur = timedelta(hours=24) if warn_count[user_id] >= 2 else timedelta(hours=1)
        await do_ban(app, user_id, user.full_name, "Lam dung he thong", duration=dur)
        await user_reply_temp(update, context,
            f"Tai khoan bi tam khoa do hanh vi bat thuong.\n"
            f"Lien he admin: {ADMIN_CONTACT}", delay=60
        )
        return True

    if count >= SPAM_WARN_THRESH:
        await log_warning(app, user, "Spam request nhieu lan", count)

    prev = request_log[user_id][-2] if len(request_log[user_id]) >= 2 else 0
    if now - prev < RATE_LIMIT_SEC:
        rate_hit_count[user_id] += 1
        if rate_hit_count[user_id] >= RATE_WARN_THRESH:
            await log_warning(app, user, "Bi rate limit nhieu lan", rate_hit_count[user_id])
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
        return web.Response(text=f"error: {e}", status=500)

# ==================== SEPAY WEBHOOK ====================
async def sepay_webhook_handler(request):
    auth_header = request.headers.get("Authorization", "")
    api_key     = auth_header.replace("Apikey ", "").replace("Bearer ", "").strip()
    if SEPAY_WEBHOOK_KEY and api_key != SEPAY_WEBHOOK_KEY:
        logging.warning(f"SePay webhook: sai API key")
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
        return web.Response(
            text='{"success": true}',
            content_type="application/json"
        )

    amount  = int(data.get("transferAmount", 0))
    content = data.get("content", "")
    ref     = data.get("referenceCode", "")

    match = re.search(r'SEVQR\s+VIP\s+(\d+)', content, re.IGNORECASE)
    if not match:
        logging.info(f"SePay: khong tim thay user_id trong: {content}")
        return web.Response(
            text='{"success": true}',
            content_type="application/json"
        )

    user_id = int(match.group(1))
    app     = request.app["tg_app"]

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
                      "Dang tich luy" if total_paid < VIP_PRICE else "Du tien")

    if total_paid >= VIP_PRICE and not granted:
        await payments_col.update_one(
            {"user_id": user_id},
            {"$set": {"granted": True}}
        )
        users_col = app.bot_data["users_col"]
        user_doc  = await users_col.find_one({"user_id": user_id})
        user_name = user_doc.get("full_name", "Khong ro") if user_doc else "Khong ro"
        await grant_vip(app, user_id, user_name)

    elif total_paid < VIP_PRICE and not granted:
        con_thieu = VIP_PRICE - total_paid
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text=(
                    f"Da nhan {amount:,}d\n"
                    f"Tong da nhan: {total_paid:,}d\n"
                    f"Con thieu: {con_thieu:,}d\n\n"
                    f"Vui long chuyen them de hoan tat."
                ),
                protect_content=True
            )
        except Exception:
            pass

    return web.Response(
        text='{"success": true}',
        content_type="application/json"
    )

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
    # ==================== WORKERS ====================
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
                        text="Noi dung cua ban da het han.\n"
                             "Vao kenh VIP de lay link xem lai nhe",
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
                        text="Lenh tam khoa cua ban da het han.\n"
                             "Quyen truy cap da duoc khoi phuc.",
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
                (7, "notified_7d", "Goi VIP cua ban se het han sau 7 ngay."),
                (3, "notified_3d", "Goi VIP cua ban se het han sau 3 ngay."),
                (1, "notified_1d", "Goi VIP cua ban se het han vao ngay mai."),
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
                                 f"Lien he admin de gia han: {ADMIN_CONTACT}",
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
                        text=f"Goi VIP cua ban da het han.\n"
                             f"Lien he admin de gia han: {ADMIN_CONTACT}",
                        protect_content=True
                    )
                except Exception:
                    pass
                await vip_col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"active": False, "expired_at": now}}
                )
                await send_log(application,
                    f"VIP het han\nID: <code>{uid}</code>\n"
                    f"Ten: {sanitize(doc.get('full_name',''))}\n"
                    f"Thoi gian: {now_str()}"
                )
        except Exception as e:
            logging.error(f"VIP worker: {e}")
        await asyncio.sleep(3600)

async def process_referral_after_24h(app: Application,
                                      new_user_id: int, ref_by: int):
    await asyncio.sleep(86400)
    users_col = app.bot_data["users_col"]
    ref_doc   = await users_col.find_one({"user_id": ref_by})
    if not ref_doc:
        return

    invite_earned   = ref_doc.get("invite_earned", 0)
    invite_3d_count = ref_doc.get("invite_3d_count", 0)
    invite_3d_reset = ref_doc.get("invite_3d_reset", datetime.now(timezone.utc))
    now             = datetime.now(timezone.utc)

    if invite_3d_reset.tzinfo is None:
        invite_3d_reset = invite_3d_reset.replace(tzinfo=timezone.utc)

    if (now - invite_3d_reset).days >= 3:
        invite_3d_count = 0
        await users_col.update_one(
            {"user_id": ref_by},
            {"$set": {"invite_3d_count": 0, "invite_3d_reset": now}}
        )

    if invite_earned >= 5 or invite_3d_count >= 5:
        return

    await users_col.update_one(
        {"user_id": ref_by},
        {"$inc": {"invite_earned": 1, "invite_3d_count": 1}}
    )

    try:
        updated = await users_col.find_one({"user_id": ref_by})
        earned  = updated.get("invite_earned", 0)
        await app.bot.send_message(
            chat_id=ref_by,
            text=f"Nguoi ban gioi thieu da o lai nhom du 24 gio.\n"
                 f"Ban nhan duoc 1 luot xem.\n"
                 f"Tong luot hien tai: {earned}/5",
            protect_content=True
        )
    except Exception:
        pass

# ==================== DEMO SYSTEM ====================
async def send_demo_to_user(app: Application, user_id: int,
                             user_name: str, number: int, query=None):
    users_col = app.bot_data["users_col"]
    demos_col = app.bot_data["demos_col"]

    user_doc = await users_col.find_one({"user_id": user_id})
    if not user_doc:
        if query:
            await query.answer("Khong tim thay thong tin cua ban!")
        return

    earned   = user_doc.get("invite_earned", 0)
    used     = user_doc.get("invite_used", 0)
    luot_con = earned - used

    if luot_con <= 0:
        if query:
            await query.answer("Ban khong con luot xem!")
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text="Ban khong con luot xem.\n"
                     "Dung /gioi_thieu de kiem them luot.",
                protect_content=True
            )
        except Exception:
            pass
        return

    demo = await demos_col.find_one({"number": number})
    if not demo or not demo.get("items"):
        if query:
            await query.answer("Khong tim thay bo nay!")
        return

    await users_col.update_one(
        {"user_id": user_id},
        {"$inc": {"invite_used": 1}}
    )

    if query:
        await query.answer(f"Dang gui bo #{number}...")

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
            text=f"Da gui bo #{number}.\nLuot con lai: {luot_con}/5",
            protect_content=True
        )
    except Exception as e:
        logging.error(f"Send demo error: {e}")

# ==================== KICK IF NOT CONFIRMED ====================
async def kick_if_not_confirmed(app: Application,
                                 chat_id, user_id, message_id):
    await asyncio.sleep(60)
    users_col = app.bot_data["users_col"]
    user_doc  = await users_col.find_one({"user_id": user_id})
    confirmed = user_doc.get("rules_confirmed", False) if user_doc else False

    if confirmed:
        return

    try:
        await app.bot.delete_message(
            chat_id=chat_id, message_id=message_id
        )
    except Exception:
        pass

    try:
        await app.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
        await app.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
    except Exception as e:
        logging.error(f"Kick error: {e}")

    await users_col.update_one(
        {"user_id": user_id},
        {"$inc": {"kick_count": 1}},
        upsert=True
    )
    user_doc   = await users_col.find_one({"user_id": user_id})
    kick_count = user_doc.get("kick_count", 1) if user_doc else 1

    if kick_count >= 4:
        banned_col = app.bot_data["banned_col"]
        await banned_col.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id":   user_id,
                "name":      "Khong ro",
                "reason":    "Khong xac nhan noi quy 4 lan",
                "ban_type":  "Tu dong",
                "expire_at": None,
                "banned_at": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        await send_log(app,
            f"Auto ban: Khong xac nhan noi quy\n"
            f"ID: <code>{user_id}</code>\n"
            f"So lan bi kick: {kick_count}\n"
            f"Thoi gian: {now_str()}"
        )

    if user_id in pending_kicks:
        del pending_kicks[user_id]

# ==================== CHAT MEMBER HANDLER ====================
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

    # ---- NHOM THUONG ----
    if GROUP_ID and chat_id == str(GROUP_ID):
        if old_status in ("left", "kicked") and new_status == "member":
            banned_col = get_banned(context)
            is_banned  = await banned_col.find_one({"user_id": user.id})
            if is_banned:
                try:
                    await context.bot.ban_chat_member(
                        chat_id=GROUP_ID, user_id=user.id
                    )
                except Exception:
                    pass
                return

            try:
                await context.bot.restrict_chat_member(
                    chat_id=GROUP_ID,
                    user_id=user.id,
                    permissions=MUTED_PERMS
                )
            except Exception as e:
                logging.error(f"Mute error: {e}")

            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "Xac nhan noi quy",
                    callback_data=f"confirm_rules_{user.id}"
                )
            ]])
            rules_text = (
                f"Chao {user.full_name},\n\n"
                f"NOI QUY CONG DONG {GROUP_NAME}\n\n"
                f"TRACH NHIEM GIAM SAT: Neu phat hien thanh vien khac co "
                f"hanh vi lam phien, spam hoac vi pham quy dinh, vui long "
                f"gui anh chup man hinh bang chung truc tiep cho Admin.\n\n"
                f"TUONG TAC VAN MINH: Khong dang tai noi dung quang cao, "
                f"lien ket spam hoac gui tin nhan rieng lam phien thanh "
                f"vien khac.\n\n"
                f"QUY TRINH DICH VU: Moi giao dich va nang cap quyen loi "
                f"VIP deu phai thuc hien thong qua Bot tu dong. Neu co loi, "
                f"vui long lien he Admin kem anh chup man hinh.\n\n"
                f"QUYEN QUAN TRI: Quan tri vien co quyen loai bo thanh vien "
                f"neu phat hien hanh vi lam dung hoac co tinh vi pham.\n\n"
                f"Bang viec xac nhan, ban cam ket da doc va dong y voi cac "
                f"quy dinh tren.\n\n"
                f"Ban co 60 giay de xac nhan."
            )
            try:
                msg = await context.bot.send_message(
                    chat_id=GROUP_ID,
                    text=rules_text,
                    reply_markup=kb
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
                            "first_seen":      datetime.now(timezone.utc),
                            "invite_earned":   0,
                            "invite_used":     0,
                            "invite_3d_count": 0,
                            "invite_3d_reset": datetime.now(timezone.utc),
                            "kick_count":      0,
                        }
                    },
                    upsert=True
                )
                task = asyncio.create_task(
                    kick_if_not_confirmed(
                        app, GROUP_ID, user.id, msg.message_id
                    )
                )
                pending_kicks[user.id] = task
            except Exception as e:
                logging.error(f"Send rules error: {e}")
        return

    # ---- KENH VIP ----
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
                {"user_id": user.id},
                {"$set": {"active": False}}
            )

# ==================== CHAT JOIN REQUEST ====================
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
                text=f"Yeu cau vao kenh VIP bi tu choi.\n"
                     f"Vui long thanh toan truoc.\n"
                     f"Got /mua de xem huong dan.",
                protect_content=True
            )
        except Exception:
            pass
# ==================== USER: /start ====================
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
        reason    = ban_doc.get("reason", "Vi pham quy dinh")
        expire_at = ban_doc.get("expire_at")
        if expire_at:
            expire_str = expire_at.strftime("%d/%m/%Y %H:%M")
            await user_reply_temp(update, context,
                f"Tai khoan bi tam khoa.\nLy do: {reason}\n"
                f"Het han luc: {expire_str}\nLien he: {ADMIN_CONTACT}",
                delay=300
            )
        else:
            await user_reply_temp(update, context,
                f"Quyen truy cap da bi thu hoi.\n"
                f"Ly do: {reason}\nLien he: {ADMIN_CONTACT}",
                delay=300
            )
        return

    if await check_user(update, context):
        return

    args = context.args

    # Xu ly referral
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
        manual_start_count[user_id] += 1
        if manual_start_count[user_id] >= MANUAL_WARN_THRESH:
            await log_warning(app, user, "Go /start thu cong nhieu lan",
                              manual_start_count[user_id])
            manual_start_count[user_id] = 0

        vip = await is_vip_member(context, user_id)
        if vip:
            await user_reply_temp(update, context,
                "Ban da la thanh vien kenh VIP.\n\n"
                "Vao kenh va bam vao link trong bai dang de xem noi dung.",
                delay=120
            )
        else:
            qr_url = make_qr_url(user_id)
            await user_reply_temp(update, context,
                f"Chao mung ban den voi he thong noi dung VIP.\n\n"
                f"Goi VIP 1 thang: {VIP_PRICE:,}d\n\n"
                f"De dang ky, chuyen khoan:\n"
                f"Ngan hang: {BANK_NAME}\n"
                f"So tai khoan: {BANK_ACCOUNT}\n"
                f"So tien: {VIP_PRICE:,}d\n"
                f"Noi dung: SEVQR VIP {user_id}\n\n"
                f"Ma QR: {qr_url}\n\n"
                f"He thong tu dong cap quyen sau khi nhan thanh toan.\n"
                f"Ho tro: {ADMIN_CONTACT}",
                delay=600
            )
        return

    key        = args[0]
    albums_col = get_albums(context)

    try:
        album = await db_retry(lambda: albums_col.find_one({"key": key}))
    except Exception:
        await user_reply_temp(update, context,
            "He thong dang xu ly du lieu. Vui long thu lai sau.", delay=60
        )
        return

    if not album:
        invalid_attempts[user_id] += 1
        count = invalid_attempts[user_id]
        if count >= INVALID_WARN_THRESH:
            await log_warning(app, user, "Bam link khong hop le nhieu lan", count)
        if count >= INVALID_AUTO_BAN:
            await do_ban(app, user_id, user.full_name, "Co tinh do link")
            await user_reply_temp(update, context,
                f"Quyen truy cap bi thu hoi tu dong.\nLien he: {ADMIN_CONTACT}",
                delay=300
            )
            return
        await user_reply_temp(update, context,
            "Du lieu khong ton tai hoac phien chia se da het han.", delay=120
        )
        return

    invalid_attempts[user_id] = 0

    vip = await is_vip_member(context, user_id)
    if not vip:
        nonmember_attempts[user_id] += 1
        count = nonmember_attempts[user_id]
        if count >= NONMEMBER_WARN_THRESH:
            await log_warning(app, user, "Truy cap trai phep nhieu lan", count)
            nonmember_attempts[user_id] = 0
        qr_url = make_qr_url(user_id)
        await user_reply_temp(update, context,
            f"Noi dung chi danh cho thanh vien VIP.\n\n"
            f"De dang ky, chuyen khoan:\n"
            f"Ngan hang: {BANK_NAME}\n"
            f"So tai khoan: {BANK_ACCOUNT}\n"
            f"So tien: {VIP_PRICE:,}d\n"
            f"Noi dung: SEVQR VIP {user_id}\n\n"
            f"Ma QR: {qr_url}\n\n"
            f"Ho tro: {ADMIN_CONTACT}",
            delay=300
        )
        return

    nonmember_attempts[user_id] = 0
    await log_view(app, user, key)

    items = album.get("items", [])
    if not items:
        await user_reply_temp(update, context,
            "Du lieu khong ton tai hoac phien chia se da het han.", delay=120
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
                "He thong dang xu ly. Vui long thu lai sau.", delay=120
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
                    "He thong dang xu ly. Vui long thu lai sau.", delay=120
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

# ==================== MEMBER COMMANDS ====================
async def cmd_mua(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user   = update.effective_user
    qr_url = make_qr_url(user.id)
    await user_reply_temp(update, context,
        f"Goi VIP 1 thang: {VIP_PRICE:,}d\n\n"
        f"Chuyen khoan:\n"
        f"Ngan hang: {BANK_NAME}\n"
        f"So tai khoan: {BANK_ACCOUNT}\n"
        f"So tien: {VIP_PRICE:,}d\n"
        f"Noi dung: SEVQR VIP {user.id}\n\n"
        f"Ma QR: {qr_url}\n\n"
        f"He thong tu dong xac nhan sau khi nhan tien.\n"
        f"Ho tro: {ADMIN_CONTACT}",
        delay=600
    )

async def cmd_luot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user      = update.effective_user
    users_col = get_users(context)
    doc       = await users_col.find_one({"user_id": user.id})
    if not doc:
        await user_reply_temp(update, context,
            "Ban chua co thong tin trong he thong.\n"
            "Vao nhom va xac nhan noi quy truoc.",
            delay=120
        )
        return
    earned   = doc.get("invite_earned", 0)
    used     = doc.get("invite_used", 0)
    luot_con = earned - used
    await user_reply_temp(update, context,
        f"Luot xem cua ban:\n\n"
        f"Da kiem: {earned}/5\n"
        f"Da dung: {used}\n"
        f"Con lai: {luot_con}\n\n"
        f"Dung /gioi_thieu de kiem them luot.\n"
        f"Dung /xem de xem noi dung demo.",
        delay=120
    )

async def cmd_gioi_thieu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    ref_url = make_ref_link(user.id)
    await user_reply_temp(update, context,
        f"Link gioi thieu cua ban:\n{ref_url}\n\n"
        f"Chia se link nay de kiem luot xem.\n"
        f"Moi nguoi vao nhom va o lai 24 gio = +1 luot.\n\n"
        f"Gioi han: 5 luot/3 ngay, toi da 5 luot suot doi.\n"
        f"Dung /luot de xem so luot hien tai.",
        delay=300
    )

async def cmd_xem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user      = update.effective_user
    users_col = get_users(context)
    demos_col = get_demos(context)

    doc = await users_col.find_one({"user_id": user.id})
    if not doc:
        await user_reply_temp(update, context,
            "Ban chua co thong tin trong he thong.", delay=120
        )
        return

    earned   = doc.get("invite_earned", 0)
    used     = doc.get("invite_used", 0)
    luot_con = earned - used

    if luot_con <= 0:
        await user_reply_temp(update, context,
            f"Ban khong con luot xem.\n"
            f"Dung /gioi_thieu de kiem them luot.",
            delay=120
        )
        return

    demos = await demos_col.find(
        {}, {"number": 1, "title": 1}
    ).sort("number", 1).to_list(length=20)

    if not demos:
        await user_reply_temp(update, context,
            "Hien chua co bo demo nao.", delay=120
        )
        return

    keyboard = []
    for d in demos:
        title = d.get("title") or f"Bo {d['number']}"
        keyboard.append([InlineKeyboardButton(
            f"#{d['number']} — {title}",
            callback_data=f"demo_{d['number']}"
        )])

    await user_reply_temp(update, context,
        f"Luot con lai: {luot_con}/5\n\nChon bo muon xem:",
        delay=300,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def cmd_help_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        return
    await user_reply_temp(update, context,
        "Huong dan su dung:\n\n"
        "/mua — Xem thong tin goi VIP va tao ma QR thanh toan\n"
        "/luot — Xem so luot xem con lai\n"
        "/gioi_thieu — Lay link gioi thieu de kiem luot\n"
        "/xem — Dung luot xem noi dung demo\n"
        "/help — Huong dan su dung\n\n"
        f"Ho tro: {ADMIN_CONTACT}",
        delay=120
    )
    # ==================== ADMIN COMMANDS ====================
async def new_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if context.user_data.get("current_key"):
        await update.message.reply_text(
            "Dang co album chua hoan thanh.\nGo /done de lay link truoc."
        )
        return
    key        = make_key()
    albums_col = get_albums(context)
    await albums_col.insert_one({
        "key": key, "items": [], "created_at": datetime.now(timezone.utc)
    })
    context.user_data["current_key"] = key
    await update.message.reply_text(
        f"Album moi da tao.\nMa: <code>{key}</code>\n\n"
        f"Forward anh hoac video vao day.\nGo /done khi xong.",
        parse_mode="HTML"
    )

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if update.message.forward_from:
        fwd   = update.message.forward_from
        uname = f"@{fwd.username}" if fwd.username else "Khong co"
        await update.message.reply_text(
            f"Thong tin:\nID: <code>{fwd.id}</code>\n"
            f"Ten: {fwd.full_name}\nUsername: {uname}",
            parse_mode="HTML"
        )
        return

    if GROUP_ID and str(update.effective_chat.id) == str(GROUP_ID):
        caption = update.message.caption or ""
        match   = re.match(r'^#(\d+)\s*(.*)', caption.strip())
        if match:
            number    = int(match.group(1))
            title     = match.group(2).strip() or f"Bo {number}"
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
                f"Da luu vao demo #{number} — {title} ({count} file)."
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
        f"Da nhan {file_type} — album co {count} file.\nGo /done khi xong."
    )

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    key = context.user_data.get("current_key")
    if not key:
        await update.message.reply_text("Khong co album nao dang tao.")
        return
    albums_col = get_albums(context)
    album      = await albums_col.find_one({"key": key})
    if not album or not album.get("items"):
        await update.message.reply_text(
            "Album chua co file nao.\nHay forward anh hoac video vao truoc."
        )
        return
    context.user_data.pop("current_key", None)
    count = len(album.get("items", []))
    await update.message.reply_text(
        f"Hoan tat. Album co {count} file.\nLink chia se:"
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
        await update.message.reply_text("Loi ket noi co so du lieu.")
        return
    if not albums:
        await update.message.reply_text("Chua co album nao.")
        return
    text = f"Danh sach album ({len(albums)}):\n\n"
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
        await update.message.reply_text("Loi ket noi co so du lieu.")
        return
    if not albums:
        await update.message.reply_text("Chua co album nao.")
        return
    for i, a in enumerate(albums, 1):
        await update.message.reply_text(
            f"{i}\nSo file: {len(a.get('items',[]))}\nLink:"
        )
        await update.message.reply_text(make_link(a["key"]))
        await asyncio.sleep(0.3)

async def check_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dung: /check <ma>")
        return
    key        = context.args[0]
    albums_col = get_albums(context)
    album      = await albums_col.find_one({"key": key})
    if not album:
        await update.message.reply_text(f"Khong tim thay album {key}.")
        return
    items  = album.get("items", [])
    videos = sum(1 for i in items if i["type"] == "video")
    photos = sum(1 for i in items if i["type"] == "photo")
    await update.message.reply_text(
        f"Album: <code>{key}</code>\n"
        f"Tong: {len(items)} | Video: {videos} | Anh: {photos}\nLink:",
        parse_mode="HTML"
    )
    await update.message.reply_text(make_link(key))

async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dung: /del <ma>")
        return
    key        = context.args[0]
    albums_col = get_albums(context)
    jobs_col   = get_jobs(context)
    result     = await albums_col.delete_one({"key": key})
    await jobs_col.delete_many({"album_key": key})
    if result.deleted_count == 0:
        await update.message.reply_text(f"Khong tim thay album {key}.")
    else:
        await update.message.reply_text(f"Da xoa album {key}.")

async def clean_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    albums_col = get_albums(context)
    cutoff     = datetime.now(timezone.utc) - timedelta(days=7)
    result     = await albums_col.delete_many({
        "$or": [{"items": []}, {"created_at": {"$lt": cutoff}}]
    })
    await update.message.reply_text(
        f"Da xoa {result.deleted_count} album trong hoac cu hon 7 ngay."
    )

async def demo_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dung: /demo_clear <so>")
        return
    try:
        number    = int(context.args[0])
        demos_col = get_demos(context)
        result    = await demos_col.delete_one({"number": number})
        if result.deleted_count:
            await update.message.reply_text(f"Da xoa demo #{number}.")
        else:
            await update.message.reply_text(f"Khong tim thay demo #{number}.")
    except ValueError:
        await update.message.reply_text("So khong hop le.")

async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    target_id, target_name, duration, reason = None, "Khong ro", None, "Vi pham quy dinh"
    args = context.args or []
    if update.message.reply_to_message:
        u = update.message.reply_to_message.from_user
        target_id, target_name = u.id, u.full_name
        if args:
            dur = parse_duration(args[0])
            if dur:
                duration = dur
                reason   = " ".join(args[1:]) if len(args) > 1 else "Vi pham quy dinh"
            else:
                reason   = " ".join(args)
    elif args:
        first = args[0]
        if first.startswith("@"):
            try:
                chat = await context.bot.get_chat(first)
                target_id, target_name = chat.id, chat.full_name
            except Exception:
                await update.message.reply_text("Khong tim thay username.")
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
                await update.message.reply_text("ID khong hop le.")
                return
            rest = args[1:]
        if rest:
            dur = parse_duration(rest[0])
            if dur:
                duration = dur
                reason   = " ".join(rest[1:]) if len(rest) > 1 else "Vi pham quy dinh"
            else:
                reason   = " ".join(rest)
    else:
        await update.message.reply_text(
            "Cach dung /ban:\n"
            "1. Reply tin nhan + /ban <ly do>\n"
            "2. /ban @username <ly do>\n"
            "3. /ban ID <ly do>\n"
            "Thoi han: 1g / 24g / 7ng\n"
            "Vi du: /ban @abc 24g quay-roi"
        )
        return
    if not target_id or target_id == ADMIN_ID:
        await update.message.reply_text("Khong the ban.")
        return
    reason = LY_DO.get(reason, reason)
    if update.effective_chat.type in ("group", "supergroup"):
        try:
            await context.bot.ban_chat_member(
                chat_id=update.effective_chat.id, user_id=target_id
            )
        except Exception as e:
            await update.message.reply_text(f"Khong the kick: {e}")
            return
    await do_ban(context.application, target_id, target_name,
                 reason, ban_type="Thu cong", duration=duration)
    try:
        expire_text = ""
        if duration:
            ea          = datetime.now(timezone(timedelta(hours=7))) + duration
            expire_text = f"\nHet han: {ea.strftime('%d/%m/%Y %H:%M')}"
        await context.bot.send_message(
            chat_id=target_id,
            text=f"Ban da bi cam.\nLy do: {reason}{expire_text}\n"
                 f"Lien he: {ADMIN_CONTACT}",
            protect_content=True
        )
    except Exception:
        pass
    expire_info = ""
    if duration:
        days  = int(duration.total_seconds() // 86400)
        hours = int(duration.total_seconds() // 3600)
        expire_info = f" ({days} ngay)" if days >= 1 else f" ({hours} gio)"
    await update.message.reply_text(
        f"Da cam {target_name}{expire_info}.\nLy do: {reason}"
    )

async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    target_id, target_name = None, "Khong ro"
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
                await update.message.reply_text("Khong tim thay username.")
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
                await update.message.reply_text("ID khong hop le.")
                return
    else:
        await update.message.reply_text("Dung: /unban @username hoac /unban ID")
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
                text="Quyen truy cap cua ban da duoc khoi phuc.",
                protect_content=True
            )
        except Exception:
            pass
        await update.message.reply_text(f"Da go cam {target_name}.")
        await log_unban(context.application, target_id)
    else:
        await update.message.reply_text("Khong tim thay trong danh sach cam.")

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
            await update.message.reply_text("ID khong hop le.")
            return
    else:
        await update.message.reply_text("Dung: /who <ID> hoac reply tin nhan")
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
        username = f"@{chat.username}" if chat.username else "Khong co"
    except Exception:
        name, username = "Khong ro", "Khong ro"
    text = (
        f"Thong tin:\nID: <code>{target_id}</code>\n"
        f"Ten: {name}\nUsername: {username}\n\n"
    )
    if ban_doc:
        es    = ban_doc['expire_at'].strftime('%d/%m/%Y %H:%M') if ban_doc.get('expire_at') else "Vinh vien"
        text += f"Trang thai: Dang bi cam\nLy do: {ban_doc.get('reason','')}\nHet han: {es}\n\n"
    else:
        text += "Trang thai ban: Binh thuong\n\n"
    if vip_doc and vip_doc.get("active"):
        ea   = vip_doc.get("expire_at")
        days = days_left(ea) if ea else 0
        text += f"VIP: Dang hoat dong\nHet han: {ea.strftime('%d/%m/%Y') if ea else ''} ({days} ngay con lai)\n\n"
    else:
        text += "VIP: Chua co hoac da het han\n\n"
    if user_doc:
        earned = user_doc.get("invite_earned", 0)
        used   = user_doc.get("invite_used", 0)
        text  += f"Luot: {earned - used} con lai ({earned} kiem / {used} da dung)"
    await update.message.reply_text(text, parse_mode="HTML")

async def extend_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Dung: /extend <ID>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID khong hop le.")
        return
    vip_col   = get_vip(context)
    users_col = get_users(context)
    doc       = await vip_col.find_one({"user_id": target_id})
    now       = datetime.now(timezone.utc)
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
    user_name = user_doc.get("full_name", "Khong ro") if user_doc else "Khong ro"
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"Goi VIP da duoc gia han thanh cong.\n\n"
                f"Het han moi: {new_expire.strftime('%d/%m/%Y')}\n"
                f"So ngay con lai: {days} ngay"
            ),
            protect_content=True
        )
    except Exception:
        pass
    await update.message.reply_text(
        f"Da gia han VIP cho <code>{target_id}</code> ({user_name}).\n"
        f"Het han moi: {new_expire.strftime('%d/%m/%Y')} ({days} ngay con lai)",
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
        await update.message.reply_text("Chua co member VIP nao.")
        return
    text = f"VIP dang hoat dong ({len(members)}):\n\n"
    for i, m in enumerate(members, 1):
        ea   = m.get("expire_at")
        days = days_left(ea) if ea else 0
        text += (
            f"{i}. <code>{m['user_id']}</code> — "
            f"{sanitize(m.get('full_name',''))} — "
            f"het {ea.strftime('%d/%m/%Y') if ea else '?'} ({days} ngay)\n"
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
            f"Trang thai he thong:\n\n"
            f"Album: {total_albums}\n"
            f"Demo: {total_demos} bo\n"
            f"User: {total_users}\n"
            f"VIP: {total_vip}\n"
            f"Cam: {total_banned} (tam: {temp_banned})\n"
            f"Job cho xoa: {pending_jobs}\n"
            f"Database: Ket noi on dinh"
        )
    except Exception:
        await update.message.reply_text("Loi ket noi co so du lieu.")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        "Lenh danh cho Admin:\n\n"
        "— NOI DUNG —\n"
        "/new — Tao album moi\n"
        "/done — Lay link chia se\n"
        "/list — Danh sach album\n"
        "/detail — Album kem link\n"
        "/check <ma> — Thong tin album\n"
        "/del <ma> — Xoa album\n"
        "/clean — Xoa album cu\n"
        "/demo_clear <so> — Xoa bo demo\n\n"
        "— THANH VIEN —\n"
        "/ban — Cam (reply / @user / ID + thoi han)\n"
        "/unban — Go cam\n"
        "/who <ID> — Thong tin tai khoan\n\n"
        "— VIP —\n"
        "/viplist — Danh sach VIP\n"
        "/extend <ID> — Gia han 1 thang\n\n"
        "— HE THONG —\n"
        "/status — Trang thai\n\n"
        "Ly do cam: quay-roi / chia-se / spam\n"
        "gia-mao / het-han / ban-lai / vi-pham\n"
        "abuse / da-nghi / nhieu-tk / hoan-tien"
    )

async def no_permission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await user_reply_temp(update, context,
        "Ban khong co quyen su dung lenh nay.", delay=120
    )

# ==================== SETUP DB ====================
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

# ==================== MAIN ====================
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
