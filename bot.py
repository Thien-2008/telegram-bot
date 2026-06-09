import os, asyncio, logging, time, secrets, string, re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web
from telegram import (
    Update, InputMediaPhoto, InputMediaVideo,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ChatPermissions, ForceReply
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackQueryHandler,
    ChatMemberHandler, ChatJoinRequestHandler
)
from telegram.error import Forbidden, BadRequest, TelegramError

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

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
SEPAY_WEBHOOK_KEY = os.environ.get("SEPAY_WEBHOOK_KEY", "")
VIP_PRICE         = int(os.environ.get("VIP_PRICE", "119000"))
BANK_ACCOUNT      = "100887150390"
BANK_NAME         = "VietinBank"
BANK_BIN          = "970415"
GROUP_NAME        = os.environ.get("GROUP_NAME", "Cong dong")

def check_env():
    missing = [v for v in ["TOKEN","ADMIN_ID","MONGO_URI","CHANNEL_ID","BOT_USERNAME","GROUP_ID"] if not os.environ.get(v)]
    if missing:
        logging.critical(f"Thieu bien moi truong: {', '.join(missing)}")
        exit(1)

# ==================== IN-MEMORY ====================
request_log:        dict = defaultdict(list)
invalid_attempts:   dict = defaultdict(int)
rate_hit_count:     dict = defaultdict(int)
nonmember_attempts: dict = defaultdict(int)
warn_count:         dict = defaultdict(int)
pending_kicks:      dict = {}
pending_bans:       dict = {}   # {admin_id: {target_id, reason}}
awaiting_ban_time:  dict = {}   # {admin_id: True}

RATE_LIMIT_SEC = 5
SPAM_WARN = 5; SPAM_TEMP = 8; SPAM_PERM = 60
INVALID_WARN = 3; INVALID_BAN = 5
RATE_WARN = 3; NONMEMBER_WARN = 3
BUFFER_MINUTES = 10

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

def parse_duration(text: str):
    m = re.match(r'^(\d+)(d|h|m)$', text.lower().strip())
    if not m: return None
    v, u = int(m.group(1)), m.group(2)
    if u == 'd': return timedelta(days=v)
    if u == 'h': return timedelta(hours=v)
    if u == 'm': return timedelta(minutes=v)

MUTED = ChatPermissions(
    can_send_messages=False, can_send_audios=False,
    can_send_documents=False, can_send_photos=False,
    can_send_videos=False, can_send_video_notes=False,
    can_send_voice_notes=False, can_send_polls=False,
    can_send_other_messages=False,
)
UNMUTED = ChatPermissions(
    can_send_messages=True, can_send_audios=True,
    can_send_documents=True, can_send_photos=True,
    can_send_videos=True, can_send_video_notes=True,
    can_send_voice_notes=True, can_send_polls=True,
    can_send_other_messages=True,
)

def get_albums(ctx):   return ctx.application.bot_data["albums_col"]
def get_banned(ctx):   return ctx.application.bot_data["banned_col"]
def get_jobs(ctx):     return ctx.application.bot_data["jobs_col"]
def get_users(ctx):    return ctx.application.bot_data["users_col"]
def get_vip(ctx):      return ctx.application.bot_data["vip_col"]
def get_payments(ctx): return ctx.application.bot_data["payments_col"]
def get_demos(ctx):    return ctx.application.bot_data["demos_col"]

def make_key():
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))
def make_link(key): return f"https://t.me/{BOT_USERNAME}?start={key}"
def make_ref_link(uid): return f"https://t.me/{BOT_USERNAME}?start=ref_{uid}"
def make_qr_img(uid):
    return (f"https://qr.sepay.vn/img?bank={BANK_NAME}&acc={BANK_ACCOUNT}"
            f"&template=compact&amount={VIP_PRICE}&des=SEVQR%20VIP%20{uid}")
def make_vietqr(uid):
    return (f"https://img.vietqr.io/image/{BANK_BIN}-{BANK_ACCOUNT}-compact.png"
            f"?amount={VIP_PRICE}&addInfo=SEVQR%20VIP%20{uid}")

def now_str():
    return datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M")
def sanitize(text, max_len=500):
    text = str(text).replace("<","&lt;").replace(">","&gt;")
    return text[:max_len]+"..." if len(text)>max_len else text
def days_left(exp):
    now = datetime.now(timezone.utc)
    if exp.tzinfo is None: exp = exp.replace(tzinfo=timezone.utc)
    return max(0, (exp-now).days)

async def bot_reply(update, text, **kw):
    return await update.message.reply_text(text, protect_content=True, **kw)

async def auto_del(bot, chat_id, msg_id, delay):
    await asyncio.sleep(delay)
    try: await bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception: pass

async def temp_reply(update, context, text, delay=120, **kw):
    msg = await update.message.reply_text(text, protect_content=True, **kw)
    asyncio.create_task(auto_del(context.bot, update.effective_chat.id, msg.message_id, delay))
    return msg

async def db_retry(op, retries=3):
    err = None
    for i in range(retries):
        try: return await op()
        except Exception as e:
            err = e; logging.error(f"DB retry {i+1}: {e}"); await asyncio.sleep(1)
    raise err

# ==================== LOG ====================
async def send_log(app, text, markup=None):
    if not LOG_GROUP_ID: return
    try:
        await app.bot.send_message(
            chat_id=LOG_GROUP_ID, text=sanitize(text),
            parse_mode="HTML", reply_markup=markup
        )
    except Exception as e: logging.error(f"Log error: {e}")

async def log_mua(app, user):
    uname = f"@{user.username}" if user.username else "Khong co"
    await send_log(app,
        f"Khoi tao hoa don mua VIP\n"
        f"ID: <code>{user.id}</code>\n"
        f"Ten: {sanitize(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Goi: 1 Thang\n"
        f"So tien: {VIP_PRICE:,}d\n"
        f"Noi dung chuyen khoan: SEVQR VIP {user.id}\n"
        f"Thoi gian: {now_str()}"
    )

async def log_payment_ok(app, user_id, name, username, amount, expire_at, pay_type):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Thanh toan VIP thanh cong\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"So tien nhan: {amount:,}d\n"
        f"Han VIP moi: {expire_at.strftime('%d/%m/%Y')}\n"
        f"Loai: {pay_type}\n"
        f"Thoi gian: {now_str()}"
    )

async def log_extend(app, target_id, days, expire_at):
    await send_log(app,
        f"Gia han VIP thu cong\n"
        f"ID nguoi nhan: <code>{target_id}</code>\n"
        f"So ngay cong them: {days} ngay\n"
        f"Han VIP moi: {expire_at.strftime('%d/%m/%Y')}\n"
        f"Thoi gian: {now_str()}"
    )

async def log_vip_approved(app, user_id, name, username):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Tu dong duyet vao kenh VIP\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Trang thai: Hop le\n"
        f"Thoi gian: {now_str()}"
    )

async def log_vip_rejected(app, user_id, name, username):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Tu choi vao kenh VIP\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Trang thai: Khong hop le\n"
        f"Thoi gian: {now_str()}"
    )

async def log_rules_confirm(app, user_id, name, username, status):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Xac nhan noi quy\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Trang thai: {status}\n"
        f"Thoi gian: {now_str()}"
    )

async def log_kick(app, user_id, name, username, kick_count):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Tu dong da nguoi dung\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Ly do: Qua 60 giay khong xac nhan noi quy\n"
        f"So lan vi pham: {kick_count}\n"
        f"Thoi gian: {now_str()}"
    )

async def log_auto_ban(app, user_id, name, username, reason):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Tu dong ban vinh vien\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Ly do: {reason}\n"
        f"Thoi gian: {now_str()}"
    )

async def log_ban_action(app, target_id, name, reason, ban_type, show_btn=False):
    kb = None
    if show_btn:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Go ban ngay", callback_data=f"unban_{target_id}")]])
    await send_log(app,
        f"Ban nguoi dung\n"
        f"ID: <code>{target_id}</code>\n"
        f"Ten: {sanitize(str(name))}\n"
        f"Ly do: {sanitize(reason)}\n"
        f"Loai: {ban_type}\n"
        f"Thoi gian: {now_str()}", markup=kb
    )

async def log_unban(app, target_id):
    await send_log(app, f"Huy ban\nID: <code>{target_id}</code>\nThoi gian: {now_str()}")

async def log_payment_partial(app, user_id, amount, total):
    await send_log(app,
        f"Thanh toan chua du\n"
        f"ID: <code>{user_id}</code>\n"
        f"Lan nay: {amount:,}d\n"
        f"Tong da tra: {total:,}d\n"
        f"Con thieu: {VIP_PRICE - total:,}d\n"
        f"Thoi gian: {now_str()}"
    )

async def log_warning(app, user, behavior, count):
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

# ==================== BAN HELPER ====================
async def do_ban(app, target_id, name, reason, ban_type="Tu dong", duration=None):
    banned_col = app.bot_data["banned_col"]
    expire_at  = datetime.now(timezone.utc) + duration if duration else None
    await banned_col.update_one(
        {"user_id": target_id},
        {"$set": {"user_id": target_id, "name": name, "reason": reason,
                  "ban_type": ban_type, "expire_at": expire_at,
                  "banned_at": datetime.now(timezone.utc)}},
        upsert=True
    )
    await log_ban_action(app, target_id, name, reason, ban_type, show_btn=(ban_type=="Tu dong"))

async def save_user(context, user):
    users_col = get_users(context)
    now = datetime.now(timezone.utc)
    await users_col.update_one(
        {"user_id": user.id},
        {"$set": {"user_id": user.id, "username": user.username,
                  "full_name": user.full_name, "last_seen": now},
         "$setOnInsert": {"first_seen": now, "invite_earned": 0,
                          "invite_used": 0, "kick_count": 0,
                          "rules_confirmed_before": False,
                          "is_muted": False, "total_views": 0}},
        upsert=True
    )

# ==================== GRANT VIP ====================
async def grant_vip(app, user_id, user_name, username=None):
    vip_col = app.bot_data["vip_col"]
    now     = datetime.now(timezone.utc)
    doc     = await vip_col.find_one({"user_id": user_id})

    # Tinh ngay het han thong minh (cong don)
    if doc and doc.get("expire_at"):
        ea = doc["expire_at"]
        if ea.tzinfo is None: ea = ea.replace(tzinfo=timezone.utc)
        base = max(ea, now)
        pay_type = "Gia han noi tiep (Da cong don)"
    else:
        base = now
        pay_type = "Mua moi (Kich hoat lan dau)"
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
        logging.error(f"Create invite link: {e}")
        return False

    await vip_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "full_name": user_name,
                  "username": username, "pending": True,
                  "invite_url": invite_url, "invite_msg_id": None,
                  "pending_expire": expire_at,
                  "notified_7d": False, "notified_3d": False, "notified_1d": False}},
        upsert=True
    )

    days = days_left(expire_at)
    kb   = InlineKeyboardMarkup([[InlineKeyboardButton(
        "Bam vao day de vao kenh VIP", url=invite_url
    )]])
    try:
        msg = await app.bot.send_message(
            chat_id=user_id,
            text=(f"Thanh toan thanh cong\n\n"
                  f"Goi VIP co hieu luc den ngay {expire_at.strftime('%d/%m/%Y')} ({days} ngay).\n\n"
                  f"Bam nut ben duoi de vao kenh VIP.\n"
                  f"Link chi dung 1 lan, het han sau 48 gio."),
            reply_markup=kb,
            protect_content=True
        )
        # Luu msg_id de sau nay xoa nut khi da vao
        await vip_col.update_one(
            {"user_id": user_id},
            {"$set": {"invite_msg_id": msg.message_id}}
        )
    except Exception as e:
        logging.error(f"Send VIP link: {e}")
        return False

    await log_payment_ok(app, user_id, user_name, username, VIP_PRICE, expire_at, pay_type)
    return True

# ==================== SPAM CHECK ====================
async def check_user(update, context):
    user = update.effective_user
    uid  = user.id
    app  = context.application
    now  = time.time()
    if user.is_bot:
        await do_ban(app, uid, user.full_name, "Phat hien tai khoan bot")
        return True
    request_log[uid] = [t for t in request_log[uid] if now-t < 60]
    request_log[uid].append(now)
    count = len(request_log[uid])
    if count >= SPAM_PERM:
        await do_ban(app, uid, user.full_name, "Spam 60 lan trong 1 phut")
        await temp_reply(update, context,
            f"Quyen truy cap bi thu hoi vinh vien.\nLy do: Spam 60 lan trong 1 phut.\n"
            f"Neu ban nghi lenh cam do nham lan, lien he: {ADMIN_CONTACT}", delay=300)
        return True
    if count >= SPAM_TEMP:
        warn_count[uid] += 1
        dur = timedelta(hours=24) if warn_count[uid] >= 2 else timedelta(hours=1)
        await do_ban(app, uid, user.full_name, "Lam dung he thong", duration=dur)
        await temp_reply(update, context,
            f"Tai khoan bi tam khoa do hanh vi bat thuong.\nLien he admin: {ADMIN_CONTACT}", delay=60)
        return True
    if count >= SPAM_WARN:
        await log_warning(app, user, "Spam request nhieu lan", count)
    prev = request_log[uid][-2] if len(request_log[uid]) >= 2 else 0
    if now - prev < RATE_LIMIT_SEC:
        rate_hit_count[uid] += 1
        if rate_hit_count[uid] >= RATE_WARN:
            await log_warning(app, user, "Bi rate limit nhieu lan", rate_hit_count[uid])
            rate_hit_count[uid] = 0
        return True
    return False

async def is_vip(context, uid):
    try:
        m = await context.bot.get_chat_member(CHANNEL_ID, uid)
        return m.status not in ("left","kicked")
    except Exception as e:
        logging.error(f"Check VIP: {e}")
        return False

# ==================== KEEP-ALIVE + WEBHOOK ====================
async def health_handler(req): return web.Response(text="ok")
async def db_health(req):
    try:
        await req.app["mongo_client"].admin.command("ping")
        return web.Response(text="ok - DB connected")
    except Exception as e: return web.Response(text=f"error: {e}", status=500)

async def sepay_handler(req):
    auth = req.headers.get("Authorization","").replace("Apikey ","").replace("Bearer ","").strip()
    if SEPAY_WEBHOOK_KEY and auth != SEPAY_WEBHOOK_KEY:
        return web.Response(status=401, text='{"success":false}', content_type="application/json")
    try: data = await req.json()
    except: return web.Response(status=400, text='{"success":false}', content_type="application/json")
    if data.get("transferType") != "in":
        return web.Response(text='{"success":true}', content_type="application/json")
    amount  = int(data.get("transferAmount", 0))
    content = data.get("content","")
    ref     = data.get("referenceCode","")
    m       = re.search(r'SEVQR\s+VIP\s+(\d+)', content, re.IGNORECASE)
    if not m: return web.Response(text='{"success":true}', content_type="application/json")
    user_id = int(m.group(1))
    app     = req.app["tg_app"]
    await process_payment(app, user_id, amount, ref, content)
    return web.Response(text='{"success":true}', content_type="application/json")

async def process_payment(app, user_id, amount, ref="", content=""):
    payments_col = app.bot_data["payments_col"]
    users_col    = app.bot_data["users_col"]
    now          = datetime.now(timezone.utc)
    await payments_col.update_one(
        {"user_id": user_id},
        {"$inc": {"total_paid": amount},
         "$push": {"transactions": {"amount": amount, "ref": ref, "content": content, "time": now}},
         "$setOnInsert": {"granted": False}},
        upsert=True
    )
    doc        = await payments_col.find_one({"user_id": user_id})
    total_paid = doc.get("total_paid", 0)
    granted    = doc.get("granted", False)
    user_doc   = await users_col.find_one({"user_id": user_id})
    user_name  = user_doc.get("full_name","Khong ro") if user_doc else "Khong ro"
    username   = user_doc.get("username") if user_doc else None
    if total_paid >= VIP_PRICE and not granted:
        await payments_col.update_one({"user_id": user_id}, {"$set": {"granted": True}})
        await grant_vip(app, user_id, user_name, username)
    elif total_paid < VIP_PRICE and not granted:
        con_thieu = VIP_PRICE - total_paid
        await log_payment_partial(app, user_id, amount, total_paid)
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text=(f"Da nhan {amount:,}d\n"
                      f"Tong da nhan: {total_paid:,}d\n"
                      f"Con thieu: {con_thieu:,}d\n\n"
                      f"Vui long chuyen them de hoan tat."),
                protect_content=True
            )
        except Exception: pass

async def start_web_server(mongo_client, tg_app):
    webapp = web.Application()
    webapp["mongo_client"] = mongo_client
    webapp["tg_app"]       = tg_app
    webapp.router.add_get("/", health_handler)
    webapp.router.add_get("/health", db_health)
    webapp.router.add_post("/sepay-webhook", sepay_handler)
    runner = web.AppRunner(webapp)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web on port {PORT}")

# ==================== WORKERS ====================
async def expire_worker(app):
    jobs_col = app.bot_data["jobs_col"]
    while True:
        try:
            now = datetime.now(timezone.utc)
            async for job in jobs_col.find({"expire_at": {"$lte": now}, "done": False}):
                for mid in job.get("message_ids",[]):
                    try: await app.bot.delete_message(chat_id=job["chat_id"], message_id=mid)
                    except (Forbidden, BadRequest): pass
                    except Exception as e: logging.error(f"Del msg: {e}")
                try:
                    await app.bot.send_message(
                        chat_id=job["chat_id"],
                        text="Noi dung cua ban da het han.\nVao kenh VIP de lay link xem lai nhe",
                        protect_content=True
                    )
                except Exception: pass
                await jobs_col.update_one({"_id": job["_id"]}, {"$set": {"done": True}})
        except Exception as e: logging.error(f"Expire worker: {e}")
        await asyncio.sleep(60)

async def unban_worker(app):
    banned_col = app.bot_data["banned_col"]
    while True:
        try:
            now = datetime.now(timezone.utc)
            async for ban in banned_col.find({"expire_at": {"$lte": now, "$ne": None}}):
                tid = ban["user_id"]
                await banned_col.delete_one({"_id": ban["_id"]})
                try:
                    await app.bot.send_message(
                        chat_id=tid,
                        text="Lenh tam khoa da het han.\nQuyen truy cap da duoc khoi phuc.",
                        protect_content=True
                    )
                except Exception: pass
                await log_unban(app, tid)
        except Exception as e: logging.error(f"Unban worker: {e}")
        await asyncio.sleep(60)

async def vip_worker(app):
    vip_col = app.bot_data["vip_col"]
    while True:
        try:
            now = datetime.now(timezone.utc)
            for db, field, msg in [
                (7,"notified_7d","Goi VIP cua ban se het han sau 7 ngay."),
                (3,"notified_3d","Goi VIP cua ban se het han sau 3 ngay."),
                (1,"notified_1d","Goi VIP cua ban se het han vao ngay mai."),
            ]:
                async for doc in vip_col.find({
                    "expire_at": {"$lte": now+timedelta(days=db), "$gt": now+timedelta(days=db-1)},
                    "active": True, field: {"$ne": True}
                }):
                    try:
                        await app.bot.send_message(
                            chat_id=doc["user_id"],
                            text=f"{msg}\nLien he admin de gia han: {ADMIN_CONTACT}",
                            protect_content=True
                        )
                        await vip_col.update_one({"_id": doc["_id"]}, {"$set": {field: True}})
                    except Exception: pass
            async for doc in vip_col.find({"expire_at": {"$lte": now}, "active": True}):
                uid = doc["user_id"]
                try:
                    await app.bot.ban_chat_member(chat_id=CHANNEL_ID, user_id=uid)
                    await app.bot.unban_chat_member(chat_id=CHANNEL_ID, user_id=uid)
                except Exception as e: logging.error(f"VIP kick {uid}: {e}")
                try:
                    await app.bot.send_message(
                        chat_id=uid,
                        text=f"Goi VIP cua ban da het han.\nLien he admin de gia han: {ADMIN_CONTACT}",
                        protect_content=True
                    )
                except Exception: pass
                # Giu nguyen data, chi set active=False
                await vip_col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"active": False, "expired_at": now}}
                )
                await send_log(app,
                    f"VIP het han\nID: <code>{uid}</code>\n"
                    f"Ten: {sanitize(doc.get('full_name',''))}\nThoi gian: {now_str()}"
                )
        except Exception as e: logging.error(f"VIP worker: {e}")
        await asyncio.sleep(3600)

async def process_referral_after_24h(app, new_uid, ref_by):
    await asyncio.sleep(86400)
    users_col = app.bot_data["users_col"]
    ref_doc   = await users_col.find_one({"user_id": ref_by})
    if not ref_doc: return
    if ref_doc.get("invite_earned", 0) >= 15: return
    await users_col.update_one({"user_id": ref_by}, {"$inc": {"invite_earned": 1}})
    try:
        updated = await users_col.find_one({"user_id": ref_by})
        earned  = updated.get("invite_earned", 0)
        await app.bot.send_message(
            chat_id=ref_by,
            text=f"Nguoi ban gioi thieu da o lai nhom du 24 gio.\n"
                 f"Ban nhan duoc 1 luot xem.\nTong luot hien tai: {earned}/15",
            protect_content=True
        )
    except Exception: pass

async def kick_if_not_confirmed(app, chat_id, user_id, message_id):
    await asyncio.sleep(60)
    users_col = app.bot_data["users_col"]
    user_doc  = await users_col.find_one({"user_id": user_id})
    if user_doc and user_doc.get("is_muted") == False: return
    try: await app.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception: pass
    try:
        await app.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
        await app.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
    except Exception as e: logging.error(f"Kick: {e}")
    await users_col.update_one({"user_id": user_id}, {"$inc": {"kick_count": 1}}, upsert=True)
    doc        = await users_col.find_one({"user_id": user_id})
    kick_count = doc.get("kick_count", 1) if doc else 1
    name       = doc.get("full_name","Khong ro") if doc else "Khong ro"
    username   = doc.get("username") if doc else None
    await log_kick(app, user_id, name, username, kick_count)
    if kick_count >= 4:
        banned_col = app.bot_data["banned_col"]
        await banned_col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "name": name, "reason": "Vi pham luong noi quy qua 4 lan",
                      "ban_type": "Tu dong", "expire_at": None,
                      "banned_at": datetime.now(timezone.utc)}},
            upsert=True
        )
        await log_auto_ban(app, user_id, name, username, "Vi pham luong noi quy qua 4 lan")
    if user_id in pending_kicks: del pending_kicks[user_id]

# ==================== CHAT MEMBER HANDLER ====================
async def chat_member_updated(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if not result: return
    chat_id    = str(result.chat.id)
    user       = result.new_chat_member.user
    old_status = result.old_chat_member.status
    new_status = result.new_chat_member.status
    app        = context.application
    try: _gid = int(GROUP_ID) if GROUP_ID else None
    except: _gid = None

    # NHOM THUONG
    if _gid and result.chat.id == _gid:
        if old_status in ("left","kicked") and new_status == "member":
            banned_col = get_banned(context)
            if await banned_col.find_one({"user_id": user.id}):
                try: await context.bot.ban_chat_member(chat_id=_gid, user_id=user.id)
                except Exception: pass
                return
            try: await context.bot.restrict_chat_member(chat_id=_gid, user_id=user.id, permissions=MUTED)
            except Exception as e: logging.error(f"Mute: {e}")

            kb = InlineKeyboardMarkup([[InlineKeyboardButton("Xac nhan noi quy", callback_data=f"confirm_rules_{user.id}")]])
            rules_text = (
                f"Chao {user.full_name},\n\n"
                f"NOI QUY CONG DONG {GROUP_NAME}\n\n"
                f"TRACH NHIEM GIAM SAT: Neu phat hien thanh vien khac co hanh vi lam phien, "
                f"spam hoac vi pham quy dinh, vui long gui anh chup man hinh bang chung truc "
                f"tiep cho Admin.\n\n"
                f"TUONG TAC VAN MINH: Khong dang tai noi dung quang cao, lien ket spam hoac "
                f"gui tin nhan rieng lam phien thanh vien khac.\n\n"
                f"QUY TRINH DICH VU: Moi giao dich va nang cap quyen loi VIP deu phai thuc "
                f"hien thong qua Bot tu dong. Neu co loi, vui long lien he Admin kem anh chup man hinh.\n\n"
                f"QUYEN QUAN TRI: Quan tri vien co quyen loai bo thanh vien neu phat hien hanh "
                f"vi lam dung hoac co tinh vi pham.\n\n"
                f"Bang viec xac nhan, ban cam ket da doc va dong y voi cac quy dinh tren.\n\n"
                f"Ban co 60 giay de xac nhan."
            )
            try:
                msg = await context.bot.send_message(chat_id=_gid, text=rules_text, reply_markup=kb)
                users_col = get_users(context)
                await users_col.update_one(
                    {"user_id": user.id},
                    {"$set": {"user_id": user.id, "username": user.username,
                              "full_name": user.full_name, "is_muted": True},
                     "$setOnInsert": {"first_seen": datetime.now(timezone.utc),
                                      "invite_earned": 0, "invite_used": 0,
                                      "kick_count": 0, "rules_confirmed_before": False,
                                      "total_views": 0}},
                    upsert=True
                )
                task = asyncio.create_task(kick_if_not_confirmed(app, _gid, user.id, msg.message_id))
                pending_kicks[user.id] = task
            except Exception as e: logging.error(f"Send rules: {e}")
        return

    # KENH VIP
    if CHANNEL_ID and chat_id == str(CHANNEL_ID):
        vip_col = app.bot_data["vip_col"]
        now     = datetime.now(timezone.utc)
        if old_status in ("left","kicked") and new_status == "member":
            doc       = await vip_col.find_one({"user_id": user.id})
            expire_at = doc.get("pending_expire") if doc else None
            if not expire_at:
                # Bao ton data cu neu co
                old_ea = doc.get("expire_at") if doc else None
                if old_ea and not doc.get("active", False):
                    if old_ea.tzinfo is None: old_ea = old_ea.replace(tzinfo=timezone.utc)
                    base = max(old_ea, now)
                else:
                    base = now
                expire_at = base + relativedelta(months=1)
            await vip_col.update_one(
                {"user_id": user.id},
                {"$set": {"user_id": user.id, "username": user.username,
                          "full_name": user.full_name, "joined_at": now,
                          "expire_at": expire_at, "active": True, "pending": False,
                          "notified_7d": False, "notified_3d": False, "notified_1d": False}},
                upsert=True
            )
            # Xoa nut invite sau khi da vao thanh cong
            invite_msg_id = doc.get("invite_msg_id") if doc else None
            if invite_msg_id:
                try:
                    await context.bot.edit_message_reply_markup(
                        chat_id=user.id, message_id=invite_msg_id, reply_markup=None
                    )
                except Exception: pass

        elif old_status == "member" and new_status == "left":
            # Bao ton data, chi set active=False
            await vip_col.update_one({"user_id": user.id}, {"$set": {"active": False}})

# ==================== JOIN REQUEST HANDLER ====================
async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req     = update.chat_join_request
    user_id = req.from_user.id
    vip_col = context.application.bot_data["vip_col"]
    if str(req.chat.id) != str(CHANNEL_ID): return
    doc = await vip_col.find_one({"user_id": user_id, "pending": True})
    if doc:
        try:
            await context.bot.approve_chat_join_request(chat_id=CHANNEL_ID, user_id=user_id)
            await log_vip_approved(context.application, user_id,
                                   req.from_user.full_name, req.from_user.username)
        except Exception as e: logging.error(f"Approve: {e}")
    else:
        try:
            await context.bot.decline_chat_join_request(chat_id=CHANNEL_ID, user_id=user_id)
            await context.bot.send_message(
                chat_id=user_id,
                text=f"Yeu cau vao kenh VIP bi tu choi.\nVui long thanh toan truoc.\nGo /mua de xem huong dan.",
                protect_content=True
            )
            await log_vip_rejected(context.application, user_id,
                                   req.from_user.full_name, req.from_user.username)
        except Exception: pass

# ==================== CALLBACK HANDLER ====================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    data  = query.data

    # Go ban
    if data.startswith("unban_") and user.id == ADMIN_ID:
        target_id  = int(data.split("_")[1])
        banned_col = get_banned(context)
        result     = await banned_col.delete_one({"user_id": target_id})
        if result.deleted_count:
            try: await context.bot.send_message(chat_id=target_id, text="Quyen truy cap da duoc khoi phuc.", protect_content=True)
            except Exception: pass
            await query.answer("Da go ban!")
            await query.edit_message_text(query.message.text + f"\n\nDa go ban luc {now_str()}")
            await log_unban(context.application, target_id)
        else: await query.answer("Khong tim thay!")
        return

    # Xac nhan noi quy
    if data.startswith("confirm_rules_"):
        target_id = int(data.split("_")[2])
        if user.id != target_id:
            await query.answer("Day khong phai nut danh cho ban!")
            return
        users_col = get_users(context)
        user_doc  = await users_col.find_one({"user_id": target_id})

        # Chi xu ly neu dang bi mute (nguoi moi)
        if not user_doc or not user_doc.get("is_muted", False):
            # Nguoi cu bam nham - bo qua hoan toan, khong log
            await query.answer()
            return

        # Nguoi moi dang bi mute - xu ly
        await users_col.update_one(
            {"user_id": target_id},
            {"$set": {"is_muted": False, "rules_confirmed": True}}
        )
        if target_id in pending_kicks:
            pending_kicks[target_id].cancel()
            del pending_kicks[target_id]
        try:
            await context.bot.restrict_chat_member(
                chat_id=GROUP_ID, user_id=target_id, permissions=UNMUTED
            )
        except Exception: pass
        try: await query.message.delete()
        except Exception: pass
        await query.answer("Xac nhan thanh cong! Chao mung ban.")

        is_new     = not user_doc.get("rules_confirmed_before", False)
        status_txt = "Lan dau vao nhom" if is_new else "Da tung vao truoc do - Tham gia lai"
        await users_col.update_one({"user_id": target_id}, {"$set": {"rules_confirmed_before": True}})
        await log_rules_confirm(context.application, target_id,
                                user_doc.get("full_name",""), user_doc.get("username"), status_txt)
        ref_by = user_doc.get("ref_by")
        if ref_by:
            asyncio.create_task(process_referral_after_24h(context.application, target_id, ref_by))
        return

    # Ban pha 2 - xac nhan thoi gian
    if data.startswith("ban_enter_time_") and user.id == ADMIN_ID:
        admin_id = int(data.split("_")[3])
        if admin_id != ADMIN_ID: await query.answer(); return
        awaiting_ban_time[ADMIN_ID] = True
        force_msg = await context.bot.send_message(
            chat_id=user.id,
            text="Nhap thoi gian cam (vi du: 1h, 2d, 30m):\n(h = gio, d = ngay, m = phut)",
            reply_markup=ForceReply(selective=True)
        )
        await query.answer("Nhap thoi gian cam ben duoi.")
        return

    # Demo chon bo
    if data.startswith("demo_"):
        number = int(data.split("_")[1])
        asyncio.create_task(send_demo(context.application, user.id, number, query))
        return

    await query.answer()

# ==================== /START ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid  = user.id
    app  = context.application
    await save_user(context, user)

    banned_col = get_banned(context)
    try: ban_doc = await db_retry(lambda: banned_col.find_one({"user_id": uid}))
    except: ban_doc = None

    if ban_doc:
        reason   = ban_doc.get("reason","Vi pham quy dinh")
        expire_at = ban_doc.get("expire_at")
        if expire_at:
            es = expire_at.strftime("%d/%m/%Y %H:%M")
            await temp_reply(update, context,
                f"Tai khoan dang bi tam khoa.\nLy do: {reason}\nHet han luc: {es}\n\n"
                f"Neu ban nghi lenh cam do nham lan, lien he: {ADMIN_CONTACT}", delay=300)
        else:
            await temp_reply(update, context,
                f"Quyen truy cap da bi thu hoi.\nLy do: {reason}\n\n"
                f"Neu ban nghi lenh cam do nham lan, lien he: {ADMIN_CONTACT}", delay=300)
        return

    if await check_user(update, context): return

    args = context.args or []

    # Xu ly referral
    if args and args[0].startswith("ref_"):
        try:
            ref_id = int(args[0].replace("ref_",""))
            if ref_id != uid:
                users_col = get_users(context)
                doc = await users_col.find_one({"user_id": uid})
                if not doc or not doc.get("ref_by"):
                    await users_col.update_one({"user_id": uid}, {"$set": {"ref_by": ref_id}}, upsert=True)
        except ValueError: pass
        args = []

    # /start khong co args - tat ca user chi thay chao + /help
    if not args:
        await temp_reply(update, context,
            f"Chao mung ban den voi he thong.\n\nGo /help de xem danh sach lenh va huong dan su dung.",
            delay=120)
        return

    # Deep link xem noi dung
    key        = args[0]
    albums_col = get_albums(context)
    try: album = await db_retry(lambda: albums_col.find_one({"key": key}))
    except:
        await temp_reply(update, context, "He thong dang xu ly. Vui long thu lai sau.", delay=60)
        return

    if not album:
        invalid_attempts[uid] += 1
        count = invalid_attempts[uid]
        if count >= INVALID_WARN: await log_warning(app, user, "Bam link khong hop le nhieu lan", count)
        if count >= INVALID_BAN:
            await do_ban(app, uid, user.full_name, "Co tinh do link")
            await temp_reply(update, context,
                f"Quyen truy cap bi thu hoi tu dong.\n\nNeu ban nghi lenh cam do nham lan, lien he: {ADMIN_CONTACT}",
                delay=300)
            return
        await temp_reply(update, context, "Du lieu khong ton tai hoac phien chia se da het han.", delay=120)
        return

    invalid_attempts[uid] = 0
    vip_ok = await is_vip(context, uid)
    if not vip_ok:
        nonmember_attempts[uid] += 1
        count = nonmember_attempts[uid]
        if count >= NONMEMBER_WARN:
            await log_warning(app, user, "Truy cap trai phep nhieu lan", count)
            nonmember_attempts[uid] = 0
        await temp_reply(update, context,
            f"Noi dung chi danh cho thanh vien VIP.\n\nGo /mua de xem huong dan mua VIP.",
            delay=300)
        return

    nonmember_attempts[uid] = 0
    await send_log(app,
        f"Truy cap noi dung\nID: <code>{uid}</code>\n"
        f"Ten: {sanitize(user.full_name)}\n"
        f"Username: {'@'+user.username if user.username else 'Khong co'}\n"
        f"Album: <code>{key}</code>\nThoi gian: {now_str()}"
    )
    # Luu tong luot xem
    users_col = get_users(context)
    await users_col.update_one({"user_id": uid}, {"$inc": {"total_views": 1}})
    await send_album(context, uid, album)

async def send_album(context, uid, album):
    items    = album.get("items",[])
    if not items: return
    # Tinh dynamic timeout
    total_sec = sum(it.get("duration",0) for it in items if it["type"]=="video")
    delete_after = total_sec + BUFFER_MINUTES * 60

    sent_ids = []
    if len(items) == 1:
        it = items[0]
        try:
            if it["type"] == "video":
                msg = await context.bot.send_video(chat_id=uid, video=it["file_id"], protect_content=True, has_spoiler=True)
            else:
                msg = await context.bot.send_photo(chat_id=uid, photo=it["file_id"], protect_content=True, has_spoiler=True)
            sent_ids.append(msg.message_id)
        except Exception as e: logging.error(f"Send: {e}"); return
    else:
        for i in range(0, len(items), 10):
            batch = items[i:i+10]
            media = [InputMediaVideo(media=it["file_id"], has_spoiler=True) if it["type"]=="video"
                     else InputMediaPhoto(media=it["file_id"], has_spoiler=True) for it in batch]
            try:
                msgs = await context.bot.send_media_group(chat_id=uid, media=media, protect_content=True)
                sent_ids.extend([m.message_id for m in msgs])
                await asyncio.sleep(0.5)
            except Exception as e: logging.error(f"Send group: {e}"); return

    if sent_ids:
        jobs_col = context.application.bot_data["jobs_col"]
        await jobs_col.insert_one({
            "chat_id": uid, "message_ids": sent_ids,
            "expire_at": datetime.now(timezone.utc) + timedelta(seconds=max(delete_after, 600)),
            "done": False
        })

# ==================== DEMO SEND ====================
async def send_demo(app, user_id, number, query=None):
    users_col = app.bot_data["users_col"]
    demos_col = app.bot_data["demos_col"]
    albums_col = app.bot_data["albums_col"]
    user_doc  = await users_col.find_one({"user_id": user_id})
    if not user_doc:
        if query: await query.answer("Khong tim thay thong tin cua ban!")
        return
    earned   = user_doc.get("invite_earned",0)
    used     = user_doc.get("invite_used",0)
    luot_con = earned - used
    if luot_con <= 0:
        if query: await query.answer("Ban khong con luot xem!")
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text="Ban khong con luot xem.\nDung /gioi_thieu de kiem them luot.",
                protect_content=True
            )
        except Exception: pass
        return
    demo = await demos_col.find_one({"number": number})
    if not demo or not demo.get("full_album_key"):
        if query: await query.answer("Bo nay hien khong kha dung!")
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text="Bo nay hien khong kha dung hoac da het han.",
                protect_content=True
            )
        except Exception: pass
        return
    album = await albums_col.find_one({"key": demo["full_album_key"]})
    if not album or not album.get("items"):
        if query: await query.answer("Khong tim thay noi dung!")
        return
    await users_col.update_one({"user_id": user_id}, {"$inc": {"invite_used": 1, "total_views": 1}})
    if query: await query.answer(f"Dang gui bo #{number}...")

    items    = album.get("items",[])
    total_sec = sum(it.get("duration",0) for it in items if it["type"]=="video")
    delete_after = total_sec + BUFFER_MINUTES * 60
    sent_ids = []
    try:
        if len(items) == 1:
            it = items[0]
            if it["type"] == "video":
                msg = await app.bot.send_video(chat_id=user_id, video=it["file_id"], protect_content=True, has_spoiler=True)
            else:
                msg = await app.bot.send_photo(chat_id=user_id, photo=it["file_id"], protect_content=True, has_spoiler=True)
            sent_ids.append(msg.message_id)
        else:
            for i in range(0, len(items), 10):
                batch = items[i:i+10]
                media = [InputMediaVideo(media=it["file_id"], has_spoiler=True) if it["type"]=="video"
                         else InputMediaPhoto(media=it["file_id"], has_spoiler=True) for it in batch]
                msgs = await app.bot.send_media_group(chat_id=user_id, media=media, protect_content=True)
                sent_ids.extend([m.message_id for m in msgs])
                await asyncio.sleep(0.5)
        if sent_ids:
            jobs_col = app.bot_data["jobs_col"]
            await jobs_col.insert_one({
                "chat_id": user_id, "message_ids": sent_ids,
                "expire_at": datetime.now(timezone.utc) + timedelta(seconds=max(delete_after, 600)),
                "done": False
            })
        updated  = await users_col.find_one({"user_id": user_id})
        luot_con = updated.get("invite_earned",0) - updated.get("invite_used",0)
        await app.bot.send_message(
            chat_id=user_id,
            text=f"Da gui bo #{number}.\nLuot con lai: {luot_con}/15",
            protect_content=True
        )
    except Exception as e: logging.error(f"Send demo: {e}")


# ==================== MEMBER COMMANDS ====================
async def cmd_mua(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type != "private":
        await temp_reply(update, context,
            f"Lenh nay chi hoat dong trong chat rieng voi bot.\nBam: @{BOT_USERNAME}", delay=60)
        return
    await save_user(context, user)
    qr_img  = make_qr_img(user.id)
    vietqr  = make_vietqr(user.id)
    await log_mua(context.application, user)
    await temp_reply(update, context,
        f"Goi VIP 1 thang: {VIP_PRICE:,}d\n\n"
        f"Chuyen khoan:\n"
        f"Ngan hang: {BANK_NAME}\n"
        f"So tai khoan: {BANK_ACCOUNT}\n"
        f"So tien: {VIP_PRICE:,}d\n"
        f"Noi dung: SEVQR VIP {user.id}\n\n"
        f"Anh QR: {qr_img}\n\n"
        f"Hoac bam link de quet QR: {vietqr}\n\n"
        f"He thong tu dong cap quyen sau khi nhan thanh toan.\n"
        f"Ho tro: {ADMIN_CONTACT}", delay=600
    )

async def cmd_luot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type != "private":
        await temp_reply(update, context,
            f"Lenh nay chi hoat dong trong chat rieng voi bot.\nBam: @{BOT_USERNAME}", delay=60)
        return
    users_col = get_users(context)
    doc = await users_col.find_one({"user_id": user.id})
    if not doc:
        await temp_reply(update, context,
            "Ban chua co thong tin trong he thong.\nVao nhom va xac nhan noi quy truoc.", delay=120)
        return
    earned   = doc.get("invite_earned",0)
    used     = doc.get("invite_used",0)
    luot_con = earned - used
    await temp_reply(update, context,
        f"Luot xem cua ban:\n\n"
        f"Da kiem: {earned}/15\n"
        f"Da dung: {used}\n"
        f"Con lai: {luot_con}\n\n"
        f"Dung /gioi_thieu de kiem them luot.\n"
        f"Dung /xem de xem noi dung demo.", delay=120
    )

async def cmd_gioi_thieu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type != "private":
        await temp_reply(update, context,
            f"Lenh nay chi hoat dong trong chat rieng voi bot.\nBam: @{BOT_USERNAME}", delay=60)
        return
    ref_url = make_ref_link(user.id)
    await temp_reply(update, context,
        f"Link gioi thieu cua ban:\n{ref_url}\n\n"
        f"Chia se link nay de kiem luot xem.\n"
        f"Moi nguoi vao nhom va o lai 24 gio = +1 luot.\n\n"
        f"Toi da 15 luot suot doi. Du moi bao nhieu nguoi\n"
        f"cung chi kiem duoc toi da 15 luot.\n"
        f"Dung /luot de xem so luot hien tai.", delay=300
    )

async def cmd_xem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type != "private":
        await temp_reply(update, context,
            f"Lenh nay chi hoat dong trong chat rieng voi bot.\nBam: @{BOT_USERNAME}", delay=60)
        return
    args = context.args or []
    demos_col = get_demos(context)

    # Neu co so bo
    number = None
    if args:
        raw = args[0].lstrip("#")
        if raw.isdigit():
            number = int(raw)

    if number is not None:
        asyncio.create_task(send_demo(context.application, user.id, number))
        return

    # Hien danh sach bo co san
    users_col = get_users(context)
    doc = await users_col.find_one({"user_id": user.id})
    if not doc:
        await temp_reply(update, context, "Ban chua co thong tin trong he thong.", delay=120)
        return
    earned   = doc.get("invite_earned",0)
    used     = doc.get("invite_used",0)
    luot_con = earned - used
    if luot_con <= 0:
        await temp_reply(update, context,
            "Ban khong con luot xem.\nDung /gioi_thieu de kiem them luot.", delay=120)
        return
    demos = await demos_col.find(
        {"full_album_key": {"$exists": True, "$ne": None}},
        {"number": 1}
    ).sort("number",1).to_list(length=20)
    if not demos:
        await temp_reply(update, context, "Hien chua co bo demo nao.", delay=120)
        return
    keyboard = []
    for d in demos:
        keyboard.append([InlineKeyboardButton(f"Bo #{d['number']}", callback_data=f"demo_{d['number']}")])
    await temp_reply(update, context,
        f"Luot con lai: {luot_con}/15\n\nChon bo muon xem hoac goc /xem [so]:",
        delay=300, reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def cmd_help_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID: return
    await temp_reply(update, context,
        "Huong dan su dung:\n\n"
        "/mua — Xem thong tin goi VIP va tao ma thanh toan\n"
        "/luot — Xem so luot xem con lai\n"
        "/gioi_thieu — Lay link gioi thieu de kiem luot\n"
        "/xem — Xem noi dung demo (goc /xem [so] de xem nhanh)\n"
        "/help — Huong dan su dung\n\n"
        f"Ho tro: {ADMIN_CONTACT}\n\n"
        "Luu y: Tat ca lenh chi hoat dong trong chat rieng voi bot.", delay=120
    )

# ==================== ADMIN: MEDIA HANDLER ====================
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return

    # Ban pha 2: ForceReply nhan thoi gian
    if (awaiting_ban_time.get(ADMIN_ID)
            and update.effective_chat.type == "private"
            and update.message.reply_to_message
            and not update.message.video
            and not update.message.photo):
        return  # xu ly o handler rieng

    # Forward lay ID
    if update.message.forward_from:
        fwd   = update.message.forward_from
        uname = f"@{fwd.username}" if fwd.username else "Khong co"
        await update.message.reply_text(
            f"Thong tin:\nID: <code>{fwd.id}</code>\n"
            f"Ten: {fwd.full_name}\nUsername: {uname}", parse_mode="HTML")
        return

    # Nhom thuong: Admin gui media kem #N -> luu demo
    try: _gid = int(GROUP_ID) if GROUP_ID else None
    except: _gid = None
    if _gid and update.effective_chat.id == _gid:
        caption = update.message.caption or update.message.text or ""
        m = re.search(r'#(\d+)', caption)
        if m:
            number    = int(m.group(1))
            demos_col = get_demos(context)
            if update.message.video:
                file_id   = update.message.video.file_id
                file_type = "video"
                duration  = update.message.video.duration or 0
            elif update.message.photo:
                file_id   = update.message.photo[-1].file_id
                file_type = "photo"
                duration  = 0
            else: return
            await demos_col.update_one(
                {"number": number},
                {"$setOnInsert": {"number": number, "full_album_key": None,
                                  "created_at": datetime.now(timezone.utc)}},
                upsert=True
            )
            await update.message.reply_text(
                f"Da ghi nhan bo #{number}.\n"
                f"Dung /setlink {number} [KEY] de lien ket album full."
            )
        return

    # Chat rieng / DM voi admin: luu vao album hien tai
    if update.effective_chat.type != "private": return
    key = context.user_data.get("current_key")
    if not key: return
    albums_col = get_albums(context)
    if update.message.video:
        file_id   = update.message.video.file_id
        file_type = "video"
        duration  = update.message.video.duration or 0
    elif update.message.photo:
        file_id   = update.message.photo[-1].file_id
        file_type = "photo"
        duration  = 0
    else: return
    await albums_col.update_one(
        {"key": key},
        {"$push": {"items": {"type": file_type, "file_id": file_id, "duration": duration}}}
    )
    album = await albums_col.find_one({"key": key}, {"items": 1})
    count = len(album.get("items",[])) if album else 0
    await update.message.reply_text(f"Da nhan {file_type} — album co {count} file.\nGo /done khi xong.")

# Handler nhan ForceReply thoi gian ban
async def handle_ban_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private": return
    if not awaiting_ban_time.get(ADMIN_ID): return
    if not update.message.reply_to_message: return

    text = (update.message.text or "").strip()
    dur  = parse_duration(text)
    if not dur:
        await update.message.reply_text("Dinh dang sai. Nhap lai: 1h, 2d, 30m")
        return

    ban_info = pending_bans.get(ADMIN_ID)
    if not ban_info:
        awaiting_ban_time.pop(ADMIN_ID, None)
        await update.message.reply_text("Het phien. Vui long thuc hien lai lenh /ban.")
        return

    target_id  = ban_info["target_id"]
    reason     = ban_info["reason"]
    target_name = ban_info.get("target_name","Khong ro")
    awaiting_ban_time.pop(ADMIN_ID, None)
    pending_bans.pop(ADMIN_ID, None)

    try: _gid = int(GROUP_ID) if GROUP_ID else None
    except: _gid = None

    if _gid:
        try: await context.bot.ban_chat_member(chat_id=_gid, user_id=target_id)
        except Exception as e: await update.message.reply_text(f"Khong the kick khoi nhom: {e}"); return

    await do_ban(context.application, target_id, target_name,
                 reason, ban_type="Thu cong", duration=dur)

    ea          = datetime.now(timezone(timedelta(hours=7))) + dur
    expire_text = f"Het han luc: {ea.strftime('%d/%m/%Y %H:%M')}"
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(f"Ban da bi cam.\nLy do: {reason}\n{expire_text}\n\n"
                  f"Neu ban nghi lenh cam do nham lan, lien he: {ADMIN_CONTACT}"),
            protect_content=True
        )
    except Exception: pass

    d = int(dur.total_seconds()//86400); h = int(dur.total_seconds()//3600)
    expire_info = f"({d} ngay)" if d >= 1 else f"({h} gio)" if h >= 1 else f"({int(dur.total_seconds()//60)} phut)"
    await update.message.reply_text(f"Da cam {target_name} {expire_info}.\nLy do: {reason}")

# ==================== ADMIN COMMANDS ====================
ADMIN_ONLY_CMDS = {"new","new_album","done","list","detail","check","del","del_album","clean",
                   "setlink","dellink","ban","who","extend","viplist","status","addluot","mock_pay"}

async def admin_cmd_in_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin goc lenh he thong trong nhom thuong -> im lang ngoai nhom, DM nhac admin."""
    if update.effective_user.id != ADMIN_ID: return
    cmd = (update.message.text or "").split()[0].lstrip("/").split("@")[0].lower()
    if cmd in ADMIN_ONLY_CMDS:
        try: await update.message.delete()
        except Exception: pass
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"Lenh /{cmd} chi hoat dong trong chat rieng voi bot.\nVui long chat rieng voi bot de su dung."
            )
        except Exception: pass

async def new_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if context.user_data.get("current_key"):
        await update.message.reply_text("Dang co album chua hoan thanh.\nGo /done de lay link truoc."); return
    key = make_key()
    albums_col = get_albums(context)
    await albums_col.insert_one({"key": key, "items": [], "created_at": datetime.now(timezone.utc)})
    context.user_data["current_key"] = key
    await update.message.reply_text(
        f"Album moi da tao.\nMa: <code>{key}</code>\n\nForward anh hoac video vao day.\nGo /done khi xong.",
        parse_mode="HTML")

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    key = context.user_data.get("current_key")
    if not key:
        await update.message.reply_text("Khong co album nao dang tao."); return
    albums_col = get_albums(context)
    album      = await albums_col.find_one({"key": key})
    if not album or not album.get("items"):
        await update.message.reply_text("Album chua co file nao.\nHay forward anh hoac video vao truoc."); return
    context.user_data.pop("current_key", None)
    count = len(album.get("items",[]))
    await update.message.reply_text(f"Hoan tat. Album co {count} file.\nKey: <code>{key}</code>\nLink chia se:", parse_mode="HTML")
    await update.message.reply_text(make_link(key))

async def list_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    albums_col = get_albums(context)
    try:
        albums = await db_retry(lambda: albums_col.find({},{"key":1,"items":1}).sort("created_at",-1).to_list(length=50))
    except: await update.message.reply_text("Loi ket noi."); return
    if not albums: await update.message.reply_text("Chua co album nao."); return
    text = f"Danh sach album ({len(albums)}):\n\n"
    for i, a in enumerate(albums, 1):
        text += f"{i}. <code>{a['key']}</code> — {len(a.get('items',[]))} file\n"
    await update.message.reply_text(text, parse_mode="HTML")

async def detail_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    albums_col = get_albums(context)
    try:
        albums = await db_retry(lambda: albums_col.find({},{"key":1,"items":1}).sort("created_at",-1).to_list(length=50))
    except: await update.message.reply_text("Loi ket noi."); return
    if not albums: await update.message.reply_text("Chua co album nao."); return
    for i, a in enumerate(albums, 1):
        await update.message.reply_text(f"{i}\nSo file: {len(a.get('items',[]))}\nLink:")
        await update.message.reply_text(make_link(a["key"]))
        await asyncio.sleep(0.3)

async def check_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args: await update.message.reply_text("Dung: /check <ma>"); return
    key        = context.args[0]
    albums_col = get_albums(context)
    album      = await albums_col.find_one({"key": key})
    if not album: await update.message.reply_text(f"Khong tim thay album {key}."); return
    items  = album.get("items",[])
    videos = sum(1 for i in items if i["type"]=="video")
    photos = sum(1 for i in items if i["type"]=="photo")
    total_dur = sum(it.get("duration",0) for it in items if it["type"]=="video")
    await update.message.reply_text(
        f"Album: <code>{key}</code>\n"
        f"Tong: {len(items)} | Video: {videos} | Anh: {photos}\n"
        f"Tong thoi luong video: {total_dur//60} phut\nLink:", parse_mode="HTML")
    await update.message.reply_text(make_link(key))

async def delete_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args: await update.message.reply_text("Dung: /del <ma>"); return
    key = context.args[0]
    r   = await get_albums(context).delete_one({"key": key})
    await get_jobs(context).delete_many({"album_key": key})
    await update.message.reply_text(f"Da xoa album {key}." if r.deleted_count else f"Khong tim thay {key}.")

async def clean_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    r = await get_albums(context).delete_many({"$or":[{"items":[]},{"created_at":{"$lt":cutoff}}]})
    await update.message.reply_text(f"Da xoa {r.deleted_count} album trong hoac cu hon 7 ngay.")

async def cmd_setlink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Dung: /setlink <so> <key_album>\nVi du: /setlink 1 abc123xyz"); return
    try: number = int(context.args[0])
    except: await update.message.reply_text("So bo phai la so nguyen."); return
    key        = context.args[1]
    albums_col = get_albums(context)
    album      = await albums_col.find_one({"key": key})
    if not album:
        await update.message.reply_text(f"Khong tim thay album voi key: {key}\nKiem tra lai bang /list."); return
    demos_col = get_demos(context)
    await demos_col.update_one(
        {"number": number},
        {"$set": {"number": number, "full_album_key": key, "updated_at": datetime.now(timezone.utc)},
         "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
        upsert=True
    )
    items = album.get("items",[])
    await update.message.reply_text(
        f"Da lien ket bo #{number} voi album {key}.\n"
        f"So file: {len(items)}\n"
        f"Thanh vien co the dung /xem {number} de xem."
    )

async def cmd_dellink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args:
        await update.message.reply_text("Dung: /dellink <so>\nVi du: /dellink 1"); return
    try: number = int(context.args[0])
    except: await update.message.reply_text("So bo phai la so nguyen."); return
    demos_col = get_demos(context)
    r = await demos_col.update_one({"number": number}, {"$set": {"full_album_key": None}})
    if r.matched_count:
        await update.message.reply_text(
            f"Da xoa lien ket album full cua bo #{number}.\n"
            f"Thanh vien goc /xem {number} se thay thong bao 'Khong kha dung'.")
    else:
        await update.message.reply_text(f"Khong tim thay bo #{number}.")

async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    # Neu goc trong nhom thuong -> im lang
    if update.effective_chat.type in ("group","supergroup"):
        try: await update.message.delete()
        except Exception: pass
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text="Lenh /ban chi hoat dong trong chat rieng voi bot (DM).\n"
                     "Vui long mo chat rieng voi bot de su dung."
            )
        except Exception: pass
        return

    args = context.args or []
    if not args and not update.message.reply_to_message:
        await update.message.reply_text(
            "Cach dung /ban trong DM:\n\n"
            "1. /ban [ID] [Ly do]\n"
            "2. /ban @tentaikhoan [Ly do]\n\n"
            "Vi du: /ban 123456789 quay-roi\n"
            "Vi du: /ban @abc spam\n\n"
            "Bot se hoi them thoi gian cam sau."
        ); return

    target_id = None; target_name = "Khong ro"; reason = "Vi pham quy dinh"

    if update.message.reply_to_message:
        u = update.message.reply_to_message.from_user
        target_id, target_name = u.id, u.full_name
        if args: reason = " ".join(args)
    elif args:
        first = args[0]
        if first.startswith("@"):
            try:
                chat = await context.bot.get_chat(first)
                target_id, target_name = chat.id, chat.full_name
            except Exception: await update.message.reply_text(f"Khong tim thay: {first}"); return
        else:
            try: target_id = int(first)
            except: await update.message.reply_text("ID khong hop le."); return
            try:
                chat = await context.bot.get_chat(target_id)
                target_name = chat.full_name
            except Exception: pass
        if len(args) > 1: reason = " ".join(args[1:])

    if not target_id or target_id == ADMIN_ID:
        await update.message.reply_text("Khong the ban."); return

    reason = LY_DO.get(reason, reason)
    banned_col = get_banned(context)
    existing   = await banned_col.find_one({"user_id": target_id})
    if existing:
        await update.message.reply_text(
            f"Tai khoan {target_name} da bi cam truoc do.\n"
            f"Ly do cu: {existing.get('reason','Khong ro')}\n"
            f"Dung /unban truoc neu muon cam lai."
        ); return

    pending_bans[ADMIN_ID] = {"target_id": target_id, "target_name": target_name, "reason": reason}
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Nhap thoi gian", callback_data=f"ban_enter_time_{ADMIN_ID}")]])
    await update.message.reply_text(
        f"Chuan bi cam:\nID: {target_id}\nTen: {target_name}\nLy do: {reason}\n\nBam nut de nhap thoi gian cam:",
        reply_markup=kb
    )

async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    target_id = None; target_name = "Khong ro"
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
            except Exception: await update.message.reply_text("Khong tim thay username."); return
        else:
            try: target_id = int(first)
            except: await update.message.reply_text("ID khong hop le."); return
            try:
                chat = await context.bot.get_chat(target_id)
                target_name = chat.full_name
            except Exception: pass
    else:
        await update.message.reply_text(
            "Cach dung /unban:\n1. Reply tin nhan + /unban\n2. /unban @user\n3. /unban ID"
        ); return
    banned_col = get_banned(context)
    r = await banned_col.delete_one({"user_id": target_id})
    if r.deleted_count:
        try: _gid = int(GROUP_ID) if GROUP_ID else None
        except: _gid = None
        if _gid:
            try: await context.bot.unban_chat_member(chat_id=_gid, user_id=target_id)
            except Exception: pass
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="Quyen truy cap cua ban da duoc khoi phuc.",
                protect_content=True
            )
        except Exception: pass
        await update.message.reply_text(f"Da go cam {target_name}.")
        await log_unban(context.application, target_id)
    else:
        await update.message.reply_text("Khong tim thay trong danh sach cam.")

async def who_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    target_id = None
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    elif context.args:
        try: target_id = int(context.args[0])
        except: await update.message.reply_text("ID khong hop le."); return
    else:
        await update.message.reply_text("Dung: /who <ID> hoac reply tin nhan"); return
    ban_doc  = await get_banned(context).find_one({"user_id": target_id})
    vip_doc  = await get_vip(context).find_one({"user_id": target_id})
    user_doc = await get_users(context).find_one({"user_id": target_id})
    try:
        chat     = await context.bot.get_chat(target_id)
        name     = chat.full_name
        username = f"@{chat.username}" if chat.username else "Khong co"
    except Exception:
        name = user_doc.get("full_name","Khong ro") if user_doc else "Khong ro"
        username = f"@{user_doc.get('username')}" if user_doc and user_doc.get("username") else "Khong co"
    text = f"Thong tin:\nID: <code>{target_id}</code>\nTen: {name}\nUsername: {username}\n\n"
    if ban_doc:
        es    = ban_doc['expire_at'].strftime('%d/%m/%Y %H:%M') if ban_doc.get('expire_at') else "Vinh vien"
        text += f"Trang thai ban: Dang bi cam\nLy do: {ban_doc.get('reason','')}\nHet han: {es}\n\n"
    else:
        text += "Trang thai ban: Binh thuong\n\n"
    if vip_doc and vip_doc.get("active"):
        ea    = vip_doc.get("expire_at")
        d     = days_left(ea) if ea else 0
        text += f"VIP: Dang hoat dong\nHet han: {ea.strftime('%d/%m/%Y') if ea else '?'} ({d} ngay con lai)\n\n"
    else:
        text += "VIP: Chua co hoac da het han\n\n"
    if user_doc:
        earned      = user_doc.get("invite_earned",0)
        used        = user_doc.get("invite_used",0)
        total_views = user_doc.get("total_views",0)
        text += f"Luot: {earned-used} con lai ({earned} kiem / {used} da dung)\nTong so luot xem: {total_views}"
    await update.message.reply_text(text, parse_mode="HTML")

async def extend_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args: await update.message.reply_text("Dung: /extend <ID>"); return
    try: target_id = int(context.args[0])
    except: await update.message.reply_text("ID khong hop le."); return
    vip_col   = get_vip(context)
    users_col = get_users(context)
    now       = datetime.now(timezone.utc)
    doc       = await vip_col.find_one({"user_id": target_id})
    if doc and doc.get("expire_at") and doc.get("active"):
        ea = doc["expire_at"]
        if ea.tzinfo is None: ea = ea.replace(tzinfo=timezone.utc)
        base = max(ea, now)
    else:
        base = now
    new_expire = base + relativedelta(months=1)
    d          = days_left(new_expire)
    await vip_col.update_one(
        {"user_id": target_id},
        {"$set": {"expire_at": new_expire, "active": True,
                  "notified_7d": False, "notified_3d": False, "notified_1d": False}},
        upsert=True
    )
    user_doc  = await users_col.find_one({"user_id": target_id})
    user_name = user_doc.get("full_name","Khong ro") if user_doc else "Khong ro"
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(f"Goi VIP da duoc gia han thanh cong.\n\n"
                  f"Han moi: {new_expire.strftime('%d/%m/%Y')}\n"
                  f"So ngay con lai: {d} ngay"),
            protect_content=True
        )
    except Exception: pass
    await update.message.reply_text(
        f"Da gia han VIP cho <code>{target_id}</code> ({user_name}).\n"
        f"Han moi: {new_expire.strftime('%d/%m/%Y')} ({d} ngay con lai)",
        parse_mode="HTML"
    )
    await log_extend(context.application, target_id, 30, new_expire)

async def vip_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    vip_col = get_vip(context)
    members = await vip_col.find({"active":True},{"user_id":1,"full_name":1,"expire_at":1}).sort("expire_at",1).to_list(50)
    if not members: await update.message.reply_text("Chua co member VIP nao."); return
    text = f"VIP dang hoat dong ({len(members)}):\n\n"
    for i, m in enumerate(members, 1):
        ea   = m.get("expire_at")
        d    = days_left(ea) if ea else 0
        text += f"{i}. <code>{m['user_id']}</code> — {sanitize(m.get('full_name',''))} — het {ea.strftime('%d/%m/%Y') if ea else '?'} ({d} ngay)\n"
    await update.message.reply_text(text, parse_mode="HTML")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        ta = await get_albums(context).count_documents({})
        tb = await get_banned(context).count_documents({})
        tt = await get_banned(context).count_documents({"expire_at":{"$ne":None}})
        tj = await get_jobs(context).count_documents({"done":False})
        tv = await get_vip(context).count_documents({"active":True})
        tu = await get_users(context).count_documents({})
        td = await get_demos(context).count_documents({})
        tl = await get_demos(context).count_documents({"full_album_key":{"$ne":None}})
        await update.message.reply_text(
            f"Trang thai he thong:\n\n"
            f"Album: {ta}\nDemo: {td} bo (da lien ket: {tl})\n"
            f"User: {tu}\nVIP: {tv}\n"
            f"Cam: {tb} (tam: {tt})\nJob cho xoa: {tj}\n"
            f"Database: Ket noi on dinh"
        )
    except Exception: await update.message.reply_text("Loi ket noi.")

async def cmd_add_luot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Dung: /addluot <ID> <so luot>"); return
    try: target_id = int(context.args[0]); so_luot = int(context.args[1])
    except: await update.message.reply_text("ID va so luot phai la so."); return
    users_col = get_users(context)
    await users_col.update_one({"user_id": target_id}, {"$inc": {"invite_earned": so_luot}}, upsert=True)
    doc    = await users_col.find_one({"user_id": target_id})
    earned = doc.get("invite_earned",0) if doc else so_luot
    used   = doc.get("invite_used",0) if doc else 0
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=f"Admin da them {so_luot} luot xem cho ban.\nLuot con lai: {earned-used}/15",
            protect_content=True
        )
    except Exception: pass
    await update.message.reply_text(
        f"Da them {so_luot} luot cho ID {target_id}.\nTong: {earned} | Da dung: {used} | Con lai: {earned-used}"
    )

async def cmd_mock_pay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "Dung: /mock_pay <ID> <so_tien> [noi_dung]\n"
            "Vi du: /mock_pay 123456789 119000\n"
            "Vi du: /mock_pay 123456789 50000 (chuyen thieu)"
        ); return
    try:
        target_id = int(context.args[0])
        amount    = int(context.args[1])
    except: await update.message.reply_text("ID va so tien phai la so."); return
    content = context.args[2] if len(context.args) > 2 else f"SEVQR VIP {target_id}"
    if "SEVQR" not in content.upper(): content = f"SEVQR VIP {target_id}"
    await update.message.reply_text(f"Dang xu ly thanh toan gia lap: {amount:,}d cho ID {target_id}...")
    await process_payment(context.application, target_id, amount, ref="MOCK", content=content)
    await update.message.reply_text("Xu ly xong. Kiem tra log va DM cua user de xac nhan.")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await update.message.reply_text(
        "Lenh danh cho Admin (chi hoat dong trong DM voi bot):\n\n"
        "— NOI DUNG —\n"
        "/new — Tao album moi\n"
        "/done — Lay link chia se\n"
        "/list — Danh sach album\n"
        "/detail — Album kem link\n"
        "/check <ma> — Thong tin album\n"
        "/del <ma> — Xoa album\n"
        "/clean — Xoa album cu hon 7 ngay\n\n"
        "— DEMO SYSTEM —\n"
        "/setlink <so> <key> — Lien ket bo demo voi album full\n"
        "  Vi du: /setlink 1 abc123xyz\n"
        "/dellink <so> — Xoa lien ket album full cua bo\n"
        "  Vi du: /dellink 1\n\n"
        "— THANH VIEN —\n"
        "/ban [ID/@user] [Ly do] — Cam 2 pha (hoi them thoi gian)\n"
        "  Thoi gian: 1h / 2d / 30m (h=gio, d=ngay, m=phut)\n"
        "  Ly do goi y: quay-roi / chia-se / spam / gia-mao\n"
        "  het-han / ban-lai / vi-pham / abuse / da-nghi\n"
        "  nhieu-tk / hoan-tien\n"
        "/unban [ID/@user] — Go cam\n"
        "/who <ID> — Xem thong tin tai khoan\n"
        "/addluot <ID> <so> — Them luot xem cho thanh vien\n\n"
        "— VIP —\n"
        "/viplist — Danh sach VIP dang hoat dong\n"
        "/extend <ID> — Gia han VIP them 1 thang\n\n"
        "— TEST & HE THONG —\n"
        "/mock_pay <ID> <so_tien> — Gia lap thanh toan de test\n"
        "/status — Tong quan trang thai\n"
        "/help — Danh sach lenh nay"
    )

async def no_permission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in ("group","supergroup"):
        await temp_reply(update, context,
            f"Lenh nay chi hoat dong trong chat rieng voi bot.\nBam: @{BOT_USERNAME}", delay=60)
    else:
        await temp_reply(update, context, "Ban khong co quyen su dung lenh nay.", delay=60)

# ==================== SETUP DB ====================
async def setup_db(app):
    client = AsyncIOMotorClient(
        MONGO_URI, maxPoolSize=10, minPoolSize=0,
        serverSelectionTimeoutMS=10000, connectTimeoutMS=10000,
        socketTimeoutMS=20000, retryWrites=True, tls=True,
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
    for k, v in cols.items(): app.bot_data[k] = v
    app.bot_data["mongo_client"] = client
    await db["albums"].create_index("key", unique=True)
    await db["jobs"].create_index([("expire_at",1),("done",1)])
    await db["banned"].create_index("user_id", unique=True)
    await db["banned"].create_index("expire_at")
    await db["users"].create_index("user_id", unique=True)
    await db["vip_members"].create_index("user_id", unique=True)
    await db["vip_members"].create_index([("expire_at",1),("active",1)])
    await db["payments"].create_index("user_id", unique=True)
    await db["demos"].create_index("number", unique=True)
    logging.info("DB connected!")
    return client

# ==================== MAIN ====================
async def main():
    check_env()
    app          = Application.builder().token(TOKEN).build()
    admin_filter = filters.User(user_id=ADMIN_ID)

    app.add_handler(CommandHandler("start", start))

    # Admin commands - chi trong DM
    app.add_handler(CommandHandler(["new","new_album"], new_album,       filters=admin_filter))
    app.add_handler(CommandHandler("done",             done,             filters=admin_filter))
    app.add_handler(CommandHandler("list",             list_albums,      filters=admin_filter))
    app.add_handler(CommandHandler("detail",           detail_albums,    filters=admin_filter))
    app.add_handler(CommandHandler("check",            check_album,      filters=admin_filter))
    app.add_handler(CommandHandler(["del","del_album"],delete_album,     filters=admin_filter))
    app.add_handler(CommandHandler("clean",            clean_albums,     filters=admin_filter))
    app.add_handler(CommandHandler("setlink",          cmd_setlink,      filters=admin_filter))
    app.add_handler(CommandHandler("dellink",          cmd_dellink,      filters=admin_filter))
    app.add_handler(CommandHandler("ban",              ban_user,         filters=admin_filter))
    app.add_handler(CommandHandler("unban",            unban_user,       filters=admin_filter))
    app.add_handler(CommandHandler("who",              who_user,         filters=admin_filter))
    app.add_handler(CommandHandler("extend",           extend_vip,       filters=admin_filter))
    app.add_handler(CommandHandler("viplist",          vip_list,         filters=admin_filter))
    app.add_handler(CommandHandler("status",           status_cmd,       filters=admin_filter))
    app.add_handler(CommandHandler("help",             help_cmd,         filters=admin_filter))
    app.add_handler(CommandHandler("addluot",          cmd_add_luot,     filters=admin_filter))
    app.add_handler(CommandHandler("mock_pay",         cmd_mock_pay,     filters=admin_filter))

    # Member commands
    app.add_handler(CommandHandler("mua",        cmd_mua))
    app.add_handler(CommandHandler("luot",       cmd_luot))
    app.add_handler(CommandHandler("gioi_thieu", cmd_gioi_thieu))
    app.add_handler(CommandHandler("xem",        cmd_xem))
    app.add_handler(CommandHandler("help",       cmd_help_user, filters=~admin_filter))

    # Media handler (admin - ca DM lan nhom thuong)
    app.add_handler(MessageHandler(
        (filters.VIDEO | filters.PHOTO | filters.FORWARDED) & admin_filter,
        handle_media
    ))

    # ForceReply handler - ban pha 2
    app.add_handler(MessageHandler(
        filters.REPLY & filters.TEXT & admin_filter & filters.ChatType.PRIVATE,
        handle_ban_time
    ))

    # Callback
    app.add_handler(CallbackQueryHandler(callback_handler))

    # ChatMember
    app.add_handler(ChatMemberHandler(chat_member_updated, ChatMemberHandler.CHAT_MEMBER))

    # ChatJoinRequest
    app.add_handler(ChatJoinRequestHandler(join_request_handler))

    # Non-admin lenh
    app.add_handler(MessageHandler(filters.COMMAND & ~admin_filter, no_permission))

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
            allowed_updates=["message","chat_member","callback_query","chat_join_request"]
        )
        await asyncio.Event().wait()

if __name__ == "__main__":
    while True:
        try: asyncio.run(main())
        except Exception as e:
            logging.error(f"Bot crashed: {e} - restart sau 10s...")
            time.sleep(10)
