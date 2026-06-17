import os, asyncio, logging, time, secrets, string, re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
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
    "gia-mao":   "Tài khoản gia mao",
    "het-han":   "Het han dang ky",
    "ban-lai":   "Ban lai quyen truy cap",
    "vi-pham":   "Vi pham quy dinh",
    "abuse":     "Hanh vi pha hoai",
    "da-nghi":   "Tài khoản dang ngo",
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
def make_vietqr(uid):
    # Deeplink mo app VietinBank tren dien thoai
    return (f"https://dl.vietqr.io/pay?app=vtb"
            f"&ba={BANK_ACCOUNT}"
            f"&bn={BANK_NAME}"
            f"&am={VIP_PRICE}"
            f"&tn=SEVQR%20VIP%20{uid}")

def make_qr_img(uid):
    # Anh QR de user luu va quet bang bat ky app ngan hang nao
    return (f"https://img.vietqr.io/image/{BANK_BIN}-{BANK_ACCOUNT}-compact.png"
            f"?amount={VIP_PRICE}&addInfo=SEVQR%20VIP%20{uid}")

def now_str():
    return datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M")
def sanitize(text, max_len=500):
    text = str(text).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
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
        f"Khởi tạo hóa đơn mua VIP\n"
        f"ID: <code>{user.id}</code>\n"
        f"Ten: {sanitize(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Gói: 1 Tháng\n"
        f"Số tiền: {VIP_PRICE:,}d\n"
        f"Nội dung chuyển khoản: SEVQR VIP {user.id}\n"
        f"Thời gian: {now_str()}"
    )

async def log_payment_ok(app, user_id, name, username, amount, expire_at, pay_type):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Thanh toán VIP thành công\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Số tiền nhận: {amount:,}d\n"
        f"Hạn VIP mới: {expire_at.strftime('%d/%m/%Y')}\n"
        f"Loại: {pay_type}\n"
        f"Thời gian: {now_str()}"
    )

async def log_extend(app, target_id, days, expire_at):
    await send_log(app,
        f"Gia hạn VIP thủ công\n"
        f"ID người nhận: <code>{target_id}</code>\n"
        f"Số ngày cộng thêm: {days} ngay\n"
        f"Hạn VIP mới: {expire_at.strftime('%d/%m/%Y')}\n"
        f"Thời gian: {now_str()}"
    )

async def log_vip_approved(app, user_id, name, username):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Tự động duyệt vào kênh VIP\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Trạng thái: Hợp lệ\n"
        f"Thời gian: {now_str()}"
    )

async def log_vip_rejected(app, user_id, name, username):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Từ chối vào kênh VIP\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Trạng thái: Không hợp lệ\n"
        f"Thời gian: {now_str()}"
    )

async def log_rules_confirm(app, user_id, name, username, status):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Xác nhận nội quy\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Trạng thái: {status}\n"
        f"Thời gian: {now_str()}"
    )

async def log_kick(app, user_id, name, username, kick_count):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Tự động đá người dùng\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Lý do: Quá 60 giây không xác nhận nội quy\n"
        f"Số lần vi phạm: {kick_count}\n"
        f"Thời gian: {now_str()}"
    )

async def log_auto_ban(app, user_id, name, username, reason):
    uname = f"@{username}" if username else "Khong co"
    await send_log(app,
        f"Tự động ban vĩnh viễn\n"
        f"ID: <code>{user_id}</code>\n"
        f"Ten: {sanitize(name)}\n"
        f"Username: {uname}\n"
        f"Lý do: {reason}\n"
        f"Thời gian: {now_str()}"
    )

async def log_ban_action(app, target_id, name, reason, ban_type, show_btn=False):
    kb = None
    if show_btn:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Gỡ ban ngay", callback_data=f"unban_{target_id}")]])
    await send_log(app,
        f"Ban người dùng\n"
        f"ID: <code>{target_id}</code>\n"
        f"Ten: {sanitize(str(name))}\n"
        f"Lý do: {sanitize(reason)}\n"
        f"Loại: {ban_type}\n"
        f"Thời gian: {now_str()}", markup=kb
    )

async def log_unban(app, target_id):
    await send_log(app, f"Hủy ban\nID: <code>{target_id}</code>\nThời gian: {now_str()}")

async def log_payment_partial(app, user_id, amount, total):
    await send_log(app,
        f"Thanh toán chưa đủ\n"
        f"ID: <code>{user_id}</code>\n"
        f"Lần này: {amount:,}d\n"
        f"Tổng đã trả: {total:,}d\n"
        f"Còn thiếu: {VIP_PRICE - total:,}d\n"
        f"Thời gian: {now_str()}"
    )

async def log_warning(app, user, behavior, count):
    uname = f"@{user.username}" if user.username else "Khong co"
    await send_log(app,
        f"Cảnh báo hành vi bất thường\n"
        f"ID: <code>{user.id}</code>\n"
        f"Ten: {sanitize(user.full_name)}\n"
        f"Username: {uname}\n"
        f"Hành vi: {sanitize(behavior)}\n"
        f"Số lần: {count}\n"
        f"Thời gian: {now_str()}"
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
        pay_type = "Gia hạn nối tiếp (Đã cộng dồn)"
    else:
        base = now
        pay_type = "Mua mới (Kích hoạt lần đầu)"
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
                  "active": True,
                  "invite_url": invite_url, "invite_msg_id": None,
                  "pending_expire": expire_at,
                  "notified_7d": False, "notified_3d": False, "notified_1d": False}},
        upsert=True
    )

    days = days_left(expire_at)
    kb   = InlineKeyboardMarkup([[InlineKeyboardButton(
        "Bấm vào đây để vào kênh VIP", url=invite_url
    )]])
    try:
        msg = await app.bot.send_message(
            chat_id=user_id,
            text=(f"Thanh toán thành công\n\n"
                  f"Gói VIP có hiệu lực đến ngày {expire_at.strftime('%d/%m/%Y')} ({days} ngay).\n\n"
                  f"Bấm nút bên dưới để vào kênh VIP.\n"
                  f"Link chỉ dùng 1 lần, hết hạn sau 48 giờ."),
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
            f"Quyền truy cập bị thu hồi vĩnh viễn.\nLý do: Spam 60 lan trong 1 phut.\n"
            f"Nếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}", delay=300)
        return True
    if count >= SPAM_TEMP:
        warn_count[uid] += 1
        dur = timedelta(hours=24) if warn_count[uid] >= 2 else timedelta(hours=1)
        await do_ban(app, uid, user.full_name, "Lam dung he thong", duration=dur)
        await temp_reply(update, context,
            f"Tài khoản bị tạm khóa do hành vi bất thường.\nLiên hệ admin: {ADMIN_CONTACT}", delay=60)
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
    logging.info(f"process_payment: user_id={user_id} amount={amount}")
    payments_col = app.bot_data["payments_col"]
    users_col    = app.bot_data["users_col"]
    now          = datetime.now(timezone.utc)

    # Buoc 1: Check transaction duplicate
    if ref:
        existing = await payments_col.find_one({"ref": ref})
        if existing:
            return

    user_doc  = await users_col.find_one({"user_id": user_id})
    user_name = user_doc.get("full_name","Không rõ") if user_doc else "Không rõ"
    username  = user_doc.get("username") if user_doc else None

    # Buoc 2: Them Transaction moi
    await payments_col.insert_one({
        "ref": ref,
        "user_id": user_id,
        "amount": amount,
        "content": content,
        "time": now,
        "action_done": False
    })

    # Buoc 3: Tính toán tổng tiền các giao dịch chưa xử lý (action_done=False)
    cursor = payments_col.find({"user_id": user_id, "action_done": False})
    total_pending = 0
    pending_ids = []
    async for tx in cursor:
        total_pending += tx["amount"]
        pending_ids.append(tx["_id"])

    logging.info(f"process_payment: total_pending={total_pending} vip_price={VIP_PRICE}")

    if total_pending >= VIP_PRICE:
        # Atomic lock status
        await payments_col.update_many(
            {"_id": {"$in": pending_ids}},
            {"$set": {"action_done": True}}
        )
        logging.info(f"process_payment: granting VIP to {user_id}")
        try:
            result = await grant_vip(app, user_id, user_name, username)
            logging.info(f"process_payment: grant_vip result={result}")
        except Exception as e:
            logging.error(f"process_payment: grant_vip exception: {e}", exc_info=True)

    elif total_pending < VIP_PRICE:
        con_thieu = VIP_PRICE - total_pending
        await log_payment_partial(app, user_id, amount, total_pending)
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text=(f"Đã nhận {amount:,}đ\n"
                      f"Tổng đã nhận: {total_pending:,}đ\n"
                      f"Còn thiếu: {con_thieu:,}đ\n\n"
                      f"Vui lòng chuyển thêm để hoàn tất."),
                protect_content=True
            )
        except Exception as e:
            logging.warning(f"Không gửi được thông báo thiếu tiền cho {user_id}: {e}")

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
                        text="Nội dung của bạn đã hết hạn.\nVào kênh VIP để lấy link xem lại nhé",
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
                        text="Lệnh tạm khóa đã hết hạn.\nQuyền truy cập đã được khôi phục.",
                        protect_content=True
                    )
                except Exception: pass
                await log_unban(app, tid)
        except Exception as e: logging.error(f"Unban worker: {e}")
        await asyncio.sleep(60)

async def vip_worker(app):
    vip_col = app.bot_data["vip_col"]
    referral_jobs_col = app.bot_data["referral_jobs_col"]
    users_col = app.bot_data["users_col"]
    try: _gid = int(GROUP_ID) if GROUP_ID else None
    except: _gid = None

    while True:
        try:
            now = datetime.now(timezone.utc)

            # --- Xu ly Referral Render-Safe ---
            if _gid:
                async for job in referral_jobs_col.find({"check_at": {"$lte": now}, "processed": False}):
                    new_uid = job["new_uid"]
                    ref_by = job["ref_by"]
                    try:
                        m = await app.bot.get_chat_member(chat_id=_gid, user_id=new_uid)
                        if m.status not in ("left", "kicked", "banned"):
                            ref_doc = await users_col.find_one({"user_id": ref_by})
                            if ref_doc and ref_doc.get("invite_earned", 0) < 15:
                                await users_col.update_one({"user_id": ref_by}, {"$inc": {"invite_earned": 1}})
                                updated = await users_col.find_one({"user_id": ref_by})
                                earned  = updated.get("invite_earned", 0)
                                try:
                                    await app.bot.send_message(
                                        chat_id=ref_by,
                                        text=f"Người bạn giới thiệu đã ở lại nhóm đủ 24 giờ.\nBạn nhận được 1 lượt xem.\nTổng lượt hiện tại: {earned}/15",
                                        protect_content=True
                                    )
                                except Exception: pass
                    except Exception as e:
                        logging.error(f"Referral check member error: {e}")
                    await referral_jobs_col.update_one({"_id": job["_id"]}, {"$set": {"processed": True}})
            # ----------------------------------

            # --- Xu ly VIP Expiration ---
            for db, field, msg in [
                (7,"notified_7d","Goi VIP cua ban sẽ hết hạn sau 7 ngày."),
                (3,"notified_3d","Goi VIP cua ban sẽ hết hạn sau 3 ngày."),
                (1,"notified_1d","Goi VIP cua ban sẽ hết hạn vào ngày mai."),
            ]:
                async for doc in vip_col.find({
                    "expire_at": {"$lte": now+timedelta(days=db), "$gt": now+timedelta(days=db-1)},
                    "active": True, field: {"$ne": True}
                }):
                    try:
                        await app.bot.send_message(
                            chat_id=doc["user_id"],
                            text=f"{msg}\nLiên hệ admin để gia hạn: {ADMIN_CONTACT}",
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
                        text=f"Goi VIP cua ban đã hết hạn.\nLiên hệ admin để gia hạn: {ADMIN_CONTACT}",
                        protect_content=True
                    )
                except Exception: pass
                
                await vip_col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"active": False, "expired_at": now}}
                )
                await send_log(app,
                    f"VIP hết hạn\nID: <code>{uid}</code>\n"
                    f"Ten: {sanitize(doc.get('full_name',''))}\nThời gian: {now_str()}"
                )
        except Exception as e: logging.error(f"VIP worker: {e}")
        await asyncio.sleep(60)

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
    name       = doc.get("full_name","Không rõ") if doc else "Không rõ"
    username   = doc.get("username") if doc else None
    await log_kick(app, user_id, name, username, kick_count)
    if kick_count >= 4:
        banned_col = app.bot_data["banned_col"]
        await banned_col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "name": name, "reason": "Vi phạm luồng nội quy quá 4 lần",
                      "ban_type": "Tu dong", "expire_at": None,
                      "banned_at": datetime.now(timezone.utc)}},
            upsert=True
        )
        await log_auto_ban(app, user_id, name, username, "Vi phạm luồng nội quy quá 4 lần")
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
        logging.info(f"ChatMember nhom thuong: {user.id} {old_status} -> {new_status}")
        if (old_status in ("left","kicked") and
                new_status in ("member","restricted")):
            banned_col = get_banned(context)
            if await banned_col.find_one({"user_id": user.id}):
                try: await context.bot.ban_chat_member(chat_id=_gid, user_id=user.id)
                except Exception: pass
                return
            try:
                await context.bot.restrict_chat_member(chat_id=_gid, user_id=user.id, permissions=MUTED)
                logging.info(f"Da mute user {user.id} trong nhom thuong")
            except Exception as e: logging.error(f"Mute error: {e}")

            kb = InlineKeyboardMarkup([[InlineKeyboardButton("Xác nhận nội quy", callback_data=f"confirm_rules_{user.id}")]])
            rules_text = (
                f"Chao {user.full_name},\n\n"
                f"NỘI QUY CỘNG ĐỒNG {GROUP_NAME}\n\n"
                f"TRÁCH NHIỆM GIÁM SÁT: Neu phat hien thanh vien khac co hanh vi lam phien, "
                f"spam hoac vi pham quy dinh, vui long gui anh chup man hinh bang chung truc "
                f"tiep cho Admin.\n\n"
                f"TƯƠNG TÁC VĂN MINH: Khong dang tai noi dung quang cao, lien ket spam hoac "
                f"gui tin nhan rieng lam phien thanh vien khac.\n\n"
                f"QUY TRÌNH DỊCH VỤ: Moi giao dich va nang cap quyen loi VIP deu phai thuc "
                f"hien thong qua Bot tu dong. Neu co loi, vui long lien he Admin kem anh chup man hinh.\n\n"
                f"QUYỀN QUẢN TRỊ: Quan tri vien co quyen loai bo thanh vien neu phat hien hanh "
                f"vi lam dung hoac co tinh vi pham.\n\n"
                f"Bằng việc xác nhận, bạn cam kết đã đọc và đồng ý với các quy định trên.\n\n"
                f"Bạn có 60 giây để xác nhận."
            )
            try:
                msg = await context.bot.send_message(chat_id=_gid, text=rules_text, reply_markup=kb)
                users_col = get_users(context)
                
                await users_col.update_one(
                    {"user_id": user.id},
                    {"$set": {"user_id": user.id, "username": user.username,
                              "full_name": user.full_name,
                              "is_muted": True,
                              "rules_confirmed": False},
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
            invite_msg_id = doc.get("invite_msg_id") if doc else None
            invite_url_old = doc.get("invite_url") if doc else None

            if invite_msg_id:
                try:
                    await context.bot.edit_message_reply_markup(
                        chat_id=user.id, message_id=invite_msg_id, reply_markup=None
                    )
                except Exception: pass

            if invite_url_old:
                try:
                    await context.bot.revoke_chat_invite_link(
                        chat_id=CHANNEL_ID, invite_link=invite_url_old
                    )
                except Exception: pass

        elif old_status == "member" and new_status == "left":
            await vip_col.update_one({"user_id": user.id}, {"$set": {"active": False}})

# ==================== JOIN REQUEST HANDLER ====================
async def new_chat_members_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try: _gid = int(GROUP_ID) if GROUP_ID else None
    except: _gid = None
    if not _gid or update.effective_chat.id != _gid: return

    for new_member in (update.message.new_chat_members or []):
        if new_member.is_bot: continue
        logging.info(f"NEW_CHAT_MEMBERS fallback: {new_member.id}")
        banned_col = context.application.bot_data["banned_col"]
        if await banned_col.find_one({"user_id": new_member.id}):
            try: await context.bot.ban_chat_member(chat_id=_gid, user_id=new_member.id)
            except Exception: pass
            continue
            
        users_col = context.application.bot_data["users_col"]
        user_doc  = await users_col.find_one({"user_id": new_member.id})
        if user_doc and user_doc.get("is_muted"):
            continue 
        try:
            await context.bot.restrict_chat_member(
                chat_id=_gid, user_id=new_member.id, permissions=MUTED
            )
        except Exception as e:
            logging.error(f"Fallback mute error: {e}")
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(
            "Xác nhận nội quy", callback_data=f"confirm_rules_{new_member.id}"
        )]])
        rules_text = (
            f"Chao {new_member.full_name},\n\n"
            f"NỘI QUY CỘNG ĐỒNG {GROUP_NAME}\n\n"
            f"TRÁCH NHIỆM GIÁM SÁT: Neu phat hien thanh vien khac co hanh vi lam phien, "
            f"spam hoac vi pham quy dinh, vui long gui anh chup man hinh bang chung cho Admin.\n\n"
            f"TƯƠNG TÁC VĂN MINH: Khong dang tai noi dung quang cao, "
            f"lien ket spam hoac gui tin nhan rieng lam phien thanh vien khac.\n\n"
            f"QUY TRÌNH DỊCH VỤ: Moi giao dich va nang cap quyen loi VIP deu thuc hien "
            f"qua Bot tu dong. Neu co loi, vui long lien he Admin kem anh chup man hinh.\n\n"
            f"QUYỀN QUẢN TRỊ: Quan tri vien co quyen loai bo thanh vien neu phat hien "
            f"hanh vi lam dung hoac co tinh vi pham.\n\n"
            f"Bằng việc xác nhận, bạn cam kết đã đọc và đồng ý với các quy định trên.\n\n"
            f"Bạn có 60 giây để xác nhận."
        )
        try:
            msg = await context.bot.send_message(
                chat_id=_gid, text=rules_text, reply_markup=kb
            )
            await users_col.update_one(
                {"user_id": new_member.id},
                {"$set": {"user_id": new_member.id, "username": new_member.username,
                          "full_name": new_member.full_name,
                          "is_muted": True, "rules_confirmed": False},
                 "$setOnInsert": {"first_seen": datetime.now(timezone.utc),
                                  "invite_earned": 0, "invite_used": 0,
                                  "kick_count": 0, "rules_confirmed_before": False,
                                  "total_views": 0}},
                upsert=True
            )
            if new_member.id in pending_kicks:
                pending_kicks[new_member.id].cancel()
            task = asyncio.create_task(
                kick_if_not_confirmed(context.application, _gid, new_member.id, msg.message_id)
            )
            pending_kicks[new_member.id] = task
        except Exception as e:
            logging.error(f"Fallback send rules error: {e}")

async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req     = update.chat_join_request
    user_id = req.from_user.id
    vip_col = context.application.bot_data["vip_col"]
    if str(req.chat.id) != str(CHANNEL_ID): return

    doc = await vip_col.find_one_and_update(
        {"user_id": user_id, "pending": True},
        {"$set": {"pending": False}},
        return_document=ReturnDocument.AFTER
    )
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
                text=f"Yêu cầu vào kênh VIP bị từ chối.\nVui lòng thanh toán trước.\nGo /mua de xem huong dan.",
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
            try: await context.bot.send_message(chat_id=target_id, text="Quyền truy cập đã được khôi phục.", protect_content=True)
            except Exception: pass
            await query.answer("Đã gỡ ban!")
            await query.edit_message_text(query.message.text + f"\n\nĐã gỡ ban lúc {now_str()}")
            await log_unban(context.application, target_id)
        else: await query.answer("Không tìm thấy!")
        return

    # Xác nhận nội quy
    if data.startswith("confirm_rules_"):
        target_id = int(data.split("_")[2])
        if user.id != target_id:
            await query.answer("Đây không phải nút dành cho bạn!")
            return
        users_col = get_users(context)
        user_doc  = await users_col.find_one({"user_id": target_id})

        if not user_doc or not user_doc.get("is_muted", False):
            await query.answer()
            return

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
        await query.answer("Xác nhận thành công! Chào mừng bạn.")

        is_new     = not user_doc.get("rules_confirmed_before", False)
        status_txt = "Lần đầu vào nhóm" if is_new else "Đã từng vào trước đó - Tham gia lại"
        await users_col.update_one({"user_id": target_id}, {"$set": {"rules_confirmed_before": True}})
        await log_rules_confirm(context.application, target_id,
                                user_doc.get("full_name",""), user_doc.get("username"), status_txt)
        
        # Add to referral jobs securely for background processing
        ref_by = user_doc.get("ref_by")
        if ref_by:
            referral_jobs_col = context.application.bot_data["referral_jobs_col"]
            await referral_jobs_col.insert_one({
                "new_uid": target_id,
                "ref_by": ref_by,
                "check_at": datetime.now(timezone.utc) + timedelta(seconds=86400),
                "processed": False
            })
        return

    # Ban pha 2
    if data.startswith("ban_enter_time_") and user.id == ADMIN_ID:
        admin_id = int(data.split("_")[3])
        if admin_id != ADMIN_ID: await query.answer(); return
        awaiting_ban_time[ADMIN_ID] = True
        force_msg = await context.bot.send_message(
            chat_id=user.id,
            text="Nhập thời gian cấm (vi du: 1h, 2d, 30m):\n(h = giờ, d = ngày, m = phút)",
            reply_markup=ForceReply(selective=True)
        )
        await query.answer("Nhập thời gian cấm ben duoi.")
        return

    # Demo
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

    vip_col_check = context.application.bot_data["vip_col"]
    pend_doc = await vip_col_check.find_one({
        "user_id": uid,
        "pending_notification": True
    })
    if pend_doc:
        invite_url  = pend_doc.get("pending_invite_url")
        expire_pend = pend_doc.get("pending_expire_at") or pend_doc.get("pending_expire")
        if invite_url and expire_pend:
            days_p = days_left(expire_pend)
            kb_p   = InlineKeyboardMarkup([[InlineKeyboardButton(
                "Bấm vào đây để vào kênh VIP", url=invite_url
            )]])
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=(f"Bạn đã thanh toán thành công trước đó.\n\n"
                          f"Gói VIP có hiệu lực đến ngày "
                          f"{expire_pend.strftime('%d/%m/%Y')} ({days_p} ngay).\n\n"
                          f"Bấm nút bên dưới để vào kênh VIP.\n"
                          f"Link chỉ dùng 1 lần."),
                    reply_markup=kb_p,
                    protect_content=True
                )
                await vip_col_check.update_one(
                    {"user_id": uid},
                    {"$set": {"pending_notification": False}}
                )
            except Exception as e:
                logging.error(f"Re-send VIP link error: {e}")

    banned_col = get_banned(context)
    try: ban_doc = await db_retry(lambda: banned_col.find_one({"user_id": uid}))
    except: ban_doc = None

    if ban_doc:
        reason   = ban_doc.get("reason","Vi pham quy dinh")
        expire_at = ban_doc.get("expire_at")
        if expire_at:
            es = expire_at.strftime("%d/%m/%Y %H:%M")
            await temp_reply(update, context,
                f"Tài khoản đang bị tạm khóa.\nLý do: {reason}\nHết hạn lúc: {es}\n\n"
                f"Nếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}", delay=300)
        else:
            await temp_reply(update, context,
                f"Quyền truy cập đã bị thu hồi.\nLý do: {reason}\n\n"
                f"Nếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}", delay=300)
        return

    if await check_user(update, context): return

    args = context.args or []

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

    if not args:
        await temp_reply(update, context,
            f"Chào mừng bạn đến với hệ thống.\n\nGõ /help để xem danh sách lệnh và hướng dẫn sử dụng.",
            delay=120)
        return

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
                f"Quyền truy cập bị thu hồi tự động.\n\nNếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}",
                delay=300)
            return
        await temp_reply(update, context, "Dữ liệu không tồn tại hoặc phiên chia sẻ đã hết hạn.", delay=120)
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
            f"Nội dung chỉ dành cho thành viên VIP.\n\nGõ /mua để xem hướng dẫn mua VIP.",
            delay=300)
        return

    nonmember_attempts[uid] = 0
    await send_log(app,
        f"Truy cap noi dung\nID: <code>{uid}</code>\n"
        f"Ten: {sanitize(user.full_name)}\n"
        f"Username: {'@'+user.username if user.username else 'Khong co'}\n"
        f"Album: <code>{key}</code>\nThời gian: {now_str()}"
    )
    users_col = get_users(context)
    await users_col.update_one({"user_id": uid}, {"$inc": {"total_views": 1}})
    await send_album(context, uid, album)

async def send_album(context, uid, album):
    items    = album.get("items",[])
    if not items: return
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
        if query: await query.answer("Không tìm thấy thông tin của bạn!")
        return
    earned   = user_doc.get("invite_earned",0)
    used     = user_doc.get("invite_used",0)
    luot_con = earned - used
    if luot_con <= 0:
        if query: await query.answer("Bạn không còn lượt xem!")
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text="Bạn không còn lượt xem.\nDùng /gioi_thieu để kiếm thêm lượt.",
                protect_content=True
            )
        except Exception: pass
        return
    demo = await demos_col.find_one({"number": number})
    if not demo or not demo.get("full_album_key"):
        if query: await query.answer("Album này hiện không khả dụng!")
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text="Album này hiện không khả dụng hoặc đã hết hạn.",
                protect_content=True
            )
        except Exception: pass
        return
    album = await albums_col.find_one({"key": demo["full_album_key"]})
    if not album or not album.get("items"):
        if query: await query.answer("Không tìm thấy nội dung!")
        return
    await users_col.update_one({"user_id": user_id}, {"$inc": {"invite_used": 1, "total_views": 1}})
    if query: await query.answer(f"Đang gửi bộ #{number}...")

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
            text=f"Đã gửi album #{number}.\nLượt còn lại: {luot_con}/15",
            protect_content=True
        )
    except Exception as e: logging.error(f"Send demo: {e}")


# ==================== MEMBER COMMANDS ====================
async def cmd_mua(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type != "private":
        await temp_reply(update, context,
            f"Lệnh này chỉ hoạt động trong chat riêng với bot.\nBấm: @{BOT_USERNAME}", delay=60)
        return
    await save_user(context, user)
    await log_mua(context.application, user)

    qr_img_url = make_vietqr(user.id)
    kb_qr = InlineKeyboardMarkup([[
        InlineKeyboardButton("Xem mã QR thanh toán", url=qr_img_url)
    ]])
    caption = (
        f"Gói VIP 1 tháng: {VIP_PRICE:,}đ\n\n"
        f"Chuyển khoản:\n"
        f"Ngân hàng: {BANK_NAME}\n"
        f"Số tài khoản: {BANK_ACCOUNT}\n"
        f"Số tiền: {VIP_PRICE:,}đ\n"
        f"Nội dung: SEVQR VIP {user.id}\n\n"
        f"Nhấn giữ vào ảnh QR để lưu về máy.\n"
        f"Sau đó mở app ngân hàng → Quét QR → chọn ảnh vừa lưu.\n\n"
        f"Hệ thống tự động cấp quyền sau khi nhận đủ tiền.\n"
        f"Hỗ trợ: {ADMIN_CONTACT}"
    )
    try:
        msg = await context.bot.send_photo(
            chat_id=user.id,
            photo=qr_img_url,
            caption=caption,
            reply_markup=kb_qr,
            protect_content=True
        )
        asyncio.create_task(auto_del(context.bot, user.id, msg.message_id, 600))
    except Exception as e:
        logging.error(f"Gui /mua loi: {e}")
        msg = await update.message.reply_text(
            caption, reply_markup=kb_qr, protect_content=True
        )
        asyncio.create_task(auto_del(context.bot, update.effective_chat.id, msg.message_id, 600))

async def cmd_luot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type != "private":
        await temp_reply(update, context,
            f"Lệnh này chỉ hoạt động trong chat riêng với bot.\nBam: @{BOT_USERNAME}", delay=60)
        return
    users_col = get_users(context)
    doc = await users_col.find_one({"user_id": user.id})
    if not doc:
        await temp_reply(update, context,
            "Bạn chưa có thông tin trong hệ thống.\nVào nhóm và xác nhận nội quy trước.", delay=120)
        return
    earned   = doc.get("invite_earned",0)
    used     = doc.get("invite_used",0)
    luot_con = earned - used
    await temp_reply(update, context,
        f"Lượt xem của bạn:\n\n"
        f"Đã kiếm: {earned}/15\n"
        f"Đã dùng: {used}\n"
        f"Còn lại: {luot_con}\n\n"
        f"Dùng /gioi_thieu để kiếm thêm lượt.\n"
        f"Dùng /xem để xem nội dung demo.", delay=120
    )

async def cmd_gioi_thieu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type != "private":
        await temp_reply(update, context,
            f"Lệnh này chỉ hoạt động trong chat riêng với bot.\nBam: @{BOT_USERNAME}", delay=60)
        return
    ref_url = make_ref_link(user.id)
    await temp_reply(update, context,
        f"Link giới thiệu của bạn:\n{ref_url}\n\n"
        f"Chia sẻ link này để kiếm lượt xem.\n"
        f"Mỗi người vào nhóm và ở lại 24 giờ = +1 lượt.\n\n"
        f"Tối đa 15 lượt suốt đời. Dù mời bao nhiêu người\n"
        f"cũng chỉ kiếm được tối đa 15 lượt.\n"
        f"Dùng /luot để xem số lượt hiện tại.", delay=300
    )

async def cmd_xem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.effective_chat.type != "private":
        await temp_reply(update, context,
            f"Lệnh này chỉ hoạt động trong chat riêng với bot.\nBam: @{BOT_USERNAME}", delay=60)
        return
    args = context.args or []
    demos_col = get_demos(context)

    number = None
    if args:
        raw = args[0].lstrip("#").strip()
        if raw.isdigit():
            number = int(raw)

    if number is not None:
        asyncio.create_task(send_demo(context.application, user.id, number))
        return

    users_col = get_users(context)
    doc = await users_col.find_one({"user_id": user.id})
    if not doc:
        await temp_reply(update, context, "Bạn chưa có thông tin trong hệ thống.", delay=120)
        return
    earned   = doc.get("invite_earned",0)
    used     = doc.get("invite_used",0)
    luot_con = earned - used
    if luot_con <= 0:
        await temp_reply(update, context,
            "Bạn không còn lượt xem.\nDùng /gioi_thieu để kiếm thêm lượt.", delay=120)
        return
    demos = await demos_col.find(
        {"full_album_key": {"$exists": True, "$ne": None}},
        {"number": 1}
    ).sort("number",1).to_list(length=20)
    if not demos:
        await temp_reply(update, context, "Hiện chưa có bộ demo nào.", delay=120)
        return
    keyboard = []
    for d in demos:
        keyboard.append([InlineKeyboardButton(f"Bo #{d['number']}", callback_data=f"demo_{d['number']}")])
    await temp_reply(update, context,
        f"Lượt còn lại: {luot_con}/15\n\nChọn album muốn xem hoặc gõ /xem [mã_số]:",
        delay=300, reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def cmd_help_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID: return
    await temp_reply(update, context,
        "Hướng dẫn sử dụng:\n\n"
        "/mua — Xem thông tin gói VIP và tạo mã thanh toán\n"
        "/luot — Xem số lượt còn lại\n"
        "/gioi_thieu — Lấy link giới thiệu để kiếm lượt\n"
        "/xem [mã_số] — Xem nội dung Album Full ngay\n"
        "/help — Hướng dẫn sử dụng\n\n"
        f"Hỗ trợ: {ADMIN_CONTACT}\n\n"
        "Lưu ý: Tất cả lệnh chỉ hoạt động trong chat riêng với bot.", delay=120
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
        return

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
                f"Dùng /setlink {number} [KEY] để liên kết album full."
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
    await update.message.reply_text(f"Đã nhận {file_type} — album có {count} file.\nGõ /done khi xong.")

# Handler nhan ForceReply thoi gian ban
async def handle_text_hashtag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Bat text thuần có #N trong nhóm thường."""
    if update.effective_chat.type == "private": return
    if update.effective_chat.type not in ("group", "supergroup"): return
    if update.effective_user.id != ADMIN_ID: return

    try: _gid = int(GROUP_ID) if GROUP_ID else None
    except: _gid = None
    if not _gid or update.effective_chat.id != _gid: return

    text = update.message.text or ""
    m    = re.search(r'#(\d+)', text)
    if not m: return
    number    = int(m.group(1))
    demos_col = get_demos(context)
    await demos_col.update_one(
        {"number": number},
        {"$setOnInsert": {"number": number, "full_album_key": None,
                          "created_at": datetime.now(timezone.utc)}},
        upsert=True
    )
    await update.message.reply_text("Da ghi nhan bo #" + str(number) + ".\nDùng /setlink " + str(number) + " [KEY] để liên kết album full.")


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
    target_name = ban_info.get("target_name","Không rõ")
    awaiting_ban_time.pop(ADMIN_ID, None)
    pending_bans.pop(ADMIN_ID, None)

    try: _gid = int(GROUP_ID) if GROUP_ID else None
    except: _gid = None

    if _gid:
        raw_secs = dur.total_seconds()
        clamped  = max(31, min(raw_secs, 365 * 24 * 3600))
        if clamped != raw_secs:
            d_orig   = int(raw_secs // 3600) if raw_secs < 86400 else int(raw_secs // 86400)
            d_clamp  = int(clamped // 3600) if clamped < 86400 else int(clamped // 86400)
            unit_o   = "gio" if raw_secs < 86400 else "ngay"
            unit_c   = "gio" if clamped < 86400 else "ngay"
            await update.message.reply_text(
                f"Thời gian nhập ({d_orig} {unit_o}) vượt giới hạn an toàn.\n"
                f"Đã tự động điều chỉnh thành {d_clamp} {unit_c}."
            )
        until_ts = int(datetime.now(timezone.utc).timestamp() + clamped)
        try:
            await context.bot.ban_chat_member(
                chat_id=_gid, user_id=target_id, until_date=until_ts
            )
        except Exception as e:
            await update.message.reply_text(f"Không thể kick khỏi nhóm: {e}")
            return

    await do_ban(context.application, target_id, target_name,
                 reason, ban_type="Thu cong", duration=dur)

    ea          = datetime.now(timezone(timedelta(hours=7))) + dur
    expire_text = f"Hết hạn lúc: {ea.strftime('%d/%m/%Y %H:%M')}"
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(f"Bạn đã bị cấm.\nLý do: {reason}\n{expire_text}\n\n"
                  f"Nếu bạn nghĩ lệnh cấm do nhầm lẫn, liên hệ: {ADMIN_CONTACT}"),
            protect_content=True
        )
    except Exception: pass

    d = int(dur.total_seconds()//86400); h = int(dur.total_seconds()//3600)
    expire_info = f"({d} ngay)" if d >= 1 else f"({h} gio)" if h >= 1 else f"({int(dur.total_seconds()//60)} phut)"
    await update.message.reply_text(f"Đã cấm {target_name} {expire_info}.\nLý do: {reason}")

# ==================== ADMIN COMMANDS ====================
ADMIN_ONLY_CMDS = {"new","new_album","done","list","detail","check","del","del_album",
                   "setlink","dellink","ban","who","extend","viplist","status","addluot","addday","delday"}

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
                text=f"Lệnh /{cmd} chỉ hoạt động trong chat riêng với bot.\nVui lòng chat riêng với bot để sử dụng."
             )
        except Exception: pass

async def new_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if context.user_data.get("current_key"):
        await update.message.reply_text("Đang có album chưa hoàn thành.\nGõ /done để lấy link trước."); return
    key = make_key()
    albums_col = get_albums(context)
    await albums_col.insert_one({"key": key, "items": [], "created_at": datetime.now(timezone.utc)})
    context.user_data["current_key"] = key
    await update.message.reply_text(
        f"Album mới đã tạo.\nMa: <code>{key}</code>\n\nForward ảnh hoặc video vào đây.\nGõ /done khi xong.",
        parse_mode="HTML")

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    key = context.user_data.get("current_key")
    if not key:
        await update.message.reply_text("Không có album nào đang tạo."); return
    albums_col = get_albums(context)
    album      = await albums_col.find_one({"key": key})
    if not album or not album.get("items"):
        await update.message.reply_text("Album chưa có file nào.\nHãy forward ảnh hoặc video vào trước."); return
    context.user_data.pop("current_key", None)
    count = len(album.get("items",[]))
    await update.message.reply_text(f"Hoàn tất. Album có {count} file.\nKey: <code>{key}</code>\nLink chia se:", parse_mode="HTML")
    await update.message.reply_text(make_link(key))

async def list_albums(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    albums_col = get_albums(context)
    try:
        albums = await db_retry(lambda: albums_col.find({},{"key":1,"items":1}).sort("created_at",-1).to_list(length=50))
    except: await update.message.reply_text("Lỗi kết nối."); return
    if not albums: await update.message.reply_text("Chua co album nao."); return
    text = f"Danh sách album ({len(albums)}):\n\n"
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
    except: await update.message.reply_text("Lỗi kết nối."); return
    if not albums: await update.message.reply_text("Chua co album nao."); return
    for i, a in enumerate(albums, 1):
        await update.message.reply_text(f"{i}\nSố file: {len(a.get('items',[]))}\nLink:")
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
    if not album: await update.message.reply_text(f"Không tìm thấy album {key}."); return
    items  = album.get("items",[])
    videos = sum(1 for i in items if i["type"]=="video")
    photos = sum(1 for i in items if i["type"]=="photo")
    total_dur = sum(it.get("duration",0) for it in items if it["type"]=="video")
    await update.message.reply_text(
        f"Album: <code>{key}</code>\n"
        f"Tổng: {len(items)} | Video: {videos} | Anh: {photos}\n"
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
    await update.message.reply_text(f"Đã xóa album {key}." if r.deleted_count else f"Không tìm thấy {key}.")

async def cmd_setlink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Dùng: /setlink <số> <key_album>\nVi du: /setlink 1 abc123xyz"); return
    try: number = int(context.args[0])
    except: await update.message.reply_text("Số bộ phải là số nguyên."); return
    key        = context.args[1]
    albums_col = get_albums(context)
    album      = await albums_col.find_one({"key": key})
    if not album:
        await update.message.reply_text(f"Không tìm thấy album voi key: {key}\nKiểm tra lại bằng /list."); return
    demos_col = get_demos(context)
    await demos_col.update_one(
        {"number": number},
        {"$set": {"number": number, "full_album_key": key, "updated_at": datetime.now(timezone.utc)},
         "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
        upsert=True
    )
    items = album.get("items",[])
    await update.message.reply_text(
        f"Da lien ket bo #{number} với album {key}.\n"
        f"Số file: {len(items)}\n"
        f"Thành viên có thể dùng /xem {number} để xem."
    )

async def cmd_dellink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args:
        await update.message.reply_text("Dùng: /dellink <số>\nVi du: /dellink 1"); return
    try: number = int(context.args[0])
    except: await update.message.reply_text("Số bộ phải là số nguyên."); return
    demos_col = get_demos(context)
    r = await demos_col.update_one({"number": number}, {"$set": {"full_album_key": None}})
    if r.matched_count:
        await update.message.reply_text(
            f"Đã xóa lien ket album full cua bo #{number}.\n"
            f"Thành viên gõ /xem {number} sẽ thấy thông báo 'Không khả dụng'.")
    else:
        await update.message.reply_text(f"Không tìm thấy bo #{number}.")

async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type in ("group","supergroup"):
        try: await update.message.delete()
        except Exception: pass
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text="Lệnh /ban chỉ hoạt động trong chat riêng với bot (DM).\n"
                     "Vui lòng mở chat riêng với bot để sử dụng."
            )
        except Exception: pass
        return

    args = context.args or []
    if not args and not update.message.reply_to_message:
        await update.message.reply_text(
            "Cách dùng /ban trong DM:\n\n"
            "1. /ban [ID] [Ly do]\n"
            "2. /ban @tentaikhoan [Ly do]\n\n"
            "Vi du: /ban 123456789 quay-roi\n"
            "Vi du: /ban @abc spam\n\n"
            "Bot sẽ hỏi thêm thời gian cấm sau."
        ); return

    target_id = None; target_name = "Không rõ"; reason = "Vi pham quy dinh"

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
            except Exception: await update.message.reply_text(f"Không tìm thấy: {first}"); return
        else:
            try: target_id = int(first)
            except: await update.message.reply_text("ID không hợp lệ."); return
            try:
                chat = await context.bot.get_chat(target_id)
                target_name = chat.full_name
            except Exception: pass
        if len(args) > 1: reason = " ".join(args[1:])

    if not target_id or target_id == ADMIN_ID:
        await update.message.reply_text("Không thể ban."); return

    reason = LY_DO.get(reason, reason)
    banned_col = get_banned(context)
    existing   = await banned_col.find_one({"user_id": target_id})
    if existing:
        await update.message.reply_text(
            f"Tài khoản {target_name} đã bị cấm trước đó.\n"
            f"Lý do cũ: {existing.get('reason','Không rõ')}\n"
            f"Dùng /unban trước nếu muốn cấm lại."
        ); return

    pending_bans[ADMIN_ID] = {"target_id": target_id, "target_name": target_name, "reason": reason}
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Nhập thời gian", callback_data=f"ban_enter_time_{ADMIN_ID}")]])
    await update.message.reply_text(
        f"Chuẩn bị cấm:\nID: {target_id}\nTen: {target_name}\nLý do: {reason}\n\nBấm nút để nhập thời gian cấm:",
        reply_markup=kb
    )

async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    target_id = None; target_name = "Không rõ"
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
            except Exception: await update.message.reply_text("Không tìm thấy username."); return
        else:
            try: target_id = int(first)
            except: await update.message.reply_text("ID không hợp lệ."); return
            try:
                chat = await context.bot.get_chat(target_id)
                target_name = chat.full_name
            except Exception: pass
    else:
        await update.message.reply_text(
            "Cách dùng /unban:\n1. Reply tin nhan + /unban\n2. /unban @user\n3. /unban ID"
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
        await update.message.reply_text(f"Đã gỡ cấm {target_name}.")
        await log_unban(context.application, target_id)
    else:
        await update.message.reply_text("Không tìm thấy trong danh sách cấm.")

async def who_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    target_id = None
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    elif context.args:
        try: target_id = int(context.args[0])
        except: await update.message.reply_text("ID không hợp lệ."); return
    else:
        await update.message.reply_text("Dùng: /who <ID> hoặc reply tin nhắn"); return
    ban_doc  = await get_banned(context).find_one({"user_id": target_id})
    vip_doc  = await get_vip(context).find_one({"user_id": target_id})
    user_doc = await get_users(context).find_one({"user_id": target_id})
    try:
        chat     = await context.bot.get_chat(target_id)
        name     = chat.full_name
        username = f"@{chat.username}" if chat.username else "Khong co"
    except Exception:
        name = user_doc.get("full_name","Không rõ") if user_doc else "Không rõ"
        username = f"@{user_doc.get('username')}" if user_doc and user_doc.get("username") else "Khong co"
    text = f"Thông tin:\nID: <code>{target_id}</code>\nTen: {name}\nUsername: {username}\n\n"
    if ban_doc:
        es    = ban_doc['expire_at'].strftime('%d/%m/%Y %H:%M') if ban_doc.get('expire_at') else "Vinh vien"
        text += f"Trạng thái ban: Đang bị cấm\nLý do: {ban_doc.get('reason','')}\nHết hạn: {es}\n\n"
    else:
        text += "Trạng thái ban: Bình thường\n\n"
    if vip_doc and vip_doc.get("expire_at"):
        ea = vip_doc["expire_at"]
        if ea.tzinfo is None: ea = ea.replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        if ea > now_utc:
            d     = days_left(ea)
            text += f"VIP: Đang hoạt động\nHết hạn: {ea.strftime('%d/%m/%Y')} ({d} ngày còn lại)\n\n"
        else:
            text += f"VIP: Đã hết hạn (hết ngày {ea.strftime('%d/%m/%Y')})\n\n"
    else:
        text += "VIP: Chưa có\n\n"
    if user_doc:
        earned      = user_doc.get("invite_earned",0)
        used        = user_doc.get("invite_used",0)
        total_views = user_doc.get("total_views",0)
        text += f"Lượt: {earned-used} còn lại ({earned} kiếm / {used} đã dùng)\nTổng số lượt xem: {total_views}"
    await update.message.reply_text(text, parse_mode="HTML")

async def extend_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args: await update.message.reply_text("Dung: /extend <ID>"); return
    try: target_id = int(context.args[0])
    except: await update.message.reply_text("ID không hợp lệ."); return
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
    user_name = user_doc.get("full_name","Không rõ") if user_doc else "Không rõ"
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(f"Gói VIP đã được gia hạn thành công.\n\n"
                  f"Hạn mới: {new_expire.strftime('%d/%m/%Y')}\n"
                  f"So ngày còn lại: {d} ngay"),
            protect_content=True
        )
    except Exception: pass
    await update.message.reply_text(
        f"Đã gia hạn VIP cho <code>{target_id}</code> ({user_name}).\n"
        f"Hạn mới: {new_expire.strftime('%d/%m/%Y')} ({d} ngày còn lại)",
        parse_mode="HTML"
    )
    await log_extend(context.application, target_id, 30, new_expire)

async def vip_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    vip_col = get_vip(context)
    members = await vip_col.find({"active":True},{"user_id":1,"full_name":1,"expire_at":1}).sort("expire_at",1).to_list(50)
    if not members: await update.message.reply_text("Chưa có member VIP nào."); return
    text = f"VIP đang hoạt động ({len(members)}):\n\n"
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
            f"Trạng thái hệ thống:\n\n"
            f"Album: {ta}\nDemo: {td} bộ (đã liên kết: {tl})\n"
            f"User: {tu}\nVIP: {tv}\n"
            f"Cấm: {tb} (tạm: {tt})\nJob chờ xóa: {tj}\n"
            f"Database: Kết nối ổn định"
        )
    except Exception: await update.message.reply_text("Lỗi kết nối.")

async def cmd_add_luot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Dung: /addluot <ID> <so luot>"); return
    try: target_id = int(context.args[0]); so_luot = int(context.args[1])
    except: await update.message.reply_text("ID và số lượt phải là số."); return
    users_col = get_users(context)
    await users_col.update_one({"user_id": target_id}, {"$inc": {"invite_earned": so_luot}}, upsert=True)
    doc    = await users_col.find_one({"user_id": target_id})
    earned = doc.get("invite_earned",0) if doc else so_luot
    used   = doc.get("invite_used",0) if doc else 0
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=f"Admin đã thêm {so_luot} lượt xem cho bạn.\nLượt còn lại: {earned-used}/15",
            protect_content=True
        )
    except Exception: pass
    await update.message.reply_text(
        f"Đã thêm {so_luot} lượt cho ID {target_id}.\nTổng: {earned} | Đã dùng: {used} | Còn lại: {earned-used}"
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await update.message.reply_text(
        "Lệnh dành cho Admin (chỉ hoạt động trong DM với bot):\n\n"
        "— NỘI DUNG —\n"
        "/new — Tạo album mới\n"
        "/done — Lấy link chia sẻ\n"
        "/list — Danh sách album\n"
        "/detail — Album kèm link\n"
        "/check <ma> — Thong tin album\n"
        "/del <ma> — Xóa album\n\n"
        "— DEMO SYSTEM —\n"
        "/setlink <so> <key> — Liên kết bộ demo với album full\n"
        "  Vi du: /setlink 1 abc123xyz\n"
        "/dellink <so> — Xóa liên kết album full của bộ\n"
        "  Vi du: /dellink 1\n\n"
        "— THÀNH VIÊN —\n"
        "/ban [ID/@user] [Ly do] — Cấm 2 pha (hỏi thêm thời gian)\n"
        "  Thời gian: 1h / 2d / 30m (h=gio, d=ngay, m=phut)\n"
        "  Lý do gợi ý: quay-roi / chia-se / spam / gia-mao\n"
        "  het-han / ban-lai / vi-pham / abuse / da-nghi\n"
        "  nhieu-tk / hoan-tien\n"
        "/unban [ID/@user] — Gỡ cấm\n"
        "/who <ID> — Xem thông tin tài khoản\n"
        "/addluot <ID> <so> — Thêm lượt xem cho thành viên\n\n"
        "— VIP —\n"
        "/viplist — Danh sach VIP đang hoạt động\n"
        "/extend <ID> — Gia hạn VIP thêm 1 tháng\n\n"
        "— TEST & HỆ THỐNG —\n"
        "/addday <ID> <số> — Cộng thêm ngày VIP\n"
        "/delday <ID> <số> — Trừ bớt ngày VIP\n"
        "/status — Tổng quan trạng thái\n"
        "/help — Danh sách lệnh này"
    )

async def cmd_add_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Dùng: /addday <ID> <số_ngày>\nVí dụ: /addday 123456789 7")
        return
    try:
        target_id = int(context.args[0])
        so_ngay   = int(context.args[1])
        if so_ngay <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("ID và số ngày phải là số nguyên dương.")
        return

    vip_col   = get_vip(context)
    users_col = get_users(context)
    now       = datetime.now(timezone.utc)
    doc       = await vip_col.find_one({"user_id": target_id})

    if doc and doc.get("expire_at"):
        ea = doc["expire_at"]
        if ea.tzinfo is None: ea = ea.replace(tzinfo=timezone.utc)
        base = max(ea, now)
    else:
        base = now

    new_expire = base + timedelta(days=so_ngay)
    d          = days_left(new_expire)

    await vip_col.update_one(
        {"user_id": target_id},
        {"$set": {"expire_at": new_expire, "active": True,
                  "notified_7d": False, "notified_3d": False, "notified_1d": False}},
        upsert=True
    )

    user_doc  = await users_col.find_one({"user_id": target_id})
    user_name = user_doc.get("full_name","Không rõ") if user_doc else "Không rõ"

    await update.message.reply_text(
        f"Đã cộng {so_ngay} ngày VIP cho <code>{target_id}</code> ({user_name}).\n"
        f"Hạn mới: {new_expire.strftime('%d/%m/%Y')} ({d} ngày còn lại)",
        parse_mode="HTML"
    )
    await send_log(context.application,
        f"Cộng ngày VIP\n"
        f"ID: <code>{target_id}</code>\n"
        f"Tên: {sanitize(user_name)}\n"
        f"Số ngày cộng: +{so_ngay} ngày\n"
        f"Hạn mới: {new_expire.strftime('%d/%m/%Y')}\n"
        f"Thời gian: {now_str()}"
    )

async def cmd_del_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if update.effective_chat.type != "private":
        await admin_cmd_in_group(update, context); return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Dùng: /delday <ID> <số_ngày>\nVí dụ: /delday 123456789 3")
        return
    try:
        target_id = int(context.args[0])
        so_ngay   = int(context.args[1])
        if so_ngay <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("ID và số ngày phải là số nguyên dương.")
        return

    vip_col   = get_vip(context)
    users_col = get_users(context)
    now       = datetime.now(timezone.utc)
    doc       = await vip_col.find_one({"user_id": target_id})

    if not doc or not doc.get("expire_at"):
        await update.message.reply_text(f"ID {target_id} chưa có VIP trong hệ thống.")
        return

    ea = doc["expire_at"]
    if ea.tzinfo is None: ea = ea.replace(tzinfo=timezone.utc)
    new_expire = ea - timedelta(days=so_ngay)
    d          = days_left(new_expire)

    if new_expire <= now:
        await vip_col.update_one(
            {"user_id": target_id},
            {"$set": {"expire_at": now, "active": False}}
        )
        await update.message.reply_text(
            f"Sau khi trừ {so_ngay} ngày, VIP của <code>{target_id}</code> đã hết hạn ngay.",
            parse_mode="HTML"
        )
        return

    await vip_col.update_one(
        {"user_id": target_id},
        {"$set": {"expire_at": new_expire}}
    )

    user_doc  = await users_col.find_one({"user_id": target_id})
    user_name = user_doc.get("full_name","Không rõ") if user_doc else "Không rõ"

    await update.message.reply_text(
        f"Đã trừ {so_ngay} ngày VIP của <code>{target_id}</code> ({user_name}).\n"
        f"Hạn mới: {new_expire.strftime('%d/%m/%Y')} ({d} ngày còn lại)",
        parse_mode="HTML"
    )
    await send_log(context.application,
        f"Trừ ngày VIP\n"
        f"ID: <code>{target_id}</code>\n"
        f"Tên: {sanitize(user_name)}\n"
        f"Số ngày trừ: -{so_ngay} ngày\n"
        f"Hạn mới: {new_expire.strftime('%d/%m/%Y')}\n"
        f"Thời gian: {now_str()}"
    )

async def no_permission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in ("group","supergroup"):
        await temp_reply(update, context,
            f"Lệnh này chỉ hoạt động trong chat riêng với bot.\nBam: @{BOT_USERNAME}", delay=60)
    else:
        await temp_reply(update, context, "Bạn không có quyền sử dụng lệnh này.", delay=60)

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
        "referral_jobs_col": db["referral_jobs"],
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
    await db["demos"].create_index("number", unique=True)
    await db["referral_jobs"].create_index([("check_at",1), ("processed",1)])
    
    logging.info("DB connected!")
    return client

# ==================== MAIN ====================
async def main():
    check_env()
    app          = Application.builder().token(TOKEN).build()
    admin_filter = filters.User(user_id=ADMIN_ID)

    try:
        _gid_int = int(GROUP_ID) if GROUP_ID else None
    except Exception:
        _gid_int = None

    app.add_handler(CommandHandler("start", start))

    # Admin commands - chi trong DM
    app.add_handler(CommandHandler(["new","new_album"], new_album,       filters=admin_filter))
    app.add_handler(CommandHandler("done",             done,             filters=admin_filter))
    app.add_handler(CommandHandler("list",             list_albums,      filters=admin_filter))
    app.add_handler(CommandHandler("detail",           detail_albums,    filters=admin_filter))
    app.add_handler(CommandHandler("check",            check_album,      filters=admin_filter))
    app.add_handler(CommandHandler(["del","del_album"],delete_album,     filters=admin_filter))
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
    app.add_handler(CommandHandler("addday",           cmd_add_day,      filters=admin_filter))
    app.add_handler(CommandHandler("delday",           cmd_del_day,      filters=admin_filter))

    # Member commands
    app.add_handler(CommandHandler("mua",        cmd_mua))
    app.add_handler(CommandHandler("luot",       cmd_luot))
    app.add_handler(CommandHandler("gioi_thieu", cmd_gioi_thieu))
    app.add_handler(CommandHandler("xem",        cmd_xem))
    app.add_handler(CommandHandler("help",       cmd_help_user, filters=~admin_filter))

    # Media handler
    if _gid_int:
        app.add_handler(MessageHandler(
            (filters.VIDEO | filters.PHOTO) &
            filters.CaptionRegex(r'^\d+') &
            filters.Chat(_gid_int) &
            admin_filter,
            handle_media
        ), group=0)
        
    app.add_handler(MessageHandler(
        (filters.VIDEO | filters.PHOTO | filters.FORWARDED) &
        admin_filter &
        filters.ChatType.PRIVATE,
        handle_media
    ), group=0)
    
    if _gid_int:
        app.add_handler(MessageHandler(
            filters.Regex(r'^\d+') & filters.Chat(_gid_int) & admin_filter,
            handle_text_hashtag
        ))

    app.add_handler(MessageHandler(
        filters.REPLY & filters.TEXT & admin_filter & filters.ChatType.PRIVATE,
        handle_ban_time
    ), group=0)

    # Callback
    app.add_handler(CallbackQueryHandler(callback_handler))

    # ChatMember
    app.add_handler(ChatMemberHandler(chat_member_updated, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(
        filters.StatusUpdate.NEW_CHAT_MEMBERS,
        new_chat_members_fallback
    ), group=1)

    # ChatJoinRequest
    app.add_handler(ChatJoinRequestHandler(join_request_handler))

    # Non-admin lenh
    app.add_handler(MessageHandler(filters.COMMAND & ~admin_filter, no_permission))

    # Global error handler
    async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
        import traceback as _tb
        tb_str = "".join(_tb.format_exception(
            None, context.error, context.error.__traceback__
        ))
        logging.error(f"=== UNHANDLED EXCEPTION ===\n{tb_str}")
        if LOG_GROUP_ID:
            try:
                await app.bot.send_message(
                    chat_id=LOG_GROUP_ID,
                    text=(f"Lỗi hệ thống\n"
                          f"<code>{sanitize(str(context.error))}</code>\n\n"
                          f"Chi tiết:\n"
                          f"<code>{sanitize(tb_str, max_len=3000)}</code>\n"
                          f"Thời gian: {now_str()}"),
                    parse_mode="HTML"
                )
            except Exception: pass
    app.add_error_handler(global_error_handler)

    async with app:
        mongo_client = await setup_db(app)
        await start_web_server(mongo_client, app)
        
        # Start Worker Threads in Background
        asyncio.create_task(expire_worker(app))
        asyncio.create_task(unban_worker(app))
        asyncio.create_task(vip_worker(app))
        
        # Keep process running
        await asyncio.Event().wait()
