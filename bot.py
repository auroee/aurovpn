# aurovpn.py — Auro VPN v26.0
# Single-file production bot: Telegram + FastAPI + SQLAlchemy Core + AdaptGroup + Platega(stub)
import asyncio, json, hmac, hashlib, logging, os, re, secrets, contextlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Any

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    CopyTextButton, Update,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from fastapi import FastAPI, Request, Response, HTTPException
from sqlalchemy import (
    text, select, update, insert, BigInteger, String, Numeric, Boolean, DateTime,
    Integer, UniqueConstraint, ForeignKey, MetaData, Table, Column, func, and_, or_,
    delete,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import IntegrityError

# ==============================================================================
# CONFIG
# ==============================================================================
def env(k: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(k, default)
    if required and not v:
        raise SystemExit(f"ENV {k} is required")
    return v or ""

def env_bool(k: str, default: str = "false") -> bool:
    return env(k, default).lower() in ("1", "true", "yes", "on")

def env_int(k: str, default: str) -> int:
    try:
        return int(env(k, default))
    except ValueError:
        return int(default)

def env_dec(k: str, default: str) -> Decimal:
    try:
        return Decimal(env(k, default))
    except Exception:
        return Decimal(default)

CFG = dict(
    BOT_TOKEN=env("BOT_TOKEN", required=True),
    BOT_USERNAME=env("BOT_USERNAME", "aurovpn_bot").lstrip("@"),
    WEBHOOK_SECRET_TG=env("WEBHOOK_SECRET_TG", "tg-secret"),
    ADMIN_IDS=[int(x) for x in env("ADMIN_IDS", "").replace(" ", "").split(",") if x.strip().isdigit()],
    OWNER_ID=int(env("OWNER_ID", "0") or "0"),
    DATABASE_URL=env("DATABASE_URL", "sqlite+aiosqlite:///./auro.db"),
    PUBLIC_BASE_URL=env("PUBLIC_BASE_URL", "https://aurovpn.cc.cd").rstrip("/"),
    TG_PATH=env("TELEGRAM_WEBHOOK_PATH", "/tg/webhook"),
    PG_PATH=env("PLATEGA_WEBHOOK_PATH", "/platega/webhook"),
    AG_PATH=env("ADAPTGROUP_WEBHOOK_PATH", "/adaptgroup/webhook"),
    HOST=env("WEBHOOK_HOST", "0.0.0.0"),
    PORT=env_int("WEBHOOK_PORT", "8080"),

    # Platega (заглушка, но с сохранением интерфейса для будущего включения)
    PLATEGA_ENABLED=env_bool("PLATEGA_ENABLED", "false"),
    PLATEGA_BASE=env("PLATEGA_BASE_URL", "https://app.platega.io").rstrip("/"),
    PLATEGA_MID=env("PLATEGA_MERCHANT_ID", "stub_mid"),
    PLATEGA_SECRET=env("PLATEGA_SECRET", "stub_secret"),
    PLATEGA_METHOD=env_int("PLATEGA_PAYMENT_METHOD", "2"),
    PLATEGA_RETURN=env("PLATEGA_RETURN_URL", "https://t.me/aurovpn_bot"),
    PLATEGA_TIMEOUT=env_int("PLATEGA_HTTP_TIMEOUT", "20"),

    # AdaptGroup
    AG_BASE=env("ADAPT_BASE_URL", "https://network-api.adaptgroup.app").rstrip("/"),
    AG_API_ID=env("ADAPT_API_ID", ""),
    AG_KEY=env("ADAPT_API_KEY", ""),
    AG_SECRET=env("ADAPT_WEBHOOK_SECRET", "ag-secret"),
    AG_IP=env("ADAPT_IP_ALLOW", ""),
    AG_TIMEOUT=env_int("ADAPT_HTTP_TIMEOUT", "25"),
    AG_PLAN_MAP=json.loads(env("ADAPTGROUP_PLAN_MAP", "{}") or "{}"),

    # Product
    REF_PERCENT=env_dec("REFERRAL_PERCENT", "15"),
    REF_HOLD_H=env_int("REFERRAL_HOLD_HOURS", "24"),
    TOPUP_MIN=env_dec("TOPUP_MIN", "50"),
    TOPUP_MAX=env_dec("TOPUP_MAX", "10000"),
    ORDER_TTL=env_int("ORDER_TTL_MINUTES", "20"),
    MAINTENANCE=env_bool("MAINTENANCE", "false"),
    WORKER_INTERVAL=env_int("WORKER_INTERVAL", "15"),

    # Legal
    TERMS_URL=env("TERMS_URL", "https://telegra.ph/Auro-VPN--Usloviya-servisa-11-23"),
    PRIVACY_URL=env("PRIVACY_URL", "https://telegra.ph/Auro-VPN--Konfidencialnost-11-23"),
    TERMS_VERSION=env("TERMS_VERSION", "2024-11-23"),
    PRIVACY_VERSION=env("PRIVACY_VERSION", "2024-11-23"),
    CHANNEL_URL=env("CHANNEL_URL", "https://t.me/aurochannel"),
    SUPPORT_URL=env("SUPPORT_URL", "https://t.me/aurovpn_bot"),
)

UTC = timezone.utc
MSK = timezone(timedelta(hours=3))

logging.basicConfig(
    level=env("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("auro")

# ==============================================================================
# HELPERS
# ==============================================================================
def now() -> datetime: return datetime.now(UTC)
def rub(x) -> Decimal: return Decimal(x).quantize(Decimal("0.01"), ROUND_HALF_UP)
def fmt_rub(x) -> str:
    x = rub(x)
    s = f"{x:,.2f}".replace(",", " ").replace(".", ",")
    return s[:-3] + " ₽" if s.endswith(",00") else s + " ₽"
def fmt_dt(dt: Optional[datetime]) -> str:
    if not dt: return "—"
    return dt.astimezone(MSK).strftime("%d.%m.%Y, %H:%M МСК")
def safe(x, n: int = 4000) -> str:
    return (str(x) if x is not None else "")[:n]
def timing_safe_eq(a: str, b: str) -> bool:
    return hmac.compare_digest((a or "").encode(), (b or "").encode())
def new_id() -> str: return secrets.token_hex(16)

# ==============================================================================
# DB
# ==============================================================================
engine = create_async_engine(CFG["DATABASE_URL"], pool_pre_ping=True, future=True)
Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
meta = MetaData()

users = Table("users", meta,
    Column("telegram_id", BigInteger, primary_key=True),
    Column("username", String(64)),
    Column("referrer_id", BigInteger, ForeignKey("users.telegram_id", ondelete="SET NULL")),
    Column("is_terms_accepted", Boolean, default=False, nullable=False),
    Column("terms_accepted_at", DateTime(timezone=True)),
    Column("notify_expiry", Boolean, default=True, nullable=False),
    Column("notify_news", Boolean, default=True, nullable=False),
    Column("notify_offers", Boolean, default=False, nullable=False),
    Column("hub_message_id", BigInteger),
    Column("is_blocked", Boolean, default=False, nullable=False),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
)

user_restrictions = Table("user_restrictions", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", BigInteger, ForeignKey("users.telegram_id", ondelete="CASCADE")),
    Column("kind", String(16), nullable=False),
    Column("reason", String(255), nullable=False),
    Column("until", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
    UniqueConstraint("user_id", "kind", name="uq_restriction_user_kind"),
)

wallets = Table("wallets", meta,
    Column("user_id", BigInteger, ForeignKey("users.telegram_id", ondelete="CASCADE"), primary_key=True),
    Column("deposit_rub", Numeric(12, 2), default=Decimal("0.00"), nullable=False),
    Column("bonus_rub", Numeric(12, 2), default=Decimal("0.00"), nullable=False),
    Column("hold_rub", Numeric(12, 2), default=Decimal("0.00"), nullable=False),
)

plans = Table("plans", meta,
    Column("code", String(16), primary_key=True),
    Column("days", Integer, nullable=False),
    Column("price_rub", Numeric(12, 2), nullable=False),
    Column("label", String(64)),
    Column("enabled", Boolean, default=True, nullable=False),
    Column("sort_order", Integer, default=0, nullable=False),
)

provider_plan_map = Table("provider_plan_map", meta,
    Column("plan_code", String(16), ForeignKey("plans.code"), primary_key=True),
    Column("provider_plan", String(64), nullable=False),
    Column("verified_at", DateTime(timezone=True)),
)

orders = Table("orders", meta,
    Column("id", String(36), primary_key=True),
    Column("user_id", BigInteger, ForeignKey("users.telegram_id")),
    Column("kind", String(12), nullable=False),
    Column("plan_code", String(16)),
    Column("amount_rub", Numeric(12, 2), nullable=False),
    Column("days_snapshot", Integer),
    Column("status", String(20), default="DRAFT", nullable=False),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
    Column("expires_at", DateTime(timezone=True)),
)

payment_attempts = Table("payment_attempts", meta,
    Column("id", String(36), primary_key=True),
    Column("order_id", String(36), ForeignKey("orders.id")),
    Column("provider", String(16), nullable=False),
    Column("external_id", String(128)),
    Column("payment_url", String(512)),
    Column("status", String(16), default="CREATED", nullable=False),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
)

payment_events = Table("payment_events", meta,
    Column("id", String(36), primary_key=True),
    Column("source", String(16), nullable=False),
    Column("external_id", String(128), nullable=False),
    Column("status", String(32), nullable=False),
    Column("body", String),
    Column("processed", Boolean, default=False, nullable=False),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
    UniqueConstraint("source", "external_id", "status", name="uq_event"),
)

ledger = Table("ledger", meta,
    Column("id", String(36), primary_key=True),
    Column("user_id", BigInteger, ForeignKey("users.telegram_id")),
    Column("kind", String(24), nullable=False),
    Column("amount_rub", Numeric(12, 2), nullable=False),
    Column("meta", String),
    Column("idem_key", String(128), unique=True),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
)

subscriptions = Table("subscriptions", meta,
    Column("user_id", BigInteger, ForeignKey("users.telegram_id"), primary_key=True),
    Column("external_id", String(128), unique=True),
    Column("subscription_url", String(512)),
    Column("plan_code", String(16)),
    Column("expires_at", DateTime(timezone=True)),
    Column("devices_limit", Integer),
    Column("status", String(16), default="PROVISIONING", nullable=False),
    Column("last_sync", DateTime(timezone=True)),
)

fulfillment_jobs = Table("fulfillment_jobs", meta,
    Column("id", String(36), primary_key=True),
    Column("order_id", String(36), ForeignKey("orders.id"), unique=True),
    Column("state", String(16), default="QUEUED", nullable=False),
    Column("attempts", Integer, default=0, nullable=False),
    Column("last_error", String(255)),
    Column("updated_at", DateTime(timezone=True), default=now, nullable=False),
)

ref_codes = Table("ref_codes", meta,
    Column("code", String(16), primary_key=True),
    Column("user_id", BigInteger, ForeignKey("users.telegram_id"), unique=True),
)

referrals = Table("referrals", meta,
    Column("invited_id", BigInteger, ForeignKey("users.telegram_id"), primary_key=True),
    Column("inviter_id", BigInteger, ForeignKey("users.telegram_id")),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
)

referral_rewards = Table("referral_rewards", meta,
    Column("id", String(36), primary_key=True),
    Column("inviter_id", BigInteger, ForeignKey("users.telegram_id")),
    Column("invited_id", BigInteger, ForeignKey("users.telegram_id")),
    Column("external_id", String(128)),
    Column("base_rub", Numeric(12, 2), nullable=False),
    Column("percent", Numeric(6, 2), nullable=False),
    Column("amount_rub", Numeric(12, 2), nullable=False),
    Column("status", String(16), default="PENDING", nullable=False),
    Column("available_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
    UniqueConstraint("invited_id", "external_id", name="uq_reward_per_payment"),
)

tickets = Table("tickets", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", BigInteger, ForeignKey("users.telegram_id")),
    Column("subject", String(32), default="GENERAL"),
    Column("status", String(12), default="OPEN"),
    Column("order_id", String(36)),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
)

ticket_messages = Table("ticket_messages", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticket_id", Integer, ForeignKey("tickets.id", ondelete="CASCADE")),
    Column("sender", String(8), nullable=False),
    Column("text", String(4000), nullable=False),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
)

admin_audit = Table("admin_audit", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("admin_id", BigInteger, nullable=False),
    Column("action", String(64), nullable=False),
    Column("target", String(64)),
    Column("details", String(2000)),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
)

SEED_PLANS = [
    ("plan_14d", 14, Decimal("149.00"), "14 дней", 10),
    ("plan_1m", 30, Decimal("299.00"), "30 дней", 20),
    ("plan_3m", 90, Decimal("799.00"), "90 дней", 30),
    ("plan_6m", 180, Decimal("1490.00"), "180 дней", 40),
    ("plan_1y", 365, Decimal("2290.00"), "365 дней", 50),
]

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(meta.create_all)
        for code, days, price, label, sort in SEED_PLANS:
            ex = await conn.execute(select(plans.c.code).where(plans.c.code == code))
            if not ex.first():
                await conn.execute(insert(plans).values(
                    code=code, days=days, price_rub=price, label=label,
                    enabled=True, sort_order=sort,
                ))
        for code, provider in CFG["AG_PLAN_MAP"].items():
            ex = await conn.execute(select(provider_plan_map.c.plan_code).where(provider_plan_map.c.plan_code == code))
            if not ex.first():
                await conn.execute(insert(provider_plan_map).values(plan_code=code, provider_plan=provider))

# ==============================================================================
# SERVICES
# ==============================================================================
class WalletSvc:
    @staticmethod
    async def ensure(s: AsyncSession, uid: int):
        r = await s.execute(select(wallets.c.user_id).where(wallets.c.user_id == uid))
        if not r.first():
            try:
                await s.execute(insert(wallets).values(user_id=uid))
            except IntegrityError:
                pass

    @staticmethod
    async def get(s: AsyncSession, uid: int):
        await WalletSvc.ensure(s, uid)
        r = await s.execute(select(wallets).where(wallets.c.user_id == uid))
        return r.mappings().first()

    @staticmethod
    async def available(s: AsyncSession, uid: int) -> Decimal:
        w = await WalletSvc.get(s, uid)
        return rub(Decimal(w["deposit_rub"]) + Decimal(w["bonus_rub"]) - Decimal(w["hold_rub"]))


class LedgerSvc:
    @staticmethod
    async def add(s: AsyncSession, uid: int, kind: str, amount: Decimal, idem_key: str, meta: str = "") -> bool:
        ex = await s.execute(select(ledger.c.id).where(ledger.c.idem_key == idem_key))
        if ex.first():
            return False
        try:
            await s.execute(insert(ledger).values(
                id=new_id(), user_id=uid, kind=kind, amount_rub=rub(amount),
                meta=meta, idem_key=idem_key,
            ))
        except IntegrityError:
            return False
        return True


class PlategaStub:
    """Заглушка Platega. Возвращает фиктивный URL и мгновенно эмулирует оплату
    через кнопку в боте. Когда будет настоящий ключ — переключить PLATEGA_ENABLED=true
    и восстановить реализации из комментариев."""
    @staticmethod
    async def create_payment(order_id: str, amount: Decimal, description: str) -> dict:
        ext = f"stub_{order_id[:12]}_{new_id()[:8]}"
        url = f"https://pay.stub.local/{ext}"
        return {"transactionId": ext, "paymentUrl": url, "status": "PENDING"}

    @staticmethod
    async def get_transaction(external_id: str) -> dict:
        # заглушка всегда «подтверждает» при проверке
        return {"status": "CONFIRMED", "transactionId": external_id}

# Раскомментировать и переключить PLATEGA_ENABLED=true при подключении:
# class Platega:
#     @staticmethod
#     async def create_payment(order_id, amount, description):
#         headers = {"X-MerchantId": CFG["PLATEGA_MID"], "X-Secret": CFG["PLATEGA_SECRET"]}
#         payload = {"paymentMethod": CFG["PLATEGA_METHOD"], "amount": float(amount),
#                    "currency": "RUB", "description": description,
#                    "orderId": order_id, "returnUrl": CFG["PLATEGA_RETURN"]}
#         async with httpx.AsyncClient(timeout=CFG["PLATEGA_TIMEOUT"]) as c:
#             r = await c.post(f"{CFG['PLATEGA_BASE']}/transaction/process", json=payload, headers=headers)
#             r.raise_for_status()
#             return r.json()

Platega = PlategaStub


class AdaptGroup:
    @staticmethod
    async def _h() -> dict:
        h = {"X-Api-Key": CFG["AG_KEY"]}
        if CFG["AG_API_ID"]:
            h["X-Api-Id"] = CFG["AG_API_ID"]
        return h

    @staticmethod
    async def create_subscription(user_id: int, plan_code: str, days: int, order_id: str) -> dict:
        provider_plan = CFG["AG_PLAN_MAP"].get(plan_code)
        if not provider_plan:
            raise RuntimeError(f"plan '{plan_code}' not mapped to provider plan")
        payload = {
            "plan": provider_plan,
            "externalUserId": str(user_id),
            "idempotencyKey": f"order-{order_id}",
            "durationDays": days,
        }
        async with httpx.AsyncClient(timeout=CFG["AG_TIMEOUT"]) as c:
            r = await c.post(f"{CFG['AG_BASE']}/api/v1/subscriptions",
                             json=payload, headers=await AdaptGroup._h())
            if r.status_code >= 400:
                raise RuntimeError(f"AG create failed {r.status_code}: {r.text[:300]}")
            return r.json()

    @staticmethod
    async def get_subscription(external_id: str) -> dict:
        async with httpx.AsyncClient(timeout=CFG["AG_TIMEOUT"]) as c:
            r = await c.get(f"{CFG['AG_BASE']}/api/v1/subscriptions/{external_id}",
                            headers=await AdaptGroup._h())
            if r.status_code >= 400:
                raise RuntimeError(f"AG get failed {r.status_code}: {r.text[:300]}")
            return r.json()

    @staticmethod
    async def extend_subscription(external_id: str, extra_days: int) -> dict:
        async with httpx.AsyncClient(timeout=CFG["AG_TIMEOUT"]) as c:
            r = await c.post(f"{CFG['AG_BASE']}/api/v1/subscriptions/{external_id}/extend",
                             json={"days": extra_days}, headers=await AdaptGroup._h())
            if r.status_code >= 400:
                raise RuntimeError(f"AG extend failed {r.status_code}: {r.text[:300]}")
            return r.json()

# ==============================================================================
# UI BUILDERS
# ==============================================================================
def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)

def btn_url(text: str, url: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, url=url)

def btn_copy(text: str, payload: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, copy_text=CopyTextButton(text=payload))

def kb(*rows: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[list(r) for r in rows])

async def get_user(s: AsyncSession, uid: int):
    r = await s.execute(select(users).where(users.c.telegram_id == uid))
    return r.mappings().first()

async def current_sub(s: AsyncSession, uid: int):
    r = await s.execute(select(subscriptions).where(subscriptions.c.user_id == uid))
    return r.mappings().first()

async def is_restricted(s: AsyncSession, uid: int, kind: str) -> Optional[str]:
    r = await s.execute(select(user_restrictions.c.reason).where(and_(
        user_restrictions.c.user_id == uid,
        user_restrictions.c.kind == kind,
        or_(user_restrictions.c.until.is_(None), user_restrictions.c.until > now()),
    )))
    row = r.first()
    return row[0] if row else None

async def get_plans(s: AsyncSession) -> list:
    r = await s.execute(select(plans).where(plans.c.enabled == True).order_by(plans.c.sort_order))
    return r.mappings().all()

# ==============================================================================
# HUB / SCREEN SYSTEM
# ==============================================================================
async def render_hub(s: AsyncSession, uid: int) -> tuple[str, InlineKeyboardMarkup]:
    u = await get_user(s, uid)
    w = await WalletSvc.get(s, uid)
    sub = await current_sub(s, uid)
    avail = rub(Decimal(w["deposit_rub"]) + Decimal(w["bonus_rub"]) - Decimal(w["hold_rub"]))

    if CFG["MAINTENANCE"]:
        return (
            "🛠 <b>Технические работы</b>\n\n"
            "Сервис временно недоступен. Новые покупки приостановлены.\n"
            "Оплаченные заказы обрабатываются в обычном режиме.",
            kb([btn("Поддержка", "support"), btn("Мои заказы", "orders")]),
        )

    if sub and sub["status"] == "ACTIVE" and sub["expires_at"] and sub["expires_at"] > now():
        left = sub["expires_at"] - now()
        left_str = f"{int(left.total_seconds() // 86400)} дн." if left.days else f"{int(left.total_seconds() // 3600)} ч"
        text = (
            f"🛡 <b>Auro VPN</b>\n\n"
            f"✅ Подписка активна\n"
            f"📅 До {fmt_dt(sub['expires_at'])} · осталось {left_str}\n"
            f"💰 Баланс: {fmt_rub(avail)}\n"
            f"📱 Устройств: {sub['devices_limit'] or '—'}"
        )
        rows = [
            [btn("🔗 Подключиться", "connect")],
            [btn("🔄 Продлить", "buy"), btn("👤 Кабинет", "cabinet")],
            [btn("🎁 Пригласить", "ref"), btn("💬 Поддержка", "support")],
        ]
    else:
        text = (
            f"🛡 <b>Auro VPN</b>\n\n"
            f"Подписка не оформлена.\n"
            f"💰 Баланс: {fmt_rub(avail)}\n\n"
            f"Выберите срок и подключитесь за минуту —\n"
            f"телефон, компьютер, планшет."
        )
        rows = [
            [btn("🚀 Оформить подписку", "buy")],
            [btn("👤 Кабинет", "cabinet"), btn("💬 Поддержка", "support")],
            [btn("🎁 Пригласить друзей", "ref"), btn("ℹ️ О сервисе", "about")],
        ]
    return text, kb(*rows)

async def _try_edit(bot: Bot, uid: int, mid: int, text: str, markup: InlineKeyboardMarkup) -> bool:
    try:
        await bot.edit_message_text(text=text, chat_id=uid, message_id=mid,
                                    reply_markup=markup, parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True)
        return True
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return True
        return False
    except Exception:
        return False

async def show_screen(bot: Bot, s: AsyncSession, uid: int, text: str, markup: InlineKeyboardMarkup, force_new: bool = False):
    u = await get_user(s, uid)
    mid = u["hub_message_id"] if u else None
    if mid and not force_new:
        if await _try_edit(bot, uid, mid, text, markup):
            return
        # не смогли отредактировать — удаляем старое и шлём новое
        with contextlib.suppress(Exception):
            await bot.delete_message(uid, mid)
    m = await bot.send_message(uid, text, reply_markup=markup, parse_mode=ParseMode.HTML,
                               disable_web_page_preview=True)
    await s.execute(update(users).where(users.c.telegram_id == uid).values(hub_message_id=m.message_id))
    await s.commit()

# ==============================================================================
# FSM
# ==============================================================================
class FS(StatesGroup):
    topup_amount = State()
    support_text = State()
    support_reply = State()
    ticket_admin_reply = State()
    admin_search = State()
    admin_give_balance = State()
    admin_ban_reason = State()

# ==============================================================================
# ROUTER
# ==============================================================================
router = Router()

# ---------- helpers used by handlers ----------
async def ensure_user(s: AsyncSession, tg_user, ref_code: Optional[str]) -> dict:
    uid = tg_user.id
    u = await get_user(s, uid)
    if u:
        if u["username"] != tg_user.username:
            await s.execute(update(users).where(users.c.telegram_id == uid)
                            .values(username=tg_user.username))
        return dict(u)
    ref_uid = None
    if ref_code:
        r = await s.execute(select(ref_codes.c.user_id).where(ref_codes.c.code == ref_code))
        row = r.first()
        if row and row[0] != uid:
            ref_uid = row[0]
    await s.execute(insert(users).values(
        telegram_id=uid, username=tg_user.username, referrer_id=ref_uid,
    ))
    await WalletSvc.ensure(s, uid)
    if ref_uid:
        try:
            await s.execute(insert(referrals).values(invited_id=uid, inviter_id=ref_uid))
        except IntegrityError:
            pass
    return dict(await get_user(s, uid))

def is_admin(uid: int) -> bool:
    return uid in CFG["ADMIN_IDS"] or (CFG["OWNER_ID"] and uid == CFG["OWNER_ID"])

async def notify_admins(bot: Bot, text: str, markup: InlineKeyboardMarkup | None = None):
    for aid in set(CFG["ADMIN_IDS"] + ([CFG["OWNER_ID"]] if CFG["OWNER_ID"] else [])):
        with contextlib.suppress(Exception):
            await bot.send_message(aid, text, reply_markup=markup, parse_mode=ParseMode.HTML)

async def audit(s: AsyncSession, admin_id: int, action: str, target: str = "", details: str = ""):
    await s.execute(insert(admin_audit).values(
        admin_id=admin_id, action=action, target=target, details=safe(details, 2000),
    ))

# ---------- /start ----------
@router.message(CommandStart(deep_link=True))
async def cmd_start_ref(m: Message, command: CommandObject, state: FSMContext, bot: Bot):
    await _start(m, state, bot, ref_code=_extract_ref(command.args))

@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext, bot: Bot):
    await _start(m, state, bot, ref_code=None)

def _extract_ref(arg: Optional[str]) -> Optional[str]:
    if not arg:
        return None
    arg = arg.strip()
    if arg.startswith("ref_"):
        return arg[4:]
    return arg or None

async def _start(m: Message, state: FSMContext, bot: Bot, ref_code: Optional[str]):
    if m.chat.type != ChatType.PRIVATE:
        return
    await state.clear()
    async with Session() as s:
        u = await ensure_user(s, m.from_user, ref_code)
        if u["is_blocked"]:
            await m.answer("🚫 Аккаунт заблокирован. Напишите в поддержку.")
            return
        if not u["is_terms_accepted"]:
            txt = (
                "👋 <b>Добро пожаловать в Auro VPN</b>\n\n"
                "VPN для телефона и компьютера — без рекламы и логов.\n\n"
                "Перед началом — коротко про правила:\n"
                "• подписка личная, ссылку не передавайте\n"
                "• возврат — только при технической неисправности\n"
                "• поддержка — в боте\n\n"
                "Продолжая, вы принимаете условия."
            )
            mk = kb(
                [btn_url("📄 Условия", CFG["TERMS_URL"]),
                 btn_url("🔒 Конфиденциальность", CFG["PRIVACY_URL"])],
                [btn("✅ Принять и продолжить", "terms_accept")],
                [btn("💬 Задать вопрос", "support")],
            )
            await show_screen(bot, s, m.from_user.id, txt, mk, force_new=True)
            await s.commit()
            return
        if is_admin(m.from_user.id) and CFG["MAINTENANCE"]:
            # админам показываем hub, но с плашкой
            pass
        text, mk = await render_hub(s, m.from_user.id)
        await show_screen(bot, s, m.from_user.id, text, mk, force_new=True)
        await s.commit()

# ---------- /cancel ----------
@router.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext, bot: Bot):
    if await state.get_state():
        await state.clear()
        await m.answer("Действие отменено.")
    async with Session() as s:
        text, mk = await render_hub(s, m.from_user.id)
        await show_screen(bot, s, m.from_user.id, text, mk)
        await s.commit()

# ---------- /admin ----------
@router.message(Command("admin"))
async def cmd_admin(m: Message, state: FSMContext, bot: Bot):
    if not is_admin(m.from_user.id):
        return
    await state.clear()
    async with Session() as s:
        total_users = (await s.execute(select(func.count()).select_from(users))).scalar() or 0
        active_subs = (await s.execute(select(func.count()).select_from(subscriptions).where(
            subscriptions.c.status == "ACTIVE",
            subscriptions.c.expires_at > now(),
        ))).scalar() or 0
        open_tickets = (await s.execute(select(func.count()).select_from(tickets).where(
            tickets.c.status == "OPEN"))).scalar() or 0
        paid_today = (await s.execute(select(func.coalesce(func.sum(orders.c.amount_rub), 0)).where(and_(
            orders.c.status.in_(["PAID", "FULFILLED"]),
            orders.c.created_at > now() - timedelta(days=1),
            orders.c.kind == "SUB",
        )))).scalar() or Decimal("0")
        text = (
            f"⚙️ <b>Админ-панель</b>\n\n"
            f"👥 Юзеров: {total_users}\n"
            f"✅ Активных подписок: {active_subs}\n"
            f"🎫 Открытых тикетов: {open_tickets}\n"
            f"💵 Оплачено за сутки: {fmt_rub(paid_today)}\n"
            f"🛠 Режим тех.работ: {'вкл' if CFG['MAINTENANCE'] else 'выкл'}"
        )
        mk = kb(
            [btn("🔍 Найти пользователя", "admin_search")],
            [btn("🎫 Тикеты", "admin_tickets")],
            [btn("📦 Заказы (ручные)", "admin_orders")],
            [btn("🎁 Выдать баланс", "admin_give_balance")],
            [btn("📢 Рассылка", "admin_broadcast")],
            [btn("❌ Закрыть", "admin_close")],
        )
        await bot.send_message(m.from_user.id, text, reply_markup=mk, parse_mode=ParseMode.HTML)

# ---------- terms ----------
@router.callback_query(F.data == "terms_accept")
async def accept_terms(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        await s.execute(update(users).where(users.c.telegram_id == cq.from_user.id).values(
            is_terms_accepted=True, terms_accepted_at=now(),
        ))
        await s.commit()
        text, mk = await render_hub(s, cq.from_user.id)
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer("Готово")

# ---------- menu ----------
@router.callback_query(F.data == "menu")
async def menu_cb(cq: CallbackQuery, bot: Bot, state: FSMContext):
    await state.clear()
    async with Session() as s:
        text, mk = await render_hub(s, cq.from_user.id)
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

# ---------- connect / guide ----------
@router.callback_query(F.data == "connect")
async def connect_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        sub = await current_sub(s, cq.from_user.id)
        if not sub or not sub["subscription_url"] or sub["status"] != "ACTIVE" or (
                sub["expires_at"] and sub["expires_at"] <= now()):
            text = "🔒 <b>Доступ ещё не готов</b>\n\nПодписка не активна.\nЕсли оплата прошла — подождите 30 секунд."
            mk = kb([btn("🔄 Проверить", "connect")], [btn("⬅️ В меню", "menu")])
        else:
            url = sub["subscription_url"]
            text = (
                f"🔗 <b>Подключение</b>\n\n"
                f"📅 Активна до {fmt_dt(sub['expires_at'])}\n\n"
                f"Скопируйте ссылку и вставьте в приложение.\n"
                f"Никому не передавайте — она даёт доступ к подписке."
            )
            mk = kb(
                [btn_copy("📋 Скопировать ссылку", url)],
                [btn("📱 Инструкция", "guide")],
                [btn("❓ Не подключается", "connect_help")],
                [btn("⬅️ В меню", "menu")],
            )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

GUIDE_APPS = {
    "android": [("Happ (рекомендуется)", "app:happ_android"), ("v2rayNG", "app:v2rayng"), ("NekoBox (GitHub)", "app:nekobox")],
    "ios":     [("Happ (рекомендуется)", "app:happ_ios"), ("Streisand", "app:streisand"), ("Shadowrocket", "app:shadowrocket")],
    "windows": [("Happ (рекомендуется)", "app:happ_win"), ("Hiddify", "app:hiddify_win"), ("v2rayN (GitHub)", "app:v2rayn")],
    "macos":   [("Happ (рекомендуется)", "app:happ_mac"), ("Hiddify", "app:hiddify_mac"), ("V2Box", "app:v2box")],
}
GUIDE_TITLE = {"android": "Android", "ios": "iPhone и iPad", "windows": "Windows", "macos": "macOS"}

APP_INSTRUCTIONS = {
    "happ_android": ("Android · Happ",
        "1. Установите Happ из Google Play или happ.su.\n"
        "2. Вернитесь в бот и скопируйте ссылку подписки.\n"
        "3. В Happ: «+» → «Подписка» → вставьте ссылку.\n"
        "4. Выберите профиль и включите подключение.",
        "https://happ.su"),
    "v2rayng": ("Android · v2rayNG",
        "1. Установите v2rayNG только с GitHub 2dust/v2rayNG.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. В v2rayNG: «+» → «Импорт из буфера».\n"
        "4. Выберите подписку → «Подключиться».",
        "https://github.com/2dust/v2rayNG/releases"),
    "nekobox": ("Android · NekoBox",
        "1. Установите NekoBox только с GitHub MatsuriDayo/NekoBoxForAndroid.\n"
        "   Версия из Google Play с мая 2024 — non-open-source.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. В NekoBox: «+» → «Импорт из буфера».\n"
        "4. Выберите подписку → «Подключиться».",
        "https://github.com/MatsuriDayo/NekoBoxForAndroid/releases"),
    "happ_ios": ("iOS · Happ",
        "1. Установите Happ из App Store.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. В Happ: «+» → «Подписка» → вставьте ссылку.\n"
        "4. Включите подключение.",
        "https://happ.su"),
    "streisand": ("iOS · Streisand",
        "1. Установите Streisand из App Store.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://apps.apple.com/app/streisand/id6450534064"),
    "shadowrocket": ("iOS · Shadowrocket",
        "1. Установите Shadowrocket из App Store (платно).\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://apps.apple.com/app/shadowrocket/id932747118"),
    "happ_win": ("Windows · Happ",
        "1. Установите Happ с happ.su или Microsoft Store.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. «+» → «Подписка» → вставьте ссылку.\n"
        "4. Выберите профиль → «Подключиться».",
        "https://happ.su"),
    "hiddify_win": ("Windows · Hiddify",
        "1. Установите Hiddify с hiddify.com.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://hiddify.com"),
    "v2rayn": ("Windows · v2rayN",
        "1. Установите v2rayN только с GitHub 2dust/v2rayN.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. «Подписки» → «Добавить» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://github.com/2dust/v2rayN/releases"),
    "happ_mac": ("macOS · Happ",
        "1. Установите Happ из App Store или happ.su.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. «+» → «Подписка» → вставьте ссылку.\n"
        "4. Включите подключение.",
        "https://happ.su"),
    "hiddify_mac": ("macOS · Hiddify",
        "1. Установите Hiddify с hiddify.com.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://hiddify.com"),
    "v2box": ("macOS · V2Box",
        "1. Установите V2Box из App Store.\n"
        "2. Скопируйте ссылку из бота.\n"
        "3. «+» → «Импорт из буфера».\n"
        "4. Выберите подписку → «Подключиться».",
        "https://apps.apple.com/app/v2box-v2ray-client/id6446814690"),
}

@router.callback_query(F.data == "guide")
async def guide_platforms(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        text = "📱 <b>На каком устройстве подключаемся?</b>"
        mk = kb(
            [btn("🤖 Android", "guide:android"), btn("🍎 iPhone/iPad", "guide:ios")],
            [btn("🪟 Windows", "guide:windows"), btn("💻 macOS", "guide:macos")],
            [btn("⬅️ Назад", "connect")],
        )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

@router.callback_query(F.data.startswith("guide:"))
async def guide_apps(cq: CallbackQuery, bot: Bot):
    platform = cq.data.split(":", 1)[1]
    if platform not in GUIDE_APPS:
        await cq.answer()
        return
    async with Session() as s:
        text = f"<b>{GUIDE_TITLE[platform]}</b>\n\nВыберите приложение."
        rows = [[btn(name, cb)] for name, cb in GUIDE_APPS[platform]]
        rows.append([btn("⬅️ Назад", "guide")])
        await show_screen(bot, s, cq.from_user.id, text, kb(*rows))
        await s.commit()
    await cq.answer()

@router.callback_query(F.data.startswith("app:"))
async def app_instr(cq: CallbackQuery, bot: Bot):
    key = cq.data.split(":", 1)[1]
    if key not in APP_INSTRUCTIONS:
        await cq.answer("Инструкция недоступна", show_alert=True)
        return
    title, body, store = APP_INSTRUCTIONS[key]
    async with Session() as s:
        sub = await current_sub(s, cq.from_user.id)
        url = sub["subscription_url"] if sub and sub["subscription_url"] else ""
        text = f"<b>{title}</b>\n\n{body}"
        rows = []
        if url:
            rows.append([btn_copy("📋 Скопировать ссылку", url)])
        rows.append([btn_url("⬇️ Установить", store)])
        rows.append([btn("Другое приложение", "guide"), btn("⬅️ Назад", "connect")])
        await show_screen(bot, s, cq.from_user.id, text, kb(*rows))
        await s.commit()
    await cq.answer()

@router.callback_query(F.data == "connect_help")
async def connect_help(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        text = (
            "❓ <b>Не подключается</b>\n\n"
            "1. Проверьте срок подписки в меню.\n"
            "2. Обновите подписку в приложении (refresh).\n"
            "3. Отключите другие VPN.\n"
            "4. Попробуйте другую сеть (Wi-Fi ↔ мобильная).\n\n"
            "Если не помогло — напишите в поддержку."
        )
        mk = kb(
            [btn("💬 В поддержку", "support")],
            [btn("📱 Инструкция", "guide"), btn("⬅️ Назад", "connect")],
        )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

# ---------- buy ----------
@router.callback_query(F.data == "buy")
async def buy_cb(cq: CallbackQuery, bot: Bot):
    if CFG["MAINTENANCE"]:
        await cq.answer("Сервис на тех.обслуживании", show_alert=True)
        return
    async with Session() as s:
        blocked = await is_restricted(s, cq.from_user.id, "BUY")
        if blocked:
            await cq.answer(f"Покупки ограничены: {blocked}", show_alert=True)
            return
        ps = await get_plans(s)
        if not ps:
            await cq.answer("Тарифы временно недоступны", show_alert=True)
            return
        rows = [[btn(f"{p['label'] or p['days']} · {fmt_rub(p['price_rub'])}", f"plan:{p['code']}")] for p in ps]
        rows.append([btn("⬅️ В меню", "menu")])
        text = (
            "📦 <b>Выберите срок подписки</b>\n\n"
            "При продлении оставшиеся дни сохраняются.\n"
            "Оплата через СБП, без автосписаний."
        )
        await show_screen(bot, s, cq.from_user.id, text, kb(*rows))
        await s.commit()
    await cq.answer()

@router.callback_query(F.data.startswith("plan:"))
async def plan_pick(cq: CallbackQuery, bot: Bot):
    code = cq.data.split(":", 1)[1]
    async with Session() as s:
        p = (await s.execute(select(plans).where(plans.c.code == code))).mappings().first()
        if not p or not p["enabled"]:
            await cq.answer("Тариф недоступен", show_alert=True)
            return
        avail = await WalletSvc.available(s, cq.from_user.id)
        rows = []
        if avail >= p["price_rub"]:
            rows.append([btn(f"💳 Списать {fmt_rub(p['price_rub'])} с баланса", f"pay_balance:{code}")])
        rows.append([btn(f"📲 Оплатить {fmt_rub(p['price_rub'])} через СБП", f"pay_sbp:{code}")])
        if avail < p["price_rub"]:
            rows.append([btn("💰 Пополнить баланс", "topup_menu")])
        rows.append([btn("⬅️ К тарифам", "buy")])
        text = (
            f"<b>{p['label'] or p['days']}</b>\n\n"
            f"Стоимость: {fmt_rub(p['price_rub'])}\n"
            f"На балансе: {fmt_rub(avail)}\n\n"
            f"Оставшиеся дни добавятся к новому сроку."
        )
        await show_screen(bot, s, cq.from_user.id, text, kb(*rows))
        await s.commit()
    await cq.answer()

# ---------- order helpers ----------
async def create_order(s: AsyncSession, uid: int, kind: str, plan_code: Optional[str],
                       amount: Decimal, days: Optional[int]) -> str:
    oid = secrets.token_hex(16)
    await s.execute(insert(orders).values(
        id=oid, user_id=uid, kind=kind, plan_code=plan_code,
        amount_rub=rub(amount), days_snapshot=days, status="PENDING",
        expires_at=now() + timedelta(minutes=CFG["ORDER_TTL"]),
    ))
    return oid

# ---------- pay by balance ----------
@router.callback_query(F.data.startswith("pay_balance:"))
async def pay_balance(cq: CallbackQuery, bot: Bot):
    code = cq.data.split(":", 1)[1]
    async with Session() as s:
        p = (await s.execute(select(plans).where(plans.c.code == code))).mappings().first()
        if not p:
            await cq.answer("Тариф недоступен", show_alert=True); return
        w = await WalletSvc.get(s, cq.from_user.id)
        avail = rub(Decimal(w["deposit_rub"]) + Decimal(w["bonus_rub"]) - Decimal(w["hold_rub"]))
        if avail < p["price_rub"]:
            await cq.answer("Недостаточно средств", show_alert=True); return
        # списание: сначала бонусы, потом депозит
        price = Decimal(p["price_rub"])
        bonus_use = min(Decimal(w["bonus_rub"]), price)
        deposit_use = price - bonus_use
        await s.execute(update(wallets).where(wallets.c.user_id == cq.from_user.id).values(
            bonus_rub=Decimal(w["bonus_rub"]) - bonus_use,
            deposit_rub=Decimal(w["deposit_rub"]) - deposit_use,
        ))
        oid = await create_order(s, cq.from_user.id, "SUB", code, price, p["days"])
        await LedgerSvc.add(s, cq.from_user.id, "SUB_PAYMENT", -price, f"order:{oid}", f"plan={code}")
        await s.execute(update(orders).where(orders.c.id == oid).values(status="PAID"))
        try:
            await s.execute(insert(fulfillment_jobs).values(id=new_id(), order_id=oid, state="QUEUED"))
        except IntegrityError:
            pass
        await s.commit()
        text = "✅ <b>Оплачено с баланса</b>\n\nПодключаем подписку — 10–30 секунд."
        mk = kb([btn("🔄 Обновить", f"order_view:{oid}")], [btn("⬅️ В меню", "menu")])
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer("Оплачено")

# ---------- pay by SBП (stub) ----------
@router.callback_query(F.data.startswith("pay_sbp:"))
async def pay_sbp(cq: CallbackQuery, bot: Bot):
    code = cq.data.split(":", 1)[1]
    async with Session() as s:
        p = (await s.execute(select(plans).where(plans.c.code == code))).mappings().first()
        if not p:
            await cq.answer("Тариф недоступен", show_alert=True); return
        oid = await create_order(s, cq.from_user.id, "SUB", code, p["price_rub"], p["days"])
        await s.commit()
        try:
            res = await Platega.create_payment(oid, p["price_rub"], f"Подписка {code}")
        except Exception as e:
            log.exception("platega create failed: %s", e)
            await s.execute(update(orders).where(orders.c.id == oid).values(status="CREATION_FAILED"))
            await s.commit()
            text = "⚠️ <b>Платёж не создан</b>\n\nСумма не списана. Попробуйте позже."
            mk = kb([btn("Повторить", f"pay_sbp:{code}")], [btn("⬅️ К тарифам", "buy")])
            await show_screen(bot, s, cq.from_user.id, text, mk)
            await s.commit()
            await cq.answer(); return
        ext = str(res.get("transactionId") or res.get("id") or "")
        url = res.get("paymentUrl") or res.get("url") or ""
        await s.execute(insert(payment_attempts).values(
            id=new_id(), order_id=oid, provider="PLATEGA",
            external_id=ext, payment_url=url, status="PENDING",
        ))
        await s.execute(update(orders).where(orders.c.id == oid).values(status="AWAITING_PAYMENT"))
        await s.commit()
        text = (
            f"💳 <b>Оплата СБП</b>\n\n"
            f"Заказ №{oid[:8]}\n"
            f"Сумма: {fmt_rub(p['price_rub'])}\n\n"
            f"Нажмите «Оплатить» — и после тестового списания вернитесь."
        )
        mk = kb(
            [btn("🟢 Оплатить (тест)", f"stub_pay:{oid}")],
            [btn("🔄 Проверить оплату", f"check_pay:{oid}")],
            [btn("⬅️ В меню", "menu")],
        )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
        if url:
            with contextlib.suppress(Exception):
                await bot.send_message(cq.from_user.id, f"🔗 Ссылка: {url}")
    await cq.answer()

# заглушка: эмулирует мгновенную оплату
@router.callback_query(F.data.startswith("stub_pay:"))
async def stub_pay(cq: CallbackQuery, bot: Bot):
    oid = cq.data.split(":", 1)[1]
    async with Session() as s:
        await finalize_payment(s, bot, oid, "PLATEGA", f"stub_{oid[:12]}", force=True)
        text = "✅ <b>Оплата получена</b>\n\nПодключаем подписку — 10–30 секунд."
        mk = kb([btn("🔄 Обновить", f"order_view:{oid}")], [btn("⬅️ В меню", "menu")])
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer("Оплачено")

@router.callback_query(F.data.startswith("check_pay:"))
async def check_pay(cq: CallbackQuery, bot: Bot):
    oid = cq.data.split(":", 1)[1]
    async with Session() as s:
        pa = (await s.execute(select(payment_attempts).where(payment_attempts.c.order_id == oid)
                              .order_by(payment_attempts.c.created_at.desc()))).mappings().first()
        if not pa:
            await cq.answer("Платёж не найден", show_alert=True); return
        try:
            res = await Platega.get_transaction(pa["external_id"] or "")
        except Exception:
            await cq.answer("Сервис оплаты недоступен. Попробуйте позже.", show_alert=True); return
        status = (res.get("status") or "").upper()
        if status == "CONFIRMED":
            await finalize_payment(s, bot, oid, "PLATEGA", pa["external_id"] or "")
            await s.commit()
            await cq.answer("Оплата подтверждена")
        else:
            await cq.answer("Оплата ещё не поступила")

# ---------- finalize ----------
async def finalize_payment(s: AsyncSession, bot: Bot, order_id: str, source: str,
                           external_id: str, force: bool = False):
    # идемпотентность
    if not force:
        ex = await s.execute(select(payment_events.c.id).where(and_(
            payment_events.c.source == source,
            payment_events.c.external_id == external_id,
            payment_events.c.status == "CONFIRMED",
        )))
        if ex.first():
            return
    try:
        await s.execute(insert(payment_events).values(
            id=new_id(), source=source, external_id=external_id,
            status="CONFIRMED", processed=True,
        ))
    except IntegrityError:
        # событие уже есть — продолжаем без падения
        pass
    await s.execute(update(payment_attempts)
                    .where(payment_attempts.c.external_id == external_id)
                    .values(status="CONFIRMED"))
    o = (await s.execute(select(orders).where(orders.c.id == order_id))).mappings().first()
    if not o:
        return
    if o["status"] in ("PAID", "FULFILLED"):
        await s.commit()
        return
    if o["status"] not in ("PENDING", "AWAITING_PAYMENT", "CREATION_FAILED"):
        await s.commit()
        return
    await s.execute(update(orders).where(orders.c.id == order_id).values(status="PAID"))
    if o["kind"] == "TOPUP":
        await s.execute(update(wallets).where(wallets.c.user_id == o["user_id"]).values(
            deposit_rub=wallets.c.deposit_rub + o["amount_rub"],
        ))
        await LedgerSvc.add(s, o["user_id"], "TOPUP", o["amount_rub"], f"topup:{order_id}")
        # реферальные бонусы с пополнений — НЕ начисляем
    else:
        try:
            await s.execute(insert(fulfillment_jobs).values(id=new_id(), order_id=order_id, state="QUEUED"))
        except IntegrityError:
            pass
        await referral_on_payment(s, o["user_id"], external_id, o["amount_rub"])
    await s.commit()

async def referral_on_payment(s: AsyncSession, invited_id: int, external_id: str, base: Decimal):
    u = (await s.execute(select(users).where(users.c.telegram_id == invited_id))).mappings().first()
    if not u or not u["referrer_id"]:
        return
    amount = rub(base * CFG["REF_PERCENT"] / Decimal(100))
    try:
        await s.execute(insert(referral_rewards).values(
            id=new_id(), inviter_id=u["referrer_id"], invited_id=invited_id,
            external_id=external_id, base_rub=rub(base), percent=CFG["REF_PERCENT"],
            amount_rub=amount, status="PENDING",
            available_at=now() + timedelta(hours=CFG["REF_HOLD_H"]),
        ))
    except IntegrityError:
        pass

# ---------- order views ----------
@router.callback_query(F.data.startswith("order_view:"))
async def order_view(cq: CallbackQuery, bot: Bot):
    oid = cq.data.split(":", 1)[1]
    async with Session() as s:
        o = (await s.execute(select(orders).where(orders.c.id == oid))).mappings().first()
        if not o or o["user_id"] != cq.from_user.id:
            await cq.answer("Заказ недоступен", show_alert=True); return
        sub = await current_sub(s, o["user_id"])
        text = (
            f"📦 <b>Заказ №{oid[:8]}</b>\n\n"
            f"Тип: {'Подписка' if o['kind']=='SUB' else 'Пополнение'}\n"
            f"Сумма: {fmt_rub(o['amount_rub'])}\n"
            f"Статус: <code>{o['status']}</code>"
        )
        rows = []
        if o["status"] == "PAID" and sub and sub["status"] == "ACTIVE":
            text += f"\n\n✅ Подписка активна до {fmt_dt(sub['expires_at'])}."
            rows.append([btn("🔗 Подключиться", "connect")])
        elif o["status"] == "PAID":
            text += "\n\n⏳ Активируем подписку..."
            rows.append([btn("🔄 Обновить", f"order_view:{oid}")])
        elif o["status"] == "AWAITING_PAYMENT":
            rows.append([btn("🔄 Проверить оплату", f"check_pay:{oid}")])
        rows.append([btn("⬅️ Мои заказы", "orders")])
        await show_screen(bot, s, cq.from_user.id, text, kb(*rows))
        await s.commit()
    await cq.answer()

@router.callback_query(F.data == "orders")
async def orders_list(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        r = await s.execute(select(orders).where(orders.c.user_id == cq.from_user.id)
                            .order_by(orders.c.created_at.desc()).limit(10))
        items = r.mappings().all()
        if not items:
            text = "📦 У вас пока нет заказов."
            mk = kb([btn("⬅️ В меню", "menu")])
        else:
            rows = [[btn(f"№{o['id'][:8]} · {fmt_rub(o['amount_rub'])} · {o['status']}",
                         f"order_view:{o['id']}")] for o in items]
            rows.append([btn("⬅️ В меню", "menu")])
            text = "📦 <b>Ваши заказы</b>"
            mk = kb(*rows)
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

# ---------- cabinet ----------
@router.callback_query(F.data == "cabinet")
async def cabinet(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        w = await WalletSvc.get(s, cq.from_user.id)
        avail = rub(Decimal(w["deposit_rub"]) + Decimal(w["bonus_rub"]) - Decimal(w["hold_rub"]))
        text = (
            f"👤 <b>Личный кабинет</b>\n\n"
            f"🆔 <code>{cq.from_user.id}</code>\n"
            f"💳 Внесено: {fmt_rub(w['deposit_rub'])}\n"
            f"🎁 Бонусы: {fmt_rub(w['bonus_rub'])}\n"
            f"💰 Доступно: {fmt_rub(avail)}"
        )
        mk = kb(
            [btn("➕ Пополнить баланс", "topup_menu")],
            [btn("📦 Мои заказы", "orders"), btn("📊 История", "history")],
            [btn("🎁 Пригласить друзей", "ref")],
            [btn("⚙️ Настройки", "settings")],
            [btn("⬅️ В меню", "menu")],
        )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

# ---------- topup ----------
@router.callback_query(F.data == "topup_menu")
async def topup_menu(cq: CallbackQuery, bot: Bot):
    text = (
        f"💰 <b>Пополнение баланса</b>\n\n"
        f"Выберите сумму или введите свою.\n"
        f"Мин. {fmt_rub(CFG['TOPUP_MIN'])} · Макс. {fmt_rub(CFG['TOPUP_MAX'])}"
    )
    mk = kb(
        [btn("150 ₽", "topup:150"), btn("300 ₽", "topup:300"), btn("500 ₽", "topup:500")],
        [btn("1 000 ₽", "topup:1000"), btn("2 500 ₽", "topup:2500")],
        [btn("✍️ Своя сумма", "topup_custom")],
        [btn("⬅️ Назад", "cabinet")],
    )
    async with Session() as s:
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

@router.callback_query(F.data.startswith("topup:"))
async def topup_fixed(cq: CallbackQuery, bot: Bot):
    amount = Decimal(cq.data.split(":", 1)[1])
    await _create_topup(cq.from_user.id, bot, amount)
    await cq.answer()

@router.callback_query(F.data == "topup_custom")
async def topup_custom(cq: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(FS.topup_amount)
    async with Session() as s:
        text = (
            "✍️ <b>Введите сумму</b>\n\n"
            f"От {fmt_rub(CFG['TOPUP_MIN'])} до {fmt_rub(CFG['TOPUP_MAX'])}.\n"
            "Например: 500"
        )
        await show_screen(bot, s, cq.from_user.id, text, kb([btn("❌ Отмена", "topup_menu")]))
        await s.commit()
    await cq.answer()

@router.message(FS.topup_amount)
async def topup_amount(m: Message, state: FSMContext, bot: Bot):
    raw = re.sub(r"[\s\u00A0_]", "", m.text or "")
    if not raw.isdigit():
        await m.answer("Введите целое число.")
        return
    amount = Decimal(raw)
    if amount < CFG["TOPUP_MIN"] or amount > CFG["TOPUP_MAX"]:
        await m.answer(f"Сумма должна быть от {fmt_rub(CFG['TOPUP_MIN'])} до {fmt_rub(CFG['TOPUP_MAX'])}.")
        return
    with contextlib.suppress(Exception):
        await m.delete()
    await state.clear()
    await _create_topup(m.from_user.id, bot, amount)

async def _create_topup(uid: int, bot: Bot, amount: Decimal):
    async with Session() as s:
        oid = await create_order(s, uid, "TOPUP", None, amount, None)
        await s.commit()
        try:
            res = await Platega.create_payment(oid, amount, "Пополнение баланса")
        except Exception:
            await s.execute(update(orders).where(orders.c.id == oid).values(status="CREATION_FAILED"))
            await s.commit()
            await show_screen(bot, s, uid,
                "⚠️ <b>Платёж не создан</b>\n\nПопробуйте позже.",
                kb([btn("⬅️ В кабинет", "cabinet")]))
            await s.commit()
            return
        ext = str(res.get("transactionId") or res.get("id") or "")
        url = res.get("paymentUrl") or res.get("url") or ""
        await s.execute(insert(payment_attempts).values(
            id=new_id(), order_id=oid, provider="PLATEGA",
            external_id=ext, payment_url=url, status="PENDING"))
        await s.execute(update(orders).where(orders.c.id == oid).values(status="AWAITING_PAYMENT"))
        await s.commit()
        text = (
            f"💰 <b>Пополнение</b>\n\n"
            f"Заказ №{oid[:8]}\n"
            f"Сумма: {fmt_rub(amount)}"
        )
        mk = kb(
            [btn("🟢 Оплатить (тест)", f"stub_pay:{oid}")],
            [btn("🔄 Проверить", f"check_pay:{oid}")],
            [btn("⬅️ В кабинет", "cabinet")],
        )
        await show_screen(bot, s, uid, text, mk)
        await s.commit()
        if url:
            with contextlib.suppress(Exception):
                await bot.send_message(uid, f"🔗 Ссылка: {url}")

# ---------- history ----------
@router.callback_query(F.data == "history")
async def history(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        r = await s.execute(select(ledger).where(ledger.c.user_id == cq.from_user.id)
                            .order_by(ledger.c.created_at.desc()).limit(15))
        items = r.mappings().all()
        if not items:
            text = "📊 История пуста."
            mk = kb([btn("⬅️ В кабинет", "cabinet")])
        else:
            lines = ["📊 <b>История операций</b>\n"]
            for it in items:
                sign = "+" if it["amount_rub"] > 0 else "−"
                lines.append(f"{fmt_dt(it['created_at'])[:10]} · {it['kind']} · {sign}{fmt_rub(abs(it['amount_rub']))}")
            text = "\n".join(lines)
            mk = kb([btn("⬅️ В кабинет", "cabinet")])
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

# ---------- referral ----------
@router.callback_query(F.data == "ref")
async def ref_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        rc = (await s.execute(select(ref_codes).where(ref_codes.c.user_id == cq.from_user.id))).mappings().first()
        if not rc:
            code = secrets.token_urlsafe(6)
            try:
                await s.execute(insert(ref_codes).values(code=code, user_id=cq.from_user.id))
            except IntegrityError:
                pass
            await s.commit()
            rc = {"code": code}
        invited = (await s.execute(select(func.count()).select_from(referrals)
                                   .where(referrals.c.inviter_id == cq.from_user.id))).scalar() or 0
        paid = (await s.execute(select(func.count(func.distinct(referral_rewards.c.invited_id)))
                                .where(referral_rewards.c.inviter_id == cq.from_user.id))).scalar() or 0
        avail = (await s.execute(select(func.coalesce(func.sum(referral_rewards.c.amount_rub), 0))
                                 .where(and_(referral_rewards.c.inviter_id == cq.from_user.id,
                                             referral_rewards.c.status == "AVAILABLE")))).scalar()
        pending = (await s.execute(select(func.coalesce(func.sum(referral_rewards.c.amount_rub), 0))
                                   .where(and_(referral_rewards.c.inviter_id == cq.from_user.id,
                                               referral_rewards.c.status == "PENDING")))).scalar()
        link = f"https://t.me/{CFG['BOT_USERNAME']}?start=ref_{rc['code']}"
        text = (
            f"🎁 <b>Приглашайте друзей</b>\n\n"
            f"Вы получаете {CFG['REF_PERCENT']}% с каждой покупки подписки вашим другом.\n"
            f"Бонус становится доступен через {CFG['REF_HOLD_H']} ч и зачисляется на баланс.\n\n"
            f"👥 Приглашено: {invited}\n"
            f"✅ Купили подписку: {paid}\n"
            f"💰 Доступно: {fmt_rub(avail)}\n"
            f"⏳ В ожидании: {fmt_rub(pending)}"
        )
        mk = kb(
            [btn_copy("📋 Скопировать приглашение", link)],
            [btn("⬅️ В меню", "menu")],
        )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

# ---------- settings ----------
@router.callback_query(F.data == "settings")
async def settings_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        u = await get_user(s, cq.from_user.id)
        def t(b): return "✅" if b else "⬜"
        text = (
            "⚙️ <b>Настройки</b>\n\n"
            "Уведомления об оплате, заказах и безопасности приходят всегда."
        )
        mk = kb(
            [btn(f"{t(u['notify_expiry'])} Напоминания о подписке", "notify:expiry")],
            [btn(f"{t(u['notify_news'])} Новости", "notify:news")],
            [btn(f"{t(u['notify_offers'])} Предложения", "notify:offers")],
            [btn_url("📄 Документы", CFG["TERMS_URL"])],
            [btn("⬅️ В кабинет", "cabinet")],
        )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

@router.callback_query(F.data.startswith("notify:"))
async def notify_toggle(cq: CallbackQuery, bot: Bot):
    field = cq.data.split(":", 1)[1]
    col = {"expiry": "notify_expiry", "news": "notify_news", "offers": "notify_offers"}[field]
    async with Session() as s:
        u = await get_user(s, cq.from_user.id)
        await s.execute(update(users).where(users.c.telegram_id == cq.from_user.id).values({col: not u[col]}))
        await s.commit()
    # перерисовываем настройки, но без повторного cq.answer()
    async with Session() as s:
        u = await get_user(s, cq.from_user.id)
        def t(b): return "✅" if b else "⬜"
        text = (
            "⚙️ <b>Настройки</b>\n\n"
            "Уведомления об оплате, заказах и безопасности приходят всегда."
        )
        mk = kb(
            [btn(f"{t(u['notify_expiry'])} Напоминания о подписке", "notify:expiry")],
            [btn(f"{t(u['notify_news'])} Новости", "notify:news")],
            [btn(f"{t(u['notify_offers'])} Предложения", "notify:offers")],
            [btn_url("📄 Документы", CFG["TERMS_URL"])],
            [btn("⬅️ В кабинет", "cabinet")],
        )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer("Сохранено")

# ---------- about ----------
@router.callback_query(F.data == "about")
async def about_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        text = (
            "ℹ️ <b>Auro VPN</b>\n\n"
            "Быстрый и стабильный VPN для телефона, компьютера и планшета.\n"
            "Один аккаунт — все устройства.\n\n"
            "Без логов активности и рекламы."
        )
        mk = kb(
            [btn_url("📢 Канал", CFG["CHANNEL_URL"])],
            [btn_url("📄 Условия", CFG["TERMS_URL"])],
            [btn_url("🔒 Конфиденциальность", CFG["PRIVACY_URL"])],
            [btn("💬 Поддержка", "support"), btn("⬅️ В меню", "menu")],
        )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

# ---------- support ----------
@router.callback_query(F.data == "support")
async def support_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        t = (await s.execute(select(tickets).where(and_(
            tickets.c.user_id == cq.from_user.id,
            tickets.c.status == "OPEN",
        )).order_by(tickets.c.id.desc()).limit(1))).mappings().first()
        if t:
            text = f"🎫 <b>Обращение №{t['id']}</b>\n\nСтатус: в работе"
            mk = kb(
                [btn("📨 Открыть", f"ticket_view:{t['id']}")],
                [btn("✉️ Новое обращение", "ticket_new")],
                [btn("⬅️ В меню", "menu")],
            )
        else:
            text = (
                "💬 <b>Поддержка</b>\n\n"
                "Поможем с оплатой, подключением и доступом.\n"
                "Отвечаем в порядке очереди."
            )
            mk = kb(
                [btn("✍️ Написать в поддержку", "ticket_new")],
                [btn("⬅️ В меню", "menu")],
            )
        await show_screen(bot, s, cq.from_user.id, text, mk)
        await s.commit()
    await cq.answer()

@router.callback_query(F.data == "ticket_new")
async def ticket_new(cq: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(FS.support_text)
    async with Session() as s:
        text = (
            "✍️ <b>Опишите проблему</b>\n\n"
            "Что случилось, когда, что уже пробовали.\n"
            "5–2000 символов.\n\n"
            "⚠️ Не отправляйте банковские данные и личную ссылку подписки."
        )
        await show_screen(bot, s, cq.from_user.id, text, kb([btn("❌ Отмена", "support")]))
        await s.commit()
    await cq.answer()

@router.message(FS.support_text)
async def ticket_text(m: Message, state: FSMContext, bot: Bot):
    text = (m.text or "").strip()
    if not (5 <= len(text) <= 2000):
        await m.answer("Введите от 5 до 2000 символов.")
        return
    async with Session() as s:
        t = await s.execute(insert(tickets).values(user_id=m.from_user.id, subject="GENERAL", status="OPEN")
                            .returning(tickets.c.id))
        tid = t.scalar()
        await s.execute(insert(ticket_messages).values(ticket_id=tid, sender="USER", text=text))
        await s.commit()
        with contextlib.suppress(Exception):
            await m.delete()
        await state.clear()
        await show_screen(bot, s, m.from_user.id,
            f"✅ <b>Обращение №{tid} отправлено</b>\n\nОжидайте ответа.",
            kb([btn("📨 Открыть", f"ticket_view:{tid}")], [btn("⬅️ В меню", "menu")]))
        await s.commit()
    await notify_admins(bot, f"🎫 Новое обращение №{tid} от <code>{m.from_user.id}</code>",
                        kb([btn("Открыть", f"admin_ticket:{tid}")]))

@router.callback_query(F.data.startswith("ticket_view:"))
async def ticket_view(cq: CallbackQuery, bot: Bot):
    tid = int(cq.data.split(":", 1)[1])
    async with Session() as s:
        t = (await s.execute(select(tickets).where(tickets.c.id == tid))).mappings().first()
        if not t or t["user_id"] != cq.from_user.id:
            await cq.answer("Недоступно", show_alert=True); return
        msgs = (await s.execute(select(ticket_messages).where(ticket_messages.c.ticket_id == tid)
                                .order_by(ticket_messages.c.id.desc()).limit(8))).mappings().all()
        msgs = list(reversed(msgs))
        lines = [f"🎫 <b>Обращение №{tid}</b>",
                 f"Статус: {'в работе' if t['status']=='OPEN' else 'закрыто'}", ""]
        for x in msgs:
            who = "👤 Вы" if x["sender"] == "USER" else "🛠 Поддержка"
            lines.append(f"{who}: {safe(x['text'], 500)}")
        rows = []
        if t["status"] == "OPEN":
            rows.append([btn("✍️ Ответить", f"ticket_reply:{tid}"), btn("🔒 Закрыть", f"ticket_close:{tid}")])
        rows.append([btn("⬅️ Назад", "support")])
        await show_screen(bot, s, cq.from_user.id, "\n".join(lines), kb(*rows))
        await s.commit()
    await cq.answer()

@router.callback_query(F.data.startswith("ticket_reply:"))
async def ticket_reply(cq: CallbackQuery, state: FSMContext, bot: Bot):
    tid = int(cq.data.split(":", 1)[1])
    await state.update_data(ticket_id=tid)
    await state.set_state(FS.support_reply)
    async with Session() as s:
        await show_screen(bot, s, cq.from_user.id,
            f"✍️ <b>Ответ в обращение №{tid}</b>\n\nВведите сообщение (1–2000).",
            kb([btn("❌ Отмена", f"ticket_view:{tid}")]))
        await s.commit()
    await cq.answer()

@router.message(FS.support_reply)
async def ticket_reply_text(m: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    tid = d.get("ticket_id")
    if not tid:
        await state.clear(); return
    text = (m.text or "").strip()
    if not (1 <= len(text) <= 2000):
        await m.answer("Введите от 1 до 2000 символов.")
        return
    async with Session() as s:
        await s.execute(insert(ticket_messages).values(ticket_id=tid, sender="USER", text=text))
        await s.commit()
        with contextlib.suppress(Exception):
            await m.delete()
        await state.clear()
        # переоткрываем тикет для просмотра
        cq_fake = type("X", (), {"from_user": m.from_user, "data": f"ticket_view:{tid}", "answer": lambda *a, **k: None})()
        await ticket_view(cq_fake, bot)

@router.callback_query(F.data.startswith("ticket_close:"))
async def ticket_close(cq: CallbackQuery, bot: Bot):
    tid = int(cq.data.split(":", 1)[1])
    async with Session() as s:
        await s.execute(update(tickets).where(and_(
            tickets.c.id == tid,
            tickets.c.user_id == cq.from_user.id,
        )).values(status="CLOSED"))
        await s.commit()
        await show_screen(bot, s, cq.from_user.id,
            f"🔒 Обращение №{tid} закрыто.",
            kb([btn("⬅️ В меню", "menu")]))
        await s.commit()
    await cq.answer("Закрыто")

# ==============================================================================
# ADMIN
# ==============================================================================
@router.callback_query(F.data == "admin_close")
async def admin_close(cq: CallbackQuery, bot: Bot):
    with contextlib.suppress(Exception):
        await cq.message.delete()
    await cq.answer()

@router.callback_query(F.data == "admin_search")
async def admin_search(cq: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(cq.from_user.id):
        await cq.answer(); return
    await state.set_state(FS.admin_search)
    await bot.send_message(cq.from_user.id, "🔍 Введите Telegram ID или @username:")
    await cq.answer()

@router.message(FS.admin_search)
async def admin_search_input(m: Message, state: FSMContext, bot: Bot):
    if not is_admin(m.from_user.id):
        return
    q = (m.text or "").strip().lstrip("@")
    async with Session() as s:
        if q.isdigit():
            u = (await s.execute(select(users).where(users.c.telegram_id == int(q)))).mappings().first()
        else:
            u = (await s.execute(select(users).where(users.c.username == q))).mappings().first()
        if not u:
            await m.answer("Не найдено.")
            await state.clear(); return
        sub = await current_sub(s, u["telegram_id"])
        w = await WalletSvc.get(s, u["telegram_id"])
        text = (
            f"👤 <b>{u['username'] or u['telegram_id']}</b>\n"
            f"ID: <code>{u['telegram_id']}</code>\n"
            f"Баланс: {fmt_rub(w['deposit_rub'])} (бонус {fmt_rub(w['bonus_rub'])})\n"
            f"Подписка: {sub['status'] if sub else '—'} до {fmt_dt(sub['expires_at']) if sub else '—'}\n"
            f"Заблокирован: {'да' if u['is_blocked'] else 'нет'}"
        )
        mk = kb(
            [btn("💰 Выдать баланс", f"admin_give:{u['telegram_id']}")],
            [btn("🚫 Блок / Разблок", f"admin_block_toggle:{u['telegram_id']}")],
            [btn("📦 Заказы", f"admin_user_orders:{u['telegram_id']}")],
            [btn("⬅️ Закрыть", "admin_close")],
        )
        await bot.send_message(m.from_user.id, text, reply_markup=mk, parse_mode=ParseMode.HTML)
    await state.clear()

@router.callback_query(F.data.startswith("admin_block_toggle:"))
async def admin_block_toggle(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id): return
    uid = int(cq.data.split(":", 1)[1])
    async with Session() as s:
        u = await get_user(s, uid)
        new = not u["is_blocked"]
        await s.execute(update(users).where(users.c.telegram_id == uid).values(is_blocked=new))
        await audit(s, cq.from_user.id, "block_toggle", str(uid), f"to={new}")
        await s.commit()
    await cq.answer("Готово")
    await cq.message.edit_reply_markup(reply_markup=None)

@router.callback_query(F.data.startswith("admin_give:"))
async def admin_give_start(cq: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(cq.from_user.id): return
    uid = int(cq.data.split(":", 1)[1])
    await state.update_data(target_uid=uid)
    await state.set_state(FS.admin_give_balance)
    await bot.send_message(cq.from_user.id, f"Введите сумму в рублях для ID <code>{uid}</code> (можно отрицательную):",
                           parse_mode=ParseMode.HTML)
    await cq.answer()

@router.message(FS.admin_give_balance)
async def admin_give_input(m: Message, state: FSMContext, bot: Bot):
    if not is_admin(m.from_user.id): return
    try:
        amount = Decimal((m.text or "").strip().replace(",", "."))
    except Exception:
        await m.answer("Некорректная сумма."); return
    d = await state.get_data()
    uid = d.get("target_uid")
    async with Session() as s:
        await WalletSvc.ensure(s, uid)
        await s.execute(update(wallets).where(wallets.c.user_id == uid).values(
            deposit_rub=wallets.c.deposit_rub + rub(amount)))
        await LedgerSvc.add(s, uid, "ADMIN_GRANT", rub(amount), f"admin:{new_id()}", f"admin={m.from_user.id}")
        await audit(s, m.from_user.id, "give_balance", str(uid), f"amount={amount}")
        await s.commit()
    await m.answer(f"✅ Начислено {fmt_rub(amount)} для {uid}")
    with contextlib.suppress(Exception):
        await bot.send_message(uid, f"💰 Баланс пополнен на {fmt_rub(amount)}")
    await state.clear()

@router.callback_query(F.data == "admin_give_balance")
async def admin_give_choose(cq: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(cq.from_user.id): return
    await state.set_state(FS.admin_search)
    await bot.send_message(cq.from_user.id, "🔍 Найдите пользователя, затем нажмите «💰 Выдать баланс».")
    await cq.answer()

@router.callback_query(F.data == "admin_tickets")
async def admin_tickets(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id): return
    async with Session() as s:
        r = await s.execute(select(tickets).where(tickets.c.status == "OPEN")
                            .order_by(tickets.c.created_at.desc()).limit(20))
        items = r.mappings().all()
        if not items:
            text = "🎫 Открытых тикетов нет."
            mk = kb([btn("⬅️ Закрыть", "admin_close")])
        else:
            rows = [[btn(f"#{t['id']} · {t['subject']} · {t['user_id']}", f"admin_ticket:{t['id']}")] for t in items]
            rows.append([btn("⬅️ Закрыть", "admin_close")])
            text = "🎫 <b>Открытые тикеты</b>"
            mk = kb(*rows)
        await bot.send_message(cq.from_user.id, text, reply_markup=mk, parse_mode=ParseMode.HTML)
    await cq.answer()

@router.callback_query(F.data.startswith("admin_ticket:"))
async def admin_ticket_view(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id): return
    tid = int(cq.data.split(":", 1)[1])
    async with Session() as s:
        t = (await s.execute(select(tickets).where(tickets.c.id == tid))).mappings().first()
        if not t:
            await cq.answer("Нет такого", show_alert=True); return
        msgs = (await s.execute(select(ticket_messages).where(ticket_messages.c.ticket_id == tid)
                                .order_by(ticket_messages.c.id.asc()))).mappings().all()
        lines = [f"🎫 <b>Тикет #{tid}</b> · user <code>{t['user_id']}</code>",
                 f"Тема: {t['subject']} · {t['status']}", ""]
        for x in msgs:
            who = "👤" if x["sender"] == "USER" else "🛠"
            lines.append(f"{who} {safe(x['text'], 800)}")
        mk = kb(
            [btn("✍️ Ответить", f"admin_ticket_reply:{tid}")],
            [btn("🔒 Закрыть", f"admin_ticket_close:{tid}")],
            [btn("⬅️ Назад", "admin_tickets")],
        )
        await bot.send_message(cq.from_user.id, "\n".join(lines), reply_markup=mk, parse_mode=ParseMode.HTML)
    await cq.answer()

@router.callback_query(F.data.startswith("admin_ticket_reply:"))
async def admin_ticket_reply_start(cq: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(cq.from_user.id): return
    tid = int(cq.data.split(":", 1)[1])
    await state.update_data(admin_ticket_id=tid)
    await state.set_state(FS.ticket_admin_reply)
    await bot.send_message(cq.from_user.id, f"Введите ответ на тикет #{tid}:")
    await cq.answer()

@router.message(FS.ticket_admin_reply)
async def admin_ticket_reply_send(m: Message, state: FSMContext, bot: Bot):
    if not is_admin(m.from_user.id): return
    d = await state.get_data()
    tid = d.get("admin_ticket_id")
    text = (m.text or "").strip()
    if not (1 <= len(text) <= 2000):
        await m.answer("1–2000 символов."); return
    async with Session() as s:
        await s.execute(insert(ticket_messages).values(ticket_id=tid, sender="ADMIN", text=text))
        t = (await s.execute(select(tickets).where(tickets.c.id == tid))).mappings().first()
        await audit(s, m.from_user.id, "ticket_reply", str(tid))
        await s.commit()
    with contextlib.suppress(Exception):
        await bot.send_message(t["user_id"], f"🛠 <b>Ответ по обращению №{tid}</b>\n\n{safe(text, 2000)}",
                               parse_mode=ParseMode.HTML)
    await m.answer("Отправлено.")
    await state.clear()

@router.callback_query(F.data.startswith("admin_ticket_close:"))
async def admin_ticket_close(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id): return
    tid = int(cq.data.split(":", 1)[1])
    async with Session() as s:
        await s.execute(update(tickets).where(tickets.c.id == tid).values(status="CLOSED"))
        t = (await s.execute(select(tickets).where(tickets.c.id == tid))).mappings().first()
        await audit(s, cq.from_user.id, "ticket_close", str(tid))
        await s.commit()
    with contextlib.suppress(Exception):
        await bot.send_message(t["user_id"], f"🔒 Обращение №{tid} закрыто.")
    await cq.answer("Закрыто")

@router.callback_query(F.data == "admin_orders")
async def admin_orders(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id): return
    async with Session() as s:
        r = await s.execute(select(fulfillment_jobs).where(fulfillment_jobs.c.state.in_(["RETRY", "MANUAL", "FAILED"]))
                            .order_by(fulfillment_jobs.c.updated_at.desc()).limit(20))
        items = r.mappings().all()
        if not items:
            text = "✅ Нет проблемных заказов."
            mk = kb([btn("⬅️ Закрыть", "admin_close")])
        else:
            rows = [[btn(f"{j['state']} · order {j['order_id'][:8]} · attempt {j['attempts']}", f"admin_order:{j['id']}")] for j in items]
            rows.append([btn("⬅️ Закрыть", "admin_close")])
            text = "📦 <b>Проблемные заказы</b>"
            mk = kb(*rows)
        await bot.send_message(cq.from_user.id, text, reply_markup=mk, parse_mode=ParseMode.HTML)
    await cq.answer()

@router.callback_query(F.data.startswith("admin_order:"))
async def admin_order_view(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id): return
    jid = cq.data.split(":", 1)[1]
    async with Session() as s:
        j = (await s.execute(select(fulfillment_jobs).where(fulfillment_jobs.c.id == jid))).mappings().first()
        if not j:
            await cq.answer("Нет", show_alert=True); return
        o = (await s.execute(select(orders).where(orders.c.id == j["order_id"]))).mappings().first()
        text = (
            f"📦 <b>Job {jid[:8]}</b>\n\n"
            f"Order: {o['id'] if o else '—'}\n"
            f"State: {j['state']} · attempts {j['attempts']}\n"
            f"Last error: {safe(j['last_error'], 200)}"
        )
        mk = kb(
            [btn("🔁 Retry", f"admin_order_retry:{jid}")],
            [btn("⬅️ Назад", "admin_orders")],
        )
        await bot.send_message(cq.from_user.id, text, reply_markup=mk, parse_mode=ParseMode.HTML)
    await cq.answer()

@router.callback_query(F.data.startswith("admin_order_retry:"))
async def admin_order_retry(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id): return
    jid = cq.data.split(":", 1)[1]
    async with Session() as s:
        await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == jid).values(
            state="QUEUED", attempts=0, updated_at=now()))
        await audit(s, cq.from_user.id, "order_retry", jid)
        await s.commit()
    await cq.answer("В очереди")
    await cq.message.edit_reply_markup(reply_markup=None)

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id): return
    await cq.answer("Функция появится позже", show_alert=True)

# ==============================================================================
# FALLBACK
# ==============================================================================
@router.message()
async def fallback(m: Message, state: FSMContext):
    if await state.get_state():
        return
    with contextlib.suppress(Exception):
        await m.delete()
    await m.answer("Откройте меню: /start")

# ==============================================================================
# BACKGROUND
# ==============================================================================
async def process_jobs_once(bot: Bot):
    async with Session() as s:
        # атомарно захватываем джобы
        r = await s.execute(text(
            "UPDATE fulfillment_jobs SET state='RUNNING', updated_at=:now "
            "WHERE id IN ("
            "  SELECT id FROM fulfillment_jobs WHERE state='QUEUED' ORDER BY updated_at LIMIT 5"
            ") RETURNING id, order_id, attempts"
        ), {"now": now()})
        jobs = r.mappings().all()
        await s.commit()

    for j in jobs:
        async with Session() as s:
            o = (await s.execute(select(orders).where(orders.c.id == j["order_id"]))).mappings().first()
            if not o or not o["plan_code"]:
                await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"])
                                .values(state="FAILED", last_error="no plan"))
                await s.commit(); continue
            try:
                existing = await current_sub(s, o["user_id"])
                if existing and existing["external_id"] and existing["status"] == "ACTIVE" \
                        and existing["expires_at"] and existing["expires_at"] > now():
                    # продление
                    try:
                        await AdaptGroup.extend_subscription(existing["external_id"], o["days_snapshot"] or 30)
                        new_exp = existing["expires_at"] + timedelta(days=o["days_snapshot"] or 30)
                        await s.execute(update(subscriptions).where(subscriptions.c.user_id == o["user_id"])
                                        .values(expires_at=new_exp, plan_code=o["plan_code"], last_sync=now()))
                        await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"])
                                        .values(state="DONE"))
                        await s.execute(update(orders).where(orders.c.id == o["id"]).values(status="FULFILLED"))
                        await s.commit()
                        with contextlib.suppress(Exception):
                            await bot.send_message(o["user_id"],
                                f"✅ Подписка продлена на {o['days_snapshot']} дн.\nДо {fmt_dt(new_exp)}")
                        continue
                    except Exception as e:
                        log.warning("extend failed, fallback to create: %s", e)
                # создание новой
                res = await AdaptGroup.create_subscription(o["user_id"], o["plan_code"],
                                                           o["days_snapshot"] or 30, o["id"])
                ext = str(res.get("subscriptionId") or res.get("id") or "")
                url = res.get("subscriptionUrl") or res.get("url") or ""
                exp_dt = None
                exp_raw = res.get("expiresAt")
                if exp_raw:
                    try:
                        exp_dt = datetime.fromisoformat(exp_raw.replace("Z", "+00:00"))
                    except Exception:
                        exp_dt = None
                if not exp_dt:
                    exp_dt = now() + timedelta(days=o["days_snapshot"] or 30)
                exists = (await s.execute(select(subscriptions).where(subscriptions.c.user_id == o["user_id"]))).first()
                if exists:
                    await s.execute(update(subscriptions).where(subscriptions.c.user_id == o["user_id"]).values(
                        external_id=ext, subscription_url=url, plan_code=o["plan_code"],
                        expires_at=exp_dt, status="ACTIVE", last_sync=now()))
                else:
                    await s.execute(insert(subscriptions).values(
                        user_id=o["user_id"], external_id=ext, subscription_url=url,
                        plan_code=o["plan_code"], expires_at=exp_dt, status="ACTIVE", last_sync=now()))
                await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"]).values(state="DONE"))
                await s.execute(update(orders).where(orders.c.id == o["id"]).values(status="FULFILLED"))
                await s.commit()
                with contextlib.suppress(Exception):
                    await bot.send_message(o["user_id"],
                        f"✅ <b>Подписка активирована</b>\n"
                        f"Добавлено: {o['days_snapshot']} дн.\n"
                        f"Действует до: {fmt_dt(exp_dt)}",
                        reply_markup=kb([btn("🔗 Подключиться", "connect")]))
            except Exception as e:
                log.exception("fulfill failed")
                new_state = "RETRY" if j["attempts"] < 5 else "MANUAL"
                await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"]).values(
                    state=new_state, attempts=j["attempts"] + 1,
                    last_error=str(e)[:200], updated_at=now()))
                await s.commit()

async def expire_orders_once():
    async with Session() as s:
        await s.execute(update(orders).where(and_(
            orders.c.status == "AWAITING_PAYMENT",
            orders.c.expires_at < now(),
        )).values(status="EXPIRED"))
        await s.commit()

async def make_rewards_available():
    async with Session() as s:
        r = await s.execute(select(referral_rewards).where(and_(
            referral_rewards.c.status == "PENDING",
            referral_rewards.c.available_at <= now(),
        )))
        rows = r.mappings().all()
        for rw in rows:
            await WalletSvc.ensure(s, rw["inviter_id"])
            await s.execute(update(wallets).where(wallets.c.user_id == rw["inviter_id"]).values(
                bonus_rub=wallets.c.bonus_rub + rw["amount_rub"]))
            await LedgerSvc.add(s, rw["inviter_id"], "REF_BONUS", rw["amount_rub"], f"ref:{rw['id']}")
            await s.execute(update(referral_rewards).where(referral_rewards.c.id == rw["id"]).values(status="AVAILABLE"))
        await s.commit()

async def expiry_reminders(bot: Bot):
    async with Session() as s:
        r = await s.execute(select(users.c.telegram_id, subscriptions.c.expires_at).select_from(
            users.join(subscriptions, users.c.telegram_id == subscriptions.c.user_id)
        ).where(and_(
            subscriptions.c.status == "ACTIVE",
            subscriptions.c.expires_at > now(),
            subscriptions.c.expires_at <= now() + timedelta(hours=25),
            users.c.notify_expiry == True,
        )))
        for uid, exp in r.all():
            with contextlib.suppress(Exception):
                await bot.send_message(uid,
                    f"⏰ <b>Подписка заканчивается</b>\n\n"
                    f"Дата: {fmt_dt(exp)}\n"
                    f"Продлите — оставшиеся дни сохранятся.",
                    reply_markup=kb([btn("🔄 Продлить", "buy")]))

async def background_worker(bot: Bot):
    while True:
        try:
            await process_jobs_once(bot)
            await expire_orders_once()
            await make_rewards_available()
            await expiry_reminders(bot)
        except Exception:
            log.exception("worker")
        await asyncio.sleep(CFG["WORKER_INTERVAL"])

# ==============================================================================
# FASTAPI
# ==============================================================================
bot = Bot(CFG["BOT_TOKEN"], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)
app = FastAPI()
_worker_task: Optional[asyncio.Task] = None

@app.post(CFG["TG_PATH"])
async def tg_webhook(request: Request):
    header = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not timing_safe_eq(header, CFG["WEBHOOK_SECRET_TG"]):
        raise HTTPException(401)
    data = await request.json()
    upd = Update.model_validate(data, context={"bot": bot})
    await dp.feed_update(bot, upd)
    return Response(status_code=200)

@app.post(CFG["PG_PATH"])
async def platega_webhook(request: Request):
    # в stub-режиме не используется
    if CFG["PLATEGA_ENABLED"]:
        if not (timing_safe_eq(request.headers.get("X-MerchantId", ""), CFG["PLATEGA_MID"]) and
                timing_safe_eq(request.headers.get("X-Secret", ""), CFG["PLATEGA_SECRET"])):
            raise HTTPException(401)
    body = await request.json()
    ext_id = str(body.get("transactionId") or body.get("id") or "")
    status = str(body.get("status") or "").upper()
    async with Session() as s:
        try:
            await s.execute(insert(payment_events).values(
                id=new_id(), source="PLATEGA", external_id=ext_id,
                status=status, body=json.dumps(body)[:1900], processed=False))
            await s.commit()
        except IntegrityError:
            await s.rollback()
        if status == "CONFIRMED":
            pa = (await s.execute(select(payment_attempts)
                                  .where(payment_attempts.c.external_id == ext_id))).mappings().first()
            if pa:
                await finalize_payment(s, bot, pa["order_id"], "PLATEGA", ext_id)
    return {"ok": True}

@app.post(CFG["AG_PATH"])
async def adaptgroup_webhook(request: Request):
    if CFG["AG_IP"] and request.client and request.client.host != CFG["AG_IP"]:
        raise HTTPException(401)
    raw = await request.body()
    sig = request.headers.get("X-Webhook-Signature", "")
    expected = hmac.new(CFG["AG_SECRET"].encode(), raw, hashlib.sha256).hexdigest()
    if not timing_safe_eq(sig, expected):
        raise HTTPException(401)
    try:
        body = json.loads(raw or b"{}")
    except Exception:
        raise HTTPException(400)
    async with Session() as s:
        ext_id = str(body.get("subscriptionId") or body.get("id") or "")
        ev = str(body.get("event") or "")
        if ext_id and ev in ("subscription.created", "subscription.renewed", "subscription.expired"):
            await s.execute(update(subscriptions).where(subscriptions.c.external_id == ext_id)
                            .values(last_sync=now()))
            await s.commit()
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    global _worker_task
    await init_db()
    await bot.set_webhook(
        url=CFG["PUBLIC_BASE_URL"] + CFG["TG_PATH"],
        secret_token=CFG["WEBHOOK_SECRET_TG"],
        allowed_updates=dp.resolve_used_update_types(),
        drop_pending_updates=True,
    )
    _worker_task = asyncio.create_task(background_worker(bot))
    log.info("Bot started, webhook=%s", CFG["PUBLIC_BASE_URL"] + CFG["TG_PATH"])

@app.on_event("shutdown")
async def on_shutdown():
    global _worker_task
    if _worker_task:
        _worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _worker_task
    await bot.session.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=CFG["HOST"], port=CFG["PORT"], log_level="info")
