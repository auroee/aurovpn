# aurovpncode.py — Auro VPN v26.0 (single-file, production-oriented)
# Единый источник правды: сервисы → роутеры → вебхуки → воркер.
from __future__ import annotations

import asyncio, hashlib, hmac, json, logging, os, re, secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional
from html import escape

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
    CopyTextButton, Update,
)
from fastapi import FastAPI, HTTPException, Request, Response
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, MetaData,
    Numeric, String, Table, UniqueConstraint, and_, func, insert, or_, select,
    text, update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# ──────────────────────────────────────────────────────────────────────────────
# 1. КОНФИГ
# ──────────────────────────────────────────────────────────────────────────────
def _env(k: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(k, default)
    if required and not v:
        raise SystemExit(f"ENV {k} is required")
    return v or ""

def _env_int(k: str, default: int) -> int:
    try: return int(_env(k, str(default)))
    except ValueError: return default

def _env_dec(k: str, default: str) -> Decimal:
    try: return Decimal(_env(k, default))
    except Exception: return Decimal(default)

CFG = dict(
    BOT_TOKEN=_env("BOT_TOKEN", required=True),
    BOT_USERNAME=_env("BOT_USERNAME", "aurovpn_bot"),
    WEBHOOK_SECRET_TG=_env("WEBHOOK_SECRET_TG", "tg-secret"),
    ADMIN_IDS=[int(x) for x in _env("ADMIN_IDS", "").split(",") if x.strip().isdigit()],
    OWNER_ID=_env_int("OWNER_ID", 0),
    CHANNEL_URL=_env("CHANNEL_URL", ""),

    DATABASE_URL=_env("DATABASE_URL", "sqlite+aiosqlite:///./auro.db"),
    PUBLIC_BASE_URL=_env("PUBLIC_BASE_URL", "https://aurovpn.cc.cd").rstrip("/"),
    TG_PATH=_env("TELEGRAM_WEBHOOK_PATH", "/tg/webhook"),
    PG_PATH=_env("PLATEGA_WEBHOOK_PATH", "/platega/webhook"),
    AG_PATH=_env("ADAPTGROUP_WEBHOOK_PATH", "/adaptgroup/webhook"),

    PLATEGA_BASE=_env("PLATEGA_BASE_URL", "https://app.platega.io").rstrip("/"),
    PLATEGA_MID=_env("PLATEGA_MERCHANT_ID"),
    PLATEGA_SECRET=_env("PLATEGA_SECRET"),
    PLATEGA_METHOD=_env_int("PLATEGA_PAYMENT_METHOD", 2),
    PLATEGA_RETURN=_env("PLATEGA_RETURN_URL", "https://t.me/aurovpn_bot"),
    PLATEGA_TIMEOUT=_env_int("PLATEGA_HTTP_TIMEOUT", 20),

    AG_BASE=_env("ADAPT_BASE_URL", "https://network-api.adaptgroup.app").rstrip("/"),
    AG_KEY=_env("ADAPT_API_KEY"),
    AG_SECRET=_env("ADAPT_WEBHOOK_SECRET", "ag-secret"),
    AG_IP=_env("ADAPT_IP_ALLOW", "").strip(),
    AG_PLAN_MAP=json.loads(_env("ADAPTGROUP_PLAN_MAP", "{}")),
    AG_TIMEOUT=_env_int("ADAPT_HTTP_TIMEOUT", 25),

    REF_PERCENT=_env_dec("REFERRAL_PERCENT", "15"),
    REF_HOLD_H=_env_int("REFERRAL_HOLD_HOURS", 24),
    TOPUP_MIN=_env_dec("TOPUP_MIN", "50"),
    TOPUP_MAX=_env_dec("TOPUP_MAX", "10000"),
    ORDER_TTL=_env_int("ORDER_TTL_MINUTES", 20),
    MAINTENANCE=_env("MAINTENANCE", "false").lower() == "true",
    WORKER_INTERVAL=_env_int("WORKER_INTERVAL", 15),
    FULFILL_MAX_ATTEMPTS=_env_int("FULFILLMENT_MAX_ATTEMPTS", 5),
    TERMS_URL=_env("TERMS_URL", "https://telegra.ph/Auro-VPN--Usloviya-servisa-11-23"),
    PRIVACY_URL=_env("PRIVACY_URL", "https://telegra.ph/Auro-VPN--Konfidencialnost-11-23"),
)

UTC = timezone.utc
MSK = timezone(timedelta(hours=3))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("auro")

# ──────────────────────────────────────────────────────────────────────────────
# 2. УТИЛИТЫ
# ──────────────────────────────────────────────────────────────────────────────
def now() -> datetime: return datetime.now(UTC)
def rub(x: Any) -> Decimal: return Decimal(str(x)).quantize(Decimal("0.01"), ROUND_HALF_UP)

def fmt_rub(x: Any) -> str:
    x = rub(x)
    s = f"{x:,.2f}".replace(",", " ").replace(".", ",")
    return s[:-3] + " ₽" if s.endswith(",00") else s + " ₽"

def fmt_dt(dt: Optional[datetime]) -> str:
    if not dt: return "—"
    return dt.astimezone(MSK).strftime("%d.%m.%Y, %H:%M МСК")

def safe(x: Any, n: int = 3800) -> str: return escape((str(x) or "")[:n])
def tse(a: str, b: str) -> bool: return hmac.compare_digest((a or "").encode(), (b or "").encode())

def progress_bar(left: timedelta, total_days: int) -> str:
    total = max(1, total_days * 86400)
    pct = max(0, min(100, int(100 - left.total_seconds() / total * 100)))
    filled = round(pct / 10)
    return "▰" * filled + "▱" * (10 - filled) + f" {pct}%"

# ──────────────────────────────────────────────────────────────────────────────
# 3. БД
# ──────────────────────────────────────────────────────────────────────────────
engine = create_async_engine(CFG["DATABASE_URL"], pool_pre_ping=True, future=True)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
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
)

plans = Table("plans", meta,
    Column("code", String(8), primary_key=True),
    Column("days", Integer, nullable=False),
    Column("price_rub", Numeric(12, 2), nullable=False),
    Column("enabled", Boolean, default=True, nullable=False),
)

provider_plan_map = Table("provider_plan_map", meta,
    Column("plan_code", String(8), ForeignKey("plans.code"), primary_key=True),
    Column("provider_plan", String(64), nullable=False),
    Column("verified_at", DateTime(timezone=True)),
)

orders = Table("orders", meta,
    Column("id", String(36), primary_key=True),
    Column("user_id", BigInteger, ForeignKey("users.telegram_id")),
    Column("kind", String(12), nullable=False),
    Column("plan_code", String(8)),
    Column("amount_rub", Numeric(12, 2), nullable=False),
    Column("days_snapshot", Integer),
    Column("status", String(16), default="DRAFT", nullable=False),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
    Column("expires_at", DateTime(timezone=True)),
)

payment_attempts = Table("payment_attempts", meta,
    Column("id", String(36), primary_key=True),
    Column("order_id", String(36), ForeignKey("orders.id")),
    Column("provider", String(16), nullable=False),
    Column("external_id", String(128), unique=True),
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
    Column("plan_code", String(8)),
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
    ("plan_14d", 14, Decimal("149.00")),
    ("plan_1m",  30, Decimal("299.00")),
    ("plan_3m",  90, Decimal("799.00")),
    ("plan_6m", 180, Decimal("1490.00")),
    ("plan_1y", 365, Decimal("2290.00")),
]

async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(meta.create_all)
        for code, days, price in SEED_PLANS:
            exists = (await conn.execute(select(plans.c.code).where(plans.c.code == code))).first()
            if not exists:
                await conn.execute(insert(plans).values(code=code, days=days, price_rub=price, enabled=True))
        for code, provider in CFG["AG_PLAN_MAP"].items():
            exists = (await conn.execute(
                select(provider_plan_map.c.plan_code).where(provider_plan_map.c.plan_code == code)
            )).first()
            if not exists:
                await conn.execute(insert(provider_plan_map).values(plan_code=code, provider_plan=provider))

# ──────────────────────────────────────────────────────────────────────────────
# 4. СЕРВИСЫ
# ──────────────────────────────────────────────────────────────────────────────
class WalletSvc:
    @staticmethod
    async def ensure(s: AsyncSession, uid: int) -> None:
        exists = (await s.execute(select(wallets.c.user_id).where(wallets.c.user_id == uid))).first()
        if not exists:
            try:
                await s.execute(insert(wallets).values(user_id=uid))
            except IntegrityError:
                await s.rollback()

    @staticmethod
    async def get(s: AsyncSession, uid: int):
        await WalletSvc.ensure(s, uid)
        r = await s.execute(select(wallets).where(wallets.c.user_id == uid))
        return r.mappings().first()

    @staticmethod
    async def available(s: AsyncSession, uid: int) -> Decimal:
        w = await WalletSvc.get(s, uid)
        return rub(w["deposit_rub"] + w["bonus_rub"])

class LedgerSvc:
    @staticmethod
    async def add(s: AsyncSession, uid: int, kind: str, amount: Decimal,
                  idem_key: str, meta: str = "") -> bool:
        exists = (await s.execute(select(ledger.c.id).where(ledger.c.idem_key == idem_key))).first()
        if exists:
            return False
        await s.execute(insert(ledger).values(
            id=secrets.token_hex(16), user_id=uid, kind=kind,
            amount_rub=rub(amount), meta=meta, idem_key=idem_key,
        ))
        return True

class Platega:
    @staticmethod
    def _h() -> dict:
        return {"X-MerchantId": CFG["PLATEGA_MID"], "X-Secret": CFG["PLATEGA_SECRET"]}

    @staticmethod
    async def create_payment(order_id: str, amount: Decimal, description: str) -> dict:
        payload = {
            "paymentMethod": CFG["PLATEGA_METHOD"],
            "amount": float(amount),
            "currency": "RUB",
            "description": description,
            "orderId": order_id,
            "returnUrl": CFG["PLATEGA_RETURN"],
        }
        async with httpx.AsyncClient(timeout=CFG["PLATEGA_TIMEOUT"]) as c:
            r = await c.post(f"{CFG['PLATEGA_BASE']}/transaction/process", json=payload, headers=Platega._h())
            r.raise_for_status()
            return r.json()

    @staticmethod
    async def get_transaction(external_id: str) -> dict:
        async with httpx.AsyncClient(timeout=CFG["PLATEGA_TIMEOUT"]) as c:
            r = await c.get(f"{CFG['PLATEGA_BASE']}/transaction/{external_id}", headers=Platega._h())
            r.raise_for_status()
            return r.json()

class AdaptGroup:
    @staticmethod
    async def create_subscription(user_id: int, plan_code: str, idempotency_key: str) -> dict:
        provider_plan = CFG["AG_PLAN_MAP"].get(plan_code)
        if not provider_plan:
            raise RuntimeError(f"plan {plan_code} not mapped")
        payload = {
            "plan": provider_plan,
            "externalUserId": str(user_id),
            "idempotencyKey": idempotency_key,
        }
        async with httpx.AsyncClient(timeout=CFG["AG_TIMEOUT"]) as c:
            r = await c.post(f"{CFG['AG_BASE']}/api/v1/subscriptions",
                             json=payload, headers={"X-Api-Key": CFG["AG_KEY"]})
            r.raise_for_status()
            return r.json()

# ──────────────────────────────────────────────────────────────────────────────
# 5. UI
# ──────────────────────────────────────────────────────────────────────────────
def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)

def url_btn(text: str, url: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, url=url)

def copy_btn(text: str, value: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, copy_text=CopyTextButton(text=value))

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

async def ensure_user(s: AsyncSession, m: Message, ref_code: Optional[str]):
    uid = m.from_user.id
    u = await get_user(s, uid)
    if u:
        if u["username"] != m.from_user.username:
            await s.execute(update(users).where(users.c.telegram_id == uid).values(username=m.from_user.username))
        return u
    ref_uid = None
    if ref_code:
        r = await s.execute(select(ref_codes.c.user_id).where(ref_codes.c.code == ref_code))
        row = r.first()
        if row and row[0] != uid:
            ref_uid = row[0]
    await s.execute(insert(users).values(
        telegram_id=uid, username=m.from_user.username, referrer_id=ref_uid,
    ))
    await WalletSvc.ensure(s, uid)
    if ref_uid:
        try:
            await s.execute(insert(referrals).values(invited_id=uid, inviter_id=ref_uid))
        except IntegrityError:
            await s.rollback()
    return await get_user(s, uid)

async def render_hub(s: AsyncSession, uid: int) -> tuple[str, InlineKeyboardMarkup]:
    u = await get_user(s, uid)
    avail = await WalletSvc.available(s, uid)
    sub = await current_sub(s, uid)

    if CFG["MAINTENANCE"]:
        return (
            "🔧 <b>Технические работы</b>\n\n"
            "Покупки временно недоступны. Оплаченные заказы обрабатываются в обычном режиме.\n\n"
            f"<blockquote>Доступно для оплаты: <b>{fmt_rub(avail)}</b></blockquote>",
            kb([btn("Кабинет", "cabinet"), btn("Поддержка", "support")]),
        )

    if sub and sub["status"] == "ACTIVE" and sub["expires_at"] and sub["expires_at"] > now():
        left = sub["expires_at"] - now()
        if left <= timedelta(hours=24):
            hh, mm = divmod(max(0, int(left.total_seconds())) // 60, 60)
            text = (
                "🛡 <b>Auro VPN</b>\n"
                f"⏳ До окончания: <b>{hh} ч {mm} мин</b>\n\n"
                "Продлите заранее — оставшийся срок сохранится.\n\n"
                f"<blockquote>💳 Баланс: <b>{fmt_rub(avail)}</b></blockquote>"
            )
            rows = [[btn("🔄 Продлить", "buy"), btn("🔗 Подключиться", "connect")],
                    [btn("👤 Кабинет", "cabinet"), btn("💬 Поддержка", "support")]]
        else:
            days = max(1, int((sub["expires_at"] - sub["created_at"] if False else
                              (sub["expires_at"] - (sub["expires_at"] - left))).total_seconds() / 86400)) if False else \
                   (sub["expires_at"] - now()).days
            bar = progress_bar(left, max(days, 1))
            text = (
                "🛡 <b>Auro VPN</b> · подписка активна\n\n"
                f"📅 До <b>{fmt_dt(sub['expires_at'])}</b>\n"
                f"<code>{bar}</code>\n\n"
                f"<blockquote>💳 Баланс: <b>{fmt_rub(avail)}</b></blockquote>"
            )
            rows = [[btn("🔗 Подключиться", "connect")],
                    [btn("🔄 Продлить", "buy"), btn("👤 Кабинет", "cabinet")],
                    [btn("🎁 Пригласить", "ref"), btn("💬 Поддержка", "support")]]
    else:
        text = (
            "🛡 <b>Auro VPN</b>\n\n"
            "Подписка не оформлена. Выберите срок — после оплаты поможем подключиться.\n\n"
            f"<blockquote>💳 Баланс: <b>{fmt_rub(avail)}</b></blockquote>"
        )
        rows = [[btn("🚀 Оформить подписку", "buy")],
                [btn("👤 Кабинет", "cabinet"), btn("🎁 Пригласить", "ref")],
                [btn("💬 Поддержка", "support"), btn("ℹ️ О сервисе", "about")]]
    return text, kb(*rows)

async def show(bot: Bot, s: AsyncSession, uid: int, text: str, markup: InlineKeyboardMarkup) -> None:
    u = await get_user(s, uid)
    mid = u["hub_message_id"] if u else None
    if mid:
        try:
            await bot.edit_message_text(text=text, chat_id=uid, message_id=mid,
                                        reply_markup=markup, parse_mode=ParseMode.HTML)
            return
        except TelegramBadRequest:
            pass
    m = await bot.send_message(chat_id=uid, text=text, reply_markup=markup, parse_mode=ParseMode.HTML)
    await s.execute(update(users).where(users.c.telegram_id == uid).values(hub_message_id=m.message_id))

# ──────────────────────────────────────────────────────────────────────────────
# 6. FSM
# ──────────────────────────────────────────────────────────────────────────────
class FS(StatesGroup):
    topup_amount = State()
    support_text = State()
    support_reply = State()
    appeal_text = State()

# ──────────────────────────────────────────────────────────────────────────────
# 7. MIDDLEWARE
# ──────────────────────────────────────────────────────────────────────────────
_last_click: dict[int, float] = {}

class Throttle:
    def __init__(self, router: Router, cooldown: float = 0.4):
        self.cooldown = cooldown
        router.callback_query.middleware(self._mw)

    async def _mw(self, handler, event: CallbackQuery, data):
        uid = event.from_user.id
        t = now().timestamp()
        if t - _last_click.get(uid, 0) < self.cooldown:
            await event.answer("⏳ Слишком быстро")
            return
        _last_click[uid] = t
        return await handler(event, data)

# ──────────────────────────────────────────────────────────────────────────────
# 8. BOT / DISPATCHER
# ──────────────────────────────────────────────────────────────────────────────
bot = Bot(CFG["BOT_TOKEN"], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
Throttle(router)
dp.include_router(router)

# ──────────────────────────────────────────────────────────────────────────────
# 9. HELPERS
# ──────────────────────────────────────────────────────────────────────────────
async def finalize_payment(s: AsyncSession, order_id: str, source: str, external_id: str) -> None:
    # идемпотентность: событие CONFIRMED уже было?
    exists = (await s.execute(select(payment_events.c.id).where(and_(
        payment_events.c.source == source,
        payment_events.c.external_id == external_id,
        payment_events.c.status == "CONFIRMED",
    )))).first()
    if exists:
        return
    try:
        await s.execute(insert(payment_events).values(
            id=secrets.token_hex(16), source=source, external_id=external_id,
            status="CONFIRMED", processed=True,
        ))
    except IntegrityError:
        await s.rollback(); return
    await s.execute(update(payment_attempts).where(payment_attempts.c.external_id == external_id)
                    .values(status="CONFIRMED"))
    o = (await s.execute(select(orders).where(orders.c.id == order_id))).mappings().first()
    if not o:
        await s.commit(); return
    await s.execute(update(orders).where(orders.c.id == order_id).values(status="PAID"))

    if o["kind"] == "TOPUP":
        await s.execute(update(wallets).where(wallets.c.user_id == o["user_id"])
                        .values(deposit_rub=wallets.c.deposit_rub + o["amount_rub"]))
        await LedgerSvc.add(s, o["user_id"], "TOPUP", o["amount_rub"], f"topup:{order_id}")
    else:
        await s.execute(insert(fulfillment_jobs).values(
            id=secrets.token_hex(16), order_id=order_id, state="QUEUED"))
    await referral_on_payment(s, o["user_id"], external_id, o["amount_rub"])
    await s.commit()

async def referral_on_payment(s: AsyncSession, invited_id: int, external_id: str, base: Decimal) -> None:
    u = (await s.execute(select(users).where(users.c.telegram_id == invited_id))).mappings().first()
    if not u or not u["referrer_id"]:
        return
    amount = rub(base * CFG["REF_PERCENT"] / Decimal(100))
    try:
        await s.execute(insert(referral_rewards).values(
            id=secrets.token_hex(16), inviter_id=u["referrer_id"], invited_id=invited_id,
            external_id=external_id, base_rub=base, percent=CFG["REF_PERCENT"],
            amount_rub=amount, status="PENDING",
            available_at=now() + timedelta(hours=CFG["REF_HOLD_H"]),
        ))
    except IntegrityError:
        await s.rollback()

async def create_order(s: AsyncSession, uid: int, kind: str, plan_code: Optional[str],
                       amount: Decimal, days: Optional[int]) -> str:
    oid = secrets.token_hex(16)
    await s.execute(insert(orders).values(
        id=oid, user_id=uid, kind=kind, plan_code=plan_code, amount_rub=rub(amount),
        days_snapshot=days, status="PENDING",
        expires_at=now() + timedelta(minutes=CFG["ORDER_TTL"]),
    ))
    return oid

async def make_sbp_payment(cq: CallbackQuery, s: AsyncSession, oid: str, amount: Decimal,
                            description: str, back_cb: str) -> None:
    try:
        res = await Platega.create_payment(oid, amount, description)
    except Exception as e:
        log.warning("platega create failed oid=%s: %s", oid, e)
        await s.execute(update(orders).where(orders.c.id == oid).values(status="CREATION_FAILED"))
        await s.commit()
        await show(bot, s, cq.from_user.id,
            "⚠️ <b>Оплата временно недоступна</b>\n\nСоздать платёж не удалось. Сумма заказа не изменилась.",
            kb([btn("🔁 Повторить", back_cb)], [btn("↩️ Назад", "menu")]))
        await cq.answer(); return

    ext = str(res.get("transactionId") or res.get("id") or "")
    url = res.get("paymentUrl") or res.get("url") or ""
    await s.execute(insert(payment_attempts).values(
        id=secrets.token_hex(16), order_id=oid, provider="PLATEGA",
        external_id=ext, payment_url=url, status="PENDING",
    ))
    await s.execute(update(orders).where(orders.c.id == oid).values(status="AWAITING_PAYMENT"))
    await s.commit()

    txt = (
        f"🔗 <b>Оплата через СБП</b>\n"
        f"Заказ №<code>{oid[:8]}</code>\n"
        f"К оплате: <b>{fmt_rub(amount)}</b>\n\n"
        f"После перевода вернитесь в бот — проверка автоматическая."
    )
    rows = []
    if url:
        rows.append([url_btn("💳 Перейти к оплате", url)])
        rows.append([copy_btn("📋 Скопировать ссылку", url)])
    rows.append([btn("✅ Проверить оплату", f"check_pay:{oid}")])
    rows.append([btn("↩️ Отменить", "menu")])
    await show(bot, s, cq.from_user.id, txt, kb(*rows))

# ──────────────────────────────────────────────────────────────────────────────
# 10. HANDLERS: старт, согласие, меню
# ──────────────────────────────────────────────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    if m.chat.type != ChatType.PRIVATE:
        return
    await state.clear()
    ref_code = None
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) == 2 and parts[1].startswith("ref_"):
        ref_code = parts[1][4:]
    async with Session() as s:
        u = await ensure_user(s, m, ref_code)
        await s.commit()
        if not u["is_terms_accepted"]:
            txt = (
                "👋 <b>Добро пожаловать в Auro VPN</b>\n\n"
                "Подключайте телефон и компьютер, управляйте подпиской\n"
                "и обращайтесь за помощью прямо здесь.\n\n"
                "<i>Перед покупкой ознакомьтесь с условиями сервиса\n"
                "и политикой конфиденциальности.</i>"
            )
            mk = kb(
                [btn("📄 Условия", "terms_open"), btn("🔒 Приватность", "privacy_open")],
                [btn("✅ Принять и продолжить", "terms_accept")],
                [btn("💬 Задать вопрос", "support")],
            )
            await show(bot, s, m.from_user.id, txt, mk)
            return
        if await is_restricted(s, m.from_user.id, "SERVICE"):
            reason = await is_restricted(s, m.from_user.id, "SERVICE")
            await show(bot, s, m.from_user.id,
                f"⚠️ <b>Операции ограничены</b>\n\nПричина: {safe(reason)}",
                kb([btn("⚖️ Апелляция", "appeal"), btn("💬 Поддержка", "support")]))
            return
        txt, mk = await render_hub(s, m.from_user.id)
        await show(bot, s, m.from_user.id, txt, mk)

@router.callback_query(F.data == "terms_accept")
async def accept_terms(cq: CallbackQuery):
    async with Session() as s:
        await s.execute(update(users).where(users.c.telegram_id == cq.from_user.id)
                        .values(is_terms_accepted=True, terms_accepted_at=now()))
        await s.commit()
        txt, mk = await render_hub(s, cq.from_user.id)
        await show(bot, s, cq.from_user.id, txt, mk)
    await cq.answer("✅ Принято")

@router.callback_query(F.data == "menu")
async def menu_cb(cq: CallbackQuery):
    async with Session() as s:
        txt, mk = await render_hub(s, cq.from_user.id)
        await show(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "terms_open")
async def terms_open(cq: CallbackQuery):
    await cq.message.answer(f"📄 {CFG['TERMS_URL']}", disable_web_page_preview=False)
    await cq.answer()

@router.callback_query(F.data == "privacy_open")
async def privacy_open(cq: CallbackQuery):
    await cq.message.answer(f"🔒 {CFG['PRIVACY_URL']}", disable_web_page_preview=False)
    await cq.answer()

# ──────────────────────────────────────────────────────────────────────────────
# 11. HANDLERS: подключение и инструкции
# ──────────────────────────────────────────────────────────────────────────────
@router.callback_query(F.data == "connect")
async def connect_cb(cq: CallbackQuery):
    async with Session() as s:
        sub = await current_sub(s, cq.from_user.id)
        if not sub or not sub["subscription_url"] or sub["status"] != "ACTIVE":
            txt = "⏳ <b>Готовим данные подключения</b>\n\nСсылка появится после подтверждения активации."
            mk = kb([btn("📦 Открыть заказы", "orders")],
                    [btn("↩️ Меню", "menu"), btn("💬 Поддержка", "support")])
        else:
            url = sub["subscription_url"]
            txt = (
                "🔗 <b>Подключение</b>\n\n"
                f"Подписка активна до <b>{fmt_dt(sub['expires_at'])}</b>.\n\n"
                "Скопируйте персональную ссылку и добавьте её в приложение.\n"
                "<i>Не передавайте ссылку посторонним — она даёт доступ к подписке.</i>"
            )
            mk = InlineKeyboardMarkup(inline_keyboard=[
                [copy_btn("📋 Скопировать ссылку", url)],
                [btn("📖 Инструкция", "guide"), btn("📱 Устройства", "devices")],
                [btn("🆘 Не подключается", "connect_help")],
                [btn("↩️ Меню", "menu")],
            ])
        await show(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "devices")
async def devices_cb(cq: CallbackQuery):
    async with Session() as s:
        sub = await current_sub(s, cq.from_user.id)
        limit = sub["devices_limit"] if sub else None
        txt = (
            "📱 <b>Устройства</b>\n\n"
            f"Лимит устройств: <b>{limit if limit else '—'}</b>\n\n"
            "<i>Отключить лишние устройства можно в приложении.</i>"
        )
        await show(bot, s, cq.from_user.id, txt, kb([btn("↩️ Назад", "connect")]))
    await cq.answer()

@router.callback_query(F.data == "guide")
async def guide_platforms(cq: CallbackQuery):
    async with Session() as s:
        await show(bot, s, cq.from_user.id, "📖 <b>На каком устройстве подключаемся?</b>", kb(
            [btn("🤖 Android", "guide:android"), btn("🍎 iPhone / iPad", "guide:ios")],
            [btn("🪟 Windows", "guide:windows"), btn("💻 macOS", "guide:macos")],
            [btn("↩️ Назад", "connect")],
        ))
    await cq.answer()

GUIDE_APPS = {
    "android": [("Happ (рекомендуется)", "app:happ_android"), ("v2rayNG", "app:v2rayng"), ("NekoBox", "app:nekobox")],
    "ios":     [("Happ (рекомендуется)", "app:happ_ios"), ("Streisand", "app:streisand"), ("Shadowrocket", "app:shadowrocket")],
    "windows": [("Happ (рекомендуется)", "app:happ_win"), ("Hiddify", "app:hiddify_win"), ("v2rayN", "app:v2rayn")],
    "macos":   [("Happ (рекомендуется)", "app:happ_mac"), ("Hiddify", "app:hiddify_mac"), ("V2Box", "app:v2box")],
}
GUIDE_TITLE = {"android": "Android", "ios": "iPhone и iPad", "windows": "Windows", "macos": "macOS"}

@router.callback_query(F.data.startswith("guide:"))
async def guide_apps(cq: CallbackQuery):
    platform = cq.data.split(":", 1)[1]
    if platform not in GUIDE_APPS:
        await cq.answer("Недоступно", show_alert=True); return
    async with Session() as s:
        txt = f"📖 <b>{GUIDE_TITLE[platform]}</b>\n\nВыберите приложение. Если ничего не установлено — начните с рекомендуемого."
        rows = [[btn(name, cb)] for name, cb in GUIDE_APPS[platform]]
        rows.append([btn("↩️ Назад", "guide")])
        await show(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

APP_INSTRUCTIONS = {
    "happ_android": ("Android · Happ",
        "1. Установите Happ из Google Play или с happ.su.\n"
        "2. Вернитесь в бот и скопируйте ссылку.\n"
        "3. В Happ: «+» → «Подписка» → вставьте ссылку.\n"
        "4. Выберите профиль и включите подключение.", "https://happ.su"),
    "v2rayng": ("Android · v2rayNG",
        "1. Установите v2rayNG только с GitHub 2dust/v2rayNG.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Импорт из буфера».\n"
        "4. Выберите подписку → «Подключиться».",
        "https://github.com/2dust/v2rayNG/releases"),
    "nekobox": ("Android · NekoBox",
        "1. Установите NekoBox только с GitHub MatsuriDayo/NekoBoxForAndroid.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Импорт из буфера».\n"
        "4. Выберите подписку → «Подключиться».",
        "https://github.com/MatsuriDayo/NekoBoxForAndroid/releases"),
    "happ_ios": ("iOS · Happ",
        "1. Установите Happ из App Store.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Подписка» → вставьте ссылку.\n"
        "4. Выберите профиль и включите подключение.", "https://happ.su"),
    "streisand": ("iOS · Streisand",
        "1. Установите Streisand из App Store.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://apps.apple.com/app/streisand/id6450534064"),
    "shadowrocket": ("iOS · Shadowrocket",
        "1. Установите Shadowrocket из App Store (платное).\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://apps.apple.com/app/shadowrocket/id932747118"),
    "happ_win": ("Windows · Happ",
        "1. Установите Happ с happ.su или Microsoft Store.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Подписка» → вставьте ссылку.\n"
        "4. Выберите профиль → «Подключиться».", "https://happ.su"),
    "hiddify_win": ("Windows · Hiddify",
        "1. Установите Hiddify с hiddify.com.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».", "https://hiddify.com"),
    "v2rayn": ("Windows · v2rayN",
        "1. Установите v2rayN только с GitHub 2dust/v2rayN.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «Подписки» → «Добавить» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://github.com/2dust/v2rayN/releases"),
    "happ_mac": ("macOS · Happ",
        "1. Установите Happ из App Store или с happ.su.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Подписка» → вставьте ссылку.\n"
        "4. Выберите профиль → «Подключиться».", "https://happ.su"),
    "hiddify_mac": ("macOS · Hiddify",
        "1. Установите Hiddify с hiddify.com.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».", "https://hiddify.com"),
    "v2box": ("macOS · V2Box",
        "1. Установите V2Box из App Store.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Импорт из буфера».\n"
        "4. Выберите подписку → «Подключиться».",
        "https://apps.apple.com/app/v2box-v2ray-client/id6446814690"),
}

@router.callback_query(F.data.startswith("app:"))
async def app_instr(cq: CallbackQuery):
    key = cq.data.split(":", 1)[1]
    if key not in APP_INSTRUCTIONS:
        await cq.answer("Инструкция временно недоступна", show_alert=True); return
    title, body, store = APP_INSTRUCTIONS[key]
    async with Session() as s:
        sub = await current_sub(s, cq.from_user.id)
        url = sub["subscription_url"] if sub and sub["subscription_url"] else ""
        txt = f"📖 <b>{title}</b>\n\n{body}\n\n<i>Устанавливайте приложение только из проверенного источника.</i>"
        rows = []
        if url:
            rows.append([copy_btn("📋 Скопировать ссылку", url)])
        rows.append([url_btn("⬇️ Установить приложение", store)])
        rows.append([btn("↩️ Выбрать другое", "guide"), btn("↩️ Назад", "connect")])
        await show(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

@router.callback_query(F.data == "connect_help")
async def connect_help(cq: CallbackQuery):
    async with Session() as s:
        txt = (
            "🆘 <b>Не получается подключиться</b>\n\n"
            "1. Проверьте срок подписки в боте.\n"
            "2. Обновите подписку в приложении.\n"
            "3. Выключите другое VPN-приложение.\n"
            "4. Повторите подключение.\n\n"
            "Если проблема осталась — напишите в поддержку."
        )
        await show(bot, s, cq.from_user.id, txt, kb(
            [btn("💬 Написать в поддержку", "support")],
            [btn("📖 Инструкция", "guide"), btn("↩️ Назад", "connect")],
        ))
    await cq.answer()

# ──────────────────────────────────────────────────────────────────────────────
# 12. HANDLERS: покупка
# ──────────────────────────────────────────────────────────────────────────────
PLAN_LABELS = {
    "plan_14d": "14 дней · 149 ₽",
    "plan_1m":  "30 дней · 299 ₽",
    "plan_3m":  "90 дней · 799 ₽",
    "plan_6m":  "180 дней · 1 490 ₽",
    "plan_1y":  "365 дней · 2 290 ₽",
}

@router.callback_query(F.data == "buy")
async def buy_cb(cq: CallbackQuery):
    async with Session() as s:
        if CFG["MAINTENANCE"]:
            await cq.answer("Сейчас технические работы", show_alert=True); return
        if await is_restricted(s, cq.from_user.id, "BUY"):
            await cq.answer("Покупки ограничены. Откройте поддержку.", show_alert=True); return
        rows = [[btn(label, f"plan:{code}")] for code, label in PLAN_LABELS.items()]
        rows.append([btn("↩️ Назад", "menu")])
        txt = (
            "💎 <b>Выберите срок</b>\n\n"
            "При продлении оставшийся срок сохраняется.\n"
            "Внешняя оплата — через СБП. Автоматических списаний нет."
        )
        await show(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

@router.callback_query(F.data.startswith("plan:"))
async def plan_pick(cq: CallbackQuery):
    code = cq.data.split(":", 1)[1]
    async with Session() as s:
        p = (await s.execute(select(plans).where(plans.c.code == code))).mappings().first()
        if not p or not p["enabled"]:
            await cq.answer("Тариф временно недоступен", show_alert=True); return
        avail = await WalletSvc.available(s, cq.from_user.id)
        rows = []
        if avail >= p["price_rub"]:
            rows.append([btn(f"💳 Списать {fmt_rub(p['price_rub'])} с баланса", f"pay_balance:{code}")])
        rows.append([btn(f"🌐 Оплатить {fmt_rub(p['price_rub'])} через СБП", f"pay_sbp:{code}")])
        if avail < p["price_rub"]:
            rows.append([btn("➕ Пополнить баланс", "topup_menu")])
        rows.append([btn("↩️ К тарифам", "buy")])
        txt = (
            f"💎 <b>Подписка на {p['days']} дней</b>\n\n"
            f"Стоимость: <b>{fmt_rub(p['price_rub'])}</b>\n"
            f"Баланс: {fmt_rub(avail)}\n\n"
            f"<i>Оставшийся срок сохранится. Точная дата — после активации.</i>"
        )
        await show(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

@router.callback_query(F.data.startswith("pay_balance:"))
async def pay_balance(cq: CallbackQuery):
    code = cq.data.split(":", 1)[1]
    async with Session() as s:
        p = (await s.execute(select(plans).where(plans.c.code == code))).mappings().first()
        if not p:
            await cq.answer("Тариф недоступен", show_alert=True); return
        w = await WalletSvc.get(s, cq.from_user.id)
        avail = rub(w["deposit_rub"] + w["bonus_rub"])
        if avail < p["price_rub"]:
            await cq.answer("Недостаточно средств", show_alert=True); return

        price = rub(p["price_rub"])
        bonus_use = min(rub(w["bonus_rub"]), price)
        deposit_use = price - bonus_use

        await s.execute(update(wallets).where(wallets.c.user_id == cq.from_user.id).values(
            bonus_rub=rub(w["bonus_rub"] - bonus_use),
            deposit_rub=rub(w["deposit_rub"] - deposit_use),
        ))
        oid = await create_order(s, cq.from_user.id, "SUB", code, price, p["days"])
        await LedgerSvc.add(s, cq.from_user.id, "SUB_PAYMENT", -price, f"order:{oid}", f"plan={code}")
        await s.execute(insert(fulfillment_jobs).values(id=secrets.token_hex(16), order_id=oid, state="QUEUED"))
        await s.execute(update(orders).where(orders.c.id == oid).values(status="PAID"))
        await s.commit()

        await show(bot, s, cq.from_user.id,
            "✅ <b>Оплата получена</b>\n\nПодключаем подписку. Повторно платить не нужно.",
            kb([btn("🔄 Обновить статус", f"order_view:{oid}")],
               [btn("↩️ Меню", "menu"), btn("💬 Поддержка", "support")]))
    await cq.answer("✅ Списано")

@router.callback_query(F.data.startswith("pay_sbp:"))
async def pay_sbp(cq: CallbackQuery):
    code = cq.data.split(":", 1)[1]
    async with Session() as s:
        p = (await s.execute(select(plans).where(plans.c.code == code))).mappings().first()
        if not p:
            await cq.answer("Тариф недоступен", show_alert=True); return
        oid = await create_order(s, cq.from_user.id, "SUB", code, p["price_rub"], p["days"])
        await s.commit()
        await make_sbp_payment(cq, s, oid, p["price_rub"], f"Подписка {code}", f"pay_sbp:{code}")
    await cq.answer()

@router.callback_query(F.data.startswith("check_pay:"))
async def check_pay(cq: CallbackQuery):
    oid = cq.data.split(":", 1)[1]
    async with Session() as s:
        pa = (await s.execute(select(payment_attempts).where(payment_attempts.c.order_id == oid))).mappings().first()
        if not pa:
            await cq.answer("Платёж не найден", show_alert=True); return
        try:
            res = await Platega.get_transaction(pa["external_id"])
        except Exception as e:
            log.warning("platega get failed: %s", e)
            await cq.answer("Сервис оплаты недоступен. Попробуйте позже.", show_alert=True); return
        if (res.get("status") or "").upper() == "CONFIRMED":
            await finalize_payment(s, oid, "PLATEGA", pa["external_id"])
            await show(bot, s, cq.from_user.id,
                "✅ <b>Оплата получена</b>\n\nПодключаем подписку. Повторно платить не нужно.",
                kb([btn("🔄 Обновить статус", f"order_view:{oid}")], [btn("↩️ Меню", "menu")]))
            await cq.answer("✅ Подтверждено")
        else:
            await cq.answer("⏳ Подтверждение пока не поступило")

# ──────────────────────────────────────────────────────────────────────────────
# 13. HANDLERS: заказы
# ──────────────────────────────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("order_view:"))
async def order_view(cq: CallbackQuery):
    oid = cq.data.split(":", 1)[1]
    async with Session() as s:
        o = (await s.execute(select(orders).where(orders.c.id == oid))).mappings().first()
        if not o or o["user_id"] != cq.from_user.id:
            await cq.answer("Заказ не найден", show_alert=True); return
        sub = await current_sub(s, o["user_id"])
        kind = "Подписка" if o["kind"] == "SUB" else "Пополнение"
        txt = (
            f"📦 <b>Заказ №<code>{oid[:8]}</code></b>\n\n"
            f"Тип: {kind}\n"
            f"Стоимость: <b>{fmt_rub(o['amount_rub'])}</b>\n"
            f"Статус: <b>{o['status']}</b>"
        )
        rows = []
        if o["status"] in ("PAID", "FULFILLED") and sub and sub["status"] == "ACTIVE":
            txt += f"\n\nПодписка активна до <b>{fmt_dt(sub['expires_at'])}</b>."
            rows.append([btn("🔗 Подключиться", "connect")])
        elif o["status"] == "PAID":
            txt += "\n\n<i>Выполнение: подключаем подписку.</i>"
            rows.append([btn("🔄 Обновить", f"order_view:{oid}")])
        rows.append([btn("💬 Помощь", "support"), btn("↩️ Назад", "orders")])
        await show(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

@router.callback_query(F.data == "orders")
async def orders_list(cq: CallbackQuery):
    async with Session() as s:
        r = await s.execute(select(orders).where(orders.c.user_id == cq.from_user.id)
                            .order_by(orders.c.created_at.desc()).limit(10))
        rows = []
        for o in r.mappings():
            rows.append([btn(f"№{o['id'][:8]} · {fmt_rub(o['amount_rub'])} · {o['status']}",
                             f"order_view:{o['id']}")])
        rows.append([btn("↩️ Назад", "menu")])
        txt = "📦 <b>Ваши заказы</b>" if len(rows) > 1 else "📦 <i>У вас пока нет заказов.</i>"
        await show(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

# ──────────────────────────────────────────────────────────────────────────────
# 14. HANDLERS: кабинет / пополнение / история
# ──────────────────────────────────────────────────────────────────────────────
@router.callback_query(F.data == "cabinet")
async def cabinet(cq: CallbackQuery):
    async with Session() as s:
        w = await WalletSvc.get(s, cq.from_user.id)
        avail = rub(w["deposit_rub"] + w["bonus_rub"])
        txt = (
            f"👤 <b>Личный кабинет</b>\n\n"
            f"ID: <code>{cq.from_user.id}</code>\n"
            f"Внесено: <b>{fmt_rub(w['deposit_rub'])}</b>\n"
            f"Бонусы: <b>{fmt_rub(w['bonus_rub'])}</b>\n"
            f"<blockquote>💳 Доступно: <b>{fmt_rub(avail)}</b></blockquote>"
        )
        await show(bot, s, cq.from_user.id, txt, kb(
            [btn("➕ Пополнить через СБП", "topup_menu")],
            [btn("📦 Мои заказы", "orders"), btn("🧾 История", "history")],
            [btn("🎁 Пригласить друзей", "ref")],
            [btn("🔔 Уведомления", "notify"), btn("📄 Документы", "docs")],
            [btn("↩️ Меню", "menu")],
        ))
    await cq.answer()

@router.callback_query(F.data == "topup_menu")
async def topup_menu(cq: CallbackQuery):
    async with Session() as s:
        await show(bot, s, cq.from_user.id,
            "➕ <b>Пополнение баланса</b>\n\n"
            "Выберите сумму или укажите свою.\n"
            f"<i>От {fmt_rub(CFG['TOPUP_MIN'])} до {fmt_rub(CFG['TOPUP_MAX'])}.</i>",
            kb(
                [btn("150 ₽", "topup:150"), btn("300 ₽", "topup:300"), btn("500 ₽", "topup:500")],
                [btn("1 000 ₽", "topup:1000"), btn("2 500 ₽", "topup:2500")],
                [btn("✏️ Своя сумма", "topup_custom")],
                [btn("↩️ Назад", "cabinet")],
            ))
    await cq.answer()

@router.callback_query(F.data.startswith("topup:"))
async def topup_fixed(cq: CallbackQuery):
    amount = Decimal(cq.data.split(":", 1)[1])
    await _do_topup(cq, amount)

@router.callback_query(F.data == "topup_custom")
async def topup_custom(cq: CallbackQuery, state: FSMContext):
    await state.set_state(FS.topup_amount)
    async with Session() as s:
        await show(bot, s, cq.from_user.id,
            "✏️ <b>Введите сумму</b>\n\n"
            f"Целое число от {int(CFG['TOPUP_MIN'])} до {int(CFG['TOPUP_MAX'])}.\n"
            "Например: <code>500</code>",
            kb([btn("↩️ Отмена", "menu")]))
    await cq.answer()

@router.message(FS.topup_amount)
async def topup_amount(m: Message, state: FSMContext):
    raw = re.sub(r"[\s\u00A0_]", "", m.text or "")
    if not raw.isdigit():
        await m.answer("Введите целое число.")
        return
    amount = Decimal(raw)
    if amount < CFG["TOPUP_MIN"] or amount > CFG["TOPUP_MAX"]:
        await m.answer(f"Сумма должна быть от {fmt_rub(CFG['TOPUP_MIN'])} до {fmt_rub(CFG['TOPUP_MAX'])}.")
        return
    try: await m.delete()
    except Exception: pass
    await state.clear()
    # эмулируем CallbackQuery для переиспользования
    class _CQ: from_user = m.from_user
    await _do_topup(_CQ, amount)

async def _do_topup(cq, amount: Decimal):
    async with Session() as s:
        oid = await create_order(s, cq.from_user.id, "TOPUP", None, amount, None)
        await s.commit()
        await make_sbp_payment(cq, s, oid, amount, "Пополнение баланса", "topup_menu")

@router.callback_query(F.data == "history")
async def history(cq: CallbackQuery):
    async with Session() as s:
        r = await s.execute(select(ledger).where(ledger.c.user_id == cq.from_user.id)
                            .order_by(ledger.c.created_at.desc()).limit(10))
        rows = []
        for it in r.mappings():
            sign = "+" if it["amount_rub"] > 0 else "−"
            rows.append([btn(f"{fmt_dt(it['created_at'])[:5]} · {it['kind']} · {sign}{fmt_rub(abs(it['amount_rub']))}",
                             "noop")])
        rows.append([btn("↩️ Назад", "cabinet")])
        txt = "🧾 <b>История</b>" if len(rows) > 1 else "🧾 <i>История пуста.</i>"
        await show(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

# ──────────────────────────────────────────────────────────────────────────────
# 15. HANDLERS: рефералка
# ──────────────────────────────────────────────────────────────────────────────
@router.callback_query(F.data == "ref")
async def ref_cb(cq: CallbackQuery):
    async with Session() as s:
        rc = (await s.execute(select(ref_codes).where(ref_codes.c.user_id == cq.from_user.id))).mappings().first()
        if not rc:
            code = secrets.token_urlsafe(6)
            try:
                await s.execute(insert(ref_codes).values(code=code, user_id=cq.from_user.id))
                await s.commit()
            except IntegrityError:
                await s.rollback()
                code = (await s.execute(select(ref_codes.c.code).where(ref_codes.c.user_id == cq.from_user.id))).scalar()
        else:
            code = rc["code"]
        invited = (await s.execute(select(func.count()).select_from(referrals)
                                   .where(referrals.c.inviter_id == cq.from_user.id))).scalar() or 0
        paid = (await s.execute(select(func.count(func.distinct(referral_rewards.c.invited_id)))
                                .where(referral_rewards.c.inviter_id == cq.from_user.id))).scalar() or 0
        avail = (await s.execute(select(func.coalesce(func.sum(referral_rewards.c.amount_rub), 0))
                                 .where(and_(referral_rewards.c.inviter_id == cq.from_user.id,
                                             referral_rewards.c.status == "AVAILABLE")))).scalar() or 0
        pending = (await s.execute(select(func.coalesce(func.sum(referral_rewards.c.amount_rub), 0))
                                   .where(and_(referral_rewards.c.inviter_id == cq.from_user.id,
                                               referral_rewards.c.status == "PENDING")))).scalar() or 0
        link = f"https://t.me/{CFG['BOT_USERNAME']}?start=ref_{code}"
        txt = (
            "🎁 <b>Приглашайте друзей</b>\n\n"
            f"<b>{CFG['REF_PERCENT']}%</b> с каждого платежа через СБП — подписка или пополнение.\n\n"
            f"Приглашено: <b>{invited}</b> · Оплатили: <b>{paid}</b>\n"
            f"<blockquote>✅ Доступно: <b>{fmt_rub(avail)}</b>\n"
            f"⏳ Ожидает: <b>{fmt_rub(pending)}</b></blockquote>"
        )
        mk = InlineKeyboardMarkup(inline_keyboard=[
            [copy_btn("📋 Скопировать ссылку", link)],
            [btn("↩️ Назад", "menu")],
        ])
        await show(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

# ──────────────────────────────────────────────────────────────────────────────
# 16. HANDLERS: поддержка
# ──────────────────────────────────────────────────────────────────────────────
@router.callback_query(F.data == "support")
async def support_cb(cq: CallbackQuery):
    async with Session() as s:
        t = (await s.execute(select(tickets).where(and_(
            tickets.c.user_id == cq.from_user.id, tickets.c.status == "OPEN",
        )).order_by(tickets.c.id.desc()).limit(1))).mappings().first()
        if t:
            txt = f"💬 <b>Обращение №{t['id']}</b>\nСтатус: в работе"
            mk = kb([btn("📨 Открыть", f"ticket_view:{t['id']}")],
                    [btn("🆕 Новое обращение", "ticket_new")],
                    [btn("↩️ Меню", "menu")])
        else:
            txt = (
                "💬 <b>Служба поддержки</b>\n\n"
                "Если у вас возникли вопросы по работе сервиса или оплате — создайте обращение."
            )
            mk = kb([btn("✍️ Написать в поддержку", "ticket_new")], [btn("↩️ Меню", "menu")])
        await show(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "ticket_new")
async def ticket_new(cq: CallbackQuery, state: FSMContext):
    await state.set_state(FS.support_text)
    async with Session() as s:
        await show(bot, s, cq.from_user.id,
            "✍️ <b>Опишите проблему</b>\n\n"
            "Что произошло и когда?\n"
            "5–2 000 символов.\n\n"
            "<i>Не отправляйте банковские коды, реквизиты карты и персональную ссылку подписки.</i>",
            kb([btn("↩️ Отмена", "support")]))
    await cq.answer()

@router.message(FS.support_text)
async def ticket_text(m: Message, state: FSMContext):
    text = (m.text or "").strip()
    if not (5 <= len(text) <= 2000):
        await m.answer("Введите от 5 до 2 000 символов.")
        return
    async with Session() as s:
        try:
            res = await s.execute(insert(tickets).values(
                user_id=m.from_user.id, subject="GENERAL", status="OPEN",
            ).returning(tickets.c.id))
            tid = res.scalar()
        except IntegrityError:
            await s.rollback()
            await m.answer("Не удалось создать обращение. Попробуйте позже.")
            return
        await s.execute(insert(ticket_messages).values(ticket_id=tid, sender="USER", text=text))
        await s.commit()
        try: await m.delete()
        except Exception: pass
        await state.clear()
        await show(bot, s, m.from_user.id,
            f"✅ <b>Обращение №{tid} отправлено</b>\n\n"
            f"Статус: ожидает ответа\n"
            f"Ответ придёт уведомлением и сохранится в переписке.",
            kb([btn("📨 Открыть", f"ticket_view:{tid}")], [btn("↩️ Меню", "menu")]))

@router.callback_query(F.data.startswith("ticket_view:"))
async def ticket_view(cq: CallbackQuery):
    tid = int(cq.data.split(":", 1)[1])
    async with Session() as s:
        await _render_ticket(s, cq.from_user.id, tid)
    await cq.answer()

async def _render_ticket(s: AsyncSession, uid: int, tid: int):
    t = (await s.execute(select(tickets).where(tickets.c.id == tid))).mappings().first()
    if not t or t["user_id"] != uid:
        return
    msgs = (await s.execute(select(ticket_messages).where(ticket_messages.c.ticket_id == tid)
                            .order_by(ticket_messages.c.id.desc()).limit(6))).mappings().all()
    lines = [f"💬 <b>Обращение №{tid}</b>",
             f"Статус: <b>{'в работе' if t['status'] == 'OPEN' else 'закрыто'}</b>", ""]
    for x in reversed(msgs):
        who = "Вы" if x["sender"] == "USER" else "Поддержка"
        lines.append(f"<b>{who}:</b> {safe(x['text'])}")
    mk = kb([btn("✍️ Ответить", f"ticket_reply:{tid}"), btn("🔒 Закрыть", f"ticket_close:{tid}")],
            [btn("↩️ Назад", "support")])
    await show(bot, s, uid, "\n".join(lines), mk)

@router.callback_query(F.data.startswith("ticket_reply:"))
async def ticket_reply(cq: CallbackQuery, state: FSMContext):
    tid = int(cq.data.split(":", 1)[1])
    await state.update_data(ticket_id=tid)
    await state.set_state(FS.support_reply)
    async with Session() as s:
        await show(bot, s, cq.from_user.id,
            f"✍️ <b>Ответ в обращение №{tid}</b>\n\nВведите сообщение (1–2 000).",
            kb([btn("↩️ Отмена", f"ticket_view:{tid}")]))
    await cq.answer()

@router.message(FS.support_reply)
async def ticket_reply_text(m: Message, state: FSMContext):
    d = await state.get_data()
    tid = d.get("ticket_id")
    text = (m.text or "").strip()
    if not (1 <= len(text) <= 2000):
        await m.answer("Введите от 1 до 2 000 символов.")
        return
    async with Session() as s:
        await s.execute(insert(ticket_messages).values(ticket_id=tid, sender="USER", text=text))
        await s.commit()
        try: await m.delete()
        except Exception: pass
        await state.clear()
        await _render_ticket(s, m.from_user.id, tid)

@router.callback_query(F.data.startswith("ticket_close:"))
async def ticket_close(cq: CallbackQuery):
    tid = int(cq.data.split(":", 1)[1])
    async with Session() as s:
        await s.execute(update(tickets).where(and_(
            tickets.c.id == tid, tickets.c.user_id == cq.from_user.id,
        )).values(status="CLOSED"))
        await s.commit()
        await show(bot, s, cq.from_user.id,
            f"🔒 Обращение №{tid} закрыто.",
            kb([btn("↩️ Меню", "menu")]))
    await cq.answer()

# ──────────────────────────────────────────────────────────────────────────────
# 17. HANDLERS: настройки / инфо
# ──────────────────────────────────────────────────────────────────────────────
@router.callback_query(F.data == "notify")
async def notify_cb(cq: CallbackQuery):
    async with Session() as s:
        u = await get_user(s, cq.from_user.id)
        def t(b): return "✅" if b else "⬜"
        txt = (
            "🔔 <b>Уведомления</b>\n\n"
            "Сообщения о платежах, заказах, безопасности и ответах поддержки\n"
            "относятся к обслуживанию аккаунта и не отключаются."
        )
        await show(bot, s, cq.from_user.id, txt, kb(
            [btn(f"{t(u['notify_expiry'])} Напоминания об окончании", "notify:expiry")],
            [btn(f"{t(u['notify_news'])} Новости сервиса", "notify:news")],
            [btn(f"{t(u['notify_offers'])} Предложения", "notify:offers")],
            [btn("↩️ Назад", "cabinet")],
        ))
    await cq.answer()

@router.callback_query(F.data.startswith("notify:"))
async def notify_toggle(cq: CallbackQuery):
    field = cq.data.split(":", 1)[1]
    col = {"expiry": "notify_expiry", "news": "notify_news", "offers": "notify_offers"}.get(field)
    if not col:
        await cq.answer(); return
    async with Session() as s:
        u = await get_user(s, cq.from_user.id)
        await s.execute(update(users).where(users.c.telegram_id == cq.from_user.id)
                        .values({col: not u[col]}))
        await s.commit()
    await cq.answer("Сохранено")

@router.callback_query(F.data == "docs")
async def docs_cb(cq: CallbackQuery):
    async with Session() as s:
        await show(bot, s, cq.from_user.id, "📄 <b>Документы и данные</b>", kb(
            [btn("📄 Условия", "terms_open"), btn("🔒 Приватность", "privacy_open")],
            [btn("🗑 Запрос удаления", "delete_req")],
            [btn("↩️ Назад", "cabinet")],
        ))
    await cq.answer()

@router.callback_query(F.data == "delete_req")
async def delete_req(cq: CallbackQuery):
    async with Session() as s:
        res = await s.execute(insert(tickets).values(
            user_id=cq.from_user.id, subject="DELETE", status="OPEN",
        ).returning(tickets.c.id))
        tid = res.scalar()
        await s.execute(insert(ticket_messages).values(
            ticket_id=tid, sender="USER", text="Запрос на удаление аккаунта",
        ))
        await s.commit()
        await show(bot, s, cq.from_user.id,
            f"✅ Запрос на удаление зарегистрирован как обращение №{tid}.\n"
            f"<i>Перед обработкой проверим активные подписки, остаток средств,\n"
            f"незавершённые платежи и обращения.</i>",
            kb([btn("↩️ Меню", "menu")]))
    await cq.answer()

@router.callback_query(F.data == "about")
async def about_cb(cq: CallbackQuery):
    async with Session() as s:
        txt = (
            "ℹ️ <b>Auro VPN</b>\n\n"
            "Подключайте телефон и компьютер, управляйте подпиской\n"
            "и обращайтесь за помощью прямо здесь.\n\n"
            "<i>Сервис не проверяет ваше соединение и не обещает конкретную скорость.</i>"
        )
        rows = []
        if CFG["CHANNEL_URL"]:
            rows.append([url_btn("📢 Канал", CFG["CHANNEL_URL"])])
        rows.append([btn("📄 Документы", "docs"), btn("💬 Поддержка", "support")])
        rows.append([btn("↩️ Меню", "menu")])
        await show(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

@router.callback_query(F.data == "appeal")
async def appeal_start(cq: CallbackQuery, state: FSMContext):
    await state.set_state(FS.appeal_text)
    async with Session() as s:
        await show(bot, s, cq.from_user.id,
            "⚖️ <b>Апелляция</b>\n\n"
            "Опишите ситуацию и причину, по которой ограничение следует пересмотреть.\n"
            "10–1 000 символов.",
            kb([btn("↩️ Отмена", "menu")]))
    await cq.answer()

@router.message(FS.appeal_text)
async def appeal_text(m: Message, state: FSMContext):
    text = (m.text or "").strip()
    if not (10 <= len(text) <= 1000):
        await m.answer("Введите от 10 до 1 000 символов.")
        return
    async with Session() as s:
        res = await s.execute(insert(tickets).values(
            user_id=m.from_user.id, subject="APPEAL", status="OPEN",
        ).returning(tickets.c.id))
        tid = res.scalar()
        await s.execute(insert(ticket_messages).values(ticket_id=tid, sender="USER", text=text))
        await s.commit()
        try: await m.delete()
        except Exception: pass
        await state.clear()
        await show(bot, s, m.from_user.id,
            f"✅ Апелляция отправлена (обращение №{tid}).\nОжидайте решения.",
            kb([btn("↩️ Меню", "menu"), btn("💬 Поддержка", "support")]))

# ──────────────────────────────────────────────────────────────────────────────
# 18. FALLBACK
# ──────────────────────────────────────────────────────────────────────────────
@router.message()
async def fallback(m: Message, state: FSMContext):
    if await state.get_state():
        return
    try: await m.delete()
    except Exception: pass
    await m.answer("↩️ Откройте меню: /start")

# ──────────────────────────────────────────────────────────────────────────────
# 19. WEBHOOKS + LIFESPAN + WORKER
# ──────────────────────────────────────────────────────────────────────────────
async def process_payment_event(external_id: str, status: str, source: str, body: dict):
    async with Session() as s:
        pa = (await s.execute(select(payment_attempts)
                              .where(payment_attempts.c.external_id == external_id))).mappings().first()
        if not pa:
            return
        if status == "CONFIRMED":
            await finalize_payment(s, pa["order_id"], source, external_id)
        elif status == "CANCELED":
            await s.execute(update(payment_attempts).where(payment_attempts.c.external_id == external_id)
                            .values(status="CANCELED"))
            await s.execute(update(orders).where(orders.c.id == pa["order_id"]).values(status="CANCELED"))
            await s.commit()
        elif status in ("DISPUTED", "PARTIALLY_REFUNDED", "CHARGEBACKED"):
            try:
                await s.execute(insert(payment_events).values(
                    id=secrets.token_hex(16), source=source, external_id=external_id,
                    status=status, body=json.dumps(body)[:1900], processed=False,
                ))
                await s.commit()
            except IntegrityError:
                await s.rollback()

async def bg_worker():
    while True:
        try:
            await process_jobs()
            await expire_orders()
            await release_rewards()
        except Exception:
            log.exception("worker")
        await asyncio.sleep(CFG["WORKER_INTERVAL"])

async def process_jobs():
    async with Session() as s:
        r = await s.execute(select(fulfillment_jobs)
                            .where(fulfillment_jobs.c.state.in_(("QUEUED", "RETRY")))
                            .limit(5).with_for_update(skip_locked=True))
        for j in r.mappings():
            o = (await s.execute(select(orders).where(orders.c.id == j["order_id"]))).mappings().first()
            if not o or not o["plan_code"]:
                await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"])
                                .values(state="FAILED", last_error="no plan"))
                await s.commit(); continue
            await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"])
                            .values(state="RUNNING", updated_at=now()))
            await s.commit()
            try:
                res = await AdaptGroup.create_subscription(
                    o["user_id"], o["plan_code"], idempotency_key=f"sub-{o['id']}")
                ext = str(res.get("subscriptionId") or res.get("id") or "")
                url = res.get("subscriptionUrl") or res.get("url") or ""
                exp = res.get("expiresAt")
                if exp:
                    try: exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                    except Exception: exp_dt = now() + timedelta(days=o["days_snapshot"] or 0)
                else:
                    exp_dt = now() + timedelta(days=o["days_snapshot"] or 0)
                exists = (await s.execute(select(subscriptions.c.user_id)
                                          .where(subscriptions.c.user_id == o["user_id"]))).first()
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
                try:
                    await bot.send_message(
                        o["user_id"],
                        f"✅ <b>Подписка готова</b>\n\n"
                        f"Добавлено: <b>{o['days_snapshot']} дней</b>\n"
                        f"Действует до <b>{fmt_dt(exp_dt)}</b>",
                    )
                except Exception:
                    log.warning("notify uid=%s failed", o["user_id"])
            except Exception as e:
                attempts = j["attempts"] + 1
                state = "MANUAL" if attempts >= CFG["FULFILL_MAX_ATTEMPTS"] else "RETRY"
                await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"]).values(
                    state=state, attempts=attempts, last_error=str(e)[:200], updated_at=now()))
                await s.commit()
                log.warning("fulfill failed oid=%s attempts=%s err=%s", o["id"], attempts, e)

async def expire_orders():
    async with Session() as s:
        await s.execute(update(orders).where(and_(
            orders.c.status == "AWAITING_PAYMENT",
            orders.c.expires_at < now(),
        )).values(status="EXPIRED"))
        await s.commit()

async def release_rewards():
    async with Session() as s:
        r = await s.execute(select(referral_rewards).where(and_(
            referral_rewards.c.status == "PENDING",
            referral_rewards.c.available_at <= now(),
        )))
        for rw in r.mappings():
            upd = await s.execute(update(referral_rewards).where(and_(
                referral_rewards.c.id == rw["id"],
                referral_rewards.c.status == "PENDING",
            )).values(status="AVAILABLE").returning(referral_rewards.c.id))
            if upd.scalar():
                await s.execute(update(wallets).where(wallets.c.user_id == rw["inviter_id"]).values(
                    bonus_rub=wallets.c.bonus_rub + rw["amount_rub"]))
        await s.commit()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await bot.set_webhook(
        url=CFG["PUBLIC_BASE_URL"] + CFG["TG_PATH"],
        secret_token=CFG["WEBHOOK_SECRET_TG"],
        allowed_updates=dp.resolve_used_update_types(),
        drop_pending_updates=True,
    )
    task = asyncio.create_task(bg_worker())
    log.info("started")
    try:
        yield
    finally:
        task.cancel()
        try: await task
        except asyncio.CancelledError: pass
        await bot.session.close()

app = FastAPI(lifespan=lifespan)

@app.post(CFG["TG_PATH"])
async def tg_webhook(request: Request):
    header = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not tse(header, CFG["WEBHOOK_SECRET_TG"]):
        raise HTTPException(401)
    data = await request.json()
    upd = Update.model_validate(data)
    await dp.feed_update(bot, upd)
    return Response(status_code=200)

@app.post(CFG["PG_PATH"])
async def platega_webhook(request: Request):
    if not (tse(request.headers.get("X-MerchantId", ""), CFG["PLATEGA_MID"]) and
            tse(request.headers.get("X-Secret", ""), CFG["PLATEGA_SECRET"])):
        raise HTTPException(401)
    body = await request.json()
    ext_id = str(body.get("transactionId") or body.get("id") or "")
    status = str(body.get("status") or "").upper()
    async with Session() as s:
        try:
            await s.execute(insert(payment_events).values(
                id=secrets.token_hex(16), source="PLATEGA", external_id=ext_id,
                status=status, body=json.dumps(body)[:1900], processed=False,
            ))
            await s.commit()
        except IntegrityError:
            await s.rollback()
    asyncio.create_task(process_payment_event(ext_id, status, "PLATEGA", body))
    return {"ok": True}

@app.post(CFG["AG_PATH"])
async def adaptgroup_webhook(request: Request):
    fwd = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    client_ip = fwd or (request.client.host if request.client else "")
    if CFG["AG_IP"] and client_ip != CFG["AG_IP"]:
        raise HTTPException(401)
    raw = await request.body()
    sig = request.headers.get("X-Webhook-Signature", "")
    expected = hmac.new(CFG["AG_SECRET"].encode(), raw, hashlib.sha256).hexdigest()
    if not tse(sig, expected):
        raise HTTPException(401)
    body = json.loads(raw or b"{}")
    async with Session() as s:
        ext_id = str(body.get("subscriptionId") or body.get("id") or "")
        ev = str(body.get("event") or "")
        if ext_id and ev in ("subscription.created", "subscription.renewed"):
            try:
                await s.execute(update(subscriptions).where(subscriptions.c.external_id == ext_id)
                                .values(last_sync=now()))
                await s.commit()
            except Exception:
                await s.rollback()
    return {"ok": True}

# ──────────────────────────────────────────────────────────────────────────────
# 20. ENTRYPOINT
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), log_level="info")
