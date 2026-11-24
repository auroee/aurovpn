# main.py — Auro VPN v25.0 (single-file skeleton, production-oriented)
import asyncio, json, hmac, hashlib, logging, os, re, secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    CopyTextButton,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from fastapi import FastAPI, Request, Response, HTTPException
from sqlalchemy import (
    text, select, update, insert, BigInteger, String, Numeric, Boolean, DateTime,
    Integer, UniqueConstraint, ForeignKey, MetaData, Table, Column, func, and_, or_
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# ------------------------------------------------------------------------------
# Конфигурация
# ------------------------------------------------------------------------------
def env(k: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(k, default)
    if required and not v:
        raise SystemExit(f"ENV {k} is required")
    return v or ""

CFG = dict(
    BOT_TOKEN=env("BOT_TOKEN", required=True),
    BOT_USERNAME=env("BOT_USERNAME", "aurovpn_bot"),
    WEBHOOK_SECRET_TG=env("WEBHOOK_SECRET_TG", "tg-secret"),
    ADMIN_IDS=[int(x) for x in env("ADMIN_IDS", "").split(",") if x.strip()],
    DATABASE_URL=env("DATABASE_URL", "sqlite+aiosqlite:///./auro.db"),
    PUBLIC_BASE_URL=env("PUBLIC_BASE_URL", "https://aurovpn.cc.cd").rstrip("/"),
    TG_PATH=env("TELEGRAM_WEBHOOK_PATH", "/tg/webhook"),
    PG_PATH=env("PLATEGA_WEBHOOK_PATH", "/platega/webhook"),
    AG_PATH=env("ADAPTGROUP_WEBHOOK_PATH", "/adaptgroup/webhook"),

    PLATEGA_BASE=env("PLATEGA_BASE_URL", "https://app.platega.io").rstrip("/"),
    PLATEGA_MID=env("PLATEGA_MERCHANT_ID"),
    PLATEGA_SECRET=env("PLATEGA_SECRET"),
    PLATEGA_METHOD=int(env("PLATEGA_PAYMENT_METHOD", "2")),
    PLATEGA_RETURN=env("PLATEGA_RETURN_URL", "https://t.me/aurovpn_bot"),

    AG_BASE=env("ADAPTGROUP_BASE_URL", "https://network-api.adaptgroup.app").rstrip("/"),
    AG_KEY=env("ADAPTGROUP_API_KEY"),
    AG_SECRET=env("ADAPTGROUP_WEBHOOK_SECRET", "ag-secret"),
    AG_IP=env("ADAPTGROUP_IP_ALLOW", ""),
    AG_PLAN_MAP=json.loads(env("ADAPTGROUP_PLAN_MAP", "{}")),

    REF_PERCENT=Decimal(env("REFERRAL_PERCENT", "15")),
    REF_HOLD_H= int(env("REFERRAL_HOLD_HOURS", "24")),
    TOPUP_MIN=Decimal(env("TOPUP_MIN", "50")),
    TOPUP_MAX=Decimal(env("TOPUP_MAX", "10000")),
    ORDER_TTL=int(env("ORDER_TTL_MINUTES", "20")),
    MAINTENANCE=env("MAINTENANCE", "false").lower() == "true",
)

UTC = timezone.utc
log = logging.getLogger("auro")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# ------------------------------------------------------------------------------
# Утилиты
# ------------------------------------------------------------------------------
def now() -> datetime: return datetime.now(UTC)
def rub(x) -> Decimal: return Decimal(x).quantize(Decimal("0.01"), ROUND_HALF_UP)
def fmt_rub(x) -> str:
    x = rub(x)
    s = f"{x:,.2f}".replace(",", " ").replace(".", ",")
    return s[:-3] + " ₽" if s.endswith(",00") else s + " ₽"
def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(timezone(timedelta(hours=3))).strftime("%d.%m.%Y, %H:%M МСК")
def safe(x) -> str: return (x or "")[:4000]
def timing_safe_eq(a: str, b: str) -> bool:
    return hmac.compare_digest((a or "").encode(), (b or "").encode())

# ------------------------------------------------------------------------------
# БД (SQLAlchemy Core, миграции при старте)
# ------------------------------------------------------------------------------
engine = create_async_engine(CFG["DATABASE_URL"], pool_pre_ping=True)
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
    Column("kind", String(16), nullable=False),  # BUY / SUPPORT / SERVICE
    Column("reason", String(255), nullable=False),
    Column("until", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), default=now, nullable=False),
    UniqueConstraint("user_id", "kind", name="uq_restriction_user_kind"),
)

wallets = Table("wallets", meta,
    Column("user_id", BigInteger, ForeignKey("users.telegram_id", ondelete="CASCADE"), primary_key=True),
    Column("deposit_rub", Numeric(12,2), default=Decimal("0.00"), nullable=False),
    Column("bonus_rub", Numeric(12,2), default=Decimal("0.00"), nullable=False),
    Column("hold_rub", Numeric(12,2), default=Decimal("0.00"), nullable=False),
)

plans = Table("plans", meta,
    Column("code", String(8), primary_key=True),
    Column("days", Integer, nullable=False),
    Column("price_rub", Numeric(12,2), nullable=False),
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
    Column("kind", String(12), nullable=False),  # SUB / TOPUP
    Column("plan_code", String(8)),
    Column("amount_rub", Numeric(12,2), nullable=False),
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
    Column("amount_rub", Numeric(12,2), nullable=False),
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
    Column("base_rub", Numeric(12,2), nullable=False),
    Column("percent", Numeric(6,2), nullable=False),
    Column("amount_rub", Numeric(12,2), nullable=False),
    Column("status", String(16), default="PENDING", nullable=False),  # PENDING/AVAILABLE/FROZEN/ADJUSTED
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
    Column("sender", String(8), nullable=False),  # USER/ADMIN
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

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(meta.create_all)
        # seed plans
        rows = [
            ("plan_14d", 14, Decimal("149.00")),
            ("plan_1m",  30, Decimal("299.00")),
            ("plan_3m",  90, Decimal("799.00")),
            ("plan_6m", 180, Decimal("1490.00")),
            ("plan_1y", 365, Decimal("2290.00")),
        ]
        for code, days, price in rows:
            exists = await conn.execute(select(plans.c.code).where(plans.c.code == code))
            if not exists.first():
                await conn.execute(insert(plans).values(code=code, days=days, price_rub=price, enabled=True))
        # provider map
        for code, provider in CFG["AG_PLAN_MAP"].items():
            exists = await conn.execute(select(provider_plan_map.c.plan_code).where(provider_plan_map.c.plan_code == code))
            if not exists.first():
                await conn.execute(insert(provider_plan_map).values(plan_code=code, provider_plan=provider))

# ------------------------------------------------------------------------------
# Сервисный слой (единственный источник правды)
# ------------------------------------------------------------------------------
class WalletSvc:
    @staticmethod
    async def ensure(s: AsyncSession, uid: int):
        r = await s.execute(select(wallets).where(wallets.c.user_id == uid))
        if not r.first():
            await s.execute(insert(wallets).values(user_id=uid))

    @staticmethod
    async def get(s: AsyncSession, uid: int):
        await WalletSvc.ensure(s, uid)
        r = await s.execute(select(wallets).where(wallets.c.user_id == uid))
        return r.mappings().first()

    @staticmethod
    async def available(s: AsyncSession, uid: int) -> Decimal:
        w = await WalletSvc.get(s, uid)
        return rub(w["deposit_rub"] + w["bonus_rub"] - w["hold_rub"])

class LedgerSvc:
    @staticmethod
    async def add(s: AsyncSession, uid: int, kind: str, amount: Decimal, idem_key: str, meta: str = ""):
        # идемпотентно: unique(idem_key)
        exists = await s.execute(select(ledger.c.id).where(ledger.c.idem_key == idem_key))
        if exists.first():
            return False
        await s.execute(insert(ledger).values(
            id=secrets.token_hex(16), user_id=uid, kind=kind,
            amount_rub=amount, meta=meta, idem_key=idem_key
        ))
        return True

class Platega:
    @staticmethod
    async def create_payment(order_id: str, amount: Decimal, description: str) -> dict:
        headers = {"X-MerchantId": CFG["PLATEGA_MID"], "X-Secret": CFG["PLATEGA_SECRET"]}
        payload = {
            "paymentMethod": CFG["PLATEGA_METHOD"],
            "amount": float(amount),
            "currency": "RUB",
            "description": description,
            "orderId": order_id,
            "returnUrl": CFG["PLATEGA_RETURN"],
        }
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(f"{CFG['PLATEGA_BASE']}/transaction/process", json=payload, headers=headers)
            r.raise_for_status()
            return r.json()

    @staticmethod
    async def get_transaction(external_id: str) -> dict:
        headers = {"X-MerchantId": CFG["PLATEGA_MID"], "X-Secret": CFG["PLATEGA_SECRET"]}
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{CFG['PLATEGA_BASE']}/transaction/{external_id}", headers=headers)
            r.raise_for_status()
            return r.json()

class AdaptGroup:
    @staticmethod
    async def _h():
        return {"X-Api-Key": CFG["AG_KEY"]}

    @staticmethod
    async def create_subscription(user_id: int, plan_code: str) -> dict:
        provider_plan = CFG["AG_PLAN_MAP"].get(plan_code)
        if not provider_plan: raise RuntimeError("plan not mapped")
        payload = {"plan": provider_plan, "externalUserId": str(user_id), "idempotencyKey": f"sub-{user_id}-{plan_code}-{int(now().timestamp())}"}
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.post(f"{CFG['AG_BASE']}/api/v1/subscriptions", json=payload, headers=await AdaptGroup._h())
            r.raise_for_status()
            return r.json()

    @staticmethod
    async def get_subscription(external_id: str) -> dict:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{CFG['AG_BASE']}/api/v1/subscriptions/{external_id}", headers=await AdaptGroup._h())
            r.raise_for_status()
            return r.json()

# ------------------------------------------------------------------------------
# UI хелперы
# ------------------------------------------------------------------------------
def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)

def kb(*rows: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[list(r) for r in rows])

async def get_user(s: AsyncSession, uid: int):
    r = await s.execute(select(users).where(users.c.telegram_id == uid))
    return r.mappings().first()

async def ensure_user(s: AsyncSession, m: Message, ref_code: Optional[str]):
    uid = m.from_user.id
    u = await get_user(s, uid)
    if u:
        await s.execute(update(users).where(users.c.telegram_id == uid).values(username=m.from_user.username))
        return u
    ref_uid = None
    if ref_code:
        r = await s.execute(select(ref_codes.c.user_id).where(ref_codes.c.code == ref_code))
        row = r.first()
        if row and row[0] != uid:
            ref_uid = row[0]
    await s.execute(insert(users).values(
        telegram_id=uid, username=m.from_user.username, referrer_id=ref_uid
    ))
    await WalletSvc.ensure(s, uid)
    if ref_uid:
        await s.execute(insert(referrals).values(invited_id=uid, inviter_id=ref_uid))
    return await get_user(s, uid)

async def is_restricted(s: AsyncSession, uid: int, kind: str) -> Optional[str]:
    r = await s.execute(select(user_restrictions).where(and_(
        user_restrictions.c.user_id == uid,
        user_restrictions.c.kind == kind,
        or_(user_restrictions.c.until.is_(None), user_restrictions.c.until > now()),
    )))
    row = r.mappings().first()
    return row["reason"] if row else None

async def current_sub(s: AsyncSession, uid: int):
    r = await s.execute(select(subscriptions).where(subscriptions.c.user_id == uid))
    return r.mappings().first()

async def render_hub(s: AsyncSession, uid: int) -> tuple[str, InlineKeyboardMarkup]:
    u = await get_user(s, uid)
    w = await WalletSvc.get(s, uid)
    sub = await current_sub(s, uid)
    avail = rub(w["deposit_rub"] + w["bonus_rub"] - w["hold_rub"])
    if sub and sub["status"] == "ACTIVE" and sub["expires_at"] and sub["expires_at"] > now():
        left = sub["expires_at"] - now()
        if left <= timedelta(hours=24):
            hh, mm = divmod(int(left.total_seconds()) // 60, 60)
            text = (f"Auro VPN\nДо окончания подписки — {hh} ч {mm} мин.\n"
                    f"Продлите заранее: оставшийся срок сохранится.\n\n"
                    f"Доступно для оплаты: {fmt_rub(avail)}")
            rows = [[btn("Продлить", "buy"), btn("Подключиться", "connect")],
                    [btn("Кабинет", "cabinet"), btn("Поддержка", "support")]]
        else:
            devices = f"Устройства: ? из {sub['devices_limit']}" if sub["devices_limit"] else ""
            text = (f"Auro VPN\nПодписка активна\nДо {fmt_dt(sub['expires_at'])}\n"
                    f"Доступно для оплаты: {fmt_rub(avail)} · {devices}")
            rows = [[btn("Подключиться", "connect")],
                    [btn("Продлить", "buy"), btn("Кабинет", "cabinet")],
                    [btn("Пригласить друзей", "ref"), btn("Поддержка", "support")]]
    else:
        text = (f"Auro VPN\nПодписка не оформлена\nДоступно для оплаты: {fmt_rub(avail)}\n\n"
                f"Выберите срок — после оплаты поможем подключиться.")
        rows = [[btn("Оформить подписку", "buy")],
                [btn("Кабинет", "cabinet"), btn("Пригласить друзей", "ref")],
                [btn("Поддержка", "support"), btn("О сервисе", "about")]]
    return text, kb(*rows)

async def upsert_hub(s: AsyncSession, bot: Bot, uid: int, text: str, markup: InlineKeyboardMarkup):
    u = await get_user(s, uid)
    mid = u["hub_message_id"] if u else None
    if mid:
        try:
            await bot.edit_message_text(text=text, chat_id=uid, message_id=mid, reply_markup=markup, parse_mode=ParseMode.HTML)
            return
        except Exception:
            pass
    m = await bot.send_message(chat_id=uid, text=text, reply_markup=markup, parse_mode=ParseMode.HTML)
    await s.execute(update(users).where(users.c.telegram_id == uid).values(hub_message_id=m.message_id))

async def show_screen(bot: Bot, s: AsyncSession, uid: int, text: str, markup: InlineKeyboardMarkup):
    await upsert_hub(s, bot, uid, text, markup)

# ------------------------------------------------------------------------------
# FSM
# ------------------------------------------------------------------------------
class FS(StatesGroup):
    topup_amount = State()
    support_text = State()
    support_reply = State()
    refund_reason = State()
    appeal_text = State()
    admin_search = State()

# ------------------------------------------------------------------------------
# Telegram роутер
# ------------------------------------------------------------------------------
router = Router()

@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext, bot: Bot):
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
            txt = ("Добро пожаловать в Auro VPN 👋\n\n"
                   "Подключайте телефон и компьютер, управляйте подпиской\n"
                   "и обращайтесь за помощью прямо здесь.\n\n"
                   "Перед покупкой ознакомьтесь с условиями сервиса\n"
                   "и политикой конфиденциальности.")
            mk = kb(
                [btn("Условия сервиса", "terms_open"), btn("Конфиденциальность", "privacy_open")],
                [btn("Принять и продолжить", "terms_accept")],
                [btn("Задать вопрос", "support")],
            )
            await show_screen(bot, s, m.from_user.id, txt, mk)
            return
        if await is_restricted(s, m.from_user.id, "SERVICE"):
            r = await is_restricted(s, m.from_user.id, "SERVICE")
            txt = f"Операции ограничены\nПричина: {safe(r)}"
            mk = kb([btn("Апелляция", "appeal"), btn("История", "history")],
                    [btn("Поддержка", "support"), btn("Документы", "docs")])
            await show_screen(bot, s, m.from_user.id, txt, mk)
            return
        txt, mk = await render_hub(s, m.from_user.id)
        await show_screen(bot, s, m.from_user.id, txt, mk)

@router.callback_query(F.data == "terms_accept")
async def accept_terms(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        await s.execute(update(users).where(users.c.telegram_id == cq.from_user.id).values(
            is_terms_accepted=True, terms_accepted_at=now()
        ))
        await s.commit()
        txt, mk = await render_hub(s, cq.from_user.id)
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "menu")
async def menu_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        txt, mk = await render_hub(s, cq.from_user.id)
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "terms_open")
async def terms_open(cq: CallbackQuery):
    await cq.message.answer("https://telegra.ph/Auro-VPN--Usloviya-servisa-11-23", disable_web_page_preview=False)
    await cq.answer()

@router.callback_query(F.data == "privacy_open")
async def privacy_open(cq: CallbackQuery):
    await cq.message.answer("https://telegra.ph/Auro-VPN--Konfidencialnost-11-23", disable_web_page_preview=False)
    await cq.answer()

# --- Подключение ---------------------------------------------------------------
@router.callback_query(F.data == "connect")
async def connect_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        sub = await current_sub(s, cq.from_user.id)
        if not sub or not sub["subscription_url"] or sub["status"] != "ACTIVE":
            txt = "Готовим данные подключения\n\nСсылка появится после подтверждения активации."
            mk = kb([btn("Открыть заказ", "orders")], [btn("Главное меню", "menu"), btn("Поддержка", "support")])
        else:
            url = sub["subscription_url"]
            txt = (f"Подключение\nПодписка активна до {fmt_dt(sub['expires_at'])}.\n\n"
                   f"Скопируйте персональную ссылку и добавьте в приложение.\n"
                   f"Не передавайте ссылку посторонним — она даёт доступ к подписке.")
            mk = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Скопировать ссылку", copy_text=CopyTextButton(text=url))],
                [btn("Инструкция", "guide"), btn("Мои устройства", "devices")],
                [btn("Не получается подключиться", "connect_help")],
                [btn("Главное меню", "menu")],
            ])
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "guide")
async def guide_platforms(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        txt = "На каком устройстве подключаемся?"
        mk = kb(
            [btn("Android", "guide:android"), btn("iPhone и iPad", "guide:ios")],
            [btn("Windows", "guide:windows"), btn("macOS", "guide:macos")],
            [btn("Назад", "connect")],
        )
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

GUIDE_APPS = {
    "android": [("Happ (рекомендуется)", "app:happ_android"), ("v2rayNG", "app:v2rayng"), ("NekoBox (GitHub)", "app:nekobox")],
    "ios":     [("Happ (рекомендуется)", "app:happ_ios"), ("Streisand", "app:streisand"), ("Shadowrocket", "app:shadowrocket")],
    "windows": [("Happ (рекомендуется)", "app:happ_win"), ("Hiddify", "app:hiddify_win"), ("v2rayN (GitHub)", "app:v2rayn")],
    "macos":   [("Happ (рекомендуется)", "app:happ_mac"), ("Hiddify", "app:hiddify_mac"), ("V2Box", "app:v2box")],
}
GUIDE_TITLE = {"android": "Android", "ios": "iPhone и iPad", "windows": "Windows", "macos": "macOS"}

@router.callback_query(F.data.startswith("guide:"))
async def guide_apps(cq: CallbackQuery, bot: Bot):
    platform = cq.data.split(":", 1)[1]
    async with Session() as s:
        txt = f"{GUIDE_TITLE[platform]}\n\nВыберите приложение. Если ничего не установлено — начните с рекомендуемого."
        rows = [[btn(name, cb)] for name, cb in GUIDE_APPS[platform]]
        rows.append([btn("Назад", "guide")])
        await show_screen(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

APP_INSTRUCTIONS = {
    "happ_android": ("Android · Happ",
        "1. Установите Happ из Google Play или happ.su.\n"
        "2. Вернитесь в бот.\n"
        "3. Скопируйте ссылку подписки.\n"
        "4. В Happ: «+» → «Подписка» → вставьте ссылку → сохранить.\n"
        "5. Выберите рекомендуемый профиль и включите подключение.",
        "https://happ.su"),
    "v2rayng": ("Android · v2rayNG",
        "1. Установите v2rayNG только с GitHub 2dust/v2rayNG.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. В v2rayNG: «+» → «Импорт из буфера».\n"
        "4. Выберите добавленную подписку → «Подключиться».",
        "https://github.com/2dust/v2rayNG/releases"),
    "nekobox": ("Android · NekoBox",
        "1. Установите NekoBox ТОЛЬКО с GitHub MatsuriDayo/NekoBoxForAndroid.\n"
        "   Версия в Google Play с мая 2024 — non-open-source.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. В NekoBox: «+» → «Импорт из буфера».\n"
        "4. Выберите подписку → «Подключиться».",
        "https://github.com/MatsuriDayo/NekoBoxForAndroid/releases"),
    "happ_ios": ("iOS · Happ",
        "1. Установите Happ из App Store.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. В Happ: «+» → «Подписка» → вставьте ссылку → сохранить.\n"
        "4. Выберите рекомендуемый профиль и включите подключение.",
        "https://happ.su"),
    "streisand": ("iOS · Streisand",
        "1. Установите Streisand из App Store.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку → сохранить.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://apps.apple.com/app/streisand/id6450534064"),
    "shadowrocket": ("iOS · Shadowrocket",
        "1. Установите Shadowrocket из App Store (приложение платное).\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку → сохранить.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://apps.apple.com/app/shadowrocket/id932747118"),
    "happ_win": ("Windows · Happ",
        "1. Установите Happ с happ.su или Microsoft Store.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Подписка» → вставьте ссылку → сохранить.\n"
        "4. Выберите профиль → «Подключиться».",
        "https://happ.su"),
    "hiddify_win": ("Windows · Hiddify",
        "1. Установите Hiddify с hiddify.com.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://hiddify.com"),
    "v2rayn": ("Windows · v2rayN",
        "1. Установите v2rayN ТОЛЬКО с GitHub 2dust/v2rayN.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «Подписки» → «Добавить» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://github.com/2dust/v2rayN/releases"),
    "happ_mac": ("macOS · Happ",
        "1. Установите Happ из App Store или happ.su.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Подписка» → вставьте ссылку → сохранить.\n"
        "4. Выберите профиль → «Подключиться».",
        "https://happ.su"),
    "hiddify_mac": ("macOS · Hiddify",
        "1. Установите Hiddify с hiddify.com.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Добавить подписку» → вставьте ссылку.\n"
        "4. Выберите подписку → «Подключиться».",
        "https://hiddify.com"),
    "v2box": ("macOS · V2Box",
        "1. Установите V2Box из App Store.\n"
        "2. Скопируйте ссылку подписки.\n"
        "3. «+» → «Импорт из буфера».\n"
        "4. Выберите подписку → «Подключиться».",
        "https://apps.apple.com/app/v2box-v2ray-client/id6446814690"),
}

@router.callback_query(F.data.startswith("app:"))
async def app_instr(cq: CallbackQuery, bot: Bot):
    key = cq.data.split(":", 1)[1]
    if key not in APP_INSTRUCTIONS:
        await cq.answer("Инструкция временно недоступна", show_alert=True)
        return
    title, body, store = APP_INSTRUCTIONS[key]
    async with Session() as s:
        sub = await current_sub(s, cq.from_user.id)
        url = sub["subscription_url"] if sub and sub["subscription_url"] else ""
        txt = f"{title}\n\n{body}\n\nУстанавливайте приложение только из проверенного источника."
        rows = []
        if url:
            rows.append([InlineKeyboardButton(text="Скопировать ссылку", copy_text=CopyTextButton(text=url))])
        rows.append([btn("Установить приложение", "noop")])
        rows.append([btn("Выбрать другое", "guide"), btn("Назад", "connect")])
        await show_screen(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

@router.callback_query(F.data == "noop")
async def noop(cq: CallbackQuery):
    await cq.answer()

@router.callback_query(F.data == "connect_help")
async def connect_help(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        txt = ("Не получается подключиться\n\n"
               "1. Проверьте срок подписки в боте.\n"
               "2. Обновите подписку в приложении.\n"
               "3. Выключите другое VPN-приложение.\n"
               "4. Повторите подключение.\n\n"
               "Если проблема осталась — напишите в поддержку.")
        mk = kb([btn("Написать в поддержку", "support")],
                [btn("Открыть инструкцию", "guide"), btn("Назад", "connect")])
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

# --- Покупка -------------------------------------------------------------------
@router.callback_query(F.data == "buy")
async def buy_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        blocked = await is_restricted(s, cq.from_user.id, "BUY")
        if blocked:
            await cq.answer("Покупки ограничены. Откройте поддержку.", show_alert=True)
            return
        rows = []
        async with Session() as s2:
            for code, label in [("plan_14d","14 дней · 149 ₽"),("plan_1m","30 дней · 299 ₽"),
                                ("plan_3m","90 дней · 799 ₽"),("plan_6m","180 дней · 1 490 ₽"),
                                ("plan_1y","365 дней · 2 290 ₽")]:
                rows.append([btn(label, f"plan:{code}")])
        rows.append([btn("Главное меню", "menu")])
        txt = ("Выберите срок\n"
               "При продлении оставшийся срок сохраняется.\n"
               "Внешняя оплата — через СБП. Автоматических списаний нет.")
        await show_screen(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

@router.callback_query(F.data.startswith("plan:"))
async def plan_pick(cq: CallbackQuery, bot: Bot):
    code = cq.data.split(":", 1)[1]
    async with Session() as s:
        p = (await s.execute(select(plans).where(plans.c.code == code))).mappings().first()
        if not p or not p["enabled"]:
            await cq.answer("Тариф временно недоступен", show_alert=True)
            return
        avail = await WalletSvc.available(s, cq.from_user.id)
        rows = []
        if avail >= p["price_rub"]:
            rows.append([btn(f"Списать {fmt_rub(p['price_rub'])} с баланса", f"pay_balance:{code}")])
        rows.append([btn(f"Оплатить {fmt_rub(p['price_rub'])} через СБП", f"pay_sbp:{code}")])
        if avail < p["price_rub"]:
            rows.append([btn("Пополнить баланс", "topup_menu")])
        rows.append([btn("К тарифам", "buy")])
        txt = (f"Подписка на {p['days']} дней\n"
               f"Стоимость: {fmt_rub(p['price_rub'])}\n"
               f"Доступно на балансе: {fmt_rub(avail)}\n"
               f"Оставшийся срок сохранится. Точная дата — после активации.")
        await show_screen(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

async def create_order(s: AsyncSession, uid: int, kind: str, plan_code: Optional[str], amount: Decimal, days: Optional[int]) -> str:
    oid = secrets.token_hex(16)
    await s.execute(insert(orders).values(
        id=oid, user_id=uid, kind=kind, plan_code=plan_code, amount_rub=amount,
        days_snapshot=days, status="PENDING", expires_at=now() + timedelta(minutes=CFG["ORDER_TTL"])
    ))
    return oid

@router.callback_query(F.data.startswith("pay_balance:"))
async def pay_balance(cq: CallbackQuery, bot: Bot):
    code = cq.data.split(":", 1)[1]
    async with Session() as s:
        p = (await s.execute(select(plans).where(plans.c.code == code))).mappings().first()
        if not p:
            await cq.answer("Тариф недоступен", show_alert=True); return
        w = await WalletSvc.get(s, cq.from_user.id)
        avail = rub(w["deposit_rub"] + w["bonus_rub"] - w["hold_rub"])
        if avail < p["price_rub"]:
            await cq.answer("Недостаточно средств", show_alert=True); return
        # резерв
        bonus_use = min(w["bonus_rub"], p["price_rub"])
        deposit_use = p["price_rub"] - bonus_use
        await s.execute(update(wallets).where(wallets.c.user_id == cq.from_user.id).values(
            bonus_rub=w["bonus_rub"] - bonus_use,
            deposit_rub=w["deposit_rub"] - deposit_use,
        ))
        oid = await create_order(s, cq.from_user.id, "SUB", code, p["price_rub"], p["days"])
        await LedgerSvc.add(s, cq.from_user.id, "SUB_PAYMENT", -p["price_rub"], f"order:{oid}", f"plan={code}")
        await s.execute(insert(fulfillment_jobs).values(id=secrets.token_hex(16), order_id=oid, state="QUEUED"))
        await s.commit()
        txt = "Оплата получена\n\nПодключаем подписку. Повторно платить не нужно."
        mk = kb([btn("Обновить статус", f"order_view:{oid}")], [btn("Главное меню", "menu"), btn("Поддержка", "support")])
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

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
            await show_screen(bot, s, cq.from_user.id,
                "Оплата временно недоступна\n\nСоздать платёж не удалось.\nСумма заказа не изменилась.",
                kb([btn("Повторить", f"pay_sbp:{code}")], [btn("К тарифам", "buy"), btn("Поддержка", "support")]))
            await cq.answer(); return
        ext = str(res.get("transactionId") or res.get("id") or "")
        url = res.get("paymentUrl") or res.get("url") or ""
        pid = secrets.token_hex(16)
        await s.execute(insert(payment_attempts).values(
            id=pid, order_id=oid, provider="PLATEGA", external_id=ext,
            payment_url=url, status="PENDING"
        ))
        await s.execute(update(orders).where(orders.c.id == oid).values(status="AWAITING_PAYMENT"))
        await s.commit()
        txt = (f"Оплата через СБП\nЗаказ №{oid[:8]}\nК оплате: {fmt_rub(p['price_rub'])}\n\n"
               f"После перевода вернитесь в бот — проверка автоматическая.")
        mk = kb([btn("Перейти к оплате", "noop")],
                [btn("Проверить оплату", f"check_pay:{oid}")],
                [btn("Прекратить оформление", "menu"), btn("Главное меню", "menu")])
        await show_screen(bot, s, cq.from_user.id, txt, mk)
        if url:
            await bot.send_message(cq.from_user.id, f"Ссылка для оплаты:\n{url}")
    await cq.answer()

@router.callback_query(F.data.startswith("check_pay:"))
async def check_pay(cq: CallbackQuery, bot: Bot):
    oid = cq.data.split(":", 1)[1]
    async with Session() as s:
        r = await s.execute(select(payment_attempts).where(payment_attempts.c.order_id == oid))
        pa = r.mappings().first()
        if not pa:
            await cq.answer("Платёж не найден", show_alert=True); return
        try:
            res = await Platega.get_transaction(pa["external_id"])
        except Exception:
            await cq.answer("Сервис оплаты недоступен. Попробуйте позже.", show_alert=True); return
        status = (res.get("status") or "").upper()
        if status == "CONFIRMED":
            await finalize_payment(s, bot, oid, "PLATEGA", pa["external_id"])
            txt = "Оплата получена\n\nПодключаем подписку. Повторно платить не нужно."
            mk = kb([btn("Обновить статус", f"order_view:{oid}")], [btn("Главное меню", "menu")])
            await show_screen(bot, s, cq.from_user.id, txt, mk)
            await cq.answer()
        else:
            await cq.answer("Подтверждение пока не поступило")

async def finalize_payment(s: AsyncSession, bot: Bot, order_id: str, source: str, external_id: str):
    # идемпотентность: событие + финансовое последствие + исполнение
    ev_exists = await s.execute(select(payment_events.c.id).where(and_(
        payment_events.c.source == source,
        payment_events.c.external_id == external_id,
        payment_events.c.status == "CONFIRMED",
    )))
    if ev_exists.first():
        return
    await s.execute(insert(payment_events).values(
        id=secrets.token_hex(16), source=source, external_id=external_id, status="CONFIRMED", processed=True
    ))
    await s.execute(update(payment_attempts).where(payment_attempts.c.external_id == external_id).values(status="CONFIRMED"))
    o = (await s.execute(select(orders).where(orders.c.id == order_id))).mappings().first()
    if not o: return
    await s.execute(update(orders).where(orders.c.id == order_id).values(status="PAID"))
    if o["kind"] == "TOPUP":
        await s.execute(update(wallets).where(wallets.c.user_id == o["user_id"]).values(
            deposit_rub=wallets.c.deposit_rub + o["amount_rub"]
        ))
        await LedgerSvc.add(s, o["user_id"], "TOPUP", o["amount_rub"], f"topup:{order_id}")
        await referral_on_payment(s, o["user_id"], external_id, o["amount_rub"])
    else:
        await s.execute(insert(fulfillment_jobs).values(id=secrets.token_hex(16), order_id=order_id, state="QUEUED"))
        await referral_on_payment(s, o["user_id"], external_id, o["amount_rub"])
    await s.commit()

async def referral_on_payment(s: AsyncSession, invited_id: int, external_id: str, base: Decimal):
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
    except Exception:
        pass  # unique constraint — уже начислено

# --- Ожидание/проверка платежа -------------------------------------------------
@router.callback_query(F.data.startswith("order_view:"))
async def order_view(cq: CallbackQuery, bot: Bot):
    oid = cq.data.split(":", 1)[1]
    async with Session() as s:
        o = (await s.execute(select(orders).where(orders.c.id == oid))).mappings().first()
        if not o:
            await cq.answer("Заказ не найден", show_alert=True); return
        sub = await current_sub(s, o["user_id"])
        txt = (f"Заказ №{oid[:8]}\n"
               f"Тип: {'Подписка' if o['kind']=='SUB' else 'Пополнение'}\n"
               f"Стоимость: {fmt_rub(o['amount_rub'])}\n"
               f"Статус: {o['status']}")
        rows = []
        if o["status"] == "PAID" and sub and sub["status"] == "ACTIVE":
            txt += f"\n\nПодписка активна до {fmt_dt(sub['expires_at'])}."
            rows.append([btn("Подключиться", "connect")])
        elif o["status"] == "PAID":
            txt += "\n\nВыполнение: подключаем подписку."
            rows.append([btn("Обновить выполнение", f"order_view:{oid}")])
        rows.append([btn("Помощь по заказу", "support"), btn("Назад", "orders")])
        await show_screen(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

@router.callback_query(F.data == "orders")
async def orders_list(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        r = await s.execute(select(orders).where(orders.c.user_id == cq.from_user.id).order_by(orders.c.created_at.desc()).limit(10))
        rows = []
        for o in r.mappings():
            rows.append([btn(f"№{o['id'][:8]} · {fmt_rub(o['amount_rub'])} · {o['status']}", f"order_view:{o['id']}")])
        rows.append([btn("Назад", "menu")])
        txt = "Ваши заказы" if rows[:-1] else "У вас пока нет заказов."
        await show_screen(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

# --- Кабинет -------------------------------------------------------------------
@router.callback_query(F.data == "cabinet")
async def cabinet(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        w = await WalletSvc.get(s, cq.from_user.id)
        avail = rub(w["deposit_rub"] + w["bonus_rub"] - w["hold_rub"])
        txt = (f"Личный кабинет\nID: {cq.from_user.id}\n"
               f"Внесённые средства: {fmt_rub(w['deposit_rub'])}\n"
               f"Бонусы: {fmt_rub(w['bonus_rub'])}\n"
               f"Доступно для подписки: {fmt_rub(avail)}")
        mk = kb(
            [btn("Пополнить через СБП", "topup_menu")],
            [btn("Мои заказы", "orders"), btn("История", "history")],
            [btn("Пригласить друзей", "ref")],
            [btn("Уведомления", "notify"), btn("Документы и данные", "docs")],
            [btn("Главное меню", "menu")],
        )
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "topup_menu")
async def topup_menu(cq: CallbackQuery, bot: Bot):
    txt = "Пополнение баланса\n\nВыберите сумму или укажите свою.\nОт 50 до 10 000 ₽."
    mk = kb(
        [btn("150 ₽", "topup:150"), btn("300 ₽", "topup:300"), btn("500 ₽", "topup:500")],
        [btn("1 000 ₽", "topup:1000"), btn("2 500 ₽", "topup:2500")],
        [btn("Своя сумма", "topup_custom")],
        [btn("Назад", "cabinet")],
    )
    async with Session() as s:
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data.startswith("topup:"))
async def topup_fixed(cq: CallbackQuery, bot: Bot):
    amount = Decimal(cq.data.split(":", 1)[1])
    await create_topup(cq, bot, amount)

@router.callback_query(F.data == "topup_custom")
async def topup_custom(cq: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(FS.topup_amount)
    async with Session() as s:
        await show_screen(bot, s, cq.from_user.id,
            "Введите сумму\n\nЦелое число от 50 до 10 000.\nНапример: 500.",
            kb([btn("Отмена", "menu")]))
    await cq.answer()

@router.message(FS.topup_amount)
async def topup_amount(m: Message, state: FSMContext, bot: Bot):
    raw = re.sub(r"[\s\u00A0_]", "", m.text or "")
    if not raw.isdigit():
        await m.answer("Введите целое число от 50 до 10 000.")
        return
    amount = Decimal(raw)
    if amount < CFG["TOPUP_MIN"] or amount > CFG["TOPUP_MAX"]:
        await m.answer(f"Введите сумму от {fmt_rub(CFG['TOPUP_MIN'])} до {fmt_rub(CFG['TOPUP_MAX'])}.")
        return
    try:
        await m.delete()
    except Exception:
        pass
    await state.clear()
    class _CQ:
        from_user = m.from_user
    await create_topup(_CQ, bot, amount)

async def create_topup(cq, bot, amount: Decimal):
    async with Session() as s:
        oid = await create_order(s, cq.from_user.id, "TOPUP", None, amount, None)
        await s.commit()
        try:
            res = await Platega.create_payment(oid, amount, "Пополнение баланса")
        except Exception:
            await s.execute(update(orders).where(orders.c.id == oid).values(status="CREATION_FAILED"))
            await s.commit()
            await show_screen(bot, s, cq.from_user.id,
                "Оплата временно недоступна\n\nСоздать платёж не удалось.",
                kb([btn("К тарифам", "cabinet"), btn("Поддержка", "support")]))
            return
        ext = str(res.get("transactionId") or res.get("id") or "")
        url = res.get("paymentUrl") or res.get("url") or ""
        await s.execute(insert(payment_attempts).values(
            id=secrets.token_hex(16), order_id=oid, provider="PLATEGA",
            external_id=ext, payment_url=url, status="PENDING"))
        await s.execute(update(orders).where(orders.c.id == oid).values(status="AWAITING_PAYMENT"))
        await s.commit()
        txt = (f"Пополнение через СБП\nЗаказ №{oid[:8]}\nНа баланс поступит: {fmt_rub(amount)}\n\n"
               f"После перевода вернитесь в бот — проверка автоматическая.")
        mk = kb([btn("Проверить оплату", f"check_pay:{oid}")],
                [btn("Главное меню", "menu")])
        await show_screen(bot, s, cq.from_user.id, txt, mk)
        if url:
            await bot.send_message(cq.from_user.id, f"Ссылка для оплаты:\n{url}")

@router.callback_query(F.data == "history")
async def history(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        r = await s.execute(select(ledger).where(ledger.c.user_id == cq.from_user.id).order_by(ledger.c.created_at.desc()).limit(10))
        rows = []
        for it in r.mappings():
            sign = "+" if it["amount_rub"] > 0 else "−"
            rows.append([btn(f"{fmt_dt(it['created_at'])[:5]} · {it['kind']} · {sign}{fmt_rub(abs(it['amount_rub']))}", "noop")])
        rows.append([btn("Назад", "cabinet")])
        txt = "История" if len(rows) > 1 else "История пуста."
        await show_screen(bot, s, cq.from_user.id, txt, kb(*rows))
    await cq.answer()

# --- Рефералка -----------------------------------------------------------------
@router.callback_query(F.data == "ref")
async def ref_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        rc = (await s.execute(select(ref_codes).where(ref_codes.c.user_id == cq.from_user.id))).mappings().first()
        if not rc:
            code = secrets.token_urlsafe(6)
            await s.execute(insert(ref_codes).values(code=code, user_id=cq.from_user.id))
            await s.commit()
            rc = {"code": code}
        inv = await s.execute(select(func.count()).select_from(referrals).where(referrals.c.inviter_id == cq.from_user.id))
        invited = inv.scalar() or 0
        paid = (await s.execute(select(func.count(func.distinct(referral_rewards.c.invited_id)))
                               .where(referral_rewards.c.inviter_id == cq.from_user.id))).scalar() or 0
        avail = (await s.execute(select(func.coalesce(func.sum(referral_rewards.c.amount_rub), 0))
                                 .where(and_(referral_rewards.c.inviter_id == cq.from_user.id,
                                             referral_rewards.c.status == "AVAILABLE")))).scalar()
        pending = (await s.execute(select(func.coalesce(func.sum(referral_rewards.c.amount_rub), 0))
                                   .where(and_(referral_rewards.c.inviter_id == cq.from_user.id,
                                               referral_rewards.c.status == "PENDING")))).scalar()
        txt = (f"Приглашайте друзей\n"
               f"15% с каждого платежа через СБП — подписка или пополнение.\n\n"
               f"Приглашено: {invited} · Сделали оплату: {paid}\n"
               f"Доступно бонусов: {fmt_rub(avail)} · Ожидает: {fmt_rub(pending)}")
        link = f"https://t.me/{CFG['BOT_USERNAME']}?start=ref_{rc['code']}"
        mk = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Скопировать приглашение", copy_text=CopyTextButton(text=link))],
            [btn("История бонусов", "noop"), btn("Правила", "noop")],
            [btn("Назад", "menu")],
        ])
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

# --- Поддержка -----------------------------------------------------------------
@router.callback_query(F.data == "support")
async def support_cb(cq: CallbackQuery, bot: Bot, state: FSMContext):
    async with Session() as s:
        r = await s.execute(select(tickets).where(and_(tickets.c.user_id == cq.from_user.id,
                                                       tickets.c.status == "OPEN")).order_by(tickets.c.id.desc()).limit(1))
        t = r.mappings().first()
        if t:
            txt = f"Обращение №{t['id']}\nСтатус: в работе"
            mk = kb([btn("Открыть", f"ticket_view:{t['id']}")],
                    [btn("Новое обращение", "ticket_new")],
                    [btn("Главное меню", "menu")])
        else:
            txt = "Служба поддержки\n\nЕсли у вас возникли вопросы по работе сервиса или оплате — создайте обращение."
            mk = kb([btn("Написать в поддержку", "ticket_new")], [btn("Главное меню", "menu")])
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "ticket_new")
async def ticket_new(cq: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(FS.support_text)
    async with Session() as s:
        await show_screen(bot, s, cq.from_user.id,
            "Опишите проблему\n\nЧто произошло и когда?\n5–2 000 символов.\n\n"
            "Не отправляйте банковские коды, реквизиты карты и персональную ссылку подписки.",
            kb([btn("Отмена", "support")]))
    await cq.answer()

@router.message(FS.support_text)
async def ticket_text(m: Message, state: FSMContext, bot: Bot):
    text = (m.text or "").strip()
    if not (5 <= len(text) <= 2000):
        await m.answer("Введите от 5 до 2 000 символов.")
        return
    async with Session() as s:
        t = await s.execute(insert(tickets).values(user_id=m.from_user.id, subject="GENERAL", status="OPEN").returning(tickets.c.id))
        tid = t.scalar()
        await s.execute(insert(ticket_messages).values(ticket_id=tid, sender="USER", text=text))
        await s.commit()
        try: await m.delete()
        except Exception: pass
        await state.clear()
        await show_screen(bot, s, m.from_user.id,
            f"Обращение №{tid} отправлено\n\nСтатус: ожидает ответа\n"
            f"Ответ придёт уведомлением и сохранится в переписке.",
            kb([btn("Открыть обращение", f"ticket_view:{tid}")], [btn("Главное меню", "menu")]))

@router.callback_query(F.data.startswith("ticket_view:"))
async def ticket_view(cq: CallbackQuery, bot: Bot):
    tid = int(cq.data.split(":", 1)[1])
    async with Session() as s:
        t = (await s.execute(select(tickets).where(tickets.c.id == tid))).mappings().first()
        if not t or t["user_id"] != cq.from_user.id:
            await cq.answer("Обращение недоступно", show_alert=True); return
        msgs = (await s.execute(select(ticket_messages).where(ticket_messages.c.ticket_id == tid).order_by(ticket_messages.c.id.desc()).limit(6))).mappings().all()
        msgs = list(reversed(msgs))
        lines = [f"Обращение №{tid}", f"Статус: {'в работе' if t['status']=='OPEN' else 'закрыто'}", ""]
        for x in msgs:
            who = "Вы" if x["sender"] == "USER" else "Поддержка"
            lines.append(f"{who}: {x['text']}")
        mk = kb([btn("Ответить", f"ticket_reply:{tid}"), btn("Закрыть", f"ticket_close:{tid}")],
                [btn("Назад", "support")])
        await show_screen(bot, s, cq.from_user.id, "\n".join(lines), mk)
    await cq.answer()

@router.callback_query(F.data.startswith("ticket_reply:"))
async def ticket_reply(cq: CallbackQuery, state: FSMContext, bot: Bot):
    tid = int(cq.data.split(":", 1)[1])
    await state.update_data(ticket_id=tid)
    await state.set_state(FS.support_reply)
    async with Session() as s:
        await show_screen(bot, s, cq.from_user.id, f"Ответ в обращение №{tid}\n\nВведите сообщение (1–2 000).",
                          kb([btn("Отмена", f"ticket_view:{tid}")]))
    await cq.answer()

@router.message(FS.support_reply)
async def ticket_reply_text(m: Message, state: FSMContext, bot: Bot):
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
        await ticket_view_by_id(s, bot, m.from_user.id, tid)

async def ticket_view_by_id(s: AsyncSession, bot: Bot, uid: int, tid: int):
    t = (await s.execute(select(tickets).where(tickets.c.id == tid))).mappings().first()
    msgs = (await s.execute(select(ticket_messages).where(ticket_messages.c.ticket_id == tid).order_by(ticket_messages.c.id.desc()).limit(6))).mappings().all()
    msgs = list(reversed(msgs))
    lines = [f"Обращение №{tid}", f"Статус: {'в работе' if t['status']=='OPEN' else 'закрыто'}", ""]
    for x in msgs:
        who = "Вы" if x["sender"] == "USER" else "Поддержка"
        lines.append(f"{who}: {x['text']}")
    mk = kb([btn("Ответить", f"ticket_reply:{tid}"), btn("Закрыть", f"ticket_close:{tid}")],
            [btn("Назад", "support")])
    await show_screen(bot, s, uid, "\n".join(lines), mk)

@router.callback_query(F.data.startswith("ticket_close:"))
async def ticket_close(cq: CallbackQuery, bot: Bot):
    tid = int(cq.data.split(":", 1)[1])
    async with Session() as s:
        await s.execute(update(tickets).where(and_(tickets.c.id == tid, tickets.c.user_id == cq.from_user.id)).values(status="CLOSED"))
        await s.commit()
        await show_screen(bot, s, cq.from_user.id, f"Обращение №{tid} закрыто.",
                          kb([btn("Главное меню", "menu")]))
    await cq.answer()

# --- Настройки/инфо ------------------------------------------------------------
@router.callback_query(F.data == "notify")
async def notify_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        u = await get_user(s, cq.from_user.id)
        txt = ("Уведомления\n\n"
               "Сообщения о платежах, заказах, безопасности и ответах поддержки\n"
               "относятся к обслуживанию аккаунта и не отключаются.")
        def t(b): return "✅" if b else "⬜"
        mk = kb(
            [btn(f"{t(u['notify_expiry'])} Напоминания об окончании", "notify:expiry")],
            [btn(f"{t(u['notify_news'])} Новости сервиса", "notify:news")],
            [btn(f"{t(u['notify_offers'])} Предложения", "notify:offers")],
            [btn("Назад", "cabinet")],
        )
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data.startswith("notify:"))
async def notify_toggle(cq: CallbackQuery, bot: Bot):
    field = cq.data.split(":", 1)[1]
    col = {"expiry":"notify_expiry","news":"notify_news","offers":"notify_offers"}[field]
    async with Session() as s:
        u = await get_user(s, cq.from_user.id)
        await s.execute(update(users).where(users.c.telegram_id == cq.from_user.id).values({col: not u[col]}))
        await s.commit()
    await notify_cb(cq, bot)

@router.callback_query(F.data == "docs")
async def docs_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        txt = "Документы и данные"
        mk = kb(
            [btn("Условия сервиса", "terms_open"), btn("Конфиденциальность", "privacy_open")],
            [btn("Запрос удаления", "delete_req")],
            [btn("Назад", "cabinet")],
        )
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "delete_req")
async def delete_req(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        t = await s.execute(insert(tickets).values(user_id=cq.from_user.id, subject="DELETE", status="OPEN").returning(tickets.c.id))
        tid = t.scalar()
        await s.execute(insert(ticket_messages).values(ticket_id=tid, sender="USER", text="Запрос на удаление аккаунта"))
        await s.commit()
        await show_screen(bot, s, cq.from_user.id,
            f"Запрос на удаление зарегистрирован как обращение №{tid}.\n"
            f"Перед обработкой проверим активные подписки, остаток средств,\n"
            f"незавершённые платежи и обращения.",
            kb([btn("Главное меню", "menu")]))
    await cq.answer()

@router.callback_query(F.data == "about")
async def about_cb(cq: CallbackQuery, bot: Bot):
    async with Session() as s:
        txt = ("Auro VPN\n\n"
               "Подключайте телефон и компьютер, управляйте подпиской и обращайтесь за помощью прямо здесь.\n\n"
               "Сервис не проверяет ваше соединение и не обещает конкретную скорость.")
        mk = kb([btn("Канал", "noop"), btn("Документы", "docs")],
                [btn("Поддержка", "support"), btn("Главное меню", "menu")])
        await show_screen(bot, s, cq.from_user.id, txt, mk)
    await cq.answer()

@router.callback_query(F.data == "appeal")
async def appeal_start(cq: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(FS.appeal_text)
    async with Session() as s:
        await show_screen(bot, s, cq.from_user.id,
            "Апелляция\n\nОпишите ситуацию и причину, по которой ограничение следует пересмотреть.\n10–1 000 символов.",
            kb([btn("Отмена", "menu")]))
    await cq.answer()

@router.message(FS.appeal_text)
async def appeal_text(m: Message, state: FSMContext, bot: Bot):
    text = (m.text or "").strip()
    if not (10 <= len(text) <= 1000):
        await m.answer("Введите от 10 до 1 000 символов.")
        return
    async with Session() as s:
        t = await s.execute(insert(tickets).values(user_id=m.from_user.id, subject="APPEAL", status="OPEN").returning(tickets.c.id))
        tid = t.scalar()
        await s.execute(insert(ticket_messages).values(ticket_id=tid, sender="USER", text=text))
        await s.commit()
        try: await m.delete()
        except Exception: pass
        await state.clear()
        await show_screen(bot, s, m.from_user.id,
            f"Апелляция отправлена (обращение №{tid}).\nОжидайте решения.",
            kb([btn("Главное меню", "menu"), btn("Поддержка", "support")]))

# --- Fallback ------------------------------------------------------------------
@router.message()
async def fallback(m: Message, state: FSMContext):
    # не в FSM: подсказываем и удаляем текст
    if await state.get_state():
        return
    try: await m.delete()
    except Exception: pass
    await m.answer("Откройте меню: /start")

# ------------------------------------------------------------------------------
# FastAPI: вебхуки + lifespan
# ------------------------------------------------------------------------------
bot = Bot(CFG["BOT_TOKEN"], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)
app = FastAPI()

async def process_confirmed_payment(external_id: str, status: str, source: str, body: dict):
    async with Session() as s:
        pa = (await s.execute(select(payment_attempts).where(payment_attempts.c.external_id == external_id))).mappings().first()
        if not pa: return
        if status == "CONFIRMED":
            await finalize_payment(s, bot, pa["order_id"], source, external_id)
        elif status == "CANCELED":
            await s.execute(update(payment_attempts).where(payment_attempts.c.external_id == external_id).values(status="CANCELED"))
            await s.execute(update(orders).where(orders.c.id == pa["order_id"]).values(status="CANCELED"))
            await s.commit()
        elif status in ("DISPUTED", "PARTIALLY_REFUNDED", "CHARGEBACKED"):
            await s.execute(insert(payment_events).values(
                id=secrets.token_hex(16), source=source, external_id=external_id,
                status=status, body=json.dumps(body)[:1900], processed=False))
            await s.commit()

@app.post(CFG["TG_PATH"])
async def tg_webhook(request: Request):
    header = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not timing_safe_eq(header, CFG["WEBHOOK_SECRET_TG"]):
        raise HTTPException(401)
    from aiogram.types import Update
    data = await request.json()
    upd = Update.model_validate(data)
    await dp.feed_update(bot, upd)
    return Response(status_code=200)

@app.post(CFG["PG_PATH"])
async def platega_webhook(request: Request):
    if not (timing_safe_eq(request.headers.get("X-MerchantId",""), CFG["PLATEGA_MID"]) and
            timing_safe_eq(request.headers.get("X-Secret",""), CFG["PLATEGA_SECRET"])):
        raise HTTPException(401)
    body = await request.json()
    ext_id = str(body.get("transactionId") or body.get("id") or "")
    status = str(body.get("status") or "").upper()
    async with Session() as s:
        try:
            await s.execute(insert(payment_events).values(
                id=secrets.token_hex(16), source="PLATEGA", external_id=ext_id,
                status=status, body=json.dumps(body)[:1900], processed=False))
            await s.commit()
        except Exception:
            await s.rollback()
    asyncio.create_task(process_confirmed_payment(ext_id, status, "PLATEGA", body))
    return {"ok": True}

@app.post(CFG["AG_PATH"])
async def adaptgroup_webhook(request: Request):
    if CFG["AG_IP"] and (request.client.host != CFG["AG_IP"]):
        raise HTTPException(401)
    raw = await request.body()
    sig = request.headers.get("X-Webhook-Signature", "")
    expected = hmac.new(CFG["AG_SECRET"].encode(), raw, hashlib.sha256).hexdigest()
    if not timing_safe_eq(sig, expected):
        raise HTTPException(401)
    body = json.loads(raw or b"{}")
    async with Session() as s:
        ext_id = str(body.get("subscriptionId") or body.get("id") or "")
        ev = str(body.get("event") or "")
        if ext_id and ev in ("subscription.created", "subscription.renewed"):
            try:
                await s.execute(update(subscriptions).where(subscriptions.c.external_id == ext_id).values(last_sync=now()))
                await s.commit()
            except Exception:
                await s.rollback()
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    await init_db()
    await bot.set_webhook(
        url=CFG["PUBLIC_BASE_URL"] + CFG["TG_PATH"],
        secret_token=CFG["WEBHOOK_SECRET_TG"],
        allowed_updates=dp.resolve_used_update_types(),
        drop_pending_updates=True,
    )
    asyncio.create_task(background_worker())

async def background_worker():
    """Исполнение заказов, напоминания, доступность бонусов, экспирация."""
    while True:
        try:
            await process_jobs_once()
            await expire_orders_once()
            await expire_inputs_none()  # placeholder
            await make_rewards_available()
        except Exception:
            log.exception("worker")
        await asyncio.sleep(15)

async def process_jobs_once():
    async with Session() as s:
        r = await s.execute(select(fulfillment_jobs).where(fulfillment_jobs.c.state == "QUEUED").limit(5))
        for j in r.mappings():
            o = (await s.execute(select(orders).where(orders.c.id == j["order_id"]))).mappings().first()
            if not o or not o["plan_code"]:
                await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"]).values(state="FAILED", last_error="no plan"))
                await s.commit(); continue
            # блокировка по подписке = сериализация
            await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"]).values(state="RUNNING", updated_at=now()))
            await s.commit()
            try:
                res = await AdaptGroup.create_subscription(o["user_id"], o["plan_code"])
                ext = str(res.get("subscriptionId") or res.get("id") or "")
                url = res.get("subscriptionUrl") or res.get("url") or ""
                exp = res.get("expiresAt")
                exp_dt = None
                if exp:
                    try: exp_dt = datetime.fromisoformat(exp.replace("Z","+00:00"))
                    except Exception: exp_dt = now() + timedelta(days=o["days_snapshot"] or 0)
                else:
                    exp_dt = now() + timedelta(days=o["days_snapshot"] or 0)
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
                try:
                    await bot.send_message(o["user_id"],
                        f"Подписка готова\nДобавлено: {o['days_snapshot']} дней\n"
                        f"Действует до: {fmt_dt(exp_dt)}")
                except Exception: pass
            except Exception as e:
                log.exception("fulfill failed")
                await s.execute(update(fulfillment_jobs).where(fulfillment_jobs.c.id == j["id"]).values(
                    state="RETRY" if j["attempts"] < 5 else "MANUAL",
                    attempts=j["attempts"] + 1, last_error=str(e)[:200], updated_at=now()))
                await s.commit()

async def expire_orders_once():
    async with Session() as s:
        await s.execute(update(orders).where(and_(
            orders.c.status == "AWAITING_PAYMENT",
            orders.c.expires_at < now()
        )).values(status="EXPIRED"))
        await s.commit()

async def make_rewards_available():
    async with Session() as s:
        r = await s.execute(select(referral_rewards).where(and_(
            referral_rewards.c.status == "PENDING",
            referral_rewards.c.available_at <= now()
        )))
        for rw in r.mappings():
            await s.execute(update(wallets).where(wallets.c.user_id == rw["inviter_id"]).values(
                bonus_rub=wallets.c.bonus_rub + rw["amount_rub"]))
            await s.execute(update(referral_rewards).where(referral_rewards.c.id == rw["id"]).values(status="AVAILABLE"))
        await s.commit()

async def expire_inputs_none():
    return

@app.on_event("shutdown")
async def on_shutdown():
    await bot.session.close()

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
