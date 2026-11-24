# -*- coding: utf-8 -*-
"""
Auro VPN Bot v24.0 — production-ready single-file implementation.
aiogram 3.x + asyncpg + aiohttp
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import html
import json
import logging
import os
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web
from dotenv import load_dotenv

# ------------------------------------------------------------------ #
# Конфигурация
# ------------------------------------------------------------------ #
load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("aurovpn")

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS: set[int] = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/aurochannel")

TERMS_URL = os.getenv("TERMS_URL", "")
PRIVACY_URL = os.getenv("PRIVACY_URL", "")

DATABASE_URL = os.environ["DATABASE_URL"]

PUBLIC_BASE_URL = os.environ["PUBLIC_BASE_URL"].rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhooks")
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "")

PLATEGA_BASE_URL = os.getenv("PLATEGA_BASE_URL", "https://app.platega.io").rstrip("/")
PLATEGA_MERCHANT_ID = os.getenv("PLATEGA_MERCHANT_ID", "")
PLATEGA_SECRET = os.getenv("PLATEGA_SECRET", "")
PLATEGA_CALLBACK_URL = os.getenv("PLATEGA_CALLBACK_URL", "")

ADAPTGROUP_BASE_URL = os.getenv("ADAPTGROUP_BASE_URL", "https://network-api.adaptgroup.app").rstrip("/")
ADAPTGROUP_API_KEY = os.getenv("ADAPTGROUP_API_KEY", "")
ADAPTGROUP_WEBHOOK_SECRET = os.getenv("ADAPTGROUP_WEBHOOK_SECRET", "")
ADAPTGROUP_WEBHOOK_PATH = os.getenv("ADAPTGROUP_WEBHOOK_PATH", "/webhooks/adaptgroup")

REFERRAL_PERCENT = Decimal(os.getenv("REFERRAL_PERCENT", "15"))
ORDER_TTL_MIN = int(os.getenv("ORDER_TTL_MIN", "15"))
FSM_TTL_MIN = int(os.getenv("FSM_TTL_MIN", "10"))

PLAN_PRICES: dict[str, Decimal] = {
    "14d": Decimal(os.getenv("PLAN_14D_PRICE", "149")),
    "1m": Decimal(os.getenv("PLAN_1M_PRICE", "299")),
    "3m": Decimal(os.getenv("PLAN_3M_PRICE", "799")),
    "6m": Decimal(os.getenv("PLAN_6M_PRICE", "1490")),
    "1y": Decimal(os.getenv("PLAN_1Y_PRICE", "2290")),
}
PLAN_LABELS: dict[str, str] = {
    "14d": "14 дней",
    "1m": "1 месяц",
    "3m": "3 месяца",
    "6m": "6 месяцев",
    "1y": "1 год",
}
PLAN_DAYS: dict[str, int] = {"14d": 14, "1m": 30, "3m": 90, "6m": 180, "1y": 365}

DEVICES_LIMIT = 5
TOPUP_MIN = Decimal("50")
TOPUP_MAX = Decimal("10000")

MSK = timezone(timedelta(hours=3))

# ------------------------------------------------------------------ #
# Утилиты
# ------------------------------------------------------------------ #
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def fmt_dt_msk(dt: datetime) -> str:
    return dt.astimezone(MSK).strftime("%d.%m.%Y")


def fmt_dt_msk_full(dt: datetime) -> str:
    return dt.astimezone(MSK).strftime("%d.%m.%Y, %H:%M")


def fmt_money(v: Decimal | int | float) -> str:
    d = Decimal(str(v)).quantize(Decimal("0.01"))
    s = f"{d:,.2f}".replace(",", " ")
    return f"{s} ₽"


def humanize_left(expires: datetime) -> str:
    delta = expires - now_utc()
    total = int(delta.total_seconds())
    if total <= 0:
        return "истекла"
    days = total // 86400
    hours = (total % 86400) // 3600
    mins = (total % 3600) // 60
    if days > 0:
        return f"{days} дн."
    if hours > 0:
        return f"{hours} ч {mins} мин"
    return f"{mins} мин"


def esc(s: Any) -> str:
    return html.escape(str(s), quote=False)


def timing_safe_eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def gen_sub_url(user_id: int) -> str:
    # В проде должен выдаваться AdaptGroup. Здесь — заглушка.
    return f"https://sub.aurovpn.net/v1/sub/auro-live-{user_id}"


# ------------------------------------------------------------------ #
# DB helpers
# ------------------------------------------------------------------ #
class DB:
    pool: asyncpg.Pool | None = None

    @classmethod
    async def init(cls) -> None:
        cls.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
        await cls.migrate()

    @classmethod
    async def close(cls) -> None:
        if cls.pool:
            await cls.pool.close()

    @classmethod
    async def migrate(cls) -> None:
        sql = """
        CREATE TABLE IF NOT EXISTS users (
            telegram_id BIGINT PRIMARY KEY,
            username VARCHAR(64),
            referrer_id BIGINT REFERENCES users(telegram_id) ON DELETE SET NULL,
            balance_rub NUMERIC(12, 2) NOT NULL DEFAULT 0.00 CHECK (balance_rub >= 0.00),
            is_terms_accepted BOOLEAN NOT NULL DEFAULT FALSE,
            terms_accepted_at TIMESTAMPTZ,
            is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
            block_reason TEXT,
            blocked_at TIMESTAMPTZ,
            is_admin BOOLEAN NOT NULL DEFAULT FALSE,
            hub_message_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_no_self_ref CHECK (telegram_id <> referrer_id)
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id BIGINT UNIQUE NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
            adapt_sub_id VARCHAR(128) UNIQUE,
            subscription_url TEXT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            warned_24h BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS orders (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id BIGINT NOT NULL REFERENCES users(telegram_id),
            order_type VARCHAR(16) NOT NULL CHECK (order_type IN ('DIRECT_SUB', 'TOPUP')),
            plan_code VARCHAR(8) CHECK (plan_code IN ('14d','1m','3m','6m','1y')),
            amount_rub NUMERIC(12, 2) NOT NULL CHECK (amount_rub >= 50.00 AND amount_rub <= 10000.00),
            status VARCHAR(24) NOT NULL DEFAULT 'PENDING'
                CHECK (status IN ('PENDING','PAID','CANCELED','EXPIRED','PAID_LATE','DISPUTED','REFUNDED')),
            platega_transaction_id VARCHAR(128) UNIQUE,
            payment_url TEXT,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS balance_audit_log (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(telegram_id),
            type VARCHAR(32) NOT NULL CHECK (type IN ('TOPUP','SUBSCRIPTION_PAYMENT','REFERRAL_REWARD','ADMIN_ADJUST','REFUND')),
            amount_rub NUMERIC(12, 2) NOT NULL,
            balance_after NUMERIC(12, 2) NOT NULL,
            comment TEXT,
            reference_id VARCHAR(128),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS tickets (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(telegram_id),
            subject_type VARCHAR(16) NOT NULL DEFAULT 'GENERAL' CHECK (subject_type IN ('GENERAL','APPEAL')),
            status VARCHAR(16) NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','CLOSED')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS ticket_messages (
            id BIGSERIAL PRIMARY KEY,
            ticket_id INT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
            sender_type VARCHAR(8) NOT NULL CHECK (sender_type IN ('USER','ADMIN')),
            sender_id BIGINT NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS processed_callbacks (
            id BIGSERIAL PRIMARY KEY,
            source VARCHAR(16) NOT NULL CHECK (source IN ('PLATEGA','ADAPTGROUP')),
            external_id VARCHAR(128) NOT NULL,
            status VARCHAR(32) NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(source, external_id, status)
        );

        CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
        CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
        CREATE INDEX IF NOT EXISTS idx_sub_expires ON subscriptions(expires_at);
        CREATE INDEX IF NOT EXISTS idx_audit_user ON balance_audit_log(user_id);
        """
        assert cls.pool is not None
        async with cls.pool.acquire() as con:
            await con.execute(sql)
        log.info("DB migrated")

    # --------- user helpers ----------
    @classmethod
    async def get_user(cls, tid: int) -> asyncpg.Record | None:
        assert cls.pool is not None
        return await cls.pool.fetchrow("SELECT * FROM users WHERE telegram_id=$1", tid)

    @classmethod
    async def upsert_user(cls, tid: int, username: str | None, referrer_id: int | None) -> asyncpg.Record:
        assert cls.pool is not None
        async with cls.pool.acquire() as con:
            row = await con.fetchrow("SELECT * FROM users WHERE telegram_id=$1", tid)
            if row is None:
                # нельзя ссылаться на себя
                if referrer_id == tid:
                    referrer_id = None
                await con.execute(
                    "INSERT INTO users(telegram_id, username, referrer_id) VALUES($1,$2,$3)",
                    tid, username, referrer_id,
                )
                row = await con.fetchrow("SELECT * FROM users WHERE telegram_id=$1", tid)
            else:
                if row["username"] != username:
                    await con.execute("UPDATE users SET username=$1 WHERE telegram_id=$2", username, tid)
        return row  # type: ignore

    @classmethod
    async def accept_terms(cls, tid: int) -> None:
        assert cls.pool is not None
        await cls.pool.execute(
            "UPDATE users SET is_terms_accepted=TRUE, terms_accepted_at=NOW() "
            "WHERE telegram_id=$1 AND is_terms_accepted=FALSE",
            tid,
        )

    @classmethod
    async def set_hub_message(cls, tid: int, message_id: int | None) -> None:
        assert cls.pool is not None
        await cls.pool.execute("UPDATE users SET hub_message_id=$1 WHERE telegram_id=$2", message_id, tid)

    @classmethod
    async def adjust_balance(
        cls, tid: int, delta: Decimal, log_type: str, comment: str, reference_id: str | None = None,
    ) -> Decimal:
        assert cls.pool is not None
        async with cls.pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    "UPDATE users SET balance_rub = balance_rub + $1 WHERE telegram_id=$2",
                    delta, tid,
                )
                bal = await con.fetchval("SELECT balance_rub FROM users WHERE telegram_id=$1", tid)
                await con.execute(
                    "INSERT INTO balance_audit_log(user_id,type,amount_rub,balance_after,comment,reference_id) "
                    "VALUES($1,$2,$3,$4,$5,$6)",
                    tid, log_type, delta, bal, comment, reference_id,
                )
        return Decimal(bal)

    # --------- subscription helpers ----------
    @classmethod
    async def get_sub(cls, tid: int) -> asyncpg.Record | None:
        assert cls.pool is not None
        return await cls.pool.fetchrow("SELECT * FROM subscriptions WHERE user_id=$1", tid)

    @classmethod
    async def upsert_sub(cls, tid: int, days: int, sub_url: str | None = None) -> asyncpg.Record:
        assert cls.pool is not None
        async with cls.pool.acquire() as con:
            row = await con.fetchrow("SELECT * FROM subscriptions WHERE user_id=$1", tid)
            if row is None:
                expires = now_utc() + timedelta(days=days)
                await con.execute(
                    "INSERT INTO subscriptions(user_id, subscription_url, expires_at) VALUES($1,$2,$3)",
                    tid, sub_url or gen_sub_url(tid), expires,
                )
            else:
                base = row["expires_at"] if row["expires_at"] > now_utc() else now_utc()
                expires = base + timedelta(days=days)
                await con.execute(
                    "UPDATE subscriptions SET expires_at=$1, is_active=TRUE, warned_24h=FALSE WHERE user_id=$2",
                    expires, tid,
                )
            return await con.fetchrow("SELECT * FROM subscriptions WHERE user_id=$1", tid)  # type: ignore

    @classmethod
    async def deactivate_sub(cls, tid: int) -> None:
        assert cls.pool is not None
        await cls.pool.execute("UPDATE subscriptions SET is_active=FALSE WHERE user_id=$1", tid)

    # --------- orders ----------
    @classmethod
    async def create_order(
        cls, tid: int, order_type: str, plan_code: str | None, amount: Decimal,
    ) -> asyncpg.Record:
        assert cls.pool is not None
        expires_at = now_utc() + timedelta(minutes=ORDER_TTL_MIN)
        row = await cls.pool.fetchrow(
            "INSERT INTO orders(user_id,order_type,plan_code,amount_rub,expires_at) "
            "VALUES($1,$2,$3,$4,$5) RETURNING *",
            tid, order_type, plan_code, amount, expires_at,
        )
        return row  # type: ignore

    @classmethod
    async def get_order(cls, order_id: str) -> asyncpg.Record | None:
        assert cls.pool is not None
        return await cls.pool.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)

    @classmethod
    async def get_order_by_platega(cls, platega_id: str) -> asyncpg.Record | None:
        assert cls.pool is not None
        return await cls.pool.fetchrow(
            "SELECT * FROM orders WHERE platega_transaction_id=$1", platega_id
        )

    @classmethod
    async def set_order_platega(cls, order_id: str, ptx: str, url: str) -> None:
        assert cls.pool is not None
        await cls.pool.execute(
            "UPDATE orders SET platega_transaction_id=$1, payment_url=$2, updated_at=NOW() WHERE id=$3",
            ptx, url, order_id,
        )

    @classmethod
    async def set_order_status(cls, order_id: str, status: str) -> None:
        assert cls.pool is not None
        await cls.pool.execute(
            "UPDATE orders SET status=$1, updated_at=NOW() WHERE id=$2",
            status, order_id,
        )

    @classmethod
    async def mark_callback_once(cls, source: str, external_id: str, status: str) -> bool:
        """True — если впервые (можно обрабатывать). False — дубликат."""
        assert cls.pool is not None
        try:
            await cls.pool.execute(
                "INSERT INTO processed_callbacks(source,external_id,status) VALUES($1,$2,$3)",
                source, external_id, status,
            )
            return True
        except asyncpg.UniqueViolationError:
            return False

    # --------- tickets ----------
    @classmethod
    async def create_ticket(cls, tid: int, subject_type: str, text: str) -> int:
        assert cls.pool is not None
        async with cls.pool.acquire() as con:
            async with con.transaction():
                ticket_id = await con.fetchval(
                    "INSERT INTO tickets(user_id,subject_type) VALUES($1,$2) RETURNING id",
                    tid, subject_type,
                )
                await con.execute(
                    "INSERT INTO ticket_messages(ticket_id,sender_type,sender_id,text) VALUES($1,'USER',$2,$3)",
                    ticket_id, tid, text,
                )
        return int(ticket_id)

    @classmethod
    async def get_open_ticket(cls, tid: int, subject_type: str) -> asyncpg.Record | None:
        assert cls.pool is not None
        return await cls.pool.fetchrow(
            "SELECT * FROM tickets WHERE user_id=$1 AND subject_type=$2 AND status='OPEN' "
            "ORDER BY id DESC LIMIT 1",
            tid, subject_type,
        )

    @classmethod
    async def add_ticket_message(cls, ticket_id: int, sender_type: str, sender_id: int, text: str) -> None:
        assert cls.pool is not None
        async with cls.pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    "INSERT INTO ticket_messages(ticket_id,sender_type,sender_id,text) VALUES($1,$2,$3,$4)",
                    ticket_id, sender_type, sender_id, text,
                )
                await con.execute("UPDATE tickets SET updated_at=NOW() WHERE id=$1", ticket_id)

    @classmethod
    async def close_ticket(cls, ticket_id: int) -> None:
        assert cls.pool is not None
        await cls.pool.execute(
            "UPDATE tickets SET status='CLOSED', updated_at=NOW() WHERE id=$1", ticket_id
        )


# ------------------------------------------------------------------ #
# HTTP клиенты
# ------------------------------------------------------------------ #
class HttpClient:
    session: aiohttp.ClientSession | None = None

    @classmethod
    async def init(cls) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        cls.session = aiohttp.ClientSession(timeout=timeout)

    @classmethod
    async def close(cls) -> None:
        if cls.session:
            await cls.session.close()


class PlategaClient:
    @staticmethod
    async def create_transaction(order_id: str, amount: Decimal) -> dict:
        """Возвращает {'transactionId':..., 'paymentUrl':...}"""
        assert HttpClient.session
        headers = {
            "X-MerchantId": PLATEGA_MERCHANT_ID,
            "X-Secret": PLATEGA_SECRET,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload_base = {
            "amount": float(amount),
            "currency": "RUB",
            "paymentMethod": 2,  # СБП
            "description": f"Заказ #{order_id}",
            "callbackUrl": PLATEGA_CALLBACK_URL,
            "metadata": {"orderId": order_id},
        }
        # "No available requisites" → retry amount+1: 1001, 2002, 3001
        amounts = [float(amount), float(amount) + 1, float(amount) + 2, float(amount) + 3]
        last_err: str | None = None
        for amt in amounts:
            payload = {**payload_base, "amount": amt}
            async with HttpClient.session.post(
                f"{PLATEGA_BASE_URL}/transaction/process", headers=headers, json=payload
            ) as resp:
                body = await resp.text()
                if resp.status in (200, 201):
                    data = json.loads(body)
                    # ожидаемые ключи: transactionId / paymentUrl — уточнить по докам
                    ptx = data.get("transactionId") or data.get("id")
                    url = data.get("paymentUrl") or data.get("url")
                    if ptx and url:
                        return {"transactionId": str(ptx), "paymentUrl": url}
                    last_err = f"bad response: {body}"
                    continue
                last_err = f"{resp.status}: {body}"
                if "requisite" in body.lower():
                    continue
                break
        raise RuntimeError(f"Platega create_transaction failed: {last_err}")

    @staticmethod
    async def get_transaction(ptx: str) -> dict:
        assert HttpClient.session
        headers = {
            "X-MerchantId": PLATEGA_MERCHANT_ID,
            "X-Secret": PLATEGA_SECRET,
            "Accept": "application/json",
        }
        async with HttpClient.session.get(
            f"{PLATEGA_BASE_URL}/transaction/{ptx}", headers=headers
        ) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Platega get_transaction {resp.status}: {body}")
            return json.loads(body)


class AdaptGroupClient:
    @staticmethod
    def _headers() -> dict[str, str]:
        return {"X-Api-Key": ADAPTGROUP_API_KEY, "Content-Type": "application/json"}

    @staticmethod
    async def create_subscription(user_id: int, days: int) -> dict:
        """Создать/продлить подписку. Уточнить схему у AdaptGroup."""
        assert HttpClient.session
        payload = {"externalId": str(user_id), "planDays": days}
        async with HttpClient.session.post(
            f"{ADAPTGROUP_BASE_URL}/v1/subscriptions",
            headers=AdaptGroupClient._headers(),
            json=payload,
        ) as resp:
            body = await resp.text()
            if resp.status not in (200, 201):
                raise RuntimeError(f"AdaptGroup create_sub {resp.status}: {body}")
            return json.loads(body)

    @staticmethod
    async def reset_sessions(adapt_sub_id: str) -> None:
        assert HttpClient.session
        async with HttpClient.session.post(
            f"{ADAPTGROUP_BASE_URL}/v1/subscriptions/{adapt_sub_id}/reset-sessions",
            headers=AdaptGroupClient._headers(),
        ) as resp:
            body = await resp.text()
            if resp.status not in (200, 202, 204):
                raise RuntimeError(f"AdaptGroup reset {resp.status}: {body}")

    @staticmethod
    async def cancel_subscription(adapt_sub_id: str) -> None:
        assert HttpClient.session
        async with HttpClient.session.delete(
            f"{ADAPTGROUP_BASE_URL}/v1/subscriptions/{adapt_sub_id}",
            headers=AdaptGroupClient._headers(),
        ) as resp:
            if resp.status not in (200, 202, 204, 404):
                body = await resp.text()
                raise RuntimeError(f"AdaptGroup cancel {resp.status}: {body}")


# ------------------------------------------------------------------ #
# FSM
# ------------------------------------------------------------------ #
class S(StatesGroup):
    appeal_text = State()
    topup_amount = State()
    support_text = State()


# ------------------------------------------------------------------ #
# Keyboards
# ------------------------------------------------------------------ #
def kb_terms() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📄 Условия сервиса", url=TERMS_URL)
    b.button(text="🔒 Конфиденциальность", url=PRIVACY_URL)
    b.button(text="✅ Принять и продолжить", callback_data="terms:accept")
    b.adjust(1, 1, 1)
    return b.as_markup()


def kb_blocked() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="⚖️ Подать апелляцию", callback_data="appeal:start")
    return b.as_markup()


def kb_hub(has_active: bool, expiring: bool) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if has_active:
        if expiring:
            b.button(text="🔄 Продлить сейчас", callback_data="nav:plans")
        else:
            b.button(text="⚡️ Подключение", callback_data="nav:connect")
            b.button(text="🔄 Продлить", callback_data="nav:plans")
        if not expiring:
            b.button(text="👤 Кабинет", callback_data="nav:cabinet")
            b.button(text="💬 Поддержка", callback_data="nav:support")
        else:
            b.button(text="⚡️ Подключение", callback_data="nav:connect")
            b.button(text="👤 Кабинет", callback_data="nav:cabinet")
            b.button(text="💬 Поддержка", callback_data="nav:support")
    else:
        b.button(text="💳 Оформить подписку", callback_data="nav:plans")
        b.button(text="👤 Кабинет", callback_data="nav:cabinet")
        b.button(text="💬 Поддержка", callback_data="nav:support")
    b.adjust(1)
    return b.as_markup()


def kb_connect() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📋 Скопировать ссылку", callback_data="copy:sub")
    b.button(text="📖 Инструкция по настройке", callback_data="nav:guides")
    b.button(text="📱 Мои устройства", callback_data="nav:devices")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    b.adjust(1)
    return b.as_markup()


def kb_guides_platforms() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🤖 Android", callback_data="guide:android")
    b.button(text="🍏 iOS", callback_data="guide:ios")
    b.button(text="🪟 Windows", callback_data="guide:windows")
    b.button(text="🍎 macOS", callback_data="guide:macos")
    b.button(text="⬅️ Назад", callback_data="nav:connect")
    b.adjust(2, 2, 1)
    return b.as_markup()


def kb_guides_apps(platform: str) -> InlineKeyboardMarkup:
    apps = {
        "android": [("Happ (рекомендуется)", "app:android:happ"), ("v2rayNG", "app:android:v2rayng"), ("NekoBox", "app:android:nekobox")],
        "ios": [("Happ (рекомендуется)", "app:ios:happ"), ("Streisand", "app:ios:streisand"), ("Shadowrocket", "app:ios:shadowrocket")],
        "windows": [("Happ (рекомендуется)", "app:windows:happ"), ("Hiddify", "app:windows:hiddify"), ("v2rayN", "app:windows:v2rayn")],
        "macos": [("Happ (рекомендуется)", "app:macos:happ"), ("Hiddify", "app:macos:hiddify"), ("V2Box", "app:macos:v2box")],
    }[platform]
    b = InlineKeyboardBuilder()
    for label, cb in apps:
        b.button(text=label, callback_data=cb)
    b.button(text="⬅️ Назад", callback_data="nav:guides")
    b.adjust(1)
    return b.as_markup()


def kb_app_detail(platform: str) -> InlineKeyboardMarkup:
    dl = {
        ("android", "happ"): "https://happ.su",
        ("android", "v2rayng"): "https://github.com/2dust/v2rayNG/releases/latest",
        ("android", "nekobox"): "https://github.com/MatsuriDayo/NekoBoxForAndroid/releases/latest",
        ("ios", "happ"): "https://apps.apple.com/app/happ-proxy-utility/id6504287215",
        ("ios", "streisand"): "https://apps.apple.com/app/streisand/id6450534064",
        ("ios", "shadowrocket"): "https://apps.apple.com/app/shadowrocket/id932747118",
        ("windows", "happ"): "https://happ.su",
        ("windows", "hiddify"): "https://hiddify.com",
        ("windows", "v2rayn"): "https://github.com/2dust/v2rayN/releases/latest",
        ("macos", "happ"): "https://happ.su",
        ("macos", "hiddify"): "https://hiddify.com",
        ("macos", "v2box"): "https://apps.apple.com/app/v2box-v2ray-client/id6446814690",
    }
    b = InlineKeyboardBuilder()
    url = dl.get((platform, ""))
    # ключ определяем на уровне хэндлера; здесь оставим общую кнопку
    b.button(text="📥 Установить", callback_data=f"install:{platform}")
    b.button(text="📋 Скопировать ссылку", callback_data="copy:sub")
    b.button(text="⬅️ К выбору приложения", callback_data=f"nav:guides:{platform}")
    b.adjust(1)
    return b.as_markup()


def kb_devices() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🔄 Сбросить сессии", callback_data="devices:reset_ask")
    b.button(text="⬅️ Назад", callback_data="nav:connect")
    b.adjust(1)
    return b.as_markup()


def kb_devices_reset_ask() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да, сбросить", callback_data="devices:reset_do")
    b.button(text="❌ Отмена", callback_data="nav:devices")
    b.adjust(1)
    return b.as_markup()


def kb_reset_done() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="⚡️ К подключению", callback_data="nav:connect")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    b.adjust(1)
    return b.as_markup()


def kb_plans() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    labels = {
        "14d": f"14 дней — {fmt_money(PLAN_PRICES['14d'])}",
        "1m": f"1 месяц — {fmt_money(PLAN_PRICES['1m'])}",
        "3m": f"3 месяца — {fmt_money(PLAN_PRICES['3m'])} (266 ₽/мес)",
        "6m": f"6 месяцев — {fmt_money(PLAN_PRICES['6m'])} (248 ₽/мес)",
        "1y": f"1 год — {fmt_money(PLAN_PRICES['1y'])} (191 ₽/мес) 🔥",
    }
    for code in ("14d", "1m", "3m", "6m", "1y"):
        b.button(text=labels[code], callback_data=f"plan:{code}")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    b.adjust(1)
    return b.as_markup()


def kb_order_confirm(plan: str, can_balance: bool) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    price = PLAN_PRICES[plan]
    if can_balance:
        b.button(text=f"⚡️ Оплатить с баланса ({fmt_money(price)})", callback_data="pay:balance")
    else:
        b.button(text="💰 Пополнить баланс", callback_data="nav:topup")
    b.button(text="💳 Оплатить через СБП", callback_data="pay:sbp")
    b.button(text="⬅️ К тарифам", callback_data="nav:plans")
    b.adjust(1)
    return b.as_markup()


def kb_order_pending(order_id: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📲 Оплатить через СБП", callback_data=f"pending:open:{order_id}")
    b.button(text="🔄 Проверить оплату", callback_data=f"pending:check:{order_id}")
    b.button(text="❌ Отменить заказ", callback_data=f"pending:cancel:{order_id}")
    b.adjust(1)
    return b.as_markup()


def kb_order_expired(order_id: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🔄 Проверить оплату", callback_data=f"pending:check:{order_id}")
    b.button(text="💳 Выбрать тариф", callback_data="nav:plans")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    b.adjust(1)
    return b.as_markup()


def kb_success() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="⚡️ Подключение", callback_data="nav:connect")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    b.adjust(1)
    return b.as_markup()


def kb_payment_error(order_id: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🔄 Попробовать снова", callback_data="nav:plans")
    b.button(text="💳 Выбрать тариф", callback_data="nav:plans")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    b.adjust(1)
    return b.as_markup()


def kb_cabinet(ref_link: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📋 Скопировать ссылку", callback_data="copy:ref")
    b.button(text="➕ Пополнить баланс", callback_data="nav:topup")
    b.button(text="📋 История", callback_data="nav:history")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    b.adjust(1)
    return b.as_markup()


def kb_topup_select() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for s in (150, 300, 500, 1000, 2500):
        b.button(text=f"+{s} ₽", callback_data=f"topup:amount:{s}")
    b.button(text="✍️ Своя сумма", callback_data="topup:custom")
    b.button(text="⬅️ В кабинет", callback_data="nav:cabinet")
    b.adjust(3, 2, 1, 1)
    return b.as_markup()


def kb_cancel(to: str = "nav:hub") -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data=to)
    return b.as_markup()


def kb_support() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✉️ Написать в поддержку", callback_data="support:create")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    b.adjust(1)
    return b.as_markup()


def kb_support_active() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✍️ Ответить", callback_data="support:reply")
    b.button(text="❌ Закрыть обращение", callback_data="support:close")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    b.adjust(1)
    return b.as_markup()


# ------------------------------------------------------------------ #
# UI helper: single live screen
# ------------------------------------------------------------------ #
async def edit_or_send(
    bot: Bot, user: asyncpg.Record, chat_id: int, text: str, kb: InlineKeyboardMarkup | None,
) -> Message:
    """Редактирует hub_message_id, если возможно; иначе отправляет новое."""
    hub_id = user["hub_message_id"]
    if hub_id:
        try:
            return await bot.edit_message_text(
                chat_id=chat_id, message_id=int(hub_id),
                text=text, reply_markup=kb, parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                # ничего не делаем
                return Message(message_id=int(hub_id), date=now_utc(), chat={"id": chat_id, "type": "private"})  # type: ignore
            log.debug("edit failed: %s — sending new", e)
        except Exception as e:  # noqa
            log.debug("edit failed hard: %s", e)

    msg = await bot.send_message(chat_id, text, reply_markup=kb,
                                 parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    await DB.set_hub_message(chat_id, msg.message_id)
    return msg


async def safe_delete(message: Message) -> None:
    with contextlib.suppress(Exception):
        await message.delete()


# ------------------------------------------------------------------ #
# Рендереры экранов
# ------------------------------------------------------------------ #
async def render_terms(bot: Bot, u: asyncpg.Record) -> None:
    text = (
        "Добро пожаловать в Auro VPN 👋\n\n"
        "Для использования сервиса необходимо ознакомиться\n"
        "и принять условия обслуживания и политику\n"
        "конфиденциальности."
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_terms())


async def render_blocked(bot: Bot, u: asyncpg.Record) -> None:
    reason = u["block_reason"] or "—"
    blocked_at = fmt_dt_msk(u["blocked_at"]) if u["blocked_at"] else "—"
    text = (
        "🚫 <b>Доступ к сервису ограничен</b>\n\n"
        f"• Причина: {esc(reason)}\n"
        f"• Дата: {blocked_at}\n"
        f"• ID аккаунта: <code>{u['telegram_id']}</code>\n\n"
        "Управление подпиской и балансом заблокировано.\n"
        "Если вы считаете, что произошла ошибка, подайте апелляцию."
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_blocked())


async def render_hub(bot: Bot, u: asyncpg.Record) -> None:
    sub = await DB.get_sub(u["telegram_id"])
    bal = fmt_money(u["balance_rub"])
    has_active = bool(sub and sub["expires_at"] > now_utc() and sub["is_active"])

    if has_active:
        left = sub["expires_at"] - now_utc()
        expiring = left <= timedelta(hours=24)
        if expiring:
            h = int(left.total_seconds() // 3600)
            m = int((left.total_seconds() % 3600) // 60)
            text = (
                "Auro VPN\n\n"
                f"⏳ Подписка истекает через {h} ч {m} мин\n"
                f"💰 Баланс: {bal} · 📱 Устройств: 0/{DEVICES_LIMIT}"
            )
            kb = kb_hub(True, True)
        else:
            text = (
                "Auro VPN\n\n"
                f"🟢 Подписка активна до {fmt_dt_msk(sub['expires_at'])} ({humanize_left(sub['expires_at'])})\n"
                f"💰 Баланс: {bal} · 📱 Устройств: 0/{DEVICES_LIMIT}"
            )
            kb = kb_hub(True, False)
    else:
        text = (
            "Auro VPN\n\n"
            "🔴 Подписка не активна\n"
            f"💰 Баланс: {bal}"
        )
        kb = kb_hub(False, False)

    await edit_or_send(bot, u, u["telegram_id"], text, kb)


async def render_connect(bot: Bot, u: asyncpg.Record) -> None:
    sub = await DB.get_sub(u["telegram_id"])
    url = sub["subscription_url"] if sub else gen_sub_url(u["telegram_id"])
    text = (
        "⚡️ <b>Подключение к сервису</b>\n\n"
        "Ваша ссылка подписки:\n\n"
        f"<code>{esc(url)}</code>"
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_connect())


async def render_guides(bot: Bot, u: asyncpg.Record) -> None:
    text = "💻 <b>Выберите ваше устройство:</b>"
    await edit_or_send(bot, u, u["telegram_id"], text, kb_guides_platforms())


GUIDE_TEXTS: dict[tuple[str, str], str] = {
    ("android", "happ"): (
        "🤖 <b>Android — Happ</b>\n\n"
        "1. Установите Happ из Google Play или с официального сайта happ.su.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте Happ → «+» → «Подписка».\n"
        "4. Вставьте ссылку и сохраните.\n"
        "5. Выберите рекомендуемый профиль.\n"
        "6. Нажмите «Подключиться».\n\n"
        "⚠️ Только Google Play или happ.su.\nНе используйте сторонние ссылки."
    ),
    ("android", "v2rayng"): (
        "🤖 <b>Android — v2rayNG</b>\n\n"
        "1. Установите v2rayNG с официального GitHub-репозитория 2dust/v2rayNG.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте v2rayNG → «+» → «Импорт из буфера».\n"
        "4. Выберите добавленную подписку.\n"
        "5. Нажмите «Подключиться».\n\n"
        "⚠️ Только GitHub 2dust/v2rayNG.\nТретьи сайты могут содержать вредоносное ПО."
    ),
    ("android", "nekobox"): (
        "🤖 <b>Android — NekoBox</b>\n\n"
        "1. Установите NekoBox ТОЛЬКО с официального GitHub-репозитория MatsuriDayo/NekoBoxForAndroid.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте NekoBox → «+» → «Импорт из буфера».\n"
        "4. Выберите добавленную подписку.\n"
        "5. Нажмите «Подключиться».\n\n"
        "⚠️ <b>ВАЖНО:</b> версия NekoBox в Google Play с мая 2024 контролируется третьей стороной и не является open-source. Не скачивайте оттуда."
    ),
    ("ios", "happ"): (
        "🍏 <b>iOS — Happ</b>\n\n"
        "1. Установите Happ из App Store.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте Happ → «+» → «Подписка».\n"
        "4. Вставьте ссылку и сохраните.\n"
        "5. Выберите рекомендуемый профиль.\n"
        "6. Нажмите «Подключиться».\n\n"
        "⚠️ Если приложение подключается, но интернет не работает — выберите в списке профилей рекомендуемый (он отмечен в приложении). Это ограничение iOS, а не ошибка настройки."
    ),
    ("ios", "streisand"): (
        "🍏 <b>iOS — Streisand</b>\n\n"
        "1. Установите Streisand из App Store.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте Streisand → «+» → «Добавить подписку».\n"
        "4. Вставьте ссылку и сохраните.\n"
        "5. Выберите добавленную подписку.\n"
        "6. Нажмите «Подключиться»."
    ),
    ("ios", "shadowrocket"): (
        "🍏 <b>iOS — Shadowrocket</b>\n\n"
        "1. Установите Shadowrocket из App Store (приложение платное, US$2.99).\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте Shadowrocket → «+» → «Добавить подписку».\n"
        "4. Вставьте ссылку и сохраните.\n"
        "5. Выберите добавленную подписку.\n"
        "6. Нажмите «Подключиться»."
    ),
    ("windows", "happ"): (
        "🪟 <b>Windows — Happ</b>\n\n"
        "1. Установите Happ с официального сайта happ.su или из Microsoft Store.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте Happ → «+» → «Подписка».\n"
        "4. Вставьте ссылку и сохраните.\n"
        "5. Выберите рекомендуемый профиль.\n"
        "6. Нажмите «Подключиться»."
    ),
    ("windows", "hiddify"): (
        "🪟 <b>Windows — Hiddify</b>\n\n"
        "1. Установите Hiddify с официального сайта hiddify.com или из Microsoft Store.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте Hiddify → «+» → «Добавить подписку».\n"
        "4. Вставьте ссылку и сохраните.\n"
        "5. Выберите добавленную подписку.\n"
        "6. Нажмите «Подключиться»."
    ),
    ("windows", "v2rayn"): (
        "🪟 <b>Windows — v2rayN</b>\n\n"
        "1. Установите v2rayN ТОЛЬКО с официального GitHub-репозитория 2dust/v2rayN.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте v2rayN → «Подписки» → «Добавить».\n"
        "4. Вставьте ссылку и сохраните.\n"
        "5. Выберите добавленную подписку.\n"
        "6. Нажмите «Подключиться».\n\n"
        "⚠️ Только GitHub 2dust/v2rayN.\nТретьи сайты содержат малварь."
    ),
    ("macos", "happ"): (
        "🍎 <b>macOS — Happ</b>\n\n"
        "1. Установите Happ из App Store или с официального сайта happ.su.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте Happ → «+» → «Подписка».\n"
        "4. Вставьте ссылку и сохраните.\n"
        "5. Выберите рекомендуемый профиль.\n"
        "6. Нажмите «Подключиться»."
    ),
    ("macos", "hiddify"): (
        "🍎 <b>macOS — Hiddify</b>\n\n"
        "1. Установите Hiddify с официального сайта hiddify.com или из App Store.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте Hiddify → «+» → «Добавить подписку».\n"
        "4. Вставьте ссылку и сохраните.\n"
        "5. Выберите добавленную подписку.\n"
        "6. Нажмите «Подключиться»."
    ),
    ("macos", "v2box"): (
        "🍎 <b>macOS — V2Box</b>\n\n"
        "1. Установите V2Box из App Store.\n"
        "2. Скопируйте ссылку подписки (кнопка ниже).\n"
        "3. Откройте V2Box → «+» → «Импорт из буфера».\n"
        "4. Выберите добавленную подписку.\n"
        "5. Нажмите «Подключиться».\n\n"
        "⚠️ V2Box содержит рекламу.\nЕсли возникнут проблемы — используйте Happ или Hiddify."
    ),
}

INSTALL_URLS: dict[tuple[str, str], str] = {
    ("android", "happ"): "https://happ.su",
    ("android", "v2rayng"): "https://github.com/2dust/v2rayNG/releases/latest",
    ("android", "nekobox"): "https://github.com/MatsuriDayo/NekoBoxForAndroid/releases/latest",
    ("ios", "happ"): "https://apps.apple.com/app/id6504287215",
    ("ios", "streisand"): "https://apps.apple.com/app/id6450534064",
    ("ios", "shadowrocket"): "https://apps.apple.com/app/id932747118",
    ("windows", "happ"): "https://happ.su",
    ("windows", "hiddify"): "https://hiddify.com",
    ("windows", "v2rayn"): "https://github.com/2dust/v2rayN/releases/latest",
    ("macos", "happ"): "https://happ.su",
    ("macos", "hiddify"): "https://hiddify.com",
    ("macos", "v2box"): "https://apps.apple.com/app/id6446814690",
}


def kb_app(platform: str, app: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    url = INSTALL_URLS.get((platform, app))
    if url:
        b.button(text="📥 Установить", url=url)
    b.button(text="📋 Скопировать ссылку", callback_data="copy:sub")
    b.button(text="⬅️ К выбору приложения", callback_data=f"nav:guides:{platform}")
    b.adjust(1)
    return b.as_markup()


async def render_devices(bot: Bot, u: asyncpg.Record) -> None:
    text = (
        "📱 <b>Подключенные устройства</b>\n\n"
        f"Использовано: 0 из {DEVICES_LIMIT}\n"
        "• Нет активных сессий"
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_devices())


async def render_plans(bot: Bot, u: asyncpg.Record) -> None:
    text = "💳 <b>Выберите срок подписки:</b>"
    await edit_or_send(bot, u, u["telegram_id"], text, kb_plans())


async def render_order_confirm(bot: Bot, u: asyncpg.Record, plan: str) -> None:
    price = PLAN_PRICES[plan]
    bal = Decimal(u["balance_rub"])
    sub = await DB.get_sub(u["telegram_id"])
    base = sub["expires_at"] if sub and sub["expires_at"] > now_utc() else now_utc()
    new_exp = base + timedelta(days=PLAN_DAYS[plan])
    text = (
        "🧾 <b>Оформление заказа</b>\n\n"
        f"• Тариф: {PLAN_LABELS[plan]}\n"
        f"• К оплате: {fmt_money(price)}\n"
        f"• Ваш баланс: {fmt_money(bal)}\n"
        f"• Подписка будет продлена до: {fmt_dt_msk(new_exp)}"
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_order_confirm(plan, bal >= price))


async def render_pending(bot: Bot, u: asyncpg.Record, order: asyncpg.Record) -> None:
    left = ""
    if order["expires_at"]:
        secs = int((order["expires_at"] - now_utc()).total_seconds())
        if secs > 0:
            left = f"{secs // 60:02d}:{secs % 60:02d}"
        else:
            left = "00:00"
    text = (
        "💳 <b>Ожидание оплаты</b>\n\n"
        f"• Заказ: #{str(order['id'])[:8]}\n"
        f"• Сумма: {fmt_money(order['amount_rub'])}\n"
        f"• Метод: СБП\n"
        f"• Осталось: {left}"
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_order_pending(str(order["id"])))


async def render_success(bot: Bot, u: asyncpg.Record, late: bool = False) -> None:
    sub = await DB.get_sub(u["telegram_id"])
    until = fmt_dt_msk(sub["expires_at"]) if sub else "—"
    bal = fmt_money(u["balance_rub"])
    if late:
        text = (
            "✅ <b>Оплата получена (с задержкой)</b>\n\n"
            f"Подписка продлена до {until}.\n"
            f"Баланс: {bal}\n\n"
            "⚠️ Оплата поступила после истечения таймера заказа."
        )
    else:
        text = (
            "✅ <b>Оплата получена!</b>\n\n"
            f"Подписка продлена до {until}.\n"
            f"Баланс: {bal}"
        )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_success())


async def render_payment_error(bot: Bot, u: asyncpg.Record, order: asyncpg.Record, reason: str) -> None:
    text = (
        "❌ <b>Оплата не прошла</b>\n\n"
        f"• Заказ: #{str(order['id'])[:8]}\n"
        f"• Сумма: {fmt_money(order['amount_rub'])}\n"
        f"• Причина: {esc(reason)}\n\n"
        "Возможные причины:\n"
        "• Недостаточно средств на карте\n"
        "• Банк отклонил операцию\n"
        "• Техническая ошибка"
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_payment_error(str(order["id"])))


async def render_cabinet(bot: Bot, u: asyncpg.Record) -> None:
    ref_link = f"https://t.me/AuroVpnBot?start=ref_{u['telegram_id']}"
    # считаем рефералов
    assert DB.pool
    invited = await DB.pool.fetchval(
        "SELECT COUNT(*) FROM users WHERE referrer_id=$1", u["telegram_id"]
    )
    earned = await DB.pool.fetchval(
        "SELECT COALESCE(SUM(amount_rub),0) FROM balance_audit_log "
        "WHERE user_id=$1 AND type='REFERRAL_REWARD'",
        u["telegram_id"],
    )
    text = (
        "👤 <b>Личный кабинет</b>\n\n"
        f"• ID: <code>{u['telegram_id']}</code>\n"
        f"• Баланс: {fmt_money(u['balance_rub'])}\n"
        f"• Приглашено: {invited} чел. (заработано {fmt_money(earned or 0)})\n\n"
        "🔗 Ваша ссылка для друзей:\n"
        f"<code>{esc(ref_link)}</code>\n\n"
        f"(Получайте {REFERRAL_PERCENT}% с каждой оплаты приглашённых)"
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_cabinet(ref_link))


async def render_topup_select(bot: Bot, u: asyncpg.Record) -> None:
    text = (
        "💰 <b>Пополнение баланса</b>\n\n"
        f"Выберите сумму или укажите свою ({int(TOPUP_MIN)}–{int(TOPUP_MAX)} ₽):"
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_topup_select())


async def render_history(bot: Bot, u: asyncpg.Record) -> None:
    assert DB.pool
    rows = await DB.pool.fetch(
        "SELECT * FROM balance_audit_log WHERE user_id=$1 ORDER BY id DESC LIMIT 20",
        u["telegram_id"],
    )
    if not rows:
        body = "История пуста."
    else:
        lines = []
        for r in rows:
            sign = "+" if r["amount_rub"] >= 0 else "−"
            amt = fmt_money(abs(r["amount_rub"]))
            cmt = esc(r["comment"] or "")
            when = fmt_dt_msk_full(r["created_at"])
            lines.append(f"• {sign}{amt} — {cmt} ({when})")
        body = "\n".join(lines)
    text = f"📋 <b>История операций:</b>\n\n{body}"
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ В кабинет", callback_data="nav:cabinet")
    await edit_or_send(bot, u, u["telegram_id"], text, b.as_markup())


async def render_support(bot: Bot, u: asyncpg.Record) -> None:
    ticket = await DB.get_open_ticket(u["telegram_id"], "GENERAL")
    if not ticket:
        text = (
            "💬 <b>Служба поддержки</b>\n\n"
            "Если у вас возникли вопросы по работе сервиса\n"
            "или оплате, создайте обращение."
        )
        await edit_or_send(bot, u, u["telegram_id"], text, kb_support())
        return
    assert DB.pool
    msgs = await DB.pool.fetch(
        "SELECT * FROM ticket_messages WHERE ticket_id=$1 ORDER BY id DESC LIMIT 5",
        ticket["id"],
    )
    msgs = list(reversed(msgs))
    lines = []
    for m in msgs:
        who = "👤 Вы" if m["sender_type"] == "USER" else "👨‍💻 Поддержка"
        lines.append(f"{who}: «{esc(m['text'])}»")
    text = (
        f"💬 <b>Обращение #{ticket['id']}</b>\n\n"
        "🟡 Статус: В обработке\n\n"
        + "\n".join(lines)
        + "\n\n💡 Отправьте сообщение в чат для ответа оператору."
    )
    await edit_or_send(bot, u, u["telegram_id"], text, kb_support_active())


# ------------------------------------------------------------------ #
# Router
# ------------------------------------------------------------------ #
router = Router()


async def _require_user(message_or_cb) -> asyncpg.Record | None:
    if isinstance(message_or_cb, CallbackQuery):
        tid = message_or_cb.from_user.id
        username = message_or_cb.from_user.username
    else:
        tid = message_or_cb.from_user.id
        username = message_or_cb.from_user.username
    u = await DB.get_user(tid)
    if u is None:
        u = await DB.upsert_user(tid, username, None)
    return u


async def _route_to_terms_or_blocked(bot: Bot, u: asyncpg.Record) -> bool:
    if not u["is_terms_accepted"]:
        await render_terms(bot, u)
        return True
    if u["is_blocked"]:
        await render_blocked(bot, u)
        return True
    return False


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    await safe_delete(message)
    args = (message.text or "").split(maxsplit=1)
    ref_id: int | None = None
    if len(args) == 2 and args[1].startswith("ref_"):
        try:
            ref_id = int(args[1][4:])
        except ValueError:
            ref_id = None

    u = await DB.get_user(message.from_user.id)
    if u is None:
        u = await DB.upsert_user(message.from_user.id, message.from_user.username, ref_id)
    else:
        u = await DB.upsert_user(message.from_user.id, message.from_user.username, None)

    if await _route_to_terms_or_blocked(message.bot, u):
        return
    await render_hub(message.bot, u)


@router.message(Command("menu"))
async def cmd_menu(message: Message, state: FSMContext) -> None:
    await state.clear()
    await safe_delete(message)
    u = await _require_user(message)
    assert u
    if await _route_to_terms_or_blocked(message.bot, u):
        return
    await render_hub(message.bot, u)


@router.callback_query(F.data == "terms:accept")
async def cb_terms_accept(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await DB.accept_terms(cb.from_user.id)
    u = await DB.get_user(cb.from_user.id)
    assert u
    await cb.answer("Принято")
    await render_hub(cb.bot, u)


@router.callback_query(F.data == "nav:hub")
async def cb_hub(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    u = await _require_user(cb)
    assert u
    if await _route_to_terms_or_blocked(cb.bot, u):
        return
    await cb.answer()
    await render_hub(cb.bot, u)


@router.callback_query(F.data == "nav:connect")
async def cb_connect(cb: CallbackQuery) -> None:
    u = await _require_user(cb)
    assert u
    if await _route_to_terms_or_blocked(cb.bot, u):
        return
    await cb.answer()
    await render_connect(cb.bot, u)


@router.callback_query(F.data.startswith("copy:"))
async def cb_copy(cb: CallbackQuery) -> None:
    what = cb.data.split(":", 1)[1] if cb.data else ""
    u = await DB.get_user(cb.from_user.id)
    assert u
    if what == "sub":
        sub = await DB.get_sub(u["telegram_id"])
        url = sub["subscription_url"] if sub else gen_sub_url(u["telegram_id"])
        await cb.answer(f"Скопировано: {url}", show_alert=True)
    elif what == "ref":
        ref_link = f"https://t.me/AuroVpnBot?start=ref_{u['telegram_id']}"
        await cb.answer(f"Скопировано: {ref_link}", show_alert=True)
    else:
        await cb.answer()


@router.callback_query(F.data == "nav:guides")
async def cb_guides(cb: CallbackQuery) -> None:
    u = await _require_user(cb)
    assert u
    if await _route_to_terms_or_blocked(cb.bot, u):
        return
    await cb.answer()
    await render_guides(cb.bot, u)


@router.callback_query(F.data.startswith("nav:guides:"))
async def cb_guides_back(cb: CallbackQuery) -> None:
    platform = (cb.data or "").split(":")[-1]
    u = await _require_user(cb)
    assert u
    await cb.answer()
    text = f"💻 <b>Выберите приложение для {platform}:</b>"
    await edit_or_send(cb.bot, u, u["telegram_id"], text, kb_guides_apps(platform))


@router.callback_query(F.data.startswith("guide:"))
async def cb_guide_platform(cb: CallbackQuery) -> None:
    platform = (cb.data or "").split(":", 1)[1]
    u = await _require_user(cb)
    assert u
    await cb.answer()
    text = f"💻 <b>Выберите приложение для {platform}:</b>"
    await edit_or_send(cb.bot, u, u["telegram_id"], text, kb_guides_apps(platform))


@router.callback_query(F.data.startswith("app:"))
async def cb_guide_app(cb: CallbackQuery) -> None:
    _, platform, app = (cb.data or "").split(":")
    u = await _require_user(cb)
    assert u
    await cb.answer()
    text = GUIDE_TEXTS.get((platform, app), "Инструкция не найдена")
    await edit_or_send(cb.bot, u, u["telegram_id"], text, kb_app(platform, app))


@router.callback_query(F.data.startswith("install:"))
async def cb_install_fallback(cb: CallbackQuery) -> None:
    await cb.answer()


@router.callback_query(F.data == "nav:devices")
async def cb_devices(cb: CallbackQuery) -> None:
    u = await _require_user(cb)
    assert u
    if await _route_to_terms_or_blocked(cb.bot, u):
        return
    await cb.answer()
    await render_devices(cb.bot, u)


@router.callback_query(F.data == "devices:reset_ask")
async def cb_devices_reset_ask(cb: CallbackQuery) -> None:
    u = await _require_user(cb)
    assert u
    await cb.answer()
    text = (
        "⚠️ <b>Сбросить активные сессии?</b>\n\n"
        "Текущие подключения будут завершены.\n"
        "Ключ подписки не изменится — повторная\n"
        "настройка устройств не требуется.\n\n"
        "После сброса достаточно переподключиться\n"
        "в приложении на каждом устройстве."
    )
    await edit_or_send(cb.bot, u, u["telegram_id"], text, kb_devices_reset_ask())


@router.callback_query(F.data == "devices:reset_do")
async def cb_devices_reset_do(cb: CallbackQuery) -> None:
    u = await _require_user(cb)
    assert u
    sub = await DB.get_sub(u["telegram_id"])
    if not sub or not sub["adapt_sub_id"]:
        await cb.answer("Нет активной подписки", show_alert=True)
        return
    await cb.answer("Сбрасываем…")
    with contextlib.suppress(Exception):
        await cb.bot.edit_message_reply_markup(
            chat_id=u["telegram_id"], message_id=cb.message.message_id,
            reply_markup=None,
        )
    try:
        await AdaptGroupClient.reset_sessions(sub["adapt_sub_id"])
    except Exception as e:
        log.exception("reset sessions failed: %s", e)
        text = "⚠️ Не удалось сбросить сессии. Попробуйте позже."
        b = InlineKeyboardBuilder()
        b.button(text="🔄 Повторить", callback_data="devices:reset_do")
        b.button(text="⬅️ Главное меню", callback_data="nav:hub")
        await edit_or_send(cb.bot, u, u["telegram_id"], text, b.as_markup())
        return
    text = (
        "✅ <b>Сессии сброшены</b>\n\n"
        "Активные подключения завершены.\n"
        "Ключ подписки не изменился — повторная\n"
        "настройка не требуется.\n\n"
        "Просто откройте приложение на каждом устройстве\n"
        "и нажмите «Подключиться»."
    )
    await edit_or_send(cb.bot, u, u["telegram_id"], text, kb_reset_done())


@router.callback_query(F.data == "nav:plans")
async def cb_plans(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    u = await _require_user(cb)
    assert u
    if await _route_to_terms_or_blocked(cb.bot, u):
        return
    await cb.answer()
    await render_plans(cb.bot, u)


@router.callback_query(F.data.startswith("plan:"))
async def cb_plan_select(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    plan = (cb.data or "").split(":", 1)[1]
    if plan not in PLAN_PRICES:
        await cb.answer("Неизвестный тариф", show_alert=True)
        return
    u = await _require_user(cb)
    assert u
    await cb.answer()
    await state.update_data(pending_plan=plan)
    await render_order_confirm(cb.bot, u, plan)


async def _create_payment_order(bot: Bot, u: asyncpg.Record, plan: str) -> None:
    price = PLAN_PRICES[plan]
    order = await DB.create_order(u["telegram_id"], "DIRECT_SUB", plan, price)
    try:
        resp = await PlategaClient.create_transaction(str(order["id"]), price)
        await DB.set_order_platega(str(order["id"]), resp["transactionId"], resp["paymentUrl"])
    except Exception as e:
        log.exception("Platega create failed: %s", e)
        order = await DB.get_order(str(order["id"]))
        assert order
        await DB.set_order_status(str(order["id"]), "CANCELED")
        await render_payment_error(bot, u, order, "Сервис оплаты временно недоступен")
        return
    order = await DB.get_order(str(order["id"]))
    assert order
    await render_pending(bot, u, order)


@router.callback_query(F.data == "pay:sbp")
async def cb_pay_sbp(cb: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    plan = data.get("pending_plan")
    if not plan:
        await cb.answer("Тариф не выбран", show_alert=True)
        return
    u = await _require_user(cb)
    assert u
    await cb.answer("Создаём платёж…")
    await _create_payment_order(cb.bot, u, plan)


@router.callback_query(F.data == "pay:balance")
async def cb_pay_balance(cb: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    plan = data.get("pending_plan")
    if not plan:
        await cb.answer("Тариф не выбран", show_alert=True)
        return
    u = await DB.get_user(cb.from_user.id)
    assert u
    price = PLAN_PRICES[plan]
    if Decimal(u["balance_rub"]) < price:
        await cb.answer("Недостаточно средств", show_alert=True)
        return
    await cb.answer("Оплачиваем…")
    # 1) списываем баланс
    try:
        await DB.adjust_balance(
            u["telegram_id"], -price, "SUBSCRIPTION_PAYMENT",
            f"Оплата подписки ({PLAN_LABELS[plan]})", reference_id=plan,
        )
    except Exception as e:
        log.exception("balance deduct failed: %s", e)
        await cb.answer("Ошибка списания", show_alert=True)
        return
    # 2) активируем/продлеваем
    try:
        resp = await AdaptGroupClient.create_subscription(u["telegram_id"], PLAN_DAYS[plan])
        adapt_id = str(resp.get("id") or resp.get("subscriptionId") or "")
        sub_url = resp.get("subscriptionUrl") or resp.get("subscription_url") or gen_sub_url(u["telegram_id"])
        assert DB.pool
        await DB.pool.execute(
            "INSERT INTO subscriptions(user_id, adapt_sub_id, subscription_url, expires_at, is_active) "
            "VALUES($1,$2,$3,$4,TRUE) "
            "ON CONFLICT (user_id) DO UPDATE SET adapt_sub_id=EXCLUDED.adapt_sub_id, "
            "subscription_url=EXCLUDED.subscription_url, expires_at=EXCLUDED.expires_at, is_active=TRUE, warned_24h=FALSE",
            u["telegram_id"], adapt_id or None, sub_url,
            now_utc() + timedelta(days=PLAN_DAYS[plan]),
        )
    except Exception as e:
        log.exception("AdaptGroup failed, rollback: %s", e)
        # ROLLBACK
        await DB.adjust_balance(
            u["telegram_id"], price, "REFUND", "Возврат: ошибка AdaptGroup", reference_id=plan,
        )
        await cb.bot.send_message(u["telegram_id"], "⚠️ Не удалось активировать подписку. Средства возвращены на баланс.")
        return
    u = await DB.get_user(u["telegram_id"])
    assert u
    await render_success(cb.bot, u)


@router.callback_query(F.data.startswith("pending:"))
async def cb_pending(cb: CallbackQuery, state: FSMContext) -> None:
    parts = (cb.data or "").split(":")
    if len(parts) != 3:
        await cb.answer()
        return
    _, action, order_id = parts
    order = await DB.get_order(order_id)
    u = await DB.get_user(cb.from_user.id)
    assert u
    if not order or order["user_id"] != u["telegram_id"]:
        await cb.answer("Заказ не найден", show_alert=True)
        return

    if action == "open":
        if order["payment_url"]:
            await cb.answer("Открываем…", url=order["payment_url"])
        else:
            await cb.answer("Ссылка недоступна", show_alert=True)
        return

    if action == "cancel":
        if order["status"] == "PENDING":
            await DB.set_order_status(order_id, "CANCELED")
        await cb.answer("Заказ отменён")
        await render_plans(cb.bot, u)
        return

    if action == "check":
        await cb.answer("Проверяем…")
        if order["status"] in ("PAID", "PAID_LATE"):
            await render_success(cb.bot, u, late=(order["status"] == "PAID_LATE"))
            return
        if not order["platega_transaction_id"]:
            await cb.answer("Транзакция не создана", show_alert=True)
            return
        try:
            info = await PlategaClient.get_transaction(order["platega_transaction_id"])
        except Exception as e:
            log.exception("Platega get failed: %s", e)
            await cb.answer("Сервис оплаты недоступен", show_alert=True)
            return
        status = str(info.get("status") or "").upper()
        if status == "CONFIRMED":
            await _apply_paid_order(order_id, late=order["status"] == "EXPIRED")
            u = await DB.get_user(u["telegram_id"])
            assert u
            order = await DB.get_order(order_id)
            assert order
            await render_success(cb.bot, u, late=(order["status"] == "PAID_LATE"))
        elif status == "CANCELED":
            await DB.set_order_status(order_id, "CANCELED")
            await render_payment_error(cb.bot, u, order, "Платёж отменён")
        else:
            await cb.answer("Платёж ещё не подтверждён", show_alert=True)
        return


async def _apply_paid_order(order_id: str, late: bool = False) -> None:
    """Активирует подписку и/или начисляет баланс по подтверждённому заказу."""
    assert DB.pool
    order = await DB.get_order(order_id)
    if not order:
        return
    if order["status"] in ("PAID", "PAID_LATE"):
        return
    async with DB.pool.acquire() as con:
        async with con.transaction():
            if order["order_type"] == "TOPUP":
                new_status = "PAID_LATE" if late else "PAID"
                await con.execute("UPDATE orders SET status=$1, updated_at=NOW() WHERE id=$2", new_status, order_id)
                await con.execute(
                    "UPDATE users SET balance_rub = balance_rub + $1 WHERE telegram_id=$2",
                    order["amount_rub"], order["user_id"],
                )
                bal = await con.fetchval("SELECT balance_rub FROM users WHERE telegram_id=$1", order["user_id"])
                await con.execute(
                    "INSERT INTO balance_audit_log(user_id,type,amount_rub,balance_after,comment,reference_id) "
                    "VALUES($1,'TOPUP',$2,$3,$4,$5)",
                    order["user_id"], order["amount_rub"], bal,
                    "Пополнение через СБП", str(order["id"]),
                )
                # реферальный бонус
                ref_id = await con.fetchval(
                    "SELECT referrer_id FROM users WHERE telegram_id=$1", order["user_id"]
                )
                if ref_id:
                    bonus = (Decimal(order["amount_rub"]) * REFERRAL_PERCENT / Decimal(100)).quantize(Decimal("0.01"))
                    if bonus > 0:
                        await con.execute(
                            "UPDATE users SET balance_rub = balance_rub + $1 WHERE telegram_id=$2",
                            bonus, ref_id,
                        )
                        rbal = await con.fetchval("SELECT balance_rub FROM users WHERE telegram_id=$1", ref_id)
                        await con.execute(
                            "INSERT INTO balance_audit_log(user_id,type,amount_rub,balance_after,comment,reference_id) "
                            "VALUES($1,'REFERRAL_REWARD',$2,$3,$4,$5)",
                            ref_id, bonus, rbal, f"Реферальный бонус {REFERRAL_PERCENT}%", str(order["id"]),
                        )
            elif order["order_type"] == "DIRECT_SUB":
                plan = order["plan_code"]
                if not plan:
                    return
                days = PLAN_DAYS[plan]
                try:
                    resp = await AdaptGroupClient.create_subscription(order["user_id"], days)
                    adapt_id = str(resp.get("id") or resp.get("subscriptionId") or "")
                    sub_url = resp.get("subscriptionUrl") or resp.get("subscription_url") or gen_sub_url(order["user_id"])
                except Exception as e:
                    log.exception("AdaptGroup activation failed: %s", e)
                    new_status = "PAID_LATE" if late else "PAID"
                    await con.execute("UPDATE orders SET status=$1, updated_at=NOW() WHERE id=$2", new_status, order_id)
                    return
                sub_row = await con.fetchrow("SELECT * FROM subscriptions WHERE user_id=$1", order["user_id"])
                base = sub_row["expires_at"] if sub_row and sub_row["expires_at"] > now_utc() else now_utc()
                new_exp = base + timedelta(days=days)
                await con.execute(
                    "INSERT INTO subscriptions(user_id, adapt_sub_id, subscription_url, expires_at, is_active) "
                    "VALUES($1,$2,$3,$4,TRUE) "
                    "ON CONFLICT (user_id) DO UPDATE SET adapt_sub_id=EXCLUDED.adapt_sub_id, "
                    "subscription_url=EXCLUDED.subscription_url, expires_at=EXCLUDED.expires_at, is_active=TRUE, warned_24h=FALSE",
                    order["user_id"], adapt_id or None, sub_url, new_exp,
                )
                new_status = "PAID_LATE" if late else "PAID"
                await con.execute("UPDATE orders SET status=$1, updated_at=NOW() WHERE id=$2", new_status, order_id)


@router.callback_query(F.data == "nav:cabinet")
async def cb_cabinet(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    u = await _require_user(cb)
    assert u
    if await _route_to_terms_or_blocked(cb.bot, u):
        return
    await cb.answer()
    await render_cabinet(cb.bot, u)


@router.callback_query(F.data == "nav:topup")
async def cb_topup(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    u = await _require_user(cb)
    assert u
    await cb.answer()
    await render_topup_select(cb.bot, u)


@router.callback_query(F.data.startswith("topup:amount:"))
async def cb_topup_amount(cb: CallbackQuery) -> None:
    amount = Decimal((cb.data or "").split(":")[-1])
    u = await DB.get_user(cb.from_user.id)
    assert u
    await cb.answer("Создаём заказ…")
    order = await DB.create_order(u["telegram_id"], "TOPUP", None, amount)
    try:
        resp = await PlategaClient.create_transaction(str(order["id"]), amount)
        await DB.set_order_platega(str(order["id"]), resp["transactionId"], resp["paymentUrl"])
    except Exception as e:
        log.exception("Platega create failed: %s", e)
        await DB.set_order_status(str(order["id"]), "CANCELED")
        order2 = await DB.get_order(str(order["id"]))
        assert order2
        await render_payment_error(cb.bot, u, order2, "Сервис оплаты временно недоступен")
        return
    order = await DB.get_order(str(order["id"]))
    assert order
    await render_pending(cb.bot, u, order)


@router.callback_query(F.data == "topup:custom")
async def cb_topup_custom(cb: CallbackQuery, state: FSMContext) -> None:
    u = await DB.get_user(cb.from_user.id)
    assert u
    await state.set_state(S.topup_amount)
    await cb.answer()
    text = (
        "✍️ <b>Введите сумму пополнения в рублях</b>\n"
        f"(Целое число от {int(TOPUP_MIN)} до {int(TOPUP_MAX)}):"
    )
    await edit_or_send(cb.bot, u, u["telegram_id"], text, kb_cancel("nav:topup"))


@router.message(S.topup_amount)
async def msg_topup_amount(message: Message, state: FSMContext) -> None:
    u = await DB.get_user(message.from_user.id)
    assert u
    raw = (message.text or "").strip().replace(" ", "")
    try:
        amount = Decimal(raw)
        if amount != amount.to_integral_value():
            raise InvalidOperation
    except (InvalidOperation, ValueError):
        await safe_delete(message)
        text = (
            "⚠️ Введите целое число от "
            f"{int(TOPUP_MIN)} до {int(TOPUP_MAX)}.\n"
            "Попробуйте снова:"
        )
        await edit_or_send(message.bot, u, u["telegram_id"], text, kb_cancel("nav:topup"))
        return
    if amount < TOPUP_MIN or amount > TOPUP_MAX:
        await safe_delete(message)
        text = (
            "⚠️ Сумма вне допустимого диапазона.\n"
            f"Введите целое число от {int(TOPUP_MIN)} до {int(TOPUP_MAX)}:"
        )
        await edit_or_send(message.bot, u, u["telegram_id"], text, kb_cancel("nav:topup"))
        return
    await safe_delete(message)
    await state.clear()
    order = await DB.create_order(u["telegram_id"], "TOPUP", None, amount)
    try:
        resp = await PlategaClient.create_transaction(str(order["id"]), amount)
        await DB.set_order_platega(str(order["id"]), resp["transactionId"], resp["paymentUrl"])
    except Exception as e:
        log.exception("Platega create failed: %s", e)
        await DB.set_order_status(str(order["id"]), "CANCELED")
        order2 = await DB.get_order(str(order["id"]))
        assert order2
        await render_payment_error(message.bot, u, order2, "Сервис оплаты временно недоступен")
        return
    order = await DB.get_order(str(order["id"]))
    assert order
    await render_pending(message.bot, u, order)


@router.callback_query(F.data == "nav:history")
async def cb_history(cb: CallbackQuery) -> None:
    u = await _require_user(cb)
    assert u
    await cb.answer()
    await render_history(cb.bot, u)


@router.callback_query(F.data == "nav:support")
async def cb_support(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    u = await _require_user(cb)
    assert u
    if await _route_to_terms_or_blocked(cb.bot, u):
        return
    await cb.answer()
    await render_support(cb.bot, u)


@router.callback_query(F.data == "support:create")
async def cb_support_create(cb: CallbackQuery, state: FSMContext) -> None:
    u = await DB.get_user(cb.from_user.id)
    assert u
    await state.set_state(S.support_text)
    await cb.answer()
    text = "✉️ <b>Опишите вашу проблему (5–2000 символов):</b>"
    await edit_or_send(cb.bot, u, u["telegram_id"], text, kb_cancel("nav:support"))


@router.callback_query(F.data == "support:reply")
async def cb_support_reply(cb: CallbackQuery, state: FSMContext) -> None:
    u = await DB.get_user(cb.from_user.id)
    assert u
    await state.set_state(S.support_text)
    await cb.answer()
    text = "✍️ <b>Введите сообщение для поддержки:</b>"
    await edit_or_send(cb.bot, u, u["telegram_id"], text, kb_cancel("nav:support"))


@router.callback_query(F.data == "support:close")
async def cb_support_close(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    u = await DB.get_user(cb.from_user.id)
    assert u
    ticket = await DB.get_open_ticket(u["telegram_id"], "GENERAL")
    if ticket:
        await DB.close_ticket(ticket["id"])
        await cb.answer("Обращение закрыто")
    else:
        await cb.answer()
    await render_support(cb.bot, u)


@router.message(S.support_text)
async def msg_support(message: Message, state: FSMContext) -> None:
    u = await DB.get_user(message.from_user.id)
    assert u
    text_raw = (message.text or "").strip()
    if len(text_raw) < 5 or len(text_raw) > 2000:
        await safe_delete(message)
        t = "⚠️ Длина сообщения должна быть от 5 до 2000 символов.\nПопробуйте снова:"
        await edit_or_send(message.bot, u, u["telegram_id"], t, kb_cancel("nav:support"))
        return
    await safe_delete(message)
    await state.clear()
    ticket = await DB.get_open_ticket(u["telegram_id"], "GENERAL")
    if ticket:
        await DB.add_ticket_message(ticket["id"], "USER", u["telegram_id"], text_raw)
    else:
        await DB.create_ticket(u["telegram_id"], "GENERAL", text_raw)
    await render_support(message.bot, u)


# ---------- Appeal ----------
@router.callback_query(F.data == "appeal:start")
async def cb_appeal_start(cb: CallbackQuery, state: FSMContext) -> None:
    u = await DB.get_user(cb.from_user.id)
    assert u
    await state.set_state(S.appeal_text)
    await cb.answer()
    text = (
        "⚖️ <b>Подача апелляции</b>\n\n"
        "Опишите подробно вашу ситуацию (10–1000 символов).\n"
        "Отправьте текст ответным сообщением:"
    )
    await edit_or_send(cb.bot, u, u["telegram_id"], text, kb_cancel("nav:hub"))


@router.message(S.appeal_text)
async def msg_appeal(message: Message, state: FSMContext) -> None:
    u = await DB.get_user(message.from_user.id)
    assert u
    txt = (message.text or "").strip()
    if len(txt) < 10 or len(txt) > 1000:
        await safe_delete(message)
        t = "⚠️ Длина текста должна быть от 10 до 1000 символов.\nПопробуйте снова:"
        await edit_or_send(message.bot, u, u["telegram_id"], t, kb_cancel("nav:hub"))
        return
    await safe_delete(message)
    await state.clear()
    await DB.create_ticket(u["telegram_id"], "APPEAL", txt)
    b = InlineKeyboardBuilder()
    b.button(text="🔄 Проверить статус", callback_data="appeal:check")
    b.button(text="⬅️ Главное меню", callback_data="nav:hub")
    text = (
        "✅ <b>Апелляция отправлена</b>\n\n"
        "Ваше обращение передано администрации.\n"
        "Ожидайте решения."
    )
    await edit_or_send(message.bot, u, u["telegram_id"], text, b.as_markup())


@router.callback_query(F.data == "appeal:check")
async def cb_appeal_check(cb: CallbackQuery) -> None:
    u = await DB.get_user(cb.from_user.id)
    assert u
    assert DB.pool
    ticket = await DB.pool.fetchrow(
        "SELECT * FROM tickets WHERE user_id=$1 AND subject_type='APPEAL' ORDER BY id DESC LIMIT 1",
        u["telegram_id"],
    )
    if not ticket:
        await cb.answer("Апелляций не найдено", show_alert=True)
        return
    if ticket["status"] == "CLOSED":
        # если разблокировали
        u = await DB.get_user(u["telegram_id"])
        assert u
        if not u["is_blocked"]:
            await cb.answer("Апелляция одобрена. Доступ восстановлен.", show_alert=True)
            await render_hub(cb.bot, u)
            return
        await cb.answer("Апелляция рассмотрена, доступ пока ограничен.", show_alert=True)
        return
    await cb.answer("Апелляция в обработке", show_alert=True)


# ---------- Text outside FSM → delete ----------
@router.message()
async def msg_fallback(message: Message, state: FSMContext) -> None:
    cur = await state.get_state()
    if cur is None:
        # вне FSM — молча удаляем
        await safe_delete(message)


# ------------------------------------------------------------------ #
# Webhooks (aiohttp)
# ------------------------------------------------------------------ #
def verify_platega_headers(request: web.Request) -> bool:
    mid = request.headers.get("X-MerchantId", "")
    sec = request.headers.get("X-Secret", "")
    return timing_safe_eq(mid, PLATEGA_MERCHANT_ID) and timing_safe_eq(sec, PLATEGA_SECRET)


def verify_adaptgroup_signature(body: bytes, signature: str) -> bool:
    if not ADAPTGROUP_WEBHOOK_SECRET or not signature:
        return False
    expected = hmac.new(ADAPTGROUP_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    sig = signature.strip()
    if sig.startswith("sha256="):
        sig = sig[len("sha256="):]
    return timing_safe_eq(expected, sig)


async def webhook_platega(request: web.Request) -> web.Response:
    if not verify_platega_headers(request):
        log.warning("Platega: invalid auth headers")
        return web.Response(status=401)
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400)
    log.info("Platega webhook: %s", body)

    ptx = str(body.get("transactionId") or body.get("id") or "")
    status = str(body.get("status") or "").upper()
    if not ptx or not status:
        return web.Response(status=200)

    # Идемпотентность
    ext_id = str(body.get("refundId") or ptx) if status == "PARTIALLY_REFUNDED" else ptx
    if not await DB.mark_callback_once("PLATEGA", ext_id, status):
        return web.Response(status=200)

    try:
        if status == "CONFIRMED":
            order = await DB.get_order_by_platega(ptx)
            if order:
                late = order["status"] == "EXPIRED"
                await _apply_paid_order(str(order["id"]), late=late)
                u = await DB.get_user(order["user_id"])
                if u:
                    with contextlib.suppress(Exception):
                        if order["order_type"] == "TOPUP":
                            await bot.send_message(u["telegram_id"], f"✅ Баланс пополнен на {fmt_money(order['amount_rub'])}")
                        else:
                            await render_success(bot, u, late=late)
        elif status == "CANCELED":
            order = await DB.get_order_by_platega(ptx)
            if order and order["status"] == "PENDING":
                await DB.set_order_status(str(order["id"]), "CANCELED")
        elif status == "DISPUTED":
            order = await DB.get_order_by_platega(ptx)
            if order:
                await DB.set_order_status(str(order["id"]), "DISPUTED")
            for admin in ADMIN_IDS:
                with contextlib.suppress(Exception):
                    await bot.send_message(admin, f"⚠️ DISPUTED по заказу {ptx}")
        elif status == "PARTIALLY_REFUNDED":
            order = await DB.get_order_by_platega(ptx)
            if order:
                refunded = Decimal(str(body.get("refundedAmount") or "0"))
                # возвращаем на баланс сумму возврата (упрощённо)
                if refunded > 0:
                    await DB.adjust_balance(
                        order["user_id"], refunded, "REFUND",
                        "Частичный возврат", str(body.get("refundId") or ptx),
                    )
        elif status == "CHARGEBACKED":
            order = await DB.get_order_by_platega(ptx)
            if order:
                await DB.set_order_status(str(order["id"]), "REFUNDED")
                # откат подписки
                sub = await DB.get_sub(order["user_id"])
                if sub and sub["adapt_sub_id"]:
                    with contextlib.suppress(Exception):
                        await AdaptGroupClient.cancel_subscription(sub["adapt_sub_id"])
                for admin in ADMIN_IDS:
                    with contextlib.suppress(Exception):
                        await bot.send_message(admin, f"⛔ CHARGEBACKED по заказу {ptx}")
    except Exception as e:
        log.exception("Platega webhook processing failed: %s", e)

    return web.Response(status=200)


async def webhook_adaptgroup(request: web.Request) -> web.Response:
    raw = await request.read()
    sig = request.headers.get("X-Webhook-Signature", "")
    if not verify_adaptgroup_signature(raw, sig):
        log.warning("AdaptGroup: invalid HMAC")
        return web.Response(status=401)
    try:
        body = json.loads(raw)
    except Exception:
        return web.Response(status=400)
    log.info("AdaptGroup webhook: %s", body)

    ext_id = str(body.get("eventId") or body.get("id") or "")
    status = str(body.get("event") or body.get("type") or "unknown")
    if ext_id and not await DB.mark_callback_once("ADAPTGROUP", ext_id, status):
        return web.Response(status=200)

    # TODO: расширить по мере появления специфики AdaptGroup
    return web.Response(status=200)


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True, "ts": int(time.time())})


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_post(f"{WEBHOOK_PATH}/platega", webhook_platega)
    app.router.add_post(ADAPTGROUP_WEBHOOK_PATH, webhook_adaptgroup)
    return app


# ------------------------------------------------------------------ #
# Background tasks
# ------------------------------------------------------------------ #
async def task_expire_orders() -> None:
    assert DB.pool
    while True:
        try:
            rows = await DB.pool.fetch(
                "UPDATE orders SET status='EXPIRED', updated_at=NOW() "
                "WHERE status='PENDING' AND expires_at IS NOT NULL AND expires_at < NOW() "
                "RETURNING *"
            )
            for r in rows:
                u = await DB.get_user(r["user_id"])
                if u:
                    order = r
                    with contextlib.suppress(Exception):
                        await edit_or_send(
                            bot, u, u["telegram_id"],
                            "⏳ <b>Время оплаты истекло</b>\n\n"
                            f"Заказ #{str(order['id'])[:8]} отменён.\n"
                            "Если вы уже оплатили — нажмите «Проверить оплату».",
                            kb_order_expired(str(order["id"])),
                        )
        except Exception as e:
            log.exception("expire_orders: %s", e)
        await asyncio.sleep(300)


async def task_notify_24h() -> None:
    assert DB.pool
    while True:
        try:
            rows = await DB.pool.fetch(
                "SELECT s.*, u.telegram_id FROM subscriptions s "
                "JOIN users u ON u.telegram_id=s.user_id "
                "WHERE s.warned_24h=FALSE AND s.is_active=TRUE "
                "AND s.expires_at BETWEEN NOW() AND NOW() + INTERVAL '24 hours'"
            )
            for r in rows:
                with contextlib.suppress(Exception):
                    await bot.send_message(
                        r["telegram_id"],
                        "⏳ Подписка Auro VPN истекает менее чем через 24 часа.\n"
                        "Продлите её, чтобы не потерять доступ.",
                    )
                await DB.pool.execute(
                    "UPDATE subscriptions SET warned_24h=TRUE WHERE id=$1", r["id"],
                )
        except Exception as e:
            log.exception("notify_24h: %s", e)
        await asyncio.sleep(3600)


async def task_cleanup_callbacks() -> None:
    assert DB.pool
    while True:
        try:
            await DB.pool.execute(
                "DELETE FROM processed_callbacks WHERE processed_at < NOW() - INTERVAL '30 days'"
            )
        except Exception as e:
            log.exception("cleanup: %s", e)
        await asyncio.sleep(86400)


# ------------------------------------------------------------------ #
# Entry point
# ------------------------------------------------------------------ #
bot: Bot  # forward declaration


async def main() -> None:
    global bot

    await DB.init()
    await HttpClient.init()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Webhook у Telegram
    webhook_url = f"{PUBLIC_BASE_URL}{WEBHOOK_PATH}/telegram"
    await bot.set_webhook(
        webhook_url,
        secret_token=WEBHOOK_SECRET_TOKEN or None,
        drop_pending_updates=True,
        allowed_updates=dp.resolve_used_update_types(),
    )
    log.info("Telegram webhook set to %s", webhook_url)

    # aiohttp-приложение для колбэков
    app = build_app()

    async def telegram_webhook(request: web.Request) -> web.Response:
        if WEBHOOK_SECRET_TOKEN:
            tok = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if not timing_safe_eq(tok, WEBHOOK_SECRET_TOKEN):
                return web.Response(status=403)
        data = await request.json()
        from aiogram.types import Update
        update = Update.model_validate(data, context={"bot": bot})
        await dp.feed_update(bot, update)
        return web.Response(status=200)

    app.router.add_post(f"{WEBHOOK_PATH}/telegram", telegram_webhook)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    log.info("HTTP server on :8080")

    bg = [
        asyncio.create_task(task_expire_orders()),
        asyncio.create_task(task_notify_24h()),
        asyncio.create_task(task_cleanup_callbacks()),
    ]

    try:
        await asyncio.Event().wait()
    finally:
        for t in bg:
            t.cancel()
        await runner.cleanup()
        await HttpClient.close()
        await DB.close()
        await bot.session.close()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
