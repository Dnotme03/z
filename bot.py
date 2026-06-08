import asyncio
import csv
import io
import json
import logging
import os
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from dotenv import load_dotenv
import aiohttp
from aiogram import Bot, Dispatcher, BaseMiddleware, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import (Message, CallbackQuery, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton, TelegramObject)
load_dotenv()
@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    API1_URL: str = os.getenv("API1_URL", "https://apis.nuviac.io/api/phone")
    API2_URL: str = os.getenv("API2_URL", "https://nv6.ek4nsh.in/api/proxy")
    API2_KEY: str = os.getenv("API2_KEY", "nightroot")
    OWNER_ID: int = int(os.getenv("OWNER_ID", "0"))
    ADMIN_IDS: List[int] = field(default_factory=lambda: [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()])
    APPROVAL_PASSWORD: str = os.getenv("APPROVAL_PASSWORD", "dkint2024")
    CSV_DIR: str = "csv"
    CSV_FILES: List[str] = field(default_factory=lambda: ["db1.csv", "db2.csv", "db3.csv", "db4.csv", "db5.csv"])
    REQUEST_TIMEOUT: int = 15
    MAX_TELEGRAM_MSG_LENGTH: int = 4096
    AUTO_DELETE_SECONDS: int = 60
    BOT_USERNAME: str = "DKINT_BOT"
    DEVELOPER_TAG: str = "@D4RKKlNG"
    def __post_init__(self):
        if not self.BOT_TOKEN:
            raise ValueError("BOT_TOKEN environment variable is required!")
config = Config()
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_format = logging.Formatter("[%(asctime)s] %(levelname)-8s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(log_format)
        console.setLevel(logging.INFO)
        logger.addHandler(console)
        from logging.handlers import RotatingFileHandler
        for fname, flevel in [("search", logging.INFO), ("error", logging.ERROR), ("startup", logging.INFO)]:
            handler = RotatingFileHandler(LOG_DIR / f"{fname}.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
            handler.setFormatter(log_format)
            handler.setLevel(flevel)
            logger.addHandler(handler)
    return logger
logger = get_logger("bot")

def clean_number(number: str) -> str:
    number = number.strip().replace(" ", "").replace("-", "")
    if number.startswith("+91"):
        number = number[3:]
    if number.startswith("91") and len(number) > 10:
        number = number[2:]
    return "".join(c for c in number if c.isdigit())

FIELD_NORMALIZATION = {
    "number": "mobile", "owner name": "name", "owner address": "address",
    "sim card": "operator", "mobile state": "circle", "connection": "connection",
    "country": "country", "hometown": "hometown", "imei number": "imei",
    "ip address": "ip_address", "language": "language", "mac address": "mac_address",
    "reference city": "reference_city", "tracker id": "tracker_id",
    "tracking history": "tracking_history", "mobile locations": "mobile_locations",
    "tower locations": "tower_locations", "owner personality": "personality",
    "complaints": "complaints", "mobile": "mobile", "name": "name",
    "fname": "father_name", "id": "id", "circle": "circle", "address": "address",
    "email": "email", "alt": "alternate_mobile", "号码": "mobile",
    "运营商": "operator", "姓名": "name", "少量性别": "gender",
    "开卡网点": "branch", "邮箱": "email",
}

def normalize_field(field_name: str) -> str:
    key = field_name.strip().lower()
    return FIELD_NORMALIZATION.get(key, key.replace(" ", "_").replace("-", "_"))

def escape_md(text: str) -> str:
    if not text:
        return ""
    text = str(text)
    special = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special:
        text = text.replace(char, '\\' + char)
    return text

class CSVLoader:
    def __init__(self):
        self.datasets: List[Dict[str, str]] = []
        self.loaded_files: List[str] = []
    async def load_all(self) -> int:
        total_records = 0
        csv_dir = Path(config.CSV_DIR)
        if not csv_dir.exists():
            logger.warning(f"CSV directory '{config.CSV_DIR}' not found")
            return 0
        for filename in config.CSV_FILES:
            filepath = csv_dir / filename
            if not filepath.exists():
                continue
            records = await self._load_csv(filepath)
            self.datasets.extend(records)
            self.loaded_files.append(filename)
            total_records += len(records)
        logger.info(f"Total CSV records loaded: {total_records} from {len(self.loaded_files)} files")
        return total_records
    async def _load_csv(self, filepath: Path) -> List[Dict[str, str]]:
        loop = asyncio.get_event_loop()
        def _read():
            records = []
            try:
                with open(filepath, "r", encoding="utf-8-sig") as f:
                    for row in csv.DictReader(f):
                        normalized = {normalize_field(k): v.strip() if v else "" for k, v in row.items()}
                        if "mobile" in normalized:
                            normalized["mobile"] = clean_number(normalized["mobile"])
                        records.append(normalized)
            except Exception as e:
                logger.error(f"Error reading {filepath}: {e}")
            return records
        return await loop.run_in_executor(None, _read)
    def search(self, number: str) -> List[Dict[str, str]]:
        number = clean_number(number)
        return [dict(r) for r in self.datasets if r.get("mobile") == number]

class APIClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
    async def query_api1(self, number: str) -> Optional[Dict[str, Any]]:
        try:
            async with self.session.get(f"{config.API1_URL}?number={number}", timeout=self.timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = data.get("data", {})
                    if result:
                        normalized = {normalize_field(k): v for k, v in result.items()}
                        normalized["_source"] = "api1"
                        return normalized
        except Exception as e:
            logger.error(f"API1 error for {number}: {e}")
        return None
    async def query_api2(self, number: str) -> Optional[List[Dict[str, Any]]]:
        try:
            async with self.session.get(f"{config.API2_URL}?key={config.API2_KEY}&num={number}", timeout=self.timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = data.get("results", [])
                    if results:
                        normalized_results = []
                        for item in results:
                            normalized = {normalize_field(k): v for k, v in item.items()}
                            normalized["_source"] = "api2"
                            normalized_results.append(normalized)
                        return normalized_results
        except Exception as e:
            logger.error(f"API2 error for {number}: {e}")
        return None

class MergeEngine:
    @staticmethod
    def merge(records: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], int]:
        merged: Dict[str, Set[str]] = defaultdict(set)
        seen_sources = set()
        for record in records:
            source = record.get("_source", "csv")
            seen_sources.add(source)
            for key, value in record.items():
                if key.startswith("_"):
                    continue
                if not value or str(value).strip() in ("", " ", "null", "none"):
                    continue
                merged[key].add(str(value).strip())
        output = {}
        for key, values in merged.items():
            cleaned = {v for v in values if v}
            if len(cleaned) == 1:
                output[key] = cleaned.pop()
            elif len(cleaned) > 1:
                output[key] = sorted(cleaned)
        return output, len(seen_sources)

csv_loader = CSVLoader()
async def initialize_search_engine() -> int:
    total = await csv_loader.load_all()
    logger.info(f"Search engine initialized with {total} CSV records")
    return total

async def search_all_sources(number: str) -> Dict[str, Any]:
    number = clean_number(number)
    all_records: List[Dict[str, Any]] = []
    async with aiohttp.ClientSession() as session:
        api = APIClient(session)
        api1_task = api.query_api1(number)
        api2_task = api.query_api2(number)
        csv_results = csv_loader.search(number)
        for r in csv_results:
            r["_source"] = "csv"
        all_records.extend(csv_results)
        api1_result, api2_result = await asyncio.gather(api1_task, api2_task, return_exceptions=True)
        if isinstance(api1_result, dict) and api1_result:
            all_records.append(api1_result)
        elif isinstance(api1_result, Exception):
            logger.warning(f"API1 failed: {api1_result}")
        if isinstance(api2_result, list) and api2_result:
            all_records.extend(api2_result)
        elif isinstance(api2_result, Exception):
            logger.warning(f"API2 failed: {api2_result}")
    if not all_records:
        return {"developer": config.DEVELOPER_TAG, "query": number, "status": "not_found", "sources_found": 0, "record": {}, "metadata": {"generated_by": f"@{config.BOT_USERNAME}"}}
    merged_data, source_count = MergeEngine.merge(all_records)
    return {"developer": config.DEVELOPER_TAG, "query": number, "status": "success", "sources_found": source_count, "record": merged_data, "metadata": {"generated_by": f"@{config.BOT_USERNAME}"}}

def format_telegram_output(data: Dict[str, Any]) -> str:
    if data["status"] == "not_found":
        return "🔍 *DKINT Search Results*\n\nQuery: `" + escape_md(data['query']) + "`\nStatus: ❌ Not Found\nNo records found in any source."
    record = data.get("record", {})
    lines = ["🔍 *DKINT Search Results*", "━━━━━━━━━━━━━━━━━━━━━", "**Developer:** " + escape_md(config.DEVELOPER_TAG), "**Query:** `" + escape_md(data['query']) + "`", "**Status:** ✅ " + escape_md(data['status'].upper()), "**Sources Found:** " + str(data['sources_found']), "━━━━━━━━━━━━━━━━━━━━━"]
    priority = ["name","mobile","father_name","id","operator","connection","circle","gender","email","alternate_mobile","address","hometown","country","language","ip_address","imei","mac_address","reference_city","tracker_id","tracking_history"]
    added = set()
    for key in priority:
        if key in record:
            val = record[key]
            label = escape_md(key.replace("_"," ").title())
            if isinstance(val, list):
                items = "\n".join("  \\- " + escape_md(v) for v in val)
                lines.append(f"**{label}:**\n{items}")
            else:
                lines.append(f"**{label}:** {escape_md(str(val))}")
            added.add(key)
    for key, val in record.items():
        if key not in added and not key.startswith("_"):
            label = escape_md(key.replace("_"," ").title())
            if isinstance(val, list):
                items = "\n".join("  \\- " + escape_md(v) for v in val)
                lines.append(f"**{label}:**\n{items}")
            else:
                lines.append(f"**{label}:** {escape_md(str(val))}")
    lines.extend(["━━━━━━━━━━━━━━━━━━━━━", "📱 Generated by: @" + escape_md(config.BOT_USERNAME)])
    return "\n".join(lines)

DB_PATH = Path("data")
DB_FILE = DB_PATH / "users.json"
class UserDatabase:
    def __init__(self):
        DB_PATH.mkdir(exist_ok=True)
        self._users: Dict[str, dict] = {}
        self._load()
    def _load(self):
        if DB_FILE.exists():
            try:
                with open(DB_FILE, "r", encoding="utf-8") as f:
                    self._users = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._users = {}
    def _save(self):
        try:
            with open(DB_FILE, "w", encoding="utf-8") as f:
                json.dump(self._users, f, indent=2, ensure_ascii=False)
        except IOError as e:
            logger.error(f"Failed to save database: {e}")
    def add_user(self, user_id: int, username: str = "", first_name: str = "") -> dict:
        uid = str(user_id)
        if uid not in self._users:
            self._users[uid] = {"user_id": user_id, "username": username, "first_name": first_name, "status": "pending", "approved_by": "", "joined_at": datetime.utcnow().isoformat()}
            self._save()
        return self._users[uid]
    def approve_user(self, user_id: int, by: str = "admin") -> Optional[dict]:
        uid = str(user_id)
        if uid in self._users:
            self._users[uid]["status"] = "approved"
            self._users[uid]["approved_by"] = by
            self._save()
            return self._users[uid]
        return None
    def decline_user(self, user_id: int) -> Optional[dict]:
        uid = str(user_id)
        if uid in self._users:
            self._users[uid]["status"] = "declined"
            self._save()
            return self._users[uid]
        return None
    def is_approved(self, user_id: int) -> bool:
        uid = str(user_id)
        user = self._users.get(uid)
        return user is not None and user.get("status") == "approved"
    def is_pending(self, user_id: int) -> bool:
        uid = str(user_id)
        user = self._users.get(uid)
        return user is not None and user.get("status") == "pending"
    def get_pending_users(self) -> List[dict]:
        return [u for u in self._users.values() if u.get("status") == "pending"]
    def is_admin_or_owner(self, user_id: int) -> bool:
        return user_id == config.OWNER_ID or user_id in config.ADMIN_IDS
db = UserDatabase()

class AuthMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: Dict[str, Any]) -> Any:
        if isinstance(event, Message):
            msg = event
            text = msg.text or ""
            command = text.split()[0].lower() if text else ""
            if command in {"/start", "/help", "/approve"}:
                return await handler(event, data)
            user_id = msg.from_user.id
            if db.is_admin_or_owner(user_id) or db.is_approved(user_id):
                return await handler(event, data)
            if msg.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
                await msg.reply(f"❌ Access Denied\n\nYou are not approved.\nPlease start the bot in DM and get approved first:\n👉 @{data['bot'].username}", parse_mode="Markdown")
            else:
                user = msg.from_user
                db.add_user(user.id, user.username or "", user.first_name or "")
                await msg.reply(f"⏳ Review Stage\n\nHello {user.first_name or 'User'}! 👋\n\nYour access request has been submitted for review.\nAn admin will review your request soon.\nYou'll receive a notification once approved.\n\n📱 Bot: @{data['bot'].username}", parse_mode="Markdown")
                await self._notify_admins(msg, data)
            return
        elif isinstance(event, CallbackQuery):
            cb = event
            if cb.data.startswith(("approve:","decline:")) or db.is_admin_or_owner(cb.from_user.id) or db.is_approved(cb.from_user.id):
                return await handler(event, data)
            await cb.answer("❌ You are not authorized!", show_alert=True)
            return
        return await handler(event, data)
    async def _notify_admins(self, message: Message, data: dict):
        user = message.from_user
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Approve", callback_data=f"approve:{user.id}"), InlineKeyboardButton(text="❌ Decline", callback_data=f"decline:{user.id}")]])
        text = f"🆕 New User Request\n\nUser: {user.full_name}\nUsername: @{user.username or 'N/A'}\nUser ID: {user.id}\nPending Total: {len(db.get_pending_users())}\n\nApprove or decline this user:"
        bot = data["bot"]
        for admin_id in config.ADMIN_IDS:
            try:
                await bot.send_message(admin_id, text, parse_mode="Markdown", reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")
        if config.OWNER_ID and config.OWNER_ID not in config.ADMIN_IDS:
            try:
                await bot.send_message(config.OWNER_ID, text, parse_mode="Markdown", reply_markup=keyboard)
            except Exception:
                pass

router = Router()
@router.startup()
async def on_startup(bot: Bot):
    logger.info("="*50)
    logger.info("DKINT BOT STARTING UP")
    logger.info("="*50)
    total = await initialize_search_engine()
    logger.info(f"CSV datasets loaded: {total} records")
    await bot.set_my_commands([{"command":"start","description":"Start the bot and register"},{"command":"num","description":"Search by phone number: /num <number>"},{"command":"help","description":"Show help information"}])
    logger.info("DKINT BOT STARTUP COMPLETE")
    logger.info("="*50)

@router.message(Command("start"))
async def cmd_start(message: Message, bot: Bot):
    user = message.from_user
    user_id = user.id
    is_owner_or_admin = db.is_admin_or_owner(user_id)
    if is_owner_or_admin:
        db.add_user(user_id, user.username or "", user.first_name or "")
        db.approve_user(user_id, by="system")
        await message.reply(f"👑 *Welcome Master!*\n\nHello {user.first_name}! You are recognized as {'Owner' if user_id == config.OWNER_ID else 'Admin'}.\nYou have full access automatically.\n\nUse `/num <phone_number>` to search.\n\nExample: `/num 9876543210`", parse_mode="Markdown")
        return
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply(f"👋 Hi {user.first_name}!\n\nPlease start me in DM to register and get approved:\n👉 @{bot.username}", parse_mode="Markdown")
        return
    if db.is_approved(user.id):
        await message.reply(f"✅ *Welcome back, {user.first_name}!*\n\nYou are already approved.\nUse `/num <phone_number>` to search.\n\nExample: `/num 9876543210`", parse_mode="Markdown")
    elif db.is_pending(user.id):
        await message.reply(f"⏳ *Review Stage*\n\nYour request is still pending admin approval.\nPlease wait for an admin to review your request.\nYou will be notified once approved.", parse_mode="Markdown")
    else:
        db.add_user(user.id, user.username or "", user.first_name or "")
        await message.reply(f"👋 *Welcome to DKINT Bot!*\n\nHello {user.first_name}! I'm an authorized internal record lookup tool.\n\n⏳ *You are in the review stage.*\nAn admin will review your request soon.\nYou will be notified once approved.\n\n📱 Contact: {config.DEVELOPER_TAG}", parse_mode="Markdown")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Approve", callback_data=f"approve:{user.id}"), InlineKeyboardButton(text="❌ Decline", callback_data=f"decline:{user.id}")]])
        text = f"🆕 New User Registration\n\nUser: {user.full_name}\nUsername: @{user.username or 'N/A'}\nUser ID: {user.id}\n\nApprove to grant access:"
        for admin_id in config.ADMIN_IDS:
            try:
                await bot.send_message(admin_id, text, parse_mode="Markdown", reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")
        if config.OWNER_ID and config.OWNER_ID not in config.ADMIN_IDS:
            try:
                await bot.send_message(config.OWNER_ID, text, parse_mode="Markdown", reply_markup=keyboard)
            except Exception:
                pass

@router.message(Command("num"))
async def cmd_num(message: Message, bot: Bot):
    user = message.from_user
    user_id = user.id
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        if not db.is_admin_or_owner(user_id) and not db.is_approved(user_id):
            await message.reply(f"❌ Access Denied\n\nYou are not approved.\nPlease start the bot in DM and get approved first:\n👉 @{bot.username}", parse_mode="Markdown")
            return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply(f"❌ Usage: /num <phone_number>\nExample: /num 9876543210", parse_mode="Markdown")
        return
    number = clean_number(args[1])
    if not number or len(number) < 10:
        await message.reply(f"❌ Invalid number.\nPlease provide a valid 10-digit phone number.", parse_mode="Markdown")
        return
    loading = await message.reply(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n⏳ Digging on API v1.... 🔄", parse_mode="Markdown")
    await asyncio.sleep(0.8)
    try:
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n⏳ Digging on API v2.... 🔄", parse_mode="Markdown")
        await asyncio.sleep(0.8)
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n⏳ Fetching CSV data.... 🔄", parse_mode="Markdown")
        await asyncio.sleep(0.6)
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n✅ Fetching CSV data.... ✔️\n⏳ Merging and deduplicating.... 🔄", parse_mode="Markdown")
        await asyncio.sleep(0.6)
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n✅ Fetching CSV data.... ✔️\n✅ Merging and deduplicating.... ✔️\n⏳ Finalizing results.... 🔄", parse_mode="Markdown")
    except Exception:
        pass
    try:
        result = await search_all_sources(number)
    except Exception as e:
        logger.error(f"Search error for {number}: {e}")
        try:
            await loading.edit_text(f"❌ Search Failed\n\nAn error occurred while processing your request.\nPlease try again later.\n\n📱 Contact: {config.DEVELOPER_TAG}", parse_mode="Markdown")
        except Exception:
            await message.reply(f"❌ Search Failed\n\nAn error occurred.\n\n📱 Contact: {config.DEVELOPER_TAG}", parse_mode="Markdown")
        return
    try:
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n✅ Fetching CSV data.... ✔️\n✅ Merging and deduplicating.... ✔️\n✅ Finalizing results.... ✔️\n━━━━━━━━━━━━━━━━━\n📤 Preparing output...", parse_mode="Markdown")
        await asyncio.sleep(0.5)
    except Exception:
        pass
    formatted = format_telegram_output(result)
    try:
        await loading.delete()
    except Exception:
        pass
    if len(formatted) > config.MAX_TELEGRAM_MSG_LENGTH:
        json_str = json.dumps(result, indent=2, ensure_ascii=False, default=str)
        sent = await message.reply_document(document=BufferedInputFile(file=json_str.encode("utf-8"), filename=f"dkint_{number}.json"), caption=f"📄 Result file for {number}\n(Auto-deletes in 60s)")
    else:
        sent = await message.reply(formatted, parse_mode="Markdown")
    await asyncio.sleep(config.AUTO_DELETE_SECONDS)
    try:
        await bot.delete_message(message.chat.id, sent.message_id)
    except Exception:
        pass

@router.message(Command("help"))
async def cmd_help(message: Message, bot: Bot):
    await message.reply(f"📖 DKINT Bot Help\n\nCommands:\n/start - Register and get started\n/num <number> - Search phone number\n/help - Show this help\n\nUsage:\n1. Send /start to register\n2. Wait for admin approval\n3. Use /num 9876543210 to search\n\nApproval via Password:\nUse /approve <password> in DM to auto-approve\n\nGroup Usage:\nMust be approved via DM first\nResults auto-delete after 60 seconds\n\nDeveloper: {config.DEVELOPER_TAG}\nBot: @{bot.username}", parse_mode="Markdown")

@router.message(Command("approve"))
async def cmd_approve_password(message: Message, command: CommandObject, bot: Bot):
    user = message.from_user
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply("Please use /approve in DM with the bot.", parse_mode="Markdown")
        return
    password = command.args.strip() if command.args else ""
    if not password:
        await message.reply(f"❌ Usage: /approve <password>\nContact {config.DEVELOPER_TAG} for the password.", parse_mode="Markdown")
        return
    if password != config.APPROVAL_PASSWORD:
        await message.reply("❌ Invalid password. Contact admin for the correct password.", parse_mode="Markdown")
        return
    db.add_user(user.id, user.username or "", user.first_name or "")
    db.approve_user(user.id, by="password")
    await message.reply(f"✅ Approval Successful!\n\nWelcome, {user.first_name}! 🎉\n\nYou now have full access.\nUse /num <phone_number> to search.\n\nExample: /num 9876543210", parse_mode="Markdown")
    logger.info(f"User {user.id} ({user.full_name}) approved via password")
    notify_ids = list(config.ADMIN_IDS)
    if config.OWNER_ID and config.OWNER_ID not in notify_ids:
        notify_ids.append(config.OWNER_ID)
    for aid in notify_ids:
        try:
            await bot.send_message(aid, f"🔑 User Self-Approved via Password\n\nUser: {user.full_name}\nUsername: @{user.username or 'N/A'}\nUser ID: {user.id}", parse_mode="Markdown")
        except Exception:
            pass

@router.callback_query(F.data.startswith("approve:"))
async def callback_approve(callback: CallbackQuery, bot: Bot):
    user_id = int(callback.data.split(":")[1])
    admin = callback.from_user
    if admin.id not in config.ADMIN_IDS and admin.id != config.OWNER_ID:
        await callback.answer("❌ You are not authorized!", show_alert=True)
        return
    user_data = db.approve_user(user_id, by=f"admin:{admin.id}")
    if user_data:
        try:
            await bot.send_message(user_id, f"✅ Approved! 🎉\n\nYour access request has been approved!\n\nYou can now use the bot.\nUse /num <phone_number> to search.\n\nExample: /num 9876543210", parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")
        try:
            await callback.message.edit_text(f"{callback.message.text}\n\n━━━━━━━━━━━━━━━━━\n✅ APPROVED by @{admin.username or 'Admin'}", parse_mode="Markdown")
        except Exception:
            pass
        await callback.answer("✅ User approved!", show_alert=False)
    else:
        await callback.answer("❌ User not found!", show_alert=True)

@router.callback_query(F.data.startswith("decline:"))
async def callback_decline(callback: CallbackQuery, bot: Bot):
    user_id = int(callback.data.split(":")[1])
    admin = callback.from_user
    if admin.id not in config.ADMIN_IDS and admin.id != config.OWNER_ID:
        await callback.answer("❌ You are not authorized!", show_alert=True)
        return
    user_data = db.decline_user(user_id)
    if user_data:
        try:
            await bot.send_message(user_id, f"❌ Access Declined\n\nYour request to use this bot has been declined.\nPlease contact {config.DEVELOPER_TAG} for assistance.", parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")
        try:
            await callback.message.edit_text(f"{callback.message.text}\n\n━━━━━━━━━━━━━━━━━\n❌ DECLINED by @{admin.username or 'Admin'}", parse_mode="Markdown")
        except Exception:
            pass
        await callback.answer("❌ User declined!", show_alert=False)
    else:
        await callback.answer("❌ User not found!", show_alert=True)

async def main():
    Path("csv").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    Path("data").mkdir(exist_ok=True)
    if not config.BOT_TOKEN:
        logger.error("BOT_TOKEN is not set!")
        sys.exit(1)
    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    dp = Dispatcher()
    dp.message.middleware(AuthMiddleware())
    dp.callback_query.middleware(AuthMiddleware())
    dp.include_router(router)
    try:
        await dp.start_polling(bot)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
