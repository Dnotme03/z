import asyncio
import csv
import json
import logging
import os
import sys
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
from aiogram.types import Message, CallbackQuery, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton, TelegramObject
load_dotenv()
@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN","8951914420:AAHBHvW3e30GKZ9cNh7tksfRbVhIeuVCrTU")
    API1_URL: str = os.getenv("API1_URL","https://apis.nuviac.io/api/phone")
    API2_URL: str = os.getenv("API2_URL","https://nv6.ek4nsh.in/api/proxy")
    API2_KEY: str = os.getenv("API2_KEY","nightroot")
    OWNER_ID: int = int(os.getenv("OWNER_ID","7325567666"))
    ADMIN_IDS: List[int] = field(default_factory=lambda:[int(x.strip()) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip()])
    APPROVAL_PASSWORD: str = os.getenv("APPROVAL_PASSWORD","dkint2024")
    CSV_DIR: str = "csv"
    CSV_FILES: List[str] = field(default_factory=lambda:["db1.csv","db2.csv","db3.csv","db4.csv","db5.csv"])
    REQUEST_TIMEOUT: int = 15
    MAX_TELEGRAM_MSG_LENGTH: int = 4096
    AUTO_DELETE_SECONDS: int = 60
    BOT_USERNAME: str = "DKINT_BOT"
    DEVELOPER_TAG: str = "@D4RKKlNG"
    PROTECTED_NUMBERS: List[str] = field(default_factory=lambda:["000000000","0000000000"])
    def __post_init__(self):
        if not self.BOT_TOKEN:
            raise ValueError("BOT_TOKEN required!")
config=Config()
LOG_DIR=Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_format=logging.Formatter("[%(asctime)s] %(levelname)-8s | %(name)s | %(message)s",datefmt="%Y-%m-%d %H:%M:%S")
def get_logger(name):
    logger=logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        console=logging.StreamHandler(sys.stdout)
        console.setFormatter(log_format)
        console.setLevel(logging.INFO)
        logger.addHandler(console)
        from logging.handlers import RotatingFileHandler
        for fn,fl in [("search",logging.INFO),("error",logging.ERROR),("startup",logging.INFO)]:
            h=RotatingFileHandler(LOG_DIR/f"{fn}.log",maxBytes=5*1024*1024,backupCount=3,encoding="utf-8")
            h.setFormatter(log_format)
            h.setLevel(fl)
            logger.addHandler(h)
    return logger
logger=get_logger("bot")

def clean_num(n):
    n=n.strip().replace(" ","").replace("-","")
    if n.startswith("+91"): n=n[3:]
    if n.startswith("91") and len(n)>10: n=n[2:]
    return "".join(c for c in n if c.isdigit())

def esc(s):
    if not s: return ""
    s=str(s)
    for c in ['_','*','[',']','(',')','~','`','>','#','+','-','=','|','{','}','.','!']:
        s=s.replace(c,'\\'+c)
    return s

FNORM={
    "number":"mobile","owner name":"name","owner address":"address",
    "sim card":"operator","mobile state":"circle","connection":"connection",
    "country":"country","hometown":"hometown","imei number":"imei",
    "ip address":"ip_address","language":"language","mac address":"mac_address",
    "reference city":"reference_city","tracker id":"tracker_id",
    "tracking history":"tracking_history","mobile locations":"mobile_locations",
    "tower locations":"tower_locations","owner personality":"personality",
    "complaints":"complaints","mobile":"mobile","name":"name",
    "fname":"father_name","id":"id","circle":"circle","address":"address",
    "email":"email","alt":"alternate_mobile","号码":"mobile",
    "运营商":"operator","姓名":"name","少量性别":"gender",
    "开卡网点":"branch","邮箱":"email",
}
def nf(f):
    k=f.strip().lower()
    return FNORM.get(k,k.replace(" ","_").replace("-","_"))

def cv(v):
    v=str(v).strip().strip('"').strip('"').strip("'")
    return " ".join(v.split())

class CSVLoader:
    def __init__(self):
        self.datasets=[]
        self.loaded=[]
    async def load_all(self):
        total=0
        d=Path(config.CSV_DIR)
        if not d.exists(): return 0
        for fn in config.CSV_FILES:
            fp=d/fn
            if not fp.exists(): continue
            recs=await self._load(fp)
            self.datasets.extend(recs)
            self.loaded.append(fn)
            total+=len(recs)
        logger.info(f"CSV loaded: {total} records from {len(self.loaded)} files")
        return total
    async def _load(self,fp):
        loop=asyncio.get_event_loop()
        def _r():
            recs=[]
            try:
                with open(fp,"r",encoding="utf-8-sig") as f:
                    for row in csv.DictReader(f):
                        norm={nf(k):cv(v) for k,v in row.items()}
                        if "mobile" in norm: norm["mobile"]=clean_num(norm["mobile"])
                        recs.append(norm)
            except Exception as e:
                logger.error(f"CSV error {fp}: {e}")
            return recs
        return await loop.run_in_executor(None,_r)
    def search(self,num):
        num=clean_num(num)
        return [dict(r) for r in self.datasets if r.get("mobile")==num]

class APIClient:
    def __init__(self,session):
        self.session=session
        self.to=aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
    async def api1(self,num):
        try:
            async with self.session.get(f"{config.API1_URL}?number={num}",timeout=self.to) as r:
                if r.status==200:
                    d=await r.json()
                    dd=d.get("data",{})
                    if dd:
                        n={}
                        for k,v in dd.items():
                            nk=nf(k)
                            n[nk]=cv(str(v)) if v else ""
                        n["_source"]="api1"
                        return n
        except Exception as e:
            logger.error(f"API1 error {num}: {e}")
        return None
    async def api2(self,num):
        try:
            async with self.session.get(f"{config.API2_URL}?key={config.API2_KEY}&num={num}",timeout=self.to) as r:
                if r.status==200:
                    d=await r.json()
                    results=d.get("results",[])
                    if results:
                        filtered=[]
                        for item in results:
                            im=clean_num(item.get("mobile",""))
                            if im==num:
                                n={}
                                for k,v in item.items():
                                    nk=nf(k)
                                    n[nk]=cv(str(v)) if v else ""
                                n["_source"]="api2"
                                filtered.append(n)
                        return filtered if filtered else None
        except Exception as e:
            logger.error(f"API2 error {num}: {e}")
        return None

class Merger:
    @staticmethod
    def merge(records):
        merged=defaultdict(set)
        seen=set()
        for rec in records:
            seen.add(rec.get("_source","csv"))
            for k,v in rec.items():
                if k.startswith("_"): continue
                v=str(v).strip()
                if not v or v.lower() in (""," ","null","none","na","-"): continue
                merged[k].add(v)
        out={}
        for key,vals in merged.items():
            cleaned=sorted({v for v in vals if v and len(v)>1 and not v.startswith("!")},key=lambda x:(len(x),x))
            if not cleaned: continue
            nonstar=[v for v in cleaned if not all(c=='*' or c=='.' for c in v.replace("x","").replace("*",""))]
            if nonstar: cleaned=nonstar
            unique=[]
            seen2=set()
            for v in cleaned:
                key_lower=v.lower().replace(" ","").replace(".","")
                if key_lower not in seen2:
                    seen2.add(key_lower)
                    unique.append(v)
            if len(unique)==1:
                out[key]=unique[0]
            elif len(unique)>1:
                cleaned_names=[v for v in unique if len(v)>3 and not any(c.isdigit() for c in v.replace(" ",""))]
                if cleaned_names:
                    unique=cleaned_names
                out[key]=unique
        return out,len(seen)

csvl=CSVLoader()
async def init_search():
    t=await csvl.load_all()
    logger.info(f"Search ready with {t} CSV records")
    return t

async def search_all(num):
    num=clean_num(num)
    if num in config.PROTECTED_NUMBERS:
        return {"developer":config.DEVELOPER_TAG,"query":num,"status":"protected","sources_found":0,"record":{},"metadata":{"generated_by":f"@{config.BOT_USERNAME}"}}
    all_recs=[]
    async with aiohttp.ClientSession() as s:
        api=APIClient(s)
        t1=api.api1(num)
        t2=api.api2(num)
        csvr=csvl.search(num)
        for r in csvr:
            r["_source"]="csv"
        all_recs.extend(csvr)
        r1,r2=await asyncio.gather(t1,t2,return_exceptions=True)
        if isinstance(r1,dict) and r1: all_recs.append(r1)
        elif isinstance(r1,Exception): logger.warning(f"API1 failed: {r1}")
        if isinstance(r2,list) and r2: all_recs.extend(r2)
        elif isinstance(r2,Exception): logger.warning(f"API2 failed: {r2}")
    if not all_recs:
        return {"developer":config.DEVELOPER_TAG,"query":num,"status":"not_found","sources_found":0,"record":{},"metadata":{"generated_by":f"@{config.BOT_USERNAME}"}}
    md,sc=Merger.merge(all_recs)
    return {"developer":config.DEVELOPER_TAG,"query":num,"status":"success","sources_found":sc,"record":md,"metadata":{"generated_by":f"@{config.BOT_USERNAME}"}}

FIELDS_ORDER=["name","mobile","father_name","id","operator","connection","circle","gender","email","alternate_mobile","address","hometown","country","language","ip_address","imei","mac_address","reference_city","tracker_id","tracking_history"]
FIELD_LABELS={"name":"Name","mobile":"Mobile","father_name":"Father Name","id":"ID","operator":"Operator","connection":"Connection","circle":"Circle","gender":"Gender","email":"Email","alternate_mobile":"Alternate Mobile","address":"Address","hometown":"Hometown","country":"Country","language":"Language","ip_address":"IP Address","imei":"IMEI","mac_address":"MAC Address","reference_city":"Reference City","tracker_id":"Tracker ID","tracking_history":"Tracking History","mobile_locations":"Mobile Locations","tower_locations":"Tower Locations","personality":"Personality","complaints":"Complaints","branch":"Branch"}

def fmt_out(data):
    q=esc(data["query"])
    if data["status"]=="not_found":
        return f"🔍 *DKINT Search Results*\n━━━━━━━━━━━━━━━━━━━━━\n**Developer:** {esc(config.DEVELOPER_TAG)}\n**Query:** `{q}`\n**Status:** ❌ Not Found\n━━━━━━━━━━━━━━━━━━━━━\nNo records found in any source.\n━━━━━━━━━━━━━━━━━━━━━\n📱 Generated by: @{esc(config.BOT_USERNAME)}"
    if data["status"]=="protected":
        return f"🔍 *DKINT Search Results*\n━━━━━━━━━━━━━━━━━━━━━\n**Developer:** {esc(config.DEVELOPER_TAG)}\n**Query:** `{q}`\n**Status:** 🚫 Protected Number\n━━━━━━━━━━━━━━━━━━━━━\nThis number is protected and cannot be searched.\n━━━━━━━━━━━━━━━━━━━━━\n📱 Generated by: @{esc(config.BOT_USERNAME)}"
    rec=data.get("record",{})
    lines=["🔍 *DKINT Search Results*","━━━━━━━━━━━━━━━━━━━━━",f"**Developer:** {esc(config.DEVELOPER_TAG)}",f"**Query:** `{q}`",f"**Status:** ✅ {esc(data['status'].upper())}",f"**Sources Found:** {data['sources_found']}","━━━━━━━━━━━━━━━━━━━━━"]
    added=set()
    for key in FIELDS_ORDER:
        if key in rec:
            val=rec[key]
            lbl=esc(FIELD_LABELS.get(key,key.replace("_"," ").title()))
            if isinstance(val,list):
                items="\n".join(f"  \\- {esc(v)}" for v in val[:5])
                if len(val)>5: items+=f"\n  \\- *+{len(val)-5} more*"
                lines.append(f"**{lbl}:**\n{items}")
            else:
                lines.append(f"**{lbl}:** {esc(str(val))}")
            added.add(key)
    for key,val in rec.items():
        if key not in added and not key.startswith("_") and key not in FIELDS_ORDER:
            lbl=esc(FIELD_LABELS.get(key,key.replace("_"," ").title()))
            if isinstance(val,list):
                items="\n".join(f"  \\- {esc(v)}" for v in val[:5])
                if len(val)>5: items+=f"\n  \\- *+{len(val)-5} more*"
                lines.append(f"**{lbl}:**\n{items}")
            else:
                lines.append(f"**{lbl}:** {esc(str(val))}")
    lines.append("━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📱 Generated by: @{esc(config.BOT_USERNAME)}")
    return "\n".join(lines)

DB_PATH=Path("data")
DB_FILE=DB_PATH/"users.json"
class DB:
    def __init__(self):
        DB_PATH.mkdir(exist_ok=True)
        self._users={}
        self._load()
    def _load(self):
        if DB_FILE.exists():
            try:
                with open(DB_FILE,"r",encoding="utf-8") as f:
                    self._users=json.load(f)
            except: self._users={}
    def _save(self):
        try:
            with open(DB_FILE,"w",encoding="utf-8") as f:
                json.dump(self._users,f,indent=2,ensure_ascii=False)
        except: pass
    def add(self,uid,un="",fn=""):
        u=str(uid)
        if u not in self._users:
            self._users[u]={"user_id":uid,"username":un,"first_name":fn,"status":"pending","approved_by":"","joined_at":datetime.utcnow().isoformat()}
            self._save()
        return self._users[u]
    def approve(self,uid,by="admin"):
        u=str(uid)
        if u in self._users:
            self._users[u]["status"]="approved"
            self._users[u]["approved_by"]=by
            self._save()
            return self._users[u]
        return None
    def decline(self,uid):
        u=str(uid)
        if u in self._users:
            self._users[u]["status"]="declined"
            self._save()
            return self._users[u]
        return None
    def is_ok(self,uid):
        u=str(uid)
        us=self._users.get(u)
        return us is not None and us.get("status")=="approved"
    def is_pending(self,uid):
        u=str(uid)
        us=self._users.get(u)
        return us is not None and us.get("status")=="pending"
    def pending_list(self):
        return [u for u in self._users.values() if u.get("status")=="pending"]
    def is_owner(self,uid):
        return uid==config.OWNER_ID or uid in config.ADMIN_IDS
    def exists(self,uid):
        return str(uid) in self._users
db=DB()

class Auth(BaseMiddleware):
    async def __call__(self,handler,event,data):
        if isinstance(event,Message):
            msg=event
            txt=msg.text or ""
            cmd=txt.split()[0].lower() if txt else ""
            if cmd in {"/start","/help","/approve"}:
                return await handler(event,data)
            uid=msg.from_user.id
            if db.is_owner(uid) or db.is_ok(uid):
                return await handler(event,data)
            if msg.chat.type in (ChatType.GROUP,ChatType.SUPERGROUP):
                await msg.reply(f"❌ Access Denied\n\nYou are not approved.\nPlease start the bot in DM and get approved first:\n👉 @{data['bot'].username}",parse_mode="Markdown")
            else:
                u=msg.from_user
                db.add(u.id,u.username or "",u.first_name or "")
                await msg.reply(f"⏳ Review Stage\n\nHello {u.first_name or 'User'}! 👋\n\nYour access request has been submitted for review.\nAn admin will review your request soon.\nYou will be notified once approved.\n\n📱 Bot: @{data['bot'].username}",parse_mode="Markdown")
                await self.notify(msg,data)
            return
        elif isinstance(event,CallbackQuery):
            cb=event
            if cb.data.startswith(("approve:","decline:")) or db.is_owner(cb.from_user.id) or db.is_ok(cb.from_user.id):
                return await handler(event,data)
            await cb.answer("❌ Not authorized!",show_alert=True)
            return
        return await handler(event,data)
    async def notify(self,msg,data):
        u=msg.from_user
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Approve",callback_data=f"approve:{u.id}"),InlineKeyboardButton(text="❌ Decline",callback_data=f"decline:{u.id}")]])
        txt=f"🆕 New User Request\n\nUser: {u.full_name}\nUsername: @{u.username or 'N/A'}\nUser ID: {u.id}\nPending: {len(db.pending_list())}\n\nApprove or decline:"
        bot=data["bot"]
        targets=list(config.ADMIN_IDS)
        if config.OWNER_ID and config.OWNER_ID not in targets:
            targets.append(config.OWNER_ID)
        for aid in targets:
            try: await bot.send_message(aid,txt,parse_mode="Markdown",reply_markup=kb)
            except: pass

router=Router()
@router.startup()
async def startup(bot):
    logger.info("="*50)
    logger.info("DKINT BOT STARTING UP")
    logger.info("="*50)
    t=await init_search()
    logger.info(f"CSV: {t} records")
    await bot.set_my_commands([{"command":"start","description":"Start the bot & register"},{"command":"num","description":"Search: /num <number>"},{"command":"help","description":"Show help"}])
    logger.info("DKINT BOT READY ✅")

@router.message(Command("start"))
async def start(message,bot):
    u=message.from_user
    uid=u.id
    if db.is_owner(uid):
        db.add(uid,u.username or "",u.first_name or "")
        db.approve(uid,by="system")
        await message.reply(f"👑 *Welcome Owner!*\n\nHello {u.first_name}! You are the bot owner.\nYou have full access automatically.\n\nUse `/num <number>` to search.\nExample: `/num 9876543210`",parse_mode="Markdown")
        return
    if uid in config.ADMIN_IDS:
        db.add(uid,u.username or "",u.first_name or "")
        db.approve(uid,by="system")
        await message.reply(f"👑 *Welcome Admin!*\n\nHello {u.first_name}! You are recognized as Admin.\nYou have full access automatically.\n\nUse `/num <number>` to search.\nExample: `/num 9876543210`",parse_mode="Markdown")
        return
    if message.chat.type in (ChatType.GROUP,ChatType.SUPERGROUP):
        await message.reply(f"👋 Hi {u.first_name}!\n\nPlease start me in DM to register:\n👉 @{bot.username}",parse_mode="Markdown")
        return
    if db.is_ok(uid):
        await message.reply(f"✅ *Welcome back, {u.first_name}!*\n\nYou are already approved.\nUse `/num <number>` to search.\nExample: `/num 9876543210`",parse_mode="Markdown")
    elif db.is_pending(uid):
        await message.reply(f"⏳ *Pending Approval*\n\nYour request is still pending admin review.\nPlease wait for approval.\nYou will be notified here once approved.",parse_mode="Markdown")
    else:
        db.add(uid,u.username or "",u.first_name or "")
        await message.reply(f"👋 *Welcome to DKINT Bot!*\n\nHello {u.first_name}!\nI am an authorized internal record lookup tool.\n\n⏳ *You are in the review stage.*\nAn admin will review your request soon.\nYou will be notified once approved.\n\n📱 Contact: {config.DEVELOPER_TAG}",parse_mode="Markdown")
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Approve",callback_data=f"approve:{uid}"),InlineKeyboardButton(text="❌ Decline",callback_data=f"decline:{uid}")]])
        txt=f"🆕 New User Registration\n\nUser: {u.full_name}\nUsername: @{u.username or 'N/A'}\nUser ID: {uid}\n\nApprove to grant access:"
        targets=list(config.ADMIN_IDS)
        if config.OWNER_ID and config.OWNER_ID not in targets: targets.append(config.OWNER_ID)
        for aid in targets:
            try: await bot.send_message(aid,txt,parse_mode="Markdown",reply_markup=kb)
            except: pass

@router.message(Command("num"))
async def num(message,bot):
    u=message.from_user
    uid=u.id
    if message.chat.type in (ChatType.GROUP,ChatType.SUPERGROUP) and not db.is_owner(uid) and not db.is_ok(uid):
        await message.reply(f"❌ Access Denied\n\nYou are not approved.\nPlease start the bot in DM and get approved first:\n👉 @{bot.username}",parse_mode="Markdown")
        return
    args=message.text.split(maxsplit=1)
    if len(args)<2:
        await message.reply(f"❌ Usage: /num <phone_number>\nExample: /num 9876543210",parse_mode="Markdown")
        return
    number=clean_num(args[1])
    if not number or len(number)<10:
        await message.reply(f"❌ Invalid number.\nPlease provide a valid 10-digit phone number.",parse_mode="Markdown")
        return
    loading=await message.reply(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n⏳ Digging on API v1.... 🔄",parse_mode="Markdown")
    await asyncio.sleep(0.8)
    try:
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n⏳ Digging on API v2.... 🔄",parse_mode="Markdown")
        await asyncio.sleep(0.8)
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n⏳ Fetching CSV data.... 🔄",parse_mode="Markdown")
        await asyncio.sleep(0.6)
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n✅ Fetching CSV data.... ✔️\n⏳ Merging and deduplicating.... 🔄",parse_mode="Markdown")
        await asyncio.sleep(0.6)
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n✅ Fetching CSV data.... ✔️\n✅ Merging and deduplicating.... ✔️\n⏳ Finalizing results.... 🔄",parse_mode="Markdown")
    except: pass
    try: result=await search_all(number)
    except Exception as e:
        logger.error(f"Search error {number}: {e}")
        try: await loading.edit_text(f"❌ Search Failed\n\nAn error occurred.\n\n📱 Contact: {config.DEVELOPER_TAG}",parse_mode="Markdown")
        except: await message.reply(f"❌ Search Failed\n\n📱 Contact: {config.DEVELOPER_TAG}",parse_mode="Markdown")
        return
    try:
        await loading.edit_text(f"🔍 DKINT Search Started\n\n📱 Number: {number}\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n✅ Fetching CSV data.... ✔️\n✅ Merging and deduplicating.... ✔️\n✅ Finalizing results.... ✔️\n━━━━━━━━━━━━━━━━━\n📤 Preparing output...",parse_mode="Markdown")
        await asyncio.sleep(0.5)
    except: pass
    formatted=fmt_out(result)
    try: await loading.delete()
    except: pass
    if len(formatted)>config.MAX_TELEGRAM_MSG_LENGTH:
        js=json.dumps(result,indent=2,ensure_ascii=False,default=str)
        sent=await message.reply_document(document=BufferedInputFile(file=js.encode("utf-8"),filename=f"dkint_{number}.json"),caption=f"📄 Result file for {number}\nAuto-deletes in 60s")
    else:
        sent=await message.reply(formatted,parse_mode="Markdown")
    await asyncio.sleep(config.AUTO_DELETE_SECONDS)
    try: await bot.delete_message(message.chat.id,sent.message_id)
    except: pass

@router.message(Command("help"))
async def help(message,bot):
    await message.reply(f"📖 DKINT Bot Help\n\nCommands:\n/start - Register and get started\n/num <number> - Search phone number\n/help - Show this help\n\nUsage:\n1. Send /start to register\n2. Wait for admin approval\n3. Use /num 9876543210 to search\n\nApproval via Password:\nUse /approve <password> in DM to auto-approve\n\nGroup Usage:\nMust be approved via DM first\nResults auto-delete after 60 seconds\n\nDeveloper: {config.DEVELOPER_TAG}\nBot: @{bot.username}",parse_mode="Markdown")

@router.message(Command("approve"))
async def approve_pw(message,command,bot):
    u=message.from_user
    if message.chat.type in (ChatType.GROUP,ChatType.SUPERGROUP):
        await message.reply("Please use /approve in DM with the bot.",parse_mode="Markdown")
        return
    pw=command.args.strip() if command.args else ""
    if not pw:
        await message.reply(f"❌ Usage: /approve <password>\nContact {config.DEVELOPER_TAG} for the password.",parse_mode="Markdown")
        return
    if pw!=config.APPROVAL_PASSWORD:
        await message.reply("❌ Invalid password. Contact admin for the correct password.",parse_mode="Markdown")
        return
    db.add(u.id,u.username or "",u.first_name or "")
    db.approve(u.id,by="password")
    await message.reply(f"✅ *Approval Successful!* 🎉\n\nWelcome, {u.first_name}!\n\nYou now have full access to DKINT Bot.\n\nUse `/num <phone_number>` to search.\nExample: `/num 9876543210`\n\nEnjoy! 🚀",parse_mode="Markdown")
    logger.info(f"User {u.id} approved via password")
    targets=list(config.ADMIN_IDS)
    if config.OWNER_ID and config.OWNER_ID not in targets: targets.append(config.OWNER_ID)
    for aid in targets:
        try: await bot.send_message(aid,f"🔑 User Self-Approved via Password\n\nUser: {u.full_name}\nUsername: @{u.username or 'N/A'}\nUser ID: {u.id}",parse_mode="Markdown")
        except: pass

@router.callback_query(F.data.startswith("approve:"))
async def cb_approve(callback,bot):
    uid=int(callback.data.split(":")[1])
    admin=callback.from_user
    if admin.id not in config.ADMIN_IDS and admin.id!=config.OWNER_ID:
        await callback.answer("❌ Not authorized!",show_alert=True)
        return
    ud=db.approve(uid,by=f"admin:{admin.id}")
    if ud:
        try:
            await bot.send_message(uid,f"✅ *Approved!* 🎉\n\nYour access request has been approved by an admin!\n\nYou can now use DKINT Bot.\n\nUse `/num <phone_number>` to search.\nExample: `/num 9876543210`\n\nWelcome aboard! 🚀",parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed notify {uid}: {e}")
        try: await callback.message.edit_text(f"{callback.message.text}\n\n━━━━━━━━━━━━━━━━━\n✅ *APPROVED* by @{admin.username or 'Admin'}",parse_mode="Markdown")
        except: pass
        await callback.answer("✅ Approved!",show_alert=False)
    else:
        await callback.answer("❌ User not found",show_alert=True)

@router.callback_query(F.data.startswith("decline:"))
async def cb_decline(callback,bot):
    uid=int(callback.data.split(":")[1])
    admin=callback.from_user
    if admin.id not in config.ADMIN_IDS and admin.id!=config.OWNER_ID:
        await callback.answer("❌ Not authorized!",show_alert=True)
        return
    ud=db.decline(uid)
    if ud:
        try:
            await bot.send_message(uid,f"❌ *Access Declined*\n\nYour request to use DKINT Bot has been declined.\nPlease contact {config.DEVELOPER_TAG} for assistance.",parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed notify {uid}: {e}")
        try: await callback.message.edit_text(f"{callback.message.text}\n\n━━━━━━━━━━━━━━━━━\n❌ *DECLINED* by @{admin.username or 'Admin'}",parse_mode="Markdown")
        except: pass
        await callback.answer("❌ Declined!",show_alert=False)
    else:
        await callback.answer("❌ User not found",show_alert=True)

async def main():
    Path("csv").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    Path("data").mkdir(exist_ok=True)
    if not config.BOT_TOKEN:
        logger.error("BOT_TOKEN not set!")
        sys.exit(1)
    bot=Bot(token=config.BOT_TOKEN,default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    dp=Dispatcher()
    dp.message.middleware(Auth())
    dp.callback_query.middleware(Auth())
    dp.include_router(router)
    try: await dp.start_polling(bot)
    except (KeyboardInterrupt,SystemExit): logger.info("Bot stopped")
    finally: await bot.session.close()

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
