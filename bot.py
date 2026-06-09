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
    BOT_TOKEN: str = os.getenv("BOT_TOKEN","")
    API1_URL: str = os.getenv("API1_URL","")
    API2_URL: str = os.getenv("API2_URL","")
    API2_KEY: str = os.getenv("API2_KEY","")
    OWNER_ID: int = int(os.getenv("OWNER_ID",""))
    ADMIN_IDS: List[int] = field(default_factory=lambda:[int(x.strip()) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip()])
    APPROVAL_PASSWORD: str = os.getenv("APPROVAL_PASSWORD","")
    CSV_DIR: str = os.getenv("CSV_DIR","csv")
    CSV_FILES: List[str] = field(default_factory=lambda:[x.strip() for x in os.getenv("CSV_FILES","db1.csv,db2.csv,db3.csv,db4.csv,db5.csv").split(",") if x.strip()])
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT","15"))
    MAX_TELEGRAM_MSG_LENGTH: int = int(os.getenv("MAX_MSG_LENGTH","4096"))
    AUTO_DELETE_SECONDS: int = int(os.getenv("AUTO_DELETE","60"))
    BOT_USERNAME: str = os.getenv("BOT_USERNAME","DKINT_BOT")
    DEVELOPER_TAG: str = os.getenv("DEVELOPER_TAG","@D4RKKlNG")
    PROTECTED_NUMBERS: List[str] = field(default_factory=lambda:[x.strip() for x in os.getenv("PROTECTED_NUMBERS","9809098789,0000000000").split(",") if x.strip()])
    BOT_VERSION: str = os.getenv("BOT_VERSION","1.0.0-PRO")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL","INFO")
    DATA_DIR: str = os.getenv("DATA_DIR","data")
    def __post_init__(self):
        if not self.BOT_TOKEN: raise ValueError("BOT_TOKEN required!")
config=Config()
LOG_DIR=Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_format=logging.Formatter("[%(asctime)s] %(levelname)-8s | %(name)s | %(message)s",datefmt="%Y-%m-%d %H:%M:%S")
def get_logger(name):
    logger=logging.getLogger(name)
    ll=getattr(logging,config.LOG_LEVEL.upper(),logging.INFO)
    logger.setLevel(ll)
    if not logger.handlers:
        console=logging.StreamHandler(sys.stdout)
        console.setFormatter(log_format)
        console.setLevel(ll)
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
    n=n.strip().replace(" ","").replace("-","").replace("+","")
    if n.startswith("91") and len(n)>10: n=n[2:]
    return "".join(c for c in n if c.isdigit())

def esc(s):
    if not s: return ""
    s=str(s)
    for c in ['_','*','[',']','(',')','~','`','>','#','+','-','=','|','{','}','.','!']:
        s=s.replace(c,'\\'+c)
    return s

def clr(v):
    v=str(v).strip().strip('"').strip('"').strip("'")
    return " ".join(v.split())

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
                        norm={nf(k):clr(v) for k,v in row.items()}
                        if "mobile" in norm: norm["mobile"]=clean_num(norm["mobile"])
                        if norm.get("mobile"): recs.append(norm)
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
                            n[nk]=clr(str(v)) if v else ""
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
                                    n[nk]=clr(str(v)) if v else ""
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
            nonstar=[v for v in cleaned if not all(c=='*' or c=='.' or c=='x' for c in v.replace("x","").replace("*","").replace(".",""))]
            if nonstar: cleaned=nonstar
            unique=[]
            seen2=set()
            for v in cleaned:
                kl=v.lower().replace(" ","").replace(".","").replace("-","")
                if kl not in seen2:
                    seen2.add(kl)
                    unique.append(v)
            if len(unique)==1:
                out[key]=unique[0]
            elif len(unique)>1:
                cnames=[v for v in unique if len(v)>3 and not any(c.isdigit() for c in v.replace(" ",""))]
                if cnames: unique=cnames
                out[key]=unique
        return out,len(seen)

csvl=CSVLoader()
async def init_search():
    t=await csvl.load_all()
    logger.info(f"Search engine ready: {t} CSV records")
    return t

async def search_all(num):
    num=clean_num(num)
    if num in config.PROTECTED_NUMBERS:
        return {"developer":config.DEVELOPER_TAG,"query":num,"status":"protected","sources_found":0,"record":{},"metadata":{"generated_by":f"@{config.BOT_USERNAME}","version":config.BOT_VERSION}}
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
        if isinstance(r2,list) and r2: all_recs.extend(r2)
    if not all_recs:
        return {"developer":config.DEVELOPER_TAG,"query":num,"status":"not_found","sources_found":0,"record":{},"metadata":{"generated_by":f"@{config.BOT_USERNAME}","version":config.BOT_VERSION}}
    md,sc=Merger.merge(all_recs)
    # Clean up messy fields
    for key in list(md.keys()):
        val=md[key]
        if isinstance(val,list):
            # Remove duplicates more aggressively
            seen_set=set()
            clean_list=[]
            for item in val:
                norm=item.lower().replace(" ","").replace(".","").replace("-","").replace("(","").replace(")","").replace("!","")
                if norm not in seen_set and len(norm)>2:
                    seen_set.add(norm)
                    clean_list.append(item)
            if len(clean_list)==1:
                md[key]=clean_list[0]
            elif len(clean_list)>1:
                md[key]=clean_list[:5]
            else:
                del md[key]
    return {"developer":config.DEVELOPER_TAG,"query":num,"status":"success","sources_found":sc,"record":md,"metadata":{"generated_by":f"@{config.BOT_USERNAME}","version":config.BOT_VERSION}}

def fmt_json(data):
    return json.dumps(data,indent=2,ensure_ascii=False,default=str)

FIELD_LABELS={"name":"Name","mobile":"Mobile","father_name":"Father Name","id":"ID","operator":"Operator","connection":"Connection","circle":"Circle","gender":"Gender","email":"Email","alternate_mobile":"Alternate Mobile","address":"Address","hometown":"Hometown","country":"Country","language":"Language","ip_address":"IP Address","imei":"IMEI","mac_address":"MAC Address","reference_city":"Reference City","tracker_id":"Tracker ID","tracking_history":"Tracking History","mobile_locations":"Mobile Locations","tower_locations":"Tower Locations","personality":"Personality","complaints":"Complaints","branch":"Branch"}

def fmt_out(data):
    q=data["query"]
    json_str=fmt_json(data)
    if data["status"]=="not_found":
        return f"🔍 *DKINT Search Results*\n━━━━━━━━━━━━━━━━━━━━━\n**Developer:** {config.DEVELOPER_TAG}\n**Query:** `{q}`\n**Status:** ❌ *Not Found*\n━━━━━━━━━━━━━━━━━━━━━\nNo records found in any source.\n━━━━━━━━━━━━━━━━━━━━━\n📱 Generated by: @{config.BOT_USERNAME}\n\n```json\n{json_str}\n```"
    if data["status"]=="protected":
        return f"🔍 *DKINT Search Results*\n━━━━━━━━━━━━━━━━━━━━━\n**Developer:** {config.DEVELOPER_TAG}\n**Query:** `{q}`\n**Status:** 🚫 *Protected Number*\n━━━━━━━━━━━━━━━━━━━━━\nThis number is protected and cannot be searched.\n━━━━━━━━━━━━━━━━━━━━━\n📱 Generated by: @{config.BOT_USERNAME}\n\n```json\n{json_str}\n```"
    rec=data.get("record",{})
    lines=["🔍 *DKINT Search Results*","━━━━━━━━━━━━━━━━━━━━━",f"**Developer:** {config.DEVELOPER_TAG}",f"**Query:** `{q}`",f"**Status:** ✅ *{data['status'].upper()}*",f"**Sources Found:** {data['sources_found']}","━━━━━━━━━━━━━━━━━━━━━"]
    order=["name","mobile","father_name","id","operator","connection","circle","gender","email","alternate_mobile","address","hometown","country","language","ip_address","imei","mac_address","reference_city","tracker_id","tracking_history"]
    added=set()
    for key in order:
        if key in rec:
            val=rec[key]
            lbl=FIELD_LABELS.get(key,key.replace("_"," ").title())
            if isinstance(val,list):
                items="\n".join(f"  \\- {esc(v)}" for v in val)
                lines.append(f"**{lbl}:**\n{items}")
            else:
                lines.append(f"**{lbl}:** {esc(str(val))}")
            added.add(key)
    for key,val in rec.items():
        if key not in added and not key.startswith("_") and key not in order:
            lbl=FIELD_LABELS.get(key,key.replace("_"," ").title())
            if isinstance(val,list):
                items="\n".join(f"  \\- {esc(v)}" for v in val)
                lines.append(f"**{lbl}:**\n{items}")
            else:
                lines.append(f"**{lbl}:** {esc(str(val))}")
    lines.append("━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📱 Generated by: @{config.BOT_USERNAME}")
    lines.append(f"")
    lines.append(f"```json\n{json_str}\n```")
    return "\n".join(lines)

DB_PATH=Path(config.DATA_DIR)
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
            self._users[u]={"user_id":uid,"username":un,"first_name":fn,"status":"pending","approved_by":"","banned":False,"searches":0,"joined_at":datetime.utcnow().isoformat()}
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
    def ban(self,uid):
        u=str(uid)
        if u in self._users:
            self._users[u]["status"]="banned"
            self._users[u]["banned"]=True
            self._save()
            return self._users[u]
        return None
    def unban(self,uid):
        u=str(uid)
        if u in self._users:
            self._users[u]["status"]="approved"
            self._users[u]["banned"]=False
            self._save()
            return self._users[u]
        return None
    def add_search(self,uid):
        u=str(uid)
        if u in self._users:
            self._users[u]["searches"]=self._users[u].get("searches",0)+1
            self._save()
    def is_ok(self,uid):
        u=str(uid)
        us=self._users.get(u)
        if us and us.get("banned"): return False
        return us is not None and us.get("status")=="approved"
    def is_banned(self,uid):
        u=str(uid)
        us=self._users.get(u)
        return us is not None and (us.get("banned") or us.get("status")=="banned")
    def is_pending(self,uid):
        u=str(uid)
        us=self._users.get(u)
        return us is not None and us.get("status")=="pending"
    def pending_list(self):
        return [u for u in self._users.values() if u.get("status")=="pending" and not u.get("banned")]
    def all_users(self):
        return list(self._users.values())
    def stats(self):
        total=len(self._users)
        approved=len([u for u in self._users.values() if u.get("status")=="approved"])
        pending=len([u for u in self._users.values() if u.get("status")=="pending"])
        banned=len([u for u in self._users.values() if u.get("banned")])
        searches=sum(u.get("searches",0) for u in self._users.values())
        return total,approved,pending,banned,searches
    def is_owner(self,uid):
        return uid==config.OWNER_ID or uid in config.ADMIN_IDS
db=DB()

class Auth(BaseMiddleware):
    async def __call__(self,handler,event,data):
        if isinstance(event,Message):
            msg=event
            txt=msg.text or ""
            cmd=txt.split()[0].lower() if txt else ""
            if cmd in {"/start","/help","/approve","/admin"}:
                return await handler(event,data)
            uid=msg.from_user.id
            if db.is_banned(uid):
                await msg.reply(f"🚫 Access Denied\n\nYou have been banned from using this bot.\nContact {config.DEVELOPER_TAG} for assistance.",parse_mode="Markdown")
                return
            if db.is_owner(uid) or db.is_ok(uid):
                return await handler(event,data)
            if msg.chat.type in (ChatType.GROUP,ChatType.SUPERGROUP):
                await msg.reply(f"❌ Access Denied\n\nYou are not approved.\nPlease start the bot in DM:\n👉 @{data['bot'].username}",parse_mode="Markdown")
            else:
                u=msg.from_user
                db.add(u.id,u.username or "",u.first_name or "")
                await msg.reply(f"⏳ *Review Stage*\n\nHello {u.first_name or 'User'}! 👋\n\nYour access request has been submitted for review.\nAn admin will review your request soon.\nYou will be notified once approved.\n\n📱 Bot: @{data['bot'].username}\n👤 Developer: {config.DEVELOPER_TAG}",parse_mode="Markdown")
                await self.notify(msg,data)
            return
        elif isinstance(event,CallbackQuery):
            cb=event
            if cb.data.startswith(("approve:","decline:","admin_","ban_","unban_")) or db.is_owner(cb.from_user.id) or db.is_ok(cb.from_user.id):
                return await handler(event,data)
            await cb.answer("❌ Not authorized!",show_alert=True)
            return
        return await handler(event,data)
    async def notify(self,msg,data):
        u=msg.from_user
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Approve",callback_data=f"approve:{u.id}"),InlineKeyboardButton(text="❌ Decline",callback_data=f"decline:{u.id}")]])
        txt=f"🆕 *New User Request*\n\n👤 **User:** {u.full_name}\n📧 **Username:** @{u.username or 'N/A'}\n🆔 **ID:** `{u.id}`\n⏳ **Pending:** {len(db.pending_list())}\n\nApprove or decline:"
        bot=data["bot"]
        targets=list(config.ADMIN_IDS)
        if config.OWNER_ID and config.OWNER_ID not in targets: targets.append(config.OWNER_ID)
        for aid in targets:
            try: await bot.send_message(aid,txt,parse_mode="Markdown",reply_markup=kb)
            except: pass

router=Router()
@router.startup()
async def startup(bot):
    logger.info("="*60)
    logger.info(f"DKINT BOT v{config.BOT_VERSION} STARTING")
    logger.info("="*60)
    t=await init_search()
    logger.info(f"CSV: {t} records loaded")
    await bot.set_my_commands([
        {"command":"start","description":"Register & get started"},
        {"command":"num","description":"Search: /num <number>"},
        {"command":"admin","description":"Admin panel"},
        {"command":"help","description":"Show help"}
    ])
    logger.info("DKINT BOT READY ✅")

@router.message(Command("start"))
async def start(message,bot):
    u=message.from_user
    uid=u.id
    if db.is_banned(uid):
        await message.reply(f"🚫 *Access Denied*\n\nYou have been banned from using this bot.\nContact {config.DEVELOPER_TAG} for assistance.",parse_mode="Markdown")
        return
    if db.is_owner(uid):
        db.add(uid,u.username or "",u.first_name or "")
        db.approve(uid,by="system")
        await message.reply(f"👑 *Welcome Owner!* 🚀\n━━━━━━━━━━━━━━━━━━━━━\n**Hello {u.first_name}!**\nYou are the bot owner with full access.\n\n📊 Use `/admin` for admin panel\n🔍 Use `/num <number>` to search\n━━━━━━━━━━━━━━━━━━━━━\n👤 **Developer:** {config.DEVELOPER_TAG}\n🤖 **Bot:** @{config.BOT_USERNAME}\n📦 **Version:** {config.BOT_VERSION}",parse_mode="Markdown")
        return
    if uid in config.ADMIN_IDS:
        db.add(uid,u.username or "",u.first_name or "")
        db.approve(uid,by="system")
        await message.reply(f"👑 *Welcome Admin!* 🚀\n━━━━━━━━━━━━━━━━━━━━━\n**Hello {u.first_name}!**\nYou are an admin with full access.\n\n📊 Use `/admin` for admin panel\n🔍 Use `/num <number>` to search\n━━━━━━━━━━━━━━━━━━━━━\n👤 **Developer:** {config.DEVELOPER_TAG}\n🤖 **Bot:** @{config.BOT_USERNAME}\n📦 **Version:** {config.BOT_VERSION}",parse_mode="Markdown")
        return
    if message.chat.type in (ChatType.GROUP,ChatType.SUPERGROUP):
        await message.reply(f"👋 Hi {u.first_name}!\n\nPlease start me in DM to register:\n👉 @{bot.username}",parse_mode="Markdown")
        return
    if db.is_ok(uid):
        await message.reply(f"✅ *Welcome Back!* 🎉\n━━━━━━━━━━━━━━━━━━━━━\n**Hello {u.first_name}!**\nYou are already approved.\n\n🔍 Use `/num <number>` to search\n━━━━━━━━━━━━━━━━━━━━━\n👤 **Developer:** {config.DEVELOPER_TAG}\n🤖 **Bot:** @{config.BOT_USERNAME}",parse_mode="Markdown")
    elif db.is_pending(uid):
        await message.reply(f"⏳ *Pending Approval*\n\nYour request is still under review.\nYou will be notified once approved.\n\n👤 **Developer:** {config.DEVELOPER_TAG}",parse_mode="Markdown")
    else:
        db.add(uid,u.username or "",u.first_name or "")
        await message.reply(f"👋 *Welcome to DKINT Bot!* 🤖\n━━━━━━━━━━━━━━━━━━━━━\nHello {u.first_name}!\nI am an authorized internal record lookup system.\n\n⏳ *You are in the review stage.*\nAn admin will review your request soon.\nYou will be notified once approved.\n━━━━━━━━━━━━━━━━━━━━━\n👤 **Developer:** {config.DEVELOPER_TAG}\n🤖 **Bot:** @{config.BOT_USERNAME}\n📦 **Version:** {config.BOT_VERSION}",parse_mode="Markdown")
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Approve",callback_data=f"approve:{uid}"),InlineKeyboardButton(text="❌ Decline",callback_data=f"decline:{uid}")]])
        txt=f"🆕 *New User Registration*\n\n👤 **User:** {u.full_name}\n📧 **Username:** @{u.username or 'N/A'}\n🆔 **ID:** `{uid}`\n\nApprove to grant access:"
        targets=list(config.ADMIN_IDS)
        if config.OWNER_ID and config.OWNER_ID not in targets: targets.append(config.OWNER_ID)
        for aid in targets:
            try: await bot.send_message(aid,txt,parse_mode="Markdown",reply_markup=kb)
            except: pass

@router.message(Command("num"))
async def num(message,bot):
    u=message.from_user
    uid=u.id
    if db.is_banned(uid):
        await message.reply(f"🚫 *Access Denied*\n\nYou have been banned.\nContact {config.DEVELOPER_TAG}",parse_mode="Markdown")
        return
    if message.chat.type in (ChatType.GROUP,ChatType.SUPERGROUP) and not db.is_owner(uid) and not db.is_ok(uid):
        await message.reply(f"❌ *Access Denied*\n\nYou are not approved.\nPlease start the bot in DM:\n👉 @{bot.username}",parse_mode="Markdown")
        return
    args=message.text.split(maxsplit=1)
    if len(args)<2:
        await message.reply(f"❌ *Usage:* `/num <phone_number>`\n📝 Example: `/num 9876543210`",parse_mode="Markdown")
        return
    number=clean_num(args[1])
    if not number or len(number)<10:
        await message.reply(f"❌ *Invalid Number*\nPlease provide a valid 10-digit phone number.",parse_mode="Markdown")
        return
    if db.is_ok(uid): db.add_search(uid)
    loading=await message.reply(f"🔍 *DKINT Search Started*\n\n📱 **Number:** `{number}`\n━━━━━━━━━━━━━━━━━\n⏳ Digging on API v1.... 🔄",parse_mode="Markdown")
    await asyncio.sleep(0.7)
    try:
        await loading.edit_text(f"🔍 *DKINT Search Started*\n\n📱 **Number:** `{number}`\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n⏳ Digging on API v2.... 🔄",parse_mode="Markdown")
        await asyncio.sleep(0.7)
        await loading.edit_text(f"🔍 *DKINT Search Started*\n\n📱 **Number:** `{number}`\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n⏳ Fetching CSV data.... 🔄",parse_mode="Markdown")
        await asyncio.sleep(0.5)
        await loading.edit_text(f"🔍 *DKINT Search Started*\n\n📱 **Number:** `{number}`\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n✅ Fetching CSV data.... ✔️\n⏳ Merging & deduplicating.... 🔄",parse_mode="Markdown")
        await asyncio.sleep(0.5)
        await loading.edit_text(f"🔍 *DKINT Search Started*\n\n📱 **Number:** `{number}`\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n✅ Fetching CSV data.... ✔️\n✅ Merging & deduplicating.... ✔️\n⏳ Finalizing results.... 🔄",parse_mode="Markdown")
    except: pass
    try: result=await search_all(number)
    except Exception as e:
        logger.error(f"Search error {number}: {e}")
        try: await loading.edit_text(f"❌ *Search Failed*\n\nAn error occurred.\n\n👤 Contact: {config.DEVELOPER_TAG}",parse_mode="Markdown")
        except: await message.reply(f"❌ *Search Failed*\n\n👤 Contact: {config.DEVELOPER_TAG}",parse_mode="Markdown")
        return
    try:
        await loading.edit_text(f"🔍 *DKINT Search Started*\n\n📱 **Number:** `{number}`\n━━━━━━━━━━━━━━━━━\n✅ Digging on API v1.... ✔️\n✅ Digging on API v2.... ✔️\n✅ Fetching CSV data.... ✔️\n✅ Merging & deduplicating.... ✔️\n✅ Finalizing results.... ✔️\n━━━━━━━━━━━━━━━━━\n📤 *Preparing output...*",parse_mode="Markdown")
        await asyncio.sleep(0.4)
    except: pass
    formatted=fmt_out(result)
    try: await loading.delete()
    except: pass
    if len(formatted)>config.MAX_TELEGRAM_MSG_LENGTH:
        js=json.dumps(result,indent=2,ensure_ascii=False,default=str)
        sent=await message.reply_document(document=BufferedInputFile(file=js.encode("utf-8"),filename=f"dkint_{number}.json"),caption=f"📄 *Result file for* `{number}`\n⏱️ Auto-deletes in 60s",parse_mode="Markdown")
    else:
        sent=await message.reply(formatted,parse_mode="Markdown")
    await asyncio.sleep(config.AUTO_DELETE_SECONDS)
    try: await bot.delete_message(message.chat.id,sent.message_id)
    except: pass

@router.message(Command("admin"))
async def admin_panel(message,bot):
    uid=message.from_user.id
    if not db.is_owner(uid):
        await message.reply(f"❌ *Access Denied*\n\nOnly owner and admins can access this panel.",parse_mode="Markdown")
        return
    total,approved,pending,banned,searches=db.stats()
    pending_list=db.pending_list()
    txt=f"👑 *DKINT Admin Panel*\n━━━━━━━━━━━━━━━━━━━━━\n📊 **Statistics:**\n• Total Users: `{total}`\n• Approved: `{approved}`\n• Pending: `{pending}`\n• Banned: `{banned}`\n• Searches: `{searches}`\n━━━━━━━━━━━━━━━━━━━━━\n📋 **Pending Approvals:** {pending}"
    kb_buttons=[]
    if pending_list:
        for pu in pending_list[:10]:
            un=pu.get("username","") or pu.get("first_name","Unknown")
            kb_buttons.append([InlineKeyboardButton(text=f"✅ {un} ({pu['user_id']})",callback_data=f"approve:{pu['user_id']}")])
    kb_buttons.append([InlineKeyboardButton(text="🔄 Refresh",callback_data="admin_refresh"),InlineKeyboardButton(text="📋 List Users",callback_data="admin_list")])
    kb_buttons.append([InlineKeyboardButton(text="❌ Close",callback_data="admin_close")])
    kb=InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    await message.reply(txt,parse_mode="Markdown",reply_markup=kb)

@router.message(Command("help"))
async def help(message,bot):
    await message.reply(f"📖 *DKINT Bot Help*\n━━━━━━━━━━━━━━━━━━━━━\n**Commands:**\n`/start` - Register & get started\n`/num <number>` - Search phone number\n`/admin` - Admin panel (admin only)\n`/help` - Show this help\n\n**Usage:**\n1️⃣ Send `/start` to register\n2️⃣ Wait for admin approval\n3️⃣ Use `/num 9876543210` to search\n\n**Self-Approval:**\nUse `/approve <password>` in DM\n\n**Group Usage:**\nMust be approved via DM first\nResults auto-delete after 60s\n━━━━━━━━━━━━━━━━━━━━━\n👤 **Developer:** {config.DEVELOPER_TAG}\n🤖 **Bot:** @{bot.username}\n📦 **Version:** {config.BOT_VERSION}",parse_mode="Markdown")

@router.message(Command("approve"))
async def approve_pw(message,command,bot):
    u=message.from_user
    if db.is_banned(u.id):
        await message.reply(f"🚫 *Access Denied*\n\nYou have been banned.",parse_mode="Markdown")
        return
    if message.chat.type in (ChatType.GROUP,ChatType.SUPERGROUP):
        await message.reply("Please use `/approve` in DM with the bot.",parse_mode="Markdown")
        return
    pw=command.args.strip() if command.args else ""
    if not pw:
        await message.reply(f"❌ *Usage:* `/approve <password>`\nContact {config.DEVELOPER_TAG} for the password.",parse_mode="Markdown")
        return
    if pw!=config.APPROVAL_PASSWORD:
        await message.reply("❌ *Invalid Password*\nContact admin for the correct password.",parse_mode="Markdown")
        return
    db.add(u.id,u.username or "",u.first_name or "")
    db.approve(u.id,by="password")
    await message.reply(f"✅ *Approval Successful!* 🎉\n━━━━━━━━━━━━━━━━━━━━━\nWelcome, **{u.first_name}**!\n\nYou now have full access to DKINT Bot.\n\n🔍 Use `/num <phone_number>` to search\n📝 Example: `/num 9876543210`\n━━━━━━━━━━━━━━━━━━━━━\n🚀 *Enjoy!*",parse_mode="Markdown")
    logger.info(f"User {u.id} approved via password")
    targets=list(config.ADMIN_IDS)
    if config.OWNER_ID and config.OWNER_ID not in targets: targets.append(config.OWNER_ID)
    for aid in targets:
        try: await bot.send_message(aid,f"🔑 *Self-Approval via Password*\n\n👤 **User:** {u.full_name}\n📧 **Username:** @{u.username or 'N/A'}\n🆔 **ID:** `{u.id}`",parse_mode="Markdown")
        except: pass

@router.callback_query(F.data.startswith("approve:"))
async def cb_approve(callback,bot):
    uid=int(callback.data.split(":")[1])
    admin=callback.from_user
    if not db.is_owner(admin.id):
        await callback.answer("❌ Not authorized!",show_alert=True)
        return
    ud=db.approve(uid,by=f"admin:{admin.id}")
    if ud:
        try:
            await bot.send_message(uid,f"✅ *Approved!* 🎉\n━━━━━━━━━━━━━━━━━━━━━\nYour access request has been approved by **@{admin.username or 'Admin'}**!\n\nYou can now use DKINT Bot.\n\n🔍 Use `/num <phone_number>` to search\n📝 Example: `/num 9876543210`\n━━━━━━━━━━━━━━━━━━━━━\n🚀 *Welcome aboard!*",parse_mode="Markdown")
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
    if not db.is_owner(admin.id):
        await callback.answer("❌ Not authorized!",show_alert=True)
        return
    ud=db.decline(uid)
    if ud:
        try:
            await bot.send_message(uid,f"❌ *Access Declined*\n\nYour request to use DKINT Bot has been declined.\nContact {config.DEVELOPER_TAG} for assistance.",parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed notify {uid}: {e}")
        try: await callback.message.edit_text(f"{callback.message.text}\n\n━━━━━━━━━━━━━━━━━\n❌ *DECLINED* by @{admin.username or 'Admin'}",parse_mode="Markdown")
        except: pass
        await callback.answer("❌ Declined!",show_alert=False)
    else:
        await callback.answer("❌ User not found",show_alert=True)

@router.callback_query(F.data=="admin_refresh")
async def cb_admin_refresh(callback,bot):
    if not db.is_owner(callback.from_user.id):
        await callback.answer("❌ Not authorized!",show_alert=True)
        return
    total,approved,pending,banned,searches=db.stats()
    pending_list=db.pending_list()
    txt=f"👑 *DKINT Admin Panel*\n━━━━━━━━━━━━━━━━━━━━━\n📊 **Statistics:**\n• Total Users: `{total}`\n• Approved: `{approved}`\n• Pending: `{pending}`\n• Banned: `{banned}`\n• Searches: `{searches}`\n━━━━━━━━━━━━━━━━━━━━━\n📋 **Pending Approvals:** {pending}"
    kb_buttons=[]
    if pending_list:
        for pu in pending_list[:10]:
            un=pu.get("username","") or pu.get("first_name","Unknown")
            kb_buttons.append([InlineKeyboardButton(text=f"✅ {un} ({pu['user_id']})",callback_data=f"approve:{pu['user_id']}")])
    kb_buttons.append([InlineKeyboardButton(text="🔄 Refresh",callback_data="admin_refresh"),InlineKeyboardButton(text="📋 List Users",callback_data="admin_list")])
    kb_buttons.append([InlineKeyboardButton(text="❌ Close",callback_data="admin_close")])
    kb=InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    try: await callback.message.edit_text(txt,parse_mode="Markdown",reply_markup=kb)
    except: pass
    await callback.answer("🔄 Refreshed!",show_alert=False)

@router.callback_query(F.data=="admin_list")
async def cb_admin_list(callback,bot):
    if not db.is_owner(callback.from_user.id):
        await callback.answer("❌ Not authorized!",show_alert=True)
        return
    users=db.all_users()
    txt=f"📋 *All Users ({len(users)})*\n━━━━━━━━━━━━━━━━━━━━━\n"
    for u in users[-20:]:
        uid=u.get("user_id","?")
        name=u.get("first_name","?") or u.get("username","?")
        status=u.get("status","?")
        banned="🚫" if u.get("banned") else ""
        searches=u.get("searches",0)
        emoji={"approved":"✅","pending":"⏳","declined":"❌","banned":"🚫"}.get(status,"❓")
        txt+=f"{emoji} `{uid}` - {name} ({searches} searches) {banned}\n"
    txt+="\n━━━━━━━━━━━━━━━━━━━━━\nSelect a user to manage:"
    kb_buttons=[]
    for u in users[-10:]:
        uid=u.get("user_id",0)
        name=u.get("first_name","?") or u.get("username","?")
        kb_buttons.append([InlineKeyboardButton(text=f"{'🚫' if u.get('banned') else '👤'} {name}",callback_data=f"admin_user:{uid}")])
    kb_buttons.append([InlineKeyboardButton(text="🔙 Back",callback_data="admin_refresh")])
    kb=InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    try: await callback.message.edit_text(txt,parse_mode="Markdown",reply_markup=kb)
    except: pass
    await callback.answer("",show_alert=False)

@router.callback_query(F.data.startswith("admin_user:"))
async def cb_admin_user(callback,bot):
    uid=int(callback.data.split(":")[1])
    admin=callback.from_user
    if not db.is_owner(admin.id):
        await callback.answer("❌ Not authorized!",show_alert=True)
        return
    users=db.all_users()
    u=None
    for x in users:
        if x.get("user_id")==uid:
            u=x
            break
    if not u:
        await callback.answer("❌ User not found!",show_alert=True)
        return
    name=u.get("first_name","?") or u.get("username","?")
    uname=u.get("username","N/A")
    status=u.get("status","?")
    banned=u.get("banned",False)
    searches=u.get("searches",0)
    joined=u.get("joined_at","?")
    txt=f"👤 *User Details*\n━━━━━━━━━━━━━━━━━━━━━\n**Name:** {name}\n**Username:** @{uname}\n**ID:** `{uid}`\n**Status:** {'🚫 Banned' if banned else '✅ Approved' if status=='approved' else '⏳ Pending' if status=='pending' else '❌ '+status}\n**Searches:** {searches}\n**Joined:** {joined[:10]}\n━━━━━━━━━━━━━━━━━━━━━"
    kb_buttons=[]
    if banned:
        kb_buttons.append([InlineKeyboardButton(text="🔓 Unban User",callback_data=f"unban:{uid}")])
    else:
        kb_buttons.append([InlineKeyboardButton(text="🚫 Ban User",callback_data=f"ban:{uid}")])
    if status=="pending":
        kb_buttons.append([InlineKeyboardButton(text="✅ Approve",callback_data=f"approve:{uid}")])
    kb_buttons.append([InlineKeyboardButton(text="🔙 Back",callback_data="admin_list")])
    kb=InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    try: await callback.message.edit_text(txt,parse_mode="Markdown",reply_markup=kb)
    except: pass
    await callback.answer("",show_alert=False)

@router.callback_query(F.data.startswith("ban:"))
async def cb_ban(callback,bot):
    uid=int(callback.data.split(":")[1])
    admin=callback.from_user
    if not db.is_owner(admin.id):
        await callback.answer("❌ Not authorized!",show_alert=True)
        return
    if uid==admin.id:
        await callback.answer("❌ Cannot ban yourself!",show_alert=True)
        return
    ud=db.ban(uid)
    if ud:
        try:
            await bot.send_message(uid,f"🚫 *You have been banned*\n\nYou can no longer use DKINT Bot.\nContact {config.DEVELOPER_TAG} if you believe this is a mistake.",parse_mode="Markdown")
        except: pass
        await callback.answer("🚫 User banned!",show_alert=False)
        # Refresh the user view
        cb2=CallbackQuery(id=callback.id,from_user=callback.from_user,message=callback.message,chat_instance=callback.chat_instance,data=f"admin_user:{uid}")
        await cb_admin_user(callback,bot)
    else:
        await callback.answer("❌ User not found!",show_alert=True)

@router.callback_query(F.data.startswith("unban:"))
async def cb_unban(callback,bot):
    uid=int(callback.data.split(":")[1])
    admin=callback.from_user
    if not db.is_owner(admin.id):
        await callback.answer("❌ Not authorized!",show_alert=True)
        return
    ud=db.unban(uid)
    if ud:
        try:
            await bot.send_message(uid,f"🔓 *You have been unbanned*\n\nYour access to DKINT Bot has been restored.\nYou can now use the bot again.",parse_mode="Markdown")
        except: pass
        await callback.answer("🔓 User unbanned!",show_alert=False)
        cb2=CallbackQuery(id=callback.id,from_user=callback.from_user,message=callback.message,chat_instance=callback.chat_instance,data=f"admin_user:{uid}")
        await cb_admin_user(callback,bot)
    else:
        await callback.answer("❌ User not found!",show_alert=True)

@router.callback_query(F.data=="admin_close")
async def cb_admin_close(callback,bot):
    try: await callback.message.delete()
    except: pass
    await callback.answer("",show_alert=False)

async def main():
    Path(config.CSV_DIR).mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    Path(config.DATA_DIR).mkdir(exist_ok=True)
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
