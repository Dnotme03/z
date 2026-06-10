import asyncio
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from dotenv import load_dotenv
import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
load_dotenv()
@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN","8951914420:AAHBHvW3e30GKZ9cNh7tksfRbVhIeuVCrTU")
    API1_URL: str = os.getenv("API1_URL","https://apis.nuviac.io/api/phone")
    API2_URL: str = os.getenv("API2_URL","https://nv6.ek4nsh.in/api/proxy")
    API2_KEY: str = os.getenv("API2_KEY","nightroot")
    BOT_PASSWORD: str = os.getenv("BOT_PASSWORD","mkdirhome")
    CSV_DIR: str = os.getenv("CSV_DIR","csv")
    CSV_FILES: List[str] = field(default_factory=lambda:[x.strip() for x in os.getenv("CSV_FILES","db1.csv,db2.csv,db3.csv,db4.csv,db5.csv").split(",") if x.strip()])
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT","15"))
    AUTO_DELETE_SECONDS: int = int(os.getenv("AUTO_DELETE","60"))
    DEVELOPER_TAG: str = os.getenv("DEVELOPER_TAG","@D4RKKlNG")
    PROTECTED_NUMBERS: List[str] = field(default_factory=lambda:[x.strip() for x in os.getenv("PROTECTED_NUMBERS","9809098789,0000000000").split(",") if x.strip()])
    def __post_init__(self):
        if not self.BOT_TOKEN: raise ValueError("BOT_TOKEN required!")
config=Config()
logging.basicConfig(level=logging.INFO,format="[%(asctime)s] %(levelname)s | %(message)s")
logger=logging.getLogger("bot")

AUTH_FILE=Path("authorized.json")
authorized_users=set()
if AUTH_FILE.exists():
    try:
        with open(AUTH_FILE) as f:
            authorized_users=set(json.load(f))
        logger.info(f"Loaded {len(authorized_users)} authorized users")
    except: pass

def save_auth():
    with open(AUTH_FILE,"w") as f:
        json.dump(list(authorized_users),f)

def clean_num(n):
    n=n.strip().replace(" ","").replace("-","").replace("+","")
    if n.startswith("91") and len(n)>10: n=n[2:]
    return "".join(c for c in n if c.isdigit())

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

def clr(v):
    v=str(v).strip().strip('"').strip('"').strip("'")
    return " ".join(v.split())

class CSVLoader:
    def __init__(self):
        self.datasets=[]
    async def load_all(self):
        total=0
        d=Path(config.CSV_DIR)
        if not d.exists(): return 0
        for fn in config.CSV_FILES:
            fp=d/fn
            if not fp.exists(): continue
            recs=await self._load(fp)
            self.datasets.extend(recs)
            total+=len(recs)
        logger.info(f"CSV: {total} records loaded")
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
            logger.error(f"API1 error: {e}")
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
            logger.error(f"API2 error: {e}")
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
            if len(unique)==1: out[key]=unique[0]
            elif len(unique)>1:
                cnames=[v for v in unique if len(v)>3 and not any(c.isdigit() for c in v.replace(" ",""))]
                if cnames: unique=cnames
                out[key]=unique
        return out,len(seen)

csvl=CSVLoader()
async def init_search():
    return await csvl.load_all()

async def search_all(num):
    num=clean_num(num)
    if num in config.PROTECTED_NUMBERS:
        return {"status":"protected","query":num,"data":{},"developed_by":config.DEVELOPER_TAG}
    all_recs=[]
    async with aiohttp.ClientSession() as s:
        api=APIClient(s)
        t1=api.api1(num)
        t2=api.api2(num)
        for r in csvl.search(num):
            r["_source"]="csv"
            all_recs.append(r)
        r1,r2=await asyncio.gather(t1,t2,return_exceptions=True)
        if isinstance(r1,dict) and r1: all_recs.append(r1)
        if isinstance(r2,list) and r2: all_recs.extend(r2)
    if not all_recs:
        return {"status":"not_found","query":num,"data":{},"developed_by":config.DEVELOPER_TAG}
    md,sc=Merger.merge(all_recs)
    for key in list(md.keys()):
        val=md[key]
        if isinstance(val,list):
            seen_set=set()
            clean_list=[]
            for item in val:
                norm=item.lower().replace(" ","").replace(".","").replace("-","").replace("(","").replace(")","").replace("!","")
                if norm not in seen_set and len(norm)>2:
                    seen_set.add(norm)
                    clean_list.append(item)
            if len(clean_list)==1: md[key]=clean_list[0]
            elif len(clean_list)>1: md[key]=clean_list[:5]
            else: del md[key]
    return {"status":"success","query":num,"data":md,"developed_by":config.DEVELOPER_TAG}

router=Router()
@router.startup()
async def startup(bot):
    logger.info("DKINT BOT STARTING")
    t=await init_search()
    logger.info(f"Ready: {t} records")
    await bot.set_my_commands([
        {"command":"start","description":"Start bot & enter password"},
        {"command":"num","description":"Search: /num <number>"}
    ])
    logger.info("DKINT BOT READY")

@router.message(Command("start"))
async def start(message,command:CommandObject):
    uid=message.from_user.id
    if uid in authorized_users:
        await message.reply("✅ *Already Authorized*\n\nUse `/num <phone_number>` to search.\nExample: `/num 9876543210`",parse_mode="Markdown")
        return
    pw=(command.args or "").strip()
    if not pw:
        await message.reply(f"🔐 *Password Required*\n\nSend `/start <password>` to authorize.\nContact developer for password.",parse_mode="Markdown")
        return
    if pw!=config.BOT_PASSWORD:
        await message.reply("❌ *Wrong Password*\n\nPlease try again with the correct password.",parse_mode="Markdown")
        return
    authorized_users.add(uid)
    save_auth()
    await message.reply("✅ *Authorized!* 🎉\n\nYou now have access.\nUse `/num <phone_number>` to search.\nExample: `/num 9876543210`",parse_mode="Markdown")

@router.message(Command("num"))
async def num(message,bot):
    uid=message.from_user.id
    if uid not in authorized_users:
        await message.reply("🔐 *Not Authorized*\n\nSend `/start <password>` in DM first.",parse_mode="Markdown")
        return
    args=message.text.split(maxsplit=1)
    if len(args)<2:
        await message.reply("❌ *Usage:* `/num <phone_number>`\nExample: `/num 9876543210`",parse_mode="Markdown")
        return
    number=clean_num(args[1])
    if not number or len(number)<10:
        await message.reply("❌ *Invalid Number*\nPlease provide a valid 10-digit phone number.",parse_mode="Markdown")
        return
    status=await message.reply(f"🔍 Processing {number}...")
    try: result=await search_all(number)
    except Exception as e:
        logger.error(f"Search error: {e}")
        await status.edit_text(f"❌ *Search Failed*\n\nAn error occurred.\nContact {config.DEVELOPER_TAG}")
        return
    json_str=json.dumps(result,indent=2,ensure_ascii=False,default=str)
    output=f"```json\n{json_str}\n```"
    await status.delete()
    sent=await message.reply(output,parse_mode="Markdown")
    await asyncio.sleep(config.AUTO_DELETE_SECONDS)
    try: await bot.delete_message(message.chat.id,sent.message_id)
    except: pass

async def main():
    Path(config.CSV_DIR).mkdir(exist_ok=True)
    if not config.BOT_TOKEN:
        logger.error("BOT_TOKEN not set!")
        sys.exit(1)
    bot=Bot(token=config.BOT_TOKEN,default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    dp=Dispatcher()
    dp.include_router(router)
    try: await dp.start_polling(bot)
    except (KeyboardInterrupt,SystemExit): logger.info("Bot stopped")
    finally: await bot.session.close()

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
