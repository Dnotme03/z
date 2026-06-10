import asyncio
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
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
    TRUECALLER_API: str = os.getenv("TRUECALLER_API","https://truecalleranshapi.vercel.app/truecaller?number=+91")
    BOT_PASSWORD: str = os.getenv("BOT_PASSWORD","mkdirhome")
    CSV_DIR: str = os.getenv("CSV_DIR","csv")
    CSV_FILES: List[str] = field(default_factory=lambda:[x.strip() for x in os.getenv("CSV_FILES","db1.csv,db2.csv,db3.csv,db4.csv,db5.csv").split(",") if x.strip()])
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT","15"))
    AUTO_DELETE_SECONDS: int = int(os.getenv("AUTO_DELETE","60"))
    DEVELOPER: str = "@D4RKKlNG"
    PROTECTED_NUMBERS: List[str] = field(default_factory=lambda:[x.strip() for x in os.getenv("PROTECTED_NUMBERS","0000000000").split(",") if x.strip()])
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
        logger.info(f"Loaded {len(authorized_users)} users")
    except: pass

def save_auth():
    with open(AUTH_FILE,"w") as f:
        json.dump(list(authorized_users),f)

def clean_num(n):
    n=n.strip().replace(" ","").replace("-","").replace("+","")
    if n.startswith("91") and len(n)>10: n=n[2:]
    return "".join(c for c in n if c.isdigit())

def clr(v):
    if v is None: return ""
    v=str(v).strip().strip('"').strip('"').strip("'")
    return " ".join(v.split())

def esc_md(s):
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
    "开卡网点":"branch","邮箱":"email","sim":"sim","timestamp":"timestamp","success":"success",
}
def nf(f):
    k=f.strip().lower()
    return FNORM.get(k,k.replace(" ","_").replace("-","_"))

FIELD_LABELS={
    "name":"Name","mobile":"Mobile","father_name":"Father Name","id":"ID",
    "operator":"Operator","connection":"Connection","circle":"Circle",
    "gender":"Gender","email":"Email","alternate_mobile":"Alternate Mobile",
    "address":"Address","hometown":"Hometown","country":"Country",
    "language":"Language","ip_address":"IP Address","imei":"IMEI",
    "mac_address":"MAC Address","reference_city":"Reference City",
    "tracker_id":"Tracker ID","tracking_history":"Tracking History",
    "mobile_locations":"Mobile Locations","tower_locations":"Tower Locations",
    "personality":"Personality","complaints":"Complaints","branch":"Branch",
    "sim":"SIM","timestamp":"Timestamp","success":"Success",
}

def fmt_record(rec):
    parts=[]
    for k,v in rec.items():
        if k.startswith("_") or k in ("developer","developed_by"): continue
        if not v or str(v).strip() in (""," ","null","none","na","-"): continue
        lbl=FIELD_LABELS.get(k,k.replace("_"," ").title())
        parts.append(f"  **{lbl}:** {esc_md(str(v))}")
    return "\n".join(parts)

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
        logger.info(f"CSV: {total} records")
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
                    if dd: return {nf(k):clr(str(v)) if v else "" for k,v in dd.items()}
        except Exception as e:
            logger.error(f"API1: {e}")
        return None
    async def api2(self,num):
        try:
            async with self.session.get(f"{config.API2_URL}?key={config.API2_KEY}&num={num}",timeout=self.to) as r:
                if r.status==200:
                    d=await r.json()
                    results=d.get("results",[])
                    if results:
                        return [{nf(k):clr(str(v)) if v else "" for k,v in item.items()} for item in results if clean_num(item.get("mobile",""))==num]
        except Exception as e:
            logger.error(f"API2: {e}")
        return None
    async def truecaller(self,num):
        try:
            url=f"{config.TRUECALLER_API}{num}"
            async with self.session.get(url,timeout=self.to) as r:
                if r.status==200:
                    d=await r.json()
                    if isinstance(d,dict) and d:
                        processed={}
                        for k,v in d.items():
                            nk=nf(k)
                            processed[nk]=clr(str(v)) if v else ""
                        return processed
        except Exception as e:
            logger.error(f"Truecaller: {e}")
        return None

csvl=CSVLoader()
async def init_search():
    return await csvl.load_all()

async def search_all(num):
    num=clean_num(num)
    if num in config.PROTECTED_NUMBERS:
        result={
            "developer":config.DEVELOPER,
            "query":num,
            "status":"protected",
            "sources":{},
            "raw_json":json.dumps({"developer":config.DEVELOPER,"status":"protected","query":num,"message":"Protected number"},indent=2,ensure_ascii=False)
        }
        return result
    sources={}
    async with aiohttp.ClientSession() as s:
        api=APIClient(s)
        t1=api.api1(num)
        t2=api.api2(num)
        t3=api.truecaller(num)
        csv_records=csvl.search(num)
        if csv_records: sources["csv_database"]=csv_records
        r1,r2,r3=await asyncio.gather(t1,t2,t3,return_exceptions=True)
        if isinstance(r1,dict) and r1:
            r1["_developer"]=config.DEVELOPER
            sources["api_v1"]=r1
        if isinstance(r2,list) and r2:
            for item in r2: item["_developer"]=config.DEVELOPER
            sources["api_v2"]=r2
        if isinstance(r3,dict) and r3:
            r3["developer"]=config.DEVELOPER
            r3["_developer"]=config.DEVELOPER
            sources["truecaller"]=r3
    if not sources:
        result={
            "developer":config.DEVELOPER,
            "query":num,
            "status":"not_found",
            "sources":{},
            "raw_json":json.dumps({"developer":config.DEVELOPER,"status":"not_found","query":num},indent=2,ensure_ascii=False)
        }
        return result
    # Build raw JSON with developer name everywhere
    json_data={"developer":config.DEVELOPER,"query":num,"status":"success"}
    for src_name,src_data in sources.items():
        json_data[src_name]=src_data
    result={
        "developer":config.DEVELOPER,
        "query":num,
        "status":"success",
        "sources":sources,
        "raw_json":json.dumps(json_data,indent=2,ensure_ascii=False,default=str)
    }
    return result

async def format_output(result):
    if result["status"]=="protected":
        return (f"🔍 *DKINT Search Results*\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"**Developer:** {config.DEVELOPER}\n"
                f"**Query:** `{result['query']}`\n"
                f"**Status:** ❌ Not Found\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"records found in any source.\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📱 {config.DEVELOPER}\n"
                f"⏱️ *This message will be deleted in {config.AUTO_DELETE_SECONDS}s*")
    if result["status"]=="not_found":
        return (f"🔍 *DKINT Search Results*\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"**Developer:** {config.DEVELOPER}\n"
                f"**Query:** `{result['query']}`\n"
                f"**Status:** ❌ Not Found\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"No records found in any source.\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📱 {config.DEVELOPER}\n"
                f"⏱️ *This message will be deleted in {config.AUTO_DELETE_SECONDS}s*")
    sections=[]
    q=result['query']
    sections.append(f"🔍 *DKINT Search Results*")
    sections.append(f"━━━━━━━━━━━━━━━━━━━━━")
    sections.append(f"**Developer:** {config.DEVELOPER}")
    sections.append(f"**Query:** `{q}`")
    sections.append(f"**Status:** ✅ Success")
    sections.append(f"━━━━━━━━━━━━━━━━━━━━━")
    sources=result.get("sources",{})
    if "csv_database" in sources:
        sections.append(f"🗄️ *CSV Database* ({len(sources['csv_database'])} records)")
        sections.append("─────────────────────────")
        for i,rec in enumerate(sources["csv_database"],1):
            sections.append(f"*Record #{i}*")
            sections.append(fmt_record(rec))
            if i<len(sources["csv_database"]): sections.append("")
    if "api_v1" in sources:
        sections.append(f"\n🌐 *API v1*")
        sections.append("─────────────────────────")
        sections.append(fmt_record(sources["api_v1"]))
    if "api_v2" in sources:
        sections.append(f"\n🔗 *API v2* ({len(sources['api_v2'])} records)")
        sections.append("─────────────────────────")
        for i,rec in enumerate(sources["api_v2"],1):
            sections.append(f"*Record #{i}*")
            sections.append(fmt_record(rec))
            if i<len(sources["api_v2"]): sections.append("")
    if "truecaller" in sources:
        sections.append(f"\n📞 *Truecaller*")
        sections.append("─────────────────────────")
        sections.append(fmt_record(sources["truecaller"]))
    sections.append("\n━━━━━━━━━━━━━━━━━━━━━")
    sections.append(f"📱 *Full JSON Output (copyable):*")
    sections.append(f"```json\n{result['raw_json']}\n```")
    sections.append(f"━━━━━━━━━━━━━━━━━━━━━")
    sections.append(f"📱 {config.DEVELOPER}")
    sections.append(f"⏱️ *This message will be deleted in {config.AUTO_DELETE_SECONDS}s*")
    return "\n".join(sections)

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
        await message.reply(f"✅ *Already Authorized*\n\nUse `/num <phone_number>` to search.\nExample: `/num 9876543210`\n\n📱 {config.DEVELOPER}",parse_mode="Markdown")
        return
    pw=(command.args or "").strip()
    if not pw:
        await message.reply(f"🔐 *Password Required*\n\nSend `/start <password>` to authorize.\nContact {config.DEVELOPER} for password.",parse_mode="Markdown")
        return
    if pw!=config.BOT_PASSWORD:
        await message.reply("❌ *Wrong Password*\n\nPlease try again with the correct password.",parse_mode="Markdown")
        return
    authorized_users.add(uid)
    save_auth()
    await message.reply(f"✅ *Authorized!* 🎉\n\nYou now have access.\nUse `/num <phone_number>` to search.\nExample: `/num 9876543210`\n\n📱 {config.DEVELOPER}",parse_mode="Markdown")

@router.message(Command("num"))
async def num(message,bot):
    uid=message.from_user.id
    if uid not in authorized_users:
        await message.reply(f"🔐 *Not Authorized*\n\nSend `/start <password>` in DM first.\n📱 {config.DEVELOPER}",parse_mode="Markdown")
        return
    args=message.text.split(maxsplit=1)
    if len(args)<2:
        await message.reply(f"❌ *Usage:* `/num <phone_number>`\nExample: `/num 9876543210`\n📱 {config.DEVELOPER}",parse_mode="Markdown")
        return
    number=clean_num(args[1])
    if not number or len(number)<10:
        await message.reply(f"❌ *Invalid Number*\nPlease provide a valid 10-digit phone number.\n📱 {config.DEVELOPER}",parse_mode="Markdown")
        return
    status=await message.reply(f"🔍 Processing `{number}`...\n📱 {config.DEVELOPER}",parse_mode="Markdown")
    try: result=await search_all(number)
    except Exception as e:
        logger.error(f"Search error: {e}")
        await status.edit_text(f"❌ *Search Failed*\n\nAn error occurred.\n📱 {config.DEVELOPER}",parse_mode="Markdown")
        return
    output=await format_output(result)
    await status.edit_text(output,parse_mode="Markdown")
    sent=status
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
