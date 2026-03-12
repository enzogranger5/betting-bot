import asyncio
import logging
import re
import json
from datetime import datetime
from telegram import Bot
from telegram.constants import ParseMode
import aiohttp

TELEGRAM_TOKEN   = "8722101020:AAFPW-Pi7qtDLrOvb9YJu7MtxS27dFeM7lQ"
TELEGRAM_CHAT_ID = "457874923"
API_FOOTBALL_KEY = "64f10f170d2be27c32f0d348ae02904e"

CHECK_INTERVAL   = 60
DANGER_THRESHOLD = 65
ALERT_COOLDOWN   = 600

BASE_URL_AF = "https://v3.football.api-sports.io"
BASE_URL_US = "https://understat.com"
BASE_URL_SF = "https://www.sofascore.com/api/v1"
BASE_URL_WS = "https://www.whoscored.com"
BASE_URL_FB = "https://fbref.com/en"
BASE_URL_FS = "https://www.flashscore.com"

UNDERSTAT_LEAGUES = {39:"EPL",140:"La_liga",135:"Serie_A",78:"Bundesliga",61:"Ligue_1",235:"RFPL"}
FBREF_LEAGUES = {39:"Premier-League",140:"La-Liga",135:"Serie-A",78:"Bundesliga",61:"Ligue-1",2:"Champions-League"}
LEAGUE_PROFILES = {
    39:{"name":"Premier League","avg_goals":2.8,"2nd_half_pct":58},
    140:{"name":"La Liga","avg_goals":2.6,"2nd_half_pct":55},
    135:{"name":"Serie A","avg_goals":2.5,"2nd_half_pct":56},
    78:{"name":"Bundesliga","avg_goals":3.1,"2nd_half_pct":57},
    61:{"name":"Ligue 1","avg_goals":2.5,"2nd_half_pct":55},
    2:{"name":"Champions League","avg_goals":2.9,"2nd_half_pct":56},
    3:{"name":"Europa League","avg_goals":2.7,"2nd_half_pct":55},
}
LEAGUES = {
    2:"Champions League 🌟",3:"Europa League 🌍",848:"Conference League 🏅",
    39:"Premier League 🏴󠁧󠁢󠁥󠁮󠁧󠁿",40:"Championship 🏴󠁧󠁢󠁥󠁮󠁧󠁿",140:"La Liga 🇪🇸",141:"La Liga 2 🇪🇸",
    135:"Serie A 🇮🇹",136:"Serie B 🇮🇹",78:"Bundesliga 🇩🇪",79:"2. Bundesliga 🇩🇪",
    61:"Ligue 1 🇫🇷",62:"Ligue 2 🇫🇷",88:"Eredivisie 🇳🇱",94:"Primeira Liga 🇵🇹",
    144:"Jupiler Pro League 🇧🇪",119:"Superliga 🇩🇰",103:"Eliteserien 🇳🇴",
    113:"Allsvenskan 🇸🇪",106:"Ekstraklasa 🇵🇱",235:"Premier League 🇷🇺",
    98:"Super Lig 🇹🇷",218:"Super League 🇬🇷",207:"Premiership 🏴󠁧󠁢󠁳󠁣󠁴󠁿",
    71:"Brasileirao A 🇧🇷",72:"Brasileirao B 🇧🇷",128:"Liga Profesional 🇦🇷",
    262:"Liga MX 🇲🇽",239:"MLS 🇺🇸",265:"Primera División 🇨🇱",266:"Primera División 🇨🇴",
    307:"Saudi Pro League 🇸🇦",323:"UAE Pro League 🇦🇪",318:"Qatar Stars 🇶🇦",
    154:"Chinese Super League 🇨🇳",45:"FA Cup 🏴󠁧󠁢󠁥󠁮󠁧󠁿",143:"Copa del Rey 🇪🇸",
    137:"Coppa Italia 🇮🇹",65:"Coupe de France 🇫🇷",81:"DFB Pokal 🇩🇪",
}

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s",level=logging.INFO,
    handlers=[logging.FileHandler("goal_bot.log"),logging.StreamHandler()])
log = logging.getLogger(__name__)
last_alert_time: dict = {}

HEADERS_BROWSER = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language":"fr-FR,fr;q=0.9,en;q=0.8",
}

def get_headers_af():
    return {"x-apisports-key":API_FOOTBALL_KEY}

async def fetch_live_fixtures(session):
    try:
        async with session.get(f"{BASE_URL_AF}/fixtures",headers=get_headers_af(),params={"live":"all"},timeout=10) as r:
            if r.status==200: return (await r.json()).get("response",[])
            return []
    except Exception as e:
        log.error(f"Erreur fixtures: {e}"); return []

async def fetch_live_stats_af(session,fixture_id):
    try:
        async with session.get(f"{BASE_URL_AF}/fixtures/statistics",headers=get_headers_af(),params={"fixture":fixture_id},timeout=10) as r:
            if r.status==200: return parse_af_stats((await r.json()).get("response",[]))
            return {}
    except Exception as e:
        log.error(f"Erreur stats AF: {e}"); return {}

async def fetch_team_history_af(session,team_id,league_id,season):
    try:
        async with session.get(f"{BASE_URL_AF}/fixtures",headers=get_headers_af(),params={"team":team_id,"league":league_id,"season":season,"last":5},timeout=10) as r:
            if r.status!=200: return None
            fixtures=(await r.json()).get("response",[])
        if not fixtures: return None
        goals=[(f["goals"]["home"] or 0)+(f["goals"]["away"] or 0) for f in fixtures]
        return round(sum(goals)/len(goals),1)
    except Exception as e:
        log.error(f"Erreur historique: {e}"); return None

def parse_af_stats(stats_response):
    result={"home":{"shots_total":0,"shots_on":0,"possession":50,"corners":0,"fouls":0},"away":{"shots_total":0,"shots_on":0,"possession":50,"corners":0,"fouls":0}}
    mapping={"Total Shots":"shots_total","Shots on Goal":"shots_on","Ball Possession":"possession","Corner Kicks":"corners","Fouls":"fouls"}
    for i,team_data in enumerate(stats_response):
        side="home" if i==0 else "away"
        for stat in team_data.get("statistics",[]):
            key=mapping.get(stat["type"])
            if key:
                val=stat["value"] or 0
                if isinstance(val,str) and "%" in val: val=int(val.replace("%",""))
                result[side][key]=int(val)
    return result

async def fetch_xg_understat(session,league_id,home_team,away_team):
    ln=UNDERSTAT_LEAGUES.get(league_id)
    if not ln: return None
    try:
        season=datetime.now().year if datetime.now().month>=7 else datetime.now().year-1
        async with session.get(f"{BASE_URL_US}/league/{ln}/{season}",headers=HEADERS_BROWSER,timeout=15) as r:
            if r.status!=200: return None
            html=await r.text()
        m=re.search(r"datesData\s*=\s*JSON\.parse\('(.+?)'\)",html)
        if not m: return None
        matches=json.loads(m.group(1).encode().decode("unicode_escape"))
        hl=home_team.lower(); al=away_team.lower()
        for match in matches:
            h=match.get("h",{}).get("title","").lower()
            a=match.get("a",{}).get("title","").lower()
            if (hl[:4] in h or h[:4] in hl) and (al[:4] in a or a[:4] in al):
                xg_h=float(match.get("xG",{}).get("h",0) or 0)
                xg_a=float(match.get("xG",{}).get("a",0) or 0)
                return {"home":round(xg_h,2),"away":round(xg_a,2),"total":round(xg_h+xg_a,2)}
        return None
    except Exception as e:
        log.error(f"Erreur Understat: {e}"); return None

async def fetch_sofascore_data(session,home_team,away_team):
    try:
        headers={**HEADERS_BROWSER,"Accept":"application/json","Referer":"https://www.sofascore.com"}
        async with session.get(f"{BASE_URL_SF}/search/multi/?q={home_team.replace(' ','+')}",headers=headers,timeout=10) as r:
            if r.status!=200: return None
            data=await r.json()
        events=data.get("events",{}).get("results",[])
        event_id=None
        hl=home_team.lower()[:5]
        for event in events:
            h=event.get("homeTeam",{}).get("name","").lower()
            status=event.get("status",{}).get("type","")
            if hl in h and status in ["inprogress","halftime"]:
                event_id=event.get("id"); break
        if not event_id: return None
        async with session.get(f"{BASE_URL_SF}/event/{event_id}/statistics",headers=headers,timeout=10) as r:
            if r.status!=200: return None
            stats_data=await r.json()
        result={"home":{"touches_box":0,"dangerous_attacks":0,"big_chances":0},"away":{"touches_box":0,"dangerous_attacks":0,"big_chances":0}}
        for group in stats_data.get("statistics",[]):
            for stat in group.get("statisticsItems",[]):
                name=stat.get("name","").lower()
                hv=int(stat.get("home",0) or 0); av=int(stat.get("away",0) or 0)
                if "touches" in name and "box" in name:
                    result["home"]["touches_box"]=hv; result["away"]["touches_box"]=av
                elif "dangerous" in name:
                    result["home"]["dangerous_attacks"]=hv; result["away"]["dangerous_attacks"]=av
                elif "big chance" in name:
                    result["home"]["big_chances"]=hv; result["away"]["big_chances"]=av
        return result
    except Exception as e:
        log.error(f"Erreur SofaScore: {e}"); return None

async def fetch_whoscored_data(session,home_team,away_team):
    try:
        async with session.get(f"{BASE_URL_WS}/Matches/Live",headers=HEADERS_BROWSER,timeout=10) as r:
            if r.status!=200: return None
            html=await r.text()
        ws_match=re.search(r'require\.config\.params\["args"\]\s*=\s*(\{.+?\});',html,re.DOTALL)
        if not ws_match: return None
        ws_data=json.loads(ws_match.group(1))
        match_data=ws_data.get("matchCentreData",{})
        def avg_rating(players):
            ratings=[float(p.get("stats",{}).get("ratings",{}).get("0",0)) for p in players if p.get("stats")]
            ratings=[r for r in ratings if r>0]
            return round(sum(ratings)/len(ratings),2) if ratings else 0
        home_r=avg_rating(match_data.get("home",{}).get("players",[]))
        away_r=avg_rating(match_data.get("away",{}).get("players",[]))
        return {"home_rating":home_r,"away_rating":away_r,"home_form":match_data.get("home",{}).get("form",""),"away_form":match_data.get("away",{}).get("form","")}
    except Exception as e:
        log.error(f"Erreur WhoScored: {e}"); return None

async def fetch_fbref_pressing(session,league_id,home_team,away_team):
    ln=FBREF_LEAGUES.get(league_id)
    if not ln: return None
    try:
        season=datetime.now().year if datetime.now().month>=7 else datetime.now().year-1
        url=f"{BASE_URL_FB}/comps/9/{season}-{season+1}/pressing/{season}-{season+1}-{ln}-Stats"
        async with session.get(url,headers=HEADERS_BROWSER,timeout=15) as r:
            if r.status!=200: return None
            html=await r.text()
        pressing_data={}
        for row in re.findall(r'<tr[^>]*>(.+?)</tr>',html,re.DOTALL):
            tm=re.search(r'data-stat="team"[^>]*>(.+?)</td>',row)
            pm=re.search(r'data-stat="ppda_att"[^>]*>(.+?)</td>',row)
            if tm and pm:
                team=re.sub(r'<[^>]+>','',tm.group(1)).strip()
                try: pressing_data[team.lower()]=float(re.sub(r'<[^>]+>','',pm.group(1)).strip())
                except: pass
        hp=ap=None
        for team,ppda in pressing_data.items():
            if home_team.lower()[:4] in team or team[:4] in home_team.lower(): hp=ppda
            if away_team.lower()[:4] in team or team[:4] in away_team.lower(): ap=ppda
        if hp is None and ap is None: return None
        def lbl(p):
            if p is None: return "N/A"
            if p<8: return "🔥 Intense"
            if p<11: return "💪 Bon"
            if p<14: return "➡️ Moyen"
            return "⚠️ Faible"
        return {"home_ppda":hp,"away_ppda":ap,"home_pressing":lbl(hp),"away_pressing":lbl(ap)}
    except Exception as e:
        log.error(f"Erreur FBref: {e}"); return None

async def fetch_flashscore_momentum(session,home_team,away_team):
    try:
        async with session.get(f"{BASE_URL_FS}/search/?q={home_team.replace(' ','+')}",headers=HEADERS_BROWSER,timeout=10) as r:
            if r.status!=200: return None
            html=await r.text()
        mid=re.search(rf'{re.escape(home_team[:5])}.+?href="/match/([a-zA-Z0-9]+)',html,re.IGNORECASE)
        if not mid: return None
        async with session.get(f"{BASE_URL_FS}/match/{mid.group(1)}/#/match-summary",headers=HEADERS_BROWSER,timeout=10) as r:
            if r.status!=200: return None
            html=await r.text()
        ha=len(re.findall(r'home.*?(?:shot|corner|attack)',html[:5000],re.IGNORECASE))
        aa=len(re.findall(r'away.*?(?:shot|corner|attack)',html[:5000],re.IGNORECASE))
        if ha>aa*1.5: mom=f"🏠 Domicile domine ({ha} vs {aa})"
        elif aa>ha*1.5: mom=f"✈️ Extérieur domine ({aa} vs {ha})"
        else: mom=f"⚖️ Équilibré ({ha} vs {aa})"
        return {"momentum":mom,"home_actions":ha,"away_actions":aa}
    except Exception as e:
        log.error(f"Erreur FlashScore: {e}"); return None

def calculate_danger_score(fixture,af_stats,sf_data,xg_data,ws_data,fb_data,fs_data,avg_h,avg_a):
    score=0; signals=[]
    elapsed=fixture["fixture"]["status"]["elapsed"] or 1
    gh=fixture["goals"]["home"] or 0; ga=fixture["goals"]["away"] or 0
    haf=af_stats.get("home",{}); aaf=af_stats.get("away",{})
    if xg_data:
        t=xg_data["total"]; mx=max(xg_data["home"],xg_data["away"])
        if t>=3.0: score+=18; signals.append(f"🎯 xG très élevé : {t}")
        elif t>=2.0: score+=14; signals.append(f"🎯 xG élevé : {t}")
        elif t>=1.2: score+=9; signals.append(f"🎯 xG : {t}")
        elif t>=0.7: score+=5
        if mx>=2.0: score=min(100,score+3); signals.append(f"⚡ Domination xG : {xg_data['home']} vs {xg_data['away']}")
    son=haf.get("shots_on",0)+aaf.get("shots_on",0); st=haf.get("shots_total",0)+aaf.get("shots_total",0)
    spm=son/elapsed
    if spm>=0.15: score+=13; signals.append(f"🔫 Rythme intense : {son}/{st}")
    elif son>=8: score+=10; signals.append(f"🔫 Tirs : {son} cadrés")
    elif son>=5: score+=7; signals.append(f"🔫 Tirs cadrés : {son}")
    elif son>=3: score+=3
    if sf_data:
        dh=sf_data["home"].get("dangerous_attacks",0); da=sf_data["away"].get("dangerous_attacks",0)
        bc=sf_data["home"].get("big_chances",0)+sf_data["away"].get("big_chances",0)
        tb=sf_data["home"].get("touches_box",0)+sf_data["away"].get("touches_box",0)
        dan=dh+da
        if dan>=30: score+=12; signals.append(f"🔥 Att. dang. : {dan}")
        elif dan>=20: score+=8; signals.append(f"🔥 Att. dang. : {dan}")
        elif dan>=10: score+=4
        if bc>=3: score=min(100,score+5); signals.append(f"💥 Grosses occasions : {bc}")
        if tb>=25: score+=10; signals.append(f"📦 Touches surface : {tb}")
        elif tb>=15: score+=7; signals.append(f"📦 Touches surface : {tb}")
        elif tb>=8: score+=4
    if fb_data:
        hp=fb_data.get("home_ppda"); ap=fb_data.get("away_ppda")
        if hp and hp<8: score+=7; signals.append(f"⚡ Pressing DOM : PPDA {hp}")
        elif hp and hp<11: score+=4
        if ap and ap<8: score+=5; signals.append(f"⚡ Pressing EXT : PPDA {ap}")
        elif ap and ap<11: score+=3
    if ws_data:
        hr=ws_data.get("home_rating",0); ar=ws_data.get("away_rating",0); mx=max(hr,ar)
        if mx>=7.5: score+=8; signals.append(f"⭐ Ratings : {hr} / {ar}")
        elif mx>=7.0: score+=5; signals.append(f"⭐ Ratings : {hr} / {ar}")
        elif mx>=6.5: score+=3
        hf=ws_data.get("home_form",""); af2=ws_data.get("away_form","")
        if hf.count("W")>=3 or af2.count("W")>=3: score=min(100,score+2); signals.append(f"📈 Forme : {hf}/{af2}")
    if fs_data:
        ta=fs_data.get("home_actions",0)+fs_data.get("away_actions",0)
        if ta>=20: score+=8; signals.append(f"🌊 {fs_data['momentum']}")
        elif ta>=12: score+=5; signals.append(f"🌊 {fs_data['momentum']}")
        elif ta>=6: score+=2
    ph=haf.get("possession",50); pa=aaf.get("possession",50); mxp=max(ph,pa)
    if mxp>=70: score+=7; dom="DOM" if ph>pa else "EXT"; signals.append(f"⚡ Possession : {mxp}% ({dom})")
    elif mxp>=60: score+=3
    corners=haf.get("corners",0)+aaf.get("corners",0)
    if corners>=10: score+=7; signals.append(f"📐 Corners : {corners}")
    elif corners>=6: score+=4; signals.append(f"📐 Corners : {corners}")
    elif corners>=3: score+=2
    diff=abs(gh-ga)
    if diff==0: score+=5; signals.append(f"⚖️ Nul ({gh}-{ga})")
    elif diff==1: score+=3; signals.append(f"⚖️ Serré ({gh}-{ga})")
    return min(score,100), signals

def entry_window(elapsed,ds):
    es=elapsed+2; ee=elapsed+5
    we=elapsed+15 if ds>=85 else elapsed+20 if ds>=75 else elapsed+25
    return f"{es}-{ee}'",f"{es}-{we}'"

def probs(ds,elapsed):
    b=ds/100
    return {"15min":min(85,int(b*65)),"30min":min(92,int(b*78)),"total":min(96,int(b*90+((90-elapsed)/90)*15))}

def odds(ds):
    if ds>=85: return "1.40-1.65"
    elif ds>=75: return "1.55-1.80"
    elif ds>=65: return "1.70-2.10"
    return "1.85-2.30"

def dlevel(s):
    if s>=88: return "🔴 EXTRÊME"
    elif s>=78: return "🟠 TRÈS ÉLEVÉ"
    elif s>=65: return "🟡 ÉLEVÉ"
    return "🟢 MODÉRÉ"

def ctx(lid,elapsed,gh,ga):
    p=LEAGUE_PROFILES.get(lid)
    if not p: return None
    parts=[]
    if elapsed>45: parts.append(f"{p['2nd_half_pct']}% des buts en 2ème MT en {p['name']}")
    if gh==ga==0 and elapsed>30: parts.append(f"0-0 après {elapsed}' → pression max")
    if abs(gh-ga)==1: parts.append("Score serré → intensification imminente")
    return " | ".join(parts) if parts else None

def should_alert(fid,score):
    if score<DANGER_THRESHOLD: return False
    return (datetime.now().timestamp()-last_alert_time.get(fid,0))>=ALERT_COOLDOWN

def format_alert(fixture,af_stats,sf_data,xg_data,ws_data,fb_data,fs_data,ds,signals,avg_h,avg_a):
    now=datetime.now().strftime("%H:%M:%S")
    lid=fixture["league"]["id"]; elapsed=fixture["fixture"]["status"]["elapsed"] or 0
    home=fixture["teams"]["home"]["name"]; away=fixture["teams"]["away"]["name"]
    gh=fixture["goals"]["home"] or 0; ga=fixture["goals"]["away"] or 0
    league=LEAGUES.get(lid,fixture["league"]["name"])
    status=fixture["fixture"]["status"]["short"]
    label="MT" if status=="HT" else f"{elapsed}'"
    haf=af_stats.get("home",{}); aaf=af_stats.get("away",{})
    entry,window=entry_window(elapsed,ds); p=probs(ds,elapsed); c=ctx(lid,elapsed,gh,ga)
    xg_line=f"xG {xg_data['home']}—{xg_data['away']} = *{xg_data['total']}*" if xg_data else "xG : N/A"
    sf_line=""
    if sf_data:
        dh=sf_data["home"].get("dangerous_attacks",0); da=sf_data["away"].get("dangerous_attacks",0)
        th=sf_data["home"].get("touches_box",0); ta=sf_data["away"].get("touches_box",0)
        bc=sf_data["home"].get("big_chances",0)+sf_data["away"].get("big_chances",0)
        sf_line=f"\n🔥 Att.dang: {dh}—{da} | 📦 Surface: {th}—{ta} | 💥 Occ: {bc}"
    fb_line=f"\n⚡ PPDA: {fb_data.get('home_ppda','N/A')} ({fb_data.get('home_pressing','')})—{fb_data.get('away_ppda','N/A')} ({fb_data.get('away_pressing','')})" if fb_data else ""
    ws_line=f"\n⭐ Ratings: {ws_data.get('home_rating',0)}—{ws_data.get('away_rating',0)} | Forme: {ws_data.get('home_form','')} / {ws_data.get('away_form','')}" if ws_data else ""
    fs_line=f"\n🌊 {fs_data['momentum']}" if fs_data else ""
    signals_text="\n".join([f"  • {s}" for s in signals]) if signals else "  • Analyse..."
    ctx_block=f"\n\n💡 _{c}_" if c else ""
    hist=f"{avg_h} / {avg_a} buts/match" if avg_h and avg_a else "N/A"
    return f"""⚡ *BUT IMMINENT DÉTECTÉ* ⚡

🏆 {league}
⚽ *{home} vs {away}*
📊 `{gh}-{ga}` | ⏱️ {label}

━━━━━━━━━━━━━━━━━━
🔥 *SCORE : {ds}/100* — {dlevel(ds)}

━━━━━━━━━━━━━━━━━━
📈 *SIGNAUX :*
{signals_text}{ctx_block}

━━━━━━━━━━━━━━━━━━
📊 *STATS (6 sources)*
🎯 {xg_line}
🔫 Tirs: {haf.get('shots_on',0)}/{haf.get('shots_total',0)}—{aaf.get('shots_on',0)}/{aaf.get('shots_total',0)}
⚡ Poss: {haf.get('possession',50)}%—{aaf.get('possession',50)}%
📐 Corners: {haf.get('corners',0)}—{aaf.get('corners',0)}{sf_line}{fb_line}{ws_line}{fs_line}

📊 Historique: {hist}

━━━━━━━━━━━━━━━━━━
🎯 Entrée: {entry} | ⚡ Cote: {odds(ds)}
⏱️ Fenêtre: {window}
📈 15min→*{p['15min']}%* | 30min→*{p['30min']}%* | FT→*{p['total']}%*

💰 *PARI:* `Prochain but — entrée {entry}`
⚠️ _Responsable_ | 🕐 _{now}_""".strip()

async def send_message(bot,text):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID,text=text,parse_mode=ParseMode.MARKDOWN)
        log.info("✅ Alerte envoyée")
    except Exception as e:
        log.error(f"Erreur Telegram: {e}")

async def monitor_loop(bot):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                log.info(f"🔍 Scan... {datetime.now().strftime('%H:%M:%S')}")
                fixtures=await fetch_live_fixtures(session)
                targets=[f for f in fixtures if f["league"]["id"] in LEAGUES]
                log.info(f"📡 {len(targets)} matchs")
                for fixture in targets:
                    fid=fixture["fixture"]["id"]; status=fixture["fixture"]["status"]["short"]
                    elapsed=fixture["fixture"]["status"]["elapsed"] or 0
                    lid=fixture["league"]["id"]; season=fixture["league"]["season"]
                    home_id=fixture["teams"]["home"]["id"]; away_id=fixture["teams"]["away"]["id"]
                    home_name=fixture["teams"]["home"]["name"]; away_name=fixture["teams"]["away"]["name"]
                    if status not in ["1H","2H","HT"]: continue
                    af_stats,xg_data,avg_h,avg_a,sf_data,fs_data=await asyncio.gather(
                        fetch_live_stats_af(session,fid),
                        fetch_xg_understat(session,lid,home_name,away_name),
                        fetch_team_history_af(session,home_id,lid,season),
                        fetch_team_history_af(session,away_id,lid,season),
                        fetch_sofascore_data(session,home_name,away_name),
                        fetch_flashscore_momentum(session,home_name,away_name),
                    )
                    ws_data=await fetch_whoscored_data(session,home_name,away_name)
                    fb_data=await fetch_fbref_pressing(session,lid,home_name,away_name)
                    if not af_stats: continue
                    ds,signals=calculate_danger_score(fixture,af_stats,sf_data,xg_data,ws_data,fb_data,fs_data,avg_h,avg_a)
                    log.info(f"📊 {home_name} vs {away_name} | {elapsed}' | {ds}/100")
                    if should_alert(fid,ds):
                        log.info(f"🚨 ALERTE : {home_name} vs {away_name} — {ds}/100")
                        await send_message(bot,format_alert(fixture,af_stats,sf_data,xg_data,ws_data,fb_data,fs_data,ds,signals,avg_h,avg_a))
                        last_alert_time[fid]=datetime.now().timestamp()
                    await asyncio.sleep(1)
            except Exception as e:
                log.error(f"Erreur boucle: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

async def main():
    bot=Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID,
        text="🤖 *BOT PRÉDICTION BUT v2.0* ✅\n\n🧠 *6 sources :*\n  • API-Football\n  • Understat (xG)\n  • SofaScore\n  • WhoScored\n  • FBref (PPDA)\n  • FlashScore\n\n🎯 Seuil : 65/100\n🏆 "+str(len(LEAGUES))+" ligues",
        parse_mode=ParseMode.MARKDOWN)
    await monitor_loop(bot)

if __name__=="__main__":
    asyncio.run(main())
