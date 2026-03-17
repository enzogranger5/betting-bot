"""
BOT PREDICTION BUT IMMINENT - VERSION 3.7
63 ligues — SofaScore (stats + momentum) + API-Football
API-Football = base fiable (seuil 45 atteignable seul)
SofaScore = bonus points sur les données live enrichies
"""

import asyncio
import logging
import re
import json
import random
from datetime import datetime
from telegram import Bot
from telegram.constants import ParseMode
import aiohttp

TELEGRAM_TOKEN   = "8722101020:AAFPW-Pi7qtDLrOvb9YJu7MtxS27dFeM7lQ"
TELEGRAM_CHAT_ID = "457874923"
API_FOOTBALL_KEY = "47dff2948a07fdef4536532d267ace24"

CHECK_INTERVAL   = 60
DANGER_THRESHOLD = 45

# ─── FILTRES ────────────────────────────────────────────────────────────────
MAX_ELAPSED     = 70
MAX_TOTAL_GOALS = 2
MAX_ZERO_ZERO   = 2
# ────────────────────────────────────────────────────────────────────────────

BASE_URL_AF = "https://v3.football.api-sports.io"
BASE_URL_SF = "https://www.sofascore.com/api/v1"
BASE_URL_US = "https://understat.com"

UNDERSTAT_LEAGUES = {
    39: "EPL", 140: "La_liga", 135: "Serie_A",
    78: "Bundesliga", 61: "Ligue_1", 235: "RFPL"
}

LEAGUE_PROFILES = {
    2:  {"name":"Champions League","avg_goals":2.9,
         "goal_peaks":{"0-15":12,"16-30":16,"31-45":18,"46-60":18,"61-75":20,"76-90":16}},
    3:  {"name":"Europa League","avg_goals":2.7,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    848:{"name":"Conference League","avg_goals":2.6,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    39: {"name":"Premier League","avg_goals":2.8,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    40: {"name":"Championship","avg_goals":2.7,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":21,"76-90":19}},
    45: {"name":"FA Cup","avg_goals":2.7,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    140:{"name":"La Liga","avg_goals":2.6,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    141:{"name":"La Liga 2","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    143:{"name":"Copa del Rey","avg_goals":2.8,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    135:{"name":"Serie A","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    136:{"name":"Serie B","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    137:{"name":"Coppa Italia","avg_goals":2.6,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    78: {"name":"Bundesliga","avg_goals":3.1,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    79: {"name":"2. Bundesliga","avg_goals":2.9,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    81: {"name":"DFB Pokal","avg_goals":3.0,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    61: {"name":"Ligue 1","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    62: {"name":"Ligue 2","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    65: {"name":"Coupe de France","avg_goals":2.7,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    88: {"name":"Eredivisie","avg_goals":3.2,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    94: {"name":"Primeira Liga","avg_goals":2.6,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    144:{"name":"Jupiler Pro League","avg_goals":2.9,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":16,"46-60":19,"61-75":21,"76-90":18}},
    197:{"name":"Super League SUI","avg_goals":2.8,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    345:{"name":"Bundesliga AUT","avg_goals":3.0,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":16,"46-60":19,"61-75":21,"76-90":18}},
    119:{"name":"Superliga DEN","avg_goals":2.7,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    103:{"name":"Eliteserien NOR","avg_goals":2.8,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    113:{"name":"Allsvenskan SUE","avg_goals":2.7,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    188:{"name":"Premier Division IRL","avg_goals":2.6,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    106:{"name":"Ekstraklasa POL","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":16,"46-60":20,"61-75":22,"76-90":19}},
    235:{"name":"Premier League RUS","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":19}},
    333:{"name":"Premier League UKR","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    172:{"name":"Parva Liga BUL","avg_goals":2.3,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":19,"61-75":23,"76-90":20}},
    283:{"name":"Liga 1 ROU","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    210:{"name":"HNL CRO","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    286:{"name":"SuperLiga SRB","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    332:{"name":"Fortuna Liga CZE","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":16,"46-60":20,"61-75":22,"76-90":19}},
    329:{"name":"Super Liga SVK","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    271:{"name":"NB I HUN","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":16,"46-60":20,"61-75":22,"76-90":19}},
    268:{"name":"PrvaLiga SVN","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    98: {"name":"Super Lig TUR","avg_goals":2.7,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    218:{"name":"Super League GRE","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    207:{"name":"Premiership SCO","avg_goals":2.6,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    244:{"name":"Ligat HaAl ISR","avg_goals":2.6,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":19}},
    322:{"name":"First Division CYP","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":16,"46-60":20,"61-75":22,"76-90":19}},
    71: {"name":"Brasileirao A","avg_goals":2.7,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    72: {"name":"Brasileirao B","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    128:{"name":"Liga Profesional ARG","avg_goals":2.6,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    265:{"name":"Primera Division CHI","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    266:{"name":"Primera Division COL","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    242:{"name":"Liga 1 PER","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    253:{"name":"LigaPro ECU","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    284:{"name":"Primera Division URU","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    262:{"name":"Liga MX MEX","avg_goals":2.6,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    239:{"name":"MLS USA","avg_goals":2.9,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    307:{"name":"Saudi Pro League","avg_goals":3.0,
         "goal_peaks":{"0-15":12,"16-30":15,"31-45":16,"46-60":19,"61-75":21,"76-90":17}},
    323:{"name":"UAE Pro League","avg_goals":2.8,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    318:{"name":"Qatar Stars","avg_goals":2.7,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    154:{"name":"Chinese Super League","avg_goals":2.8,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    292:{"name":"J1 League JPN","avg_goals":2.5,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    255:{"name":"K League 1 KOR","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    169:{"name":"A-League AUS","avg_goals":2.8,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    233:{"name":"Botola Pro MAR","avg_goals":2.2,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":14,"46-60":19,"61-75":24,"76-90":20}},
    200:{"name":"Ligue 1 TUN","avg_goals":2.2,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":14,"46-60":19,"61-75":24,"76-90":20}},
    273:{"name":"PSL AFS","avg_goals":2.4,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
}

LEAGUES = {
    2:"Champions League",3:"Europa League",848:"Conference League",
    39:"Premier League ENG",40:"Championship ENG",45:"FA Cup ENG",
    140:"La Liga ESP",141:"La Liga 2 ESP",143:"Copa del Rey ESP",
    135:"Serie A ITA",136:"Serie B ITA",137:"Coppa Italia ITA",
    78:"Bundesliga GER",79:"2. Bundesliga GER",81:"DFB Pokal GER",
    61:"Ligue 1 FRA",62:"Ligue 2 FRA",65:"Coupe de France FRA",
    88:"Eredivisie NED",94:"Primeira Liga POR",144:"Jupiler Pro League BEL",
    197:"Super League SUI",345:"Bundesliga AUT",
    119:"Superliga DEN",103:"Eliteserien NOR",113:"Allsvenskan SUE",
    188:"Premier Division IRL",
    106:"Ekstraklasa POL",235:"Premier League RUS",333:"Premier League UKR",
    172:"Parva Liga BUL",283:"Liga 1 ROU",210:"HNL CRO",
    286:"SuperLiga SRB",332:"Fortuna Liga CZE",329:"Super Liga SVK",
    271:"NB I HUN",268:"PrvaLiga SVN",
    98:"Super Lig TUR",218:"Super League GRE",207:"Premiership SCO",
    244:"Ligat HaAl ISR",322:"First Division CYP",
    71:"Brasileirao A",72:"Brasileirao B",128:"Liga Profesional ARG",
    265:"Primera Division CHI",266:"Primera Division COL",
    242:"Liga 1 PER",253:"LigaPro ECU",284:"Primera Division URU",
    262:"Liga MX MEX",239:"MLS USA",
    307:"Saudi Pro League",323:"UAE Pro League",318:"Qatar Stars",
    154:"Chinese Super League",292:"J1 League JPN",255:"K League 1 KOR",
    169:"A-League AUS",
    233:"Botola Pro MAR",200:"Ligue 1 TUN",273:"PSL AFS",
}

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO,
    handlers=[logging.FileHandler("goal_bot.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

last_alert_score: dict = {}
sf_id_cache: dict = {}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

def get_sf_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "*/*",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.sofascore.com/",
        "Origin": "https://www.sofascore.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

def get_headers_af():
    return {"x-apisports-key": API_FOOTBALL_KEY}

HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

async def fetch_live_fixtures(session):
    try:
        async with session.get(f"{BASE_URL_AF}/fixtures", headers=get_headers_af(),
                               params={"live": "all"}, timeout=10) as r:
            if r.status == 200:
                return (await r.json()).get("response", [])
            return []
    except Exception as e:
        log.error(f"Erreur fixtures: {e}"); return []

async def fetch_live_stats_af(session, fixture_id):
    try:
        async with session.get(f"{BASE_URL_AF}/fixtures/statistics",
                               headers=get_headers_af(),
                               params={"fixture": fixture_id}, timeout=10) as r:
            if r.status == 200:
                return parse_af_stats((await r.json()).get("response", []))
            return {}
    except Exception as e:
        log.error(f"Erreur stats AF: {e}"); return {}

def parse_af_stats(stats_response):
    result = {
        "home": {"shots_total":0,"shots_on":0,"possession":50,"corners":0,"fouls":0},
        "away": {"shots_total":0,"shots_on":0,"possession":50,"corners":0,"fouls":0}
    }
    mapping = {
        "Total Shots":"shots_total","Shots on Goal":"shots_on",
        "Ball Possession":"possession","Corner Kicks":"corners","Fouls":"fouls"
    }
    for i, team_data in enumerate(stats_response):
        side = "home" if i == 0 else "away"
        for stat in team_data.get("statistics", []):
            key = mapping.get(stat["type"])
            if key:
                val = stat["value"] or 0
                if isinstance(val, str) and "%" in val:
                    val = int(val.replace("%", ""))
                result[side][key] = int(val)
    return result

async def fetch_team_history_af(session, team_id, league_id, season):
    try:
        async with session.get(f"{BASE_URL_AF}/fixtures", headers=get_headers_af(),
                               params={"team":team_id,"league":league_id,
                                       "season":season,"last":10}, timeout=10) as r:
            if r.status != 200: return None
            fixtures = (await r.json()).get("response", [])
        if not fixtures: return None

        goals_total=[]; form=[]; over25=0; btts=0; scored=[]; conceded=[]
        for f in fixtures:
            gh = f["goals"]["home"] or 0
            ga = f["goals"]["away"] or 0
            total = gh + ga
            goals_total.append(total)
            is_home    = f["teams"]["home"]["id"] == team_id
            team_goals = gh if is_home else ga
            opp_goals  = ga if is_home else gh
            scored.append(team_goals); conceded.append(opp_goals)
            if team_goals > opp_goals:    form.append("W")
            elif team_goals == opp_goals: form.append("D")
            else:                         form.append("L")
            if total > 2.5:                       over25 += 1
            if team_goals > 0 and opp_goals > 0:  btts   += 1

        n = len(fixtures)
        recent5 = fixtures[:5]
        zero_zero_5 = sum(
            1 for f in recent5
            if (f["goals"]["home"] or 0) == 0 and (f["goals"]["away"] or 0) == 0
        )
        return {
            "avg":          round(sum(goals_total)/n, 1),
            "avg5":         round(sum(goals_total[:5])/min(5,n), 1),
            "scored_avg":   round(sum(scored)/n, 1),
            "conceded_avg": round(sum(conceded)/n, 1),
            "over25_pct":   round(over25/n*100),
            "btts_pct":     round(btts/n*100),
            "form":         "".join(form[:5]),
            "wins":         form.count("W"),
            "zero_zero_5":  zero_zero_5,
        }
    except Exception as e:
        log.error(f"Erreur historique: {e}"); return None

async def fetch_scenario_history(session, league_id, season, score_home, score_away, elapsed):
    try:
        async with session.get(
                f"{BASE_URL_AF}/fixtures", headers=get_headers_af(),
                params={"league":league_id,"season":season,
                        "status":"FT","last":200}, timeout=15) as r:
            if r.status != 200: return None
            fixtures = (await r.json()).get("response", [])
        if not fixtures: return None
        similar = []
        for f in fixtures:
            fh = f["goals"]["home"] or 0; fa = f["goals"]["away"] or 0
            total_goals = fh + fa
            for event in f.get("events", []):
                if event.get("type") == "Goal":
                    ev_min = event.get("time", {}).get("elapsed", 0)
                    if abs(ev_min - elapsed) <= 15:
                        similar.append({
                            "had_more_goals": total_goals > (score_home + score_away),
                            "goal_minute": ev_min
                        }); break
        if len(similar) < 5: return None
        total      = len(similar)
        with_goal  = sum(1 for m in similar if m["had_more_goals"])
        conversion = round(with_goal/total*100, 1)
        goal_mins  = [m["goal_minute"] for m in similar if m["had_more_goals"]]
        avg_min    = round(sum(goal_mins)/len(goal_mins)) if goal_mins else elapsed+15
        return {"total_matches":total,"matches_with_goal":with_goal,
                "conversion":conversion,"avg_goal_minute":avg_min}
    except Exception as e:
        log.error(f"Erreur scenario: {e}"); return None

def _sf_val(val):
    if val is None: return 0
    try:
        return int(str(val).replace("%","").replace(" ","").split("/")[0].strip())
    except: return 0

async def fetch_sofascore_id(session, home_team, away_team):
    cache_key = f"{home_team[:4].lower()}_{away_team[:4].lower()}"
    if cache_key in sf_id_cache:
        return sf_id_cache[cache_key]
    try:
        async with session.get(
                f"{BASE_URL_SF}/sport/football/events/live",
                headers=get_sf_headers(), timeout=10) as r:
            if r.status != 200:
                log.debug(f"SofaScore /live status {r.status}"); return None
            data = await r.json()
        hl = home_team.lower(); al = away_team.lower()
        for event in data.get("events", []):
            h = event.get("homeTeam", {}).get("name", "").lower()
            a = event.get("awayTeam", {}).get("name", "").lower()
            if ((hl[:4] in h or h[:4] in hl) and (al[:4] in a or a[:4] in al)):
                eid = event.get("id")
                if eid:
                    sf_id_cache[cache_key] = eid
                    return eid
        return None
    except Exception as e:
        log.debug(f"Erreur SofaScore /live: {e}"); return None

async def fetch_sofascore_stats(session, event_id):
    try:
        async with session.get(
                f"{BASE_URL_SF}/event/{event_id}/statistics",
                headers=get_sf_headers(), timeout=10) as r:
            if r.status != 200: return None
            data = await r.json()
        result = {
            "shots_on_home":0,"shots_on_away":0,
            "shots_total_home":0,"shots_total_away":0,
            "shots_box_home":0,"shots_box_away":0,
            "woodwork_home":0,"woodwork_away":0,
            "saves_home":0,"saves_away":0,
            "corners_home":0,"corners_away":0,
            "possession_home":50,"possession_away":50,
            "touches_box_home":0,"touches_box_away":0,
            "blocked_home":0,"blocked_away":0,
        }
        KEY_MAP = {
            "shotsOnGoal":          ("shots_on_home",    "shots_on_away"),
            "totalShotsOnGoal":     ("shots_total_home", "shots_total_away"),
            "goalKeeperSaves":      ("saves_home",       "saves_away"),
            "cornerKicks":          ("corners_home",     "corners_away"),
            "ballPossession":       ("possession_home",  "possession_away"),
            "touchesInOppBox":      ("touches_box_home", "touches_box_away"),
            "touchesInPenaltyArea": ("touches_box_home", "touches_box_away"),
            "hitWoodwork":          ("woodwork_home",    "woodwork_away"),
            "totalShotsInsideBox":  ("shots_box_home",   "shots_box_away"),
            "blockedScoringAttempt":("blocked_home",     "blocked_away"),
        }
        for period in data.get("statistics", []):
            if period.get("period") != "ALL": continue
            for group in period.get("groups", []):
                for item in group.get("statisticsItems", []):
                    key = item.get("key","")
                    if key in KEY_MAP:
                        kh, ka = KEY_MAP[key]
                        result[kh] = _sf_val(item.get("homeValue", item.get("home", 0)))
                        result[ka] = _sf_val(item.get("awayValue", item.get("away", 0)))
        log.info(
            f"OK SofaScore tirs={result['shots_on_home']}+{result['shots_on_away']} "
            f"surface={result['touches_box_home']}+{result['touches_box_away']} "
            f"saves={result['saves_home']}+{result['saves_away']} "
            f"bois={result['woodwork_home']+result['woodwork_away']}"
        )
        return result
    except Exception as e:
        log.debug(f"Erreur SofaScore stats: {e}"); return None

async def fetch_sofascore_momentum(session, event_id, elapsed):
    try:
        async with session.get(
                f"{BASE_URL_SF}/event/{event_id}/graph",
                headers=get_sf_headers(), timeout=10) as r:
            if r.status != 200: return None
            data = await r.json()
        points = data.get("graphPoints", [])
        if not points: return None
        recent_min = max(1, elapsed - 8)
        recent = [p for p in points if p.get("minute", 0) >= recent_min]
        if not recent:
            recent = points[-8:] if len(points) >= 8 else points
        home_pts  = sum(p["value"] for p in recent if p.get("value", 0) > 0)
        away_pts  = abs(sum(p["value"] for p in recent if p.get("value", 0) < 0))
        total_pts = home_pts + away_pts
        if total_pts == 0: return {"side":"balanced","intensity":0,"label":"Equilibre"}
        home_pct = home_pts / total_pts * 100
        away_pct = away_pts / total_pts * 100
        intensity = min(100, int(sum(abs(p.get("value",0)) for p in recent) / len(recent) * 3))
        if home_pct >= 70:   side = "home_strong"; label = "Domicile domine fortement"
        elif home_pct >= 58: side = "home_slight"; label = "Legere domination Domicile"
        elif away_pct >= 70: side = "away_strong"; label = "Exterieur domine fortement"
        elif away_pct >= 58: side = "away_slight"; label = "Legere domination Exterieur"
        else:                side = "balanced";    label = "Equilibre"
        log.info(f"OK SofaScore momentum : {label} (intensite={intensity})")
        return {"side":side,"intensity":intensity,"label":label,
                "home_pct":round(home_pct),"away_pct":round(away_pct)}
    except Exception as e:
        log.debug(f"Erreur SofaScore momentum: {e}"); return None

async def fetch_sofascore_data(session, home_team, away_team, elapsed):
    event_id = await fetch_sofascore_id(session, home_team, away_team)
    if not event_id:
        return None, None
    stats, momentum = await asyncio.gather(
        fetch_sofascore_stats(session, event_id),
        fetch_sofascore_momentum(session, event_id, elapsed),
    )
    return stats, momentum

async def fetch_xg_understat(session, league_id, home_team, away_team):
    ln = UNDERSTAT_LEAGUES.get(league_id)
    if not ln: return None
    try:
        season = datetime.now().year if datetime.now().month >= 7 else datetime.now().year-1
        async with session.get(f"{BASE_URL_US}/league/{ln}/{season}",
                               headers=HEADERS_BROWSER, timeout=15) as r:
            if r.status != 200: return None
            html = await r.text()
        m = re.search(r"datesData\s*=\s*JSON\.parse\('(.+?)'\)", html)
        if not m: return None
        matches = json.loads(m.group(1).encode().decode("unicode_escape"))
        hl = home_team.lower(); al = away_team.lower()
        for match in matches:
            h = match.get("h", {}).get("title", "").lower()
            a = match.get("a", {}).get("title", "").lower()
            if (hl[:4] in h or h[:4] in hl) and (al[:4] in a or a[:4] in al):
                xg_h = float(match.get("xG", {}).get("h", 0) or 0)
                xg_a = float(match.get("xG", {}).get("a", 0) or 0)
                return {"home":round(xg_h,2),"away":round(xg_a,2),
                        "total":round(xg_h+xg_a,2)}
        return None
    except Exception as e:
        log.debug(f"Erreur Understat: {e}"); return None

def get_interval(elapsed):
    if elapsed <= 15:   return "0-15"
    elif elapsed <= 30: return "16-30"
    elif elapsed <= 45: return "31-45"
    elif elapsed <= 60: return "46-60"
    elif elapsed <= 75: return "61-75"
    else:               return "76-90"

def calculate_danger_score(fixture, af_stats, sf_stats, sf_momentum,
                           xg_us, hist_home, hist_away, scenario):
    score = 0; signals = []
    elapsed = fixture["fixture"]["status"]["elapsed"] or 1
    gh  = fixture["goals"]["home"] or 0
    ga  = fixture["goals"]["away"] or 0
    lid = fixture["league"]["id"]
    haf = af_stats.get("home", {}); aaf = af_stats.get("away", {})

    if sf_stats and (sf_stats.get("shots_on_home",0) + sf_stats.get("shots_on_away",0)) > 0:
        son_h = sf_stats["shots_on_home"]; son_a = sf_stats["shots_on_away"]
        st_h  = sf_stats.get("shots_total_home", son_h); st_a = sf_stats.get("shots_total_away", son_a)
    else:
        son_h = haf.get("shots_on",0); son_a = aaf.get("shots_on",0)
        st_h  = haf.get("shots_total",0); st_a = aaf.get("shots_total",0)

    son = son_h+son_a; st = st_h+st_a; spm = son/elapsed
    if spm >= 0.25:   score += 22; signals.append(f"Rythme TRES intense : {son} cadres/{st} tirs")
    elif spm >= 0.15: score += 16; signals.append(f"Rythme intense : {son}/{st} tirs")
    elif son >= 8:    score += 13; signals.append(f"Beaucoup de tirs cadres : {son}/{st}")
    elif son >= 5:    score += 9;  signals.append(f"Tirs cadres : {son}/{st}")
    elif son >= 3:    score += 5;  signals.append(f"Tirs cadres : {son}")
    elif son >= 1:    score += 2

    cor_h = sf_stats.get("corners_home",0) if sf_stats else haf.get("corners",0)
    cor_a = sf_stats.get("corners_away",0) if sf_stats else aaf.get("corners",0)
    corners = cor_h+cor_a
    if corners >= 10:  score += 8; signals.append(f"Nombreux corners : {corners}")
    elif corners >= 7: score += 5; signals.append(f"Corners : {corners}")
    elif corners >= 4: score += 3; signals.append(f"Corners : {corners}")
    elif corners >= 2: score += 1

    diff = abs(gh-ga)
    if diff == 0:   score += 7; signals.append(f"Score nul ({gh}-{ga}) — pression max")
    elif diff == 1: score += 4; signals.append(f"Score serre ({gh}-{ga})")

    if scenario:
        conv = scenario["conversion"]; tot = scenario["total_matches"]
        if conv >= 85:   score += 15; signals.append(f"Historique : {conv}% ({scenario['matches_with_goal']}/{tot})")
        elif conv >= 70: score += 10; signals.append(f"Historique : {conv}% ({scenario['matches_with_goal']}/{tot})")
        elif conv >= 55: score += 6;  signals.append(f"Historique : {conv}%")
        elif conv >= 40: score += 3

    profile = LEAGUE_PROFILES.get(lid)
    if profile:
        interval = get_interval(elapsed)
        peak = profile.get("goal_peaks",{}).get(interval,15)
        if peak >= 22:   score += 8; signals.append(f"Pic de buts max : {peak}% en {interval}min")
        elif peak >= 18: score += 5; signals.append(f"Pic de buts eleve : {peak}%")
        elif peak >= 15: score += 2

    bonus_form = 0
    for hist in [hist_home, hist_away]:
        if not hist: continue
        if hist.get("over25_pct",0) >= 70:    bonus_form += 3
        elif hist.get("over25_pct",0) >= 50:  bonus_form += 1
        if hist.get("btts_pct",0) >= 70:      bonus_form += 2
        elif hist.get("btts_pct",0) >= 50:    bonus_form += 1
        if hist.get("scored_avg",0) >= 2.0:   bonus_form += 2
        elif hist.get("scored_avg",0) >= 1.5: bonus_form += 1
    if bonus_form > 0:
        bonus_form = min(bonus_form, 12); score += bonus_form
        parts = []
        if hist_home: parts.append(f"DOM o25={hist_home.get('over25_pct',0)}% btts={hist_home.get('btts_pct',0)}%")
        if hist_away: parts.append(f"EXT o25={hist_away.get('over25_pct',0)}% btts={hist_away.get('btts_pct',0)}%")
        signals.append(f"Forme offensive : {' | '.join(parts)} (+{bonus_form}pts)")

    if xg_us and xg_us.get("total",0) > 0:
        t = xg_us["total"]
        if t >= 3.0:   score += 15; signals.append(f"xG TRES eleve : {t} (Understat)")
        elif t >= 2.0: score += 10; signals.append(f"xG eleve : {t} (Understat)")
        elif t >= 1.2: score += 6;  signals.append(f"xG : {t} (Understat)")
        elif t >= 0.5: score += 3

    if sf_stats:
        tob = sf_stats.get("touches_box_home",0) + sf_stats.get("touches_box_away",0)
        if tob >= 35:   score += 10; signals.append(f"Pression surface : {tob} touches")
        elif tob >= 22: score += 6;  signals.append(f"Touches surface : {tob}")
        elif tob >= 12: score += 3
        wood = sf_stats.get("woodwork_home",0) + sf_stats.get("woodwork_away",0)
        if wood >= 2:   score += 6; signals.append(f"Bois touche(s) : {wood}")
        elif wood >= 1: score += 3; signals.append(f"Bois touche : {wood}")
        saves = sf_stats.get("saves_home",0) + sf_stats.get("saves_away",0)
        if saves >= 6:   score += 6; signals.append(f"Gardiens tres sollicites : {saves} arrets")
        elif saves >= 4: score += 4; signals.append(f"Gardiens sollicites : {saves} arrets")
        elif saves >= 2: score += 2
        box = sf_stats.get("shots_box_home",0) + sf_stats.get("shots_box_away",0)
        if box >= 10:  score += 5; signals.append(f"Tirs dans surface : {box}")
        elif box >= 6: score += 3

    if sf_momentum:
        side = sf_momentum.get("side","balanced")
        intensity = sf_momentum.get("intensity",0)
        label = sf_momentum.get("label","")
        if side in ("home_strong","away_strong"):
            score += min(15, 8 + intensity//10); signals.append(f"Momentum fort : {label}")
        elif side in ("home_slight","away_slight"):
            score += min(8, 4 + intensity//15); signals.append(f"Momentum : {label}")

    return min(score, 100), signals

def is_alert_allowed_now():
    """Bloque les alertes entre 23h30 et 07h00"""
    from datetime import time
    now = datetime.now().time()
    if time(23, 30) <= now or now < time(7, 0):
        return False
    return True

def should_alert(fid, ds, gh, ga, scenario):
    if ds < DANGER_THRESHOLD: return False
    if not is_alert_allowed_now(): return False
    prev = last_alert_score.get(fid)
    if prev is None: return True
    return (gh, ga) != prev

def danger_level(s):
    if s >= 80:   return "TRES ELEVE 🔴"
    elif s >= 65: return "ELEVE 🟠"
    elif s >= 50: return "MODERE 🟡"
    return "FAIBLE ⚪"

def form_emojis(form_str):
    mapping = {"W":"✅","D":"🟡","L":"❌"}
    return " ".join(mapping.get(c,"⚪") for c in form_str[:5])

def target_odds(ds):
    if ds >= 80:   return "1.35–1.55"
    elif ds >= 65: return "1.50–1.75"
    elif ds >= 50: return "1.65–1.95"
    return "1.85–2.20"

def entry_window(elapsed, ds):
    es = elapsed+2; ee = elapsed+5
    we = elapsed+12 if ds >= 80 else elapsed+18 if ds >= 65 else elapsed+25
    return f"{es}–{ee}min", f"{es}–{we}min"

def calc_prob(ds, elapsed, scenario):
    ptot = min(95, int(ds/100*90 + (90-elapsed)/90*10))
    if scenario and scenario["conversion"] > 0:
        ptot = min(95, int((ptot/100*0.55 + scenario["conversion"]/100*0.45)*100))
    return ptot

def format_alert(fixture, af_stats, sf_stats, sf_momentum, xg_us,
                 ds, signals, hist_home, hist_away, scenario):
    now     = datetime.now().strftime("%H:%M")
    lid     = fixture["league"]["id"]
    elapsed = fixture["fixture"]["status"]["elapsed"] or 0
    home    = fixture["teams"]["home"]["name"]
    away    = fixture["teams"]["away"]["name"]
    gh      = fixture["goals"]["home"] or 0
    ga      = fixture["goals"]["away"] or 0
    league  = LEAGUES.get(lid, fixture["league"]["name"])
    label   = "MT" if fixture["fixture"]["status"]["short"] == "HT" else f"{elapsed}'"
    haf = af_stats.get("home",{}); aaf = af_stats.get("away",{})
    entry, window = entry_window(elapsed, ds)
    prob = calc_prob(ds, elapsed, scenario)

    if sf_stats and (sf_stats.get("shots_on_home",0)+sf_stats.get("shots_on_away",0)) > 0:
        son_h = sf_stats["shots_on_home"]; son_a = sf_stats["shots_on_away"]
        st_h  = sf_stats.get("shots_total_home",son_h); st_a = sf_stats.get("shots_total_away",son_a)
        cor_h = sf_stats.get("corners_home",0); cor_a = sf_stats.get("corners_away",0)
        pos_h = sf_stats.get("possession_home",50); pos_a = sf_stats.get("possession_away",50)
    else:
        son_h = haf.get("shots_on",0); son_a = aaf.get("shots_on",0)
        st_h  = haf.get("shots_total",0); st_a = aaf.get("shots_total",0)
        cor_h = haf.get("corners",0); cor_a = aaf.get("corners",0)
        pos_h = haf.get("possession",50); pos_a = aaf.get("possession",50)

    if xg_us and xg_us.get("total",0) > 0:
        xg_str = f"xG {xg_us['home']}–{xg_us['away']} = <b>{xg_us['total']}</b> <i>(Understat)</i>"
    else:
        xg_str = "xG : N/A"

    extras = []
    if sf_stats:
        tob   = sf_stats.get("touches_box_home",0)+sf_stats.get("touches_box_away",0)
        saves = sf_stats.get("saves_home",0)+sf_stats.get("saves_away",0)
        wood  = sf_stats.get("woodwork_home",0)+sf_stats.get("woodwork_away",0)
        if tob > 0:   extras.append(f"Surface : {tob} touches")
        if saves > 0: extras.append(f"Arrêts : {saves}")
        if wood > 0:  extras.append(f"Bois : {wood}")
    if sf_momentum and sf_momentum.get("side","balanced") != "balanced":
        extras.append(f"Momentum : {sf_momentum['label']}")
    extras_line = ("\n  " + "  |  ".join(extras)) if extras else ""

    sf_ok = "✅ SF" if sf_stats else "⚪ AF"
    hf_str = form_emojis(hist_home.get("form","?????")) if hist_home else "N/A"
    af_str = form_emojis(hist_away.get("form","?????")) if hist_away else "N/A"
    avg_buts = "N/A"
    if hist_home and hist_away:
        avg_buts = round((hist_home.get("avg5",0)+hist_away.get("avg5",0))/2, 1)
    signals_str = "\n".join(f"  • {s}" for s in signals[:5]) if signals else "  • Analyse..."

    return (
        f"🟢 <b>ALERTE BUT IMMINENT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 <b>{league}</b> · {label}\n\n"
        f"🏟 <b>{home}</b>  <b>{gh} – {ga}</b>  <b>{away}</b>\n\n"
        f"📊 Score de danger : <b>{ds}/100</b> — {danger_level(ds)} {sf_ok}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>Signaux clés</b>\n"
        f"{signals_str}\n\n"
        f"📈 <b>Stats live</b>\n"
        f"  Tirs cadrés  : {son_h}/{st_h} – {son_a}/{st_a}\n"
        f"  Possession   : {pos_h}% – {pos_a}%\n"
        f"  Corners      : {cor_h} – {cor_a}\n"
        f"  {xg_str}{extras_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔁 <b>Forme récente (5 matchs)</b>\n"
        f"  {home[:12]:<12}  {hf_str}\n"
        f"  {away[:12]:<12}  {af_str}\n"
        f"  Moy. buts/match : <b>{avg_buts}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>Entrée</b> : {entry}  |  Cote cible : <b>{target_odds(ds)}</b>\n"
        f"⏱ Fenêtre but : {window}\n"
        f"📉 Proba fin de match : <b>{prob}%</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>FootBot v3.7 · {now}</i>"
    )

async def send_message(bot, text):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.HTML)
        log.info("Alerte envoyee")
    except Exception as e:
        log.error(f"Erreur Telegram: {e}")

async def monitor_loop(bot):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                log.info(f"Scan... {datetime.now().strftime('%H:%M:%S')}")
                fixtures = await fetch_live_fixtures(session)
                targets  = [f for f in fixtures if f["league"]["id"] in LEAGUES]
                log.info(f"{len(targets)} matchs en direct")
                sf_id_cache.clear()

                for fixture in targets:
                    fid       = fixture["fixture"]["id"]
                    status    = fixture["fixture"]["status"]["short"]
                    elapsed   = fixture["fixture"]["status"]["elapsed"] or 0
                    lid       = fixture["league"]["id"]
                    season    = fixture["league"]["season"]
                    home_id   = fixture["teams"]["home"]["id"]
                    away_id   = fixture["teams"]["away"]["id"]
                    home_name = fixture["teams"]["home"]["name"]
                    away_name = fixture["teams"]["away"]["name"]
                    gh = fixture["goals"]["home"] or 0
                    ga = fixture["goals"]["away"] or 0

                    if status not in ["1H","2H","HT"]: continue

                    if elapsed > MAX_ELAPSED:
                        log.info(f"SKIP {home_name} vs {away_name} : {elapsed}' > {MAX_ELAPSED}'")
                        continue

                    if (gh+ga) > MAX_TOTAL_GOALS:
                        log.info(f"SKIP {home_name} vs {away_name} : {gh+ga} buts")
                        continue

                    (af_stats, xg_us, hist_home, hist_away,
                     sf_result) = await asyncio.gather(
                        fetch_live_stats_af(session, fid),
                        fetch_xg_understat(session, lid, home_name, away_name),
                        fetch_team_history_af(session, home_id, lid, season),
                        fetch_team_history_af(session, away_id, lid, season),
                        fetch_sofascore_data(session, home_name, away_name, elapsed),
                    )

                    sf_stats, sf_momentum = sf_result if sf_result else (None, None)

                    if not af_stats: continue

                    home_zz = hist_home.get("zero_zero_5",0) if hist_home else 0
                    away_zz = hist_away.get("zero_zero_5",0) if hist_away else 0
                    if home_zz > MAX_ZERO_ZERO or away_zz > MAX_ZERO_ZERO:
                        log.info(f"SKIP {home_name} vs {away_name} : 0-0 DOM={home_zz} EXT={away_zz}")
                        continue

                    scenario = await fetch_scenario_history(session, lid, season, gh, ga, elapsed)

                    ds, signals = calculate_danger_score(
                        fixture, af_stats, sf_stats, sf_momentum,
                        xg_us, hist_home, hist_away, scenario)

                    h_form = hist_home.get("form","?") if hist_home else "?"
                    a_form = hist_away.get("form","?") if hist_away else "?"
                    sf_ok  = "OK SofaScore" if sf_stats else "-- SofaScore"
                    sf_mom = f"momentum={sf_momentum['label']}" if sf_momentum else "no momentum"
                    us_ok  = f"OK Understat xG={xg_us['total']}" if xg_us else "-- Understat"
                    log.info(
                        f"{home_name} vs {away_name} | {elapsed}' | {gh}-{ga} | {ds}/100\n"
                        f"   {sf_ok} ({sf_mom})  {us_ok}\n"
                        f"   DOM={h_form}(0-0:{home_zz}/5) EXT={a_form}(0-0:{away_zz}/5)"
                    )

                    if should_alert(fid, ds, gh, ga, scenario):
                        log.info(f"ALERTE {home_name} vs {away_name} | {ds}/100 | {gh}-{ga}")
                        await send_message(bot, format_alert(
                            fixture, af_stats, sf_stats, sf_momentum, xg_us,
                            ds, signals, hist_home, hist_away, scenario))
                        last_alert_score[fid] = (gh, ga)

                    await asyncio.sleep(1)

            except Exception as e:
                log.error(f"Erreur boucle: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=(
            "<b>BOT PREDICTION BUT v3.7</b>\n\n"
            "<b>Sources :</b>\n"
            "• API-Football — base fiable\n"
            "  (tirs, corners, historique, forme)\n"
            "• SofaScore — bonus live :\n"
            "  tirs precis, touches surface,\n"
            "  bois, arrets gardien, momentum\n"
            "• Understat — xG backup (6 ligues)\n\n"
            "<b>Scoring :</b>\n"
            "• API-Football seul → 45+ possible\n"
            "• SofaScore actif → jusqu'a 90+\n"
            "• Alerte = seuil atteint\n\n"
            "<b>Filtres :</b>\n"
            f"• Pas d'alerte apres {MAX_ELAPSED}'\n"
            f"• Skip si {MAX_TOTAL_GOALS+1}+ buts\n"
            f"• Skip si {MAX_ZERO_ZERO+1}+ matchs 0-0 sur 5\n"
            "• Anti-doublon par score\n"
            "• Inactif de 23h30 à 07h00\n\n"
            f"Seuil : {DANGER_THRESHOLD}/100"
        ),
        parse_mode=ParseMode.HTML
    )
    await monitor_loop(bot)

if __name__ == "__main__":
    asyncio.run(main())
