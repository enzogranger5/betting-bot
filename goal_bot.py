"""
BOT PREDICTION BUT IMMINENT - VERSION 3.4
63 ligues — forme des equipes — fallback Markdown
"""

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
API_FOOTBALL_KEY = "47dff2948a07fdef4536532d267ace24"

CHECK_INTERVAL   = 60
DANGER_THRESHOLD = 45
ALERT_COOLDOWN   = 600

BASE_URL_AF = "https://v3.football.api-sports.io"
BASE_URL_US = "https://understat.com"
BASE_URL_SF = "https://www.sofascore.com/api/v1"
BASE_URL_WS = "https://www.whoscored.com"
BASE_URL_FB = "https://fbref.com/en"
BASE_URL_FM = "https://www.fotmob.com/api"

UNDERSTAT_LEAGUES = {39:"EPL",140:"La_liga",135:"Serie_A",78:"Bundesliga",61:"Ligue_1",235:"RFPL"}
FBREF_LEAGUES = {39:"Premier-League",140:"La-Liga",135:"Serie-A",78:"Bundesliga",61:"Ligue-1",2:"Champions-League"}

LEAGUE_PROFILES = {
    2:  {"name":"Champions League","avg_goals":2.9,"2nd_half_pct":56,
         "goal_peaks":{"0-15":12,"16-30":16,"31-45":18,"46-60":18,"61-75":20,"76-90":16}},
    3:  {"name":"Europa League","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    848:{"name":"Conference League","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    39: {"name":"Premier League","avg_goals":2.8,"2nd_half_pct":58,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    40: {"name":"Championship","avg_goals":2.7,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":21,"76-90":19}},
    45: {"name":"FA Cup","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    140:{"name":"La Liga","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    141:{"name":"La Liga 2","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    143:{"name":"Copa del Rey","avg_goals":2.8,"2nd_half_pct":56,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    135:{"name":"Serie A","avg_goals":2.5,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    136:{"name":"Serie B","avg_goals":2.4,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    137:{"name":"Coppa Italia","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    78: {"name":"Bundesliga","avg_goals":3.1,"2nd_half_pct":57,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    79: {"name":"2. Bundesliga","avg_goals":2.9,"2nd_half_pct":56,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    81: {"name":"DFB Pokal","avg_goals":3.0,"2nd_half_pct":57,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    61: {"name":"Ligue 1","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    62: {"name":"Ligue 2","avg_goals":2.4,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    65: {"name":"Coupe de France","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    88: {"name":"Eredivisie","avg_goals":3.2,"2nd_half_pct":57,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    94: {"name":"Primeira Liga","avg_goals":2.6,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    144:{"name":"Jupiler Pro League","avg_goals":2.9,"2nd_half_pct":57,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":16,"46-60":19,"61-75":21,"76-90":18}},
    197:{"name":"Super League SUI","avg_goals":2.8,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    345:{"name":"Bundesliga AUT","avg_goals":3.0,"2nd_half_pct":57,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":16,"46-60":19,"61-75":21,"76-90":18}},
    119:{"name":"Superliga DEN","avg_goals":2.7,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    103:{"name":"Eliteserien NOR","avg_goals":2.8,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    113:{"name":"Allsvenskan SUE","avg_goals":2.7,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    188:{"name":"Premier Division IRL","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    106:{"name":"Ekstraklasa POL","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":16,"46-60":20,"61-75":22,"76-90":19}},
    235:{"name":"Premier League RUS","avg_goals":2.5,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":19}},
    333:{"name":"Premier League UKR","avg_goals":2.4,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    172:{"name":"Parva Liga BUL","avg_goals":2.3,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":19,"61-75":23,"76-90":20}},
    283:{"name":"Liga 1 ROU","avg_goals":2.4,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    210:{"name":"HNL CRO","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    286:{"name":"SuperLiga SRB","avg_goals":2.4,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    332:{"name":"Fortuna Liga CZE","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":16,"46-60":20,"61-75":22,"76-90":19}},
    329:{"name":"Super Liga SVK","avg_goals":2.4,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    271:{"name":"NB I HUN","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":16,"46-60":20,"61-75":22,"76-90":19}},
    268:{"name":"PrvaLiga SVN","avg_goals":2.4,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    98: {"name":"Super Lig TUR","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    218:{"name":"Super League GRE","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    207:{"name":"Premiership SCO","avg_goals":2.6,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    244:{"name":"Ligat HaAl ISR","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":19}},
    322:{"name":"First Division CYP","avg_goals":2.5,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":16,"46-60":20,"61-75":22,"76-90":19}},
    71: {"name":"Brasileirao A","avg_goals":2.7,"2nd_half_pct":57,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    72: {"name":"Brasileirao B","avg_goals":2.5,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    128:{"name":"Liga Profesional ARG","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    265:{"name":"Primera Division CHI","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    266:{"name":"Primera Division COL","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    242:{"name":"Liga 1 PER","avg_goals":2.4,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    253:{"name":"LigaPro ECU","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    284:{"name":"Primera Division URU","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    262:{"name":"Liga MX MEX","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    239:{"name":"MLS USA","avg_goals":2.9,"2nd_half_pct":57,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    307:{"name":"Saudi Pro League","avg_goals":3.0,"2nd_half_pct":56,
         "goal_peaks":{"0-15":12,"16-30":15,"31-45":16,"46-60":19,"61-75":21,"76-90":17}},
    323:{"name":"UAE Pro League","avg_goals":2.8,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    318:{"name":"Qatar Stars","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    154:{"name":"Chinese Super League","avg_goals":2.8,"2nd_half_pct":56,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    292:{"name":"J1 League JPN","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    255:{"name":"K League 1 KOR","avg_goals":2.4,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    169:{"name":"A-League AUS","avg_goals":2.8,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    233:{"name":"Botola Pro MAR","avg_goals":2.2,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":14,"46-60":19,"61-75":24,"76-90":20}},
    200:{"name":"Ligue 1 TUN","avg_goals":2.2,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":14,"46-60":19,"61-75":24,"76-90":20}},
    273:{"name":"PSL AFS","avg_goals":2.4,"2nd_half_pct":55,
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
last_alert_time: dict = {}

HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

def get_headers_af():
    return {"x-apisports-key": API_FOOTBALL_KEY}

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

async def fetch_team_history_af(session, team_id, league_id, season):
    try:
        async with session.get(f"{BASE_URL_AF}/fixtures", headers=get_headers_af(),
                               params={"team": team_id, "league": league_id,
                                       "season": season, "last": 10}, timeout=10) as r:
            if r.status != 200: return None
            fixtures = (await r.json()).get("response", [])
        if not fixtures: return None

        goals_total = []
        form = []
        over25 = 0
        btts = 0
        scored = []
        conceded = []

        for f in fixtures:
            gh = f["goals"]["home"] or 0
            ga = f["goals"]["away"] or 0
            total = gh + ga
            goals_total.append(total)

            is_home = f["teams"]["home"]["id"] == team_id
            team_goals = gh if is_home else ga
            opp_goals  = ga if is_home else gh
            scored.append(team_goals)
            conceded.append(opp_goals)

            if team_goals > opp_goals:    form.append("W")
            elif team_goals == opp_goals: form.append("D")
            else:                         form.append("L")

            if total > 2.5: over25 += 1
            if team_goals > 0 and opp_goals > 0: btts += 1

        n = len(fixtures)
        return {
            "avg":          round(sum(goals_total) / n, 1),
            "avg5":         round(sum(goals_total[:5]) / min(5, n), 1),
            "scored_avg":   round(sum(scored) / n, 1),
            "conceded_avg": round(sum(conceded) / n, 1),
            "over25_pct":   round(over25 / n * 100),
            "btts_pct":     round(btts / n * 100),
            "form":         "".join(form[:5]),
            "wins":         form.count("W"),
            "fixtures":     fixtures,
        }
    except Exception as e:
        log.error(f"Erreur historique: {e}"); return None

def parse_af_stats(stats_response):
    result = {
        "home": {"shots_total": 0, "shots_on": 0, "possession": 50, "corners": 0, "fouls": 0},
        "away": {"shots_total": 0, "shots_on": 0, "possession": 50, "corners": 0, "fouls": 0}
    }
    mapping = {
        "Total Shots": "shots_total", "Shots on Goal": "shots_on",
        "Ball Possession": "possession", "Corner Kicks": "corners", "Fouls": "fouls"
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

async def fetch_xg_understat(session, league_id, home_team, away_team):
    ln = UNDERSTAT_LEAGUES.get(league_id)
    if not ln: return None
    try:
        season = datetime.now().year if datetime.now().month >= 7 else datetime.now().year - 1
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
                return {"home": round(xg_h, 2), "away": round(xg_a, 2),
                        "total": round(xg_h + xg_a, 2)}
        return None
    except Exception as e:
        log.error(f"Erreur Understat: {e}"); return None

async def fetch_sofascore_data(session, home_team, away_team):
    try:
        headers = {**HEADERS_BROWSER, "Accept": "application/json",
                   "Referer": "https://www.sofascore.com"}
        async with session.get(
                f"{BASE_URL_SF}/search/multi/?q={home_team.replace(' ', '+')}",
                headers=headers, timeout=10) as r:
            if r.status != 200: return None
            data = await r.json()
        events = data.get("events", {}).get("results", [])
        event_id = None; hl = home_team.lower()[:5]
        for event in events:
            h = event.get("homeTeam", {}).get("name", "").lower()
            status = event.get("status", {}).get("type", "")
            if hl in h and status in ["inprogress", "halftime"]:
                event_id = event.get("id"); break
        if not event_id: return None
        async with session.get(f"{BASE_URL_SF}/event/{event_id}/statistics",
                               headers=headers, timeout=10) as r:
            if r.status != 200: return None
            stats_data = await r.json()
        result = {
            "home": {"touches_box": 0, "dangerous_attacks": 0, "big_chances": 0},
            "away": {"touches_box": 0, "dangerous_attacks": 0, "big_chances": 0}
        }
        for group in stats_data.get("statistics", []):
            for stat in group.get("statisticsItems", []):
                name = stat.get("name", "").lower()
                hv = int(stat.get("home", 0) or 0); av = int(stat.get("away", 0) or 0)
                if "touches" in name and "box" in name:
                    result["home"]["touches_box"] = hv; result["away"]["touches_box"] = av
                elif "dangerous" in name:
                    result["home"]["dangerous_attacks"] = hv; result["away"]["dangerous_attacks"] = av
                elif "big chance" in name:
                    result["home"]["big_chances"] = hv; result["away"]["big_chances"] = av
        return result
    except Exception as e:
        log.error(f"Erreur SofaScore: {e}"); return None

async def fetch_whoscored_data(session, home_team, away_team):
    try:
        async with session.get(f"{BASE_URL_WS}/Matches/Live",
                               headers=HEADERS_BROWSER, timeout=10) as r:
            if r.status != 200: return None
            html = await r.text()
        ws_match = re.search(r'require\.config\.params\["args"\]\s*=\s*(\{.+?\});',
                             html, re.DOTALL)
        if not ws_match: return None
        match_data = json.loads(ws_match.group(1)).get("matchCentreData", {})
        def avg_rating(players):
            ratings = [float(p.get("stats", {}).get("ratings", {}).get("0", 0))
                       for p in players if p.get("stats")]
            ratings = [r for r in ratings if r > 0]
            return round(sum(ratings) / len(ratings), 2) if ratings else 0
        return {
            "home_rating": avg_rating(match_data.get("home", {}).get("players", [])),
            "away_rating": avg_rating(match_data.get("away", {}).get("players", [])),
            "home_form":   match_data.get("home", {}).get("form", ""),
            "away_form":   match_data.get("away", {}).get("form", ""),
        }
    except Exception as e:
        log.error(f"Erreur WhoScored: {e}"); return None

async def fetch_fbref_pressing(session, league_id, home_team, away_team):
    ln = FBREF_LEAGUES.get(league_id)
    if not ln: return None
    try:
        season = datetime.now().year if datetime.now().month >= 7 else datetime.now().year - 1
        url = (f"{BASE_URL_FB}/comps/9/{season}-{season+1}/pressing/"
               f"{season}-{season+1}-{ln}-Stats")
        async with session.get(url, headers=HEADERS_BROWSER, timeout=15) as r:
            if r.status != 200: return None
            html = await r.text()
        pressing_data = {}
        for row in re.findall(r'<tr[^>]*>(.+?)</tr>', html, re.DOTALL):
            tm = re.search(r'data-stat="team"[^>]*>(.+?)</td>', row)
            pm = re.search(r'data-stat="ppda_att"[^>]*>(.+?)</td>', row)
            if tm and pm:
                team = re.sub(r'<[^>]+>', '', tm.group(1)).strip()
                try:
                    pressing_data[team.lower()] = float(
                        re.sub(r'<[^>]+>', '', pm.group(1)).strip())
                except: pass
        hp = ap = None
        for team, ppda in pressing_data.items():
            if home_team.lower()[:4] in team or team[:4] in home_team.lower(): hp = ppda
            if away_team.lower()[:4] in team or team[:4] in away_team.lower(): ap = ppda
        if hp is None and ap is None: return None
        def lbl(p):
            if p is None: return "N/A"
            if p < 8:  return "Intense"
            if p < 11: return "Bon"
            if p < 14: return "Moyen"
            return "Faible"
        return {"home_ppda": hp, "away_ppda": ap,
                "home_pressing": lbl(hp), "away_pressing": lbl(ap)}
    except Exception as e:
        log.error(f"Erreur FBref: {e}"); return None

async def fetch_fotmob_data(session, home_team, away_team):
    try:
        async with session.get(
                f"{BASE_URL_FM}/matches",
                headers={**HEADERS_BROWSER, "Accept": "application/json"},
                params={"date": datetime.now().strftime("%Y%m%d")},
                timeout=10) as r:
            if r.status != 200: return None
            data = await r.json()
        match_id = None; hl = home_team.lower(); al = away_team.lower()
        for league in data.get("leagues", []):
            for match in league.get("matches", []):
                h = match.get("home", {}).get("name", "").lower()
                a = match.get("away", {}).get("name", "").lower()
                if (match.get("status", {}).get("started", False) and
                        not match.get("status", {}).get("finished", False)):
                    if (hl[:4] in h or h[:4] in hl) and (al[:4] in a or a[:4] in al):
                        match_id = match.get("id"); break
            if match_id: break
        if not match_id: return None
        async with session.get(
                f"{BASE_URL_FM}/matchDetails",
                headers={**HEADERS_BROWSER, "Accept": "application/json"},
                params={"matchId": match_id}, timeout=10) as r:
            if r.status != 200: return None
            match_data = await r.json()
        result = {"xg_home": 0.0, "xg_away": 0.0, "xg_total": 0.0,
                  "momentum": "Equilibre", "home_dominant": False, "away_dominant": False}
        for section in match_data.get("content", {}).get("stats", {}).get("stats", []):
            for stat in section.get("stats", []):
                title = stat.get("title", "").lower(); vals = stat.get("stats", [])
                if len(vals) >= 2 and ("expected goals" in title or "xg" in title):
                    try:
                        result["xg_home"]  = float(vals[0])
                        result["xg_away"]  = float(vals[1])
                        result["xg_total"] = round(result["xg_home"] + result["xg_away"], 2)
                    except: pass
        entries = match_data.get("content", {}).get("momentum", {}).get("entries", [])
        if entries:
            recent = entries[-5:]
            hs  = sum(1 for e in recent if e.get("value", 0) > 0)
            as_ = sum(1 for e in recent if e.get("value", 0) < 0)
            if hs >= 4:    result["momentum"] = "Domicile domine"; result["home_dominant"] = True
            elif as_ >= 4: result["momentum"] = "Exterieur domine"; result["away_dominant"] = True
            elif hs >= 3:  result["momentum"] = "Legere dom. Domicile"
            elif as_ >= 3: result["momentum"] = "Legere dom. Exterieur"
        return result
    except Exception as e:
        log.error(f"Erreur FotMob: {e}"); return None

async def fetch_scenario_history(session, league_id, season, score_home, score_away, elapsed):
    try:
        async with session.get(
                f"{BASE_URL_AF}/fixtures", headers=get_headers_af(),
                params={"league": league_id, "season": season,
                        "status": "FT", "last": 200}, timeout=15) as r:
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
        total     = len(similar)
        with_goal = sum(1 for m in similar if m["had_more_goals"])
        conversion = round(with_goal / total * 100, 1)
        goal_mins  = [m["goal_minute"] for m in similar if m["had_more_goals"]]
        avg_min    = round(sum(goal_mins) / len(goal_mins)) if goal_mins else elapsed + 15
        slots = {"0-15": 0, "16-30": 0, "31-45": 0, "46-60": 0, "61-75": 0, "76-90": 0}
        for gm in goal_mins:
            if gm <= 15:   slots["0-15"] += 1
            elif gm <= 30: slots["16-30"] += 1
            elif gm <= 45: slots["31-45"] += 1
            elif gm <= 60: slots["46-60"] += 1
            elif gm <= 75: slots["61-75"] += 1
            else:          slots["76-90"] += 1
        return {"total_matches": total, "matches_with_goal": with_goal,
                "conversion": conversion, "avg_goal_minute": avg_min,
                "dominant_interval": max(slots, key=slots.get)}
    except Exception as e:
        log.error(f"Erreur scenario: {e}"); return None

def get_interval(elapsed):
    if elapsed <= 15:   return "0-15"
    elif elapsed <= 30: return "16-30"
    elif elapsed <= 45: return "31-45"
    elif elapsed <= 60: return "46-60"
    elif elapsed <= 75: return "61-75"
    else:               return "76-90"

def calculate_danger_score(fixture, af_stats, sf_data, xg_us, ws_data,
                           fb_data, fm_data, hist_home, hist_away, scenario):
    score = 0; signals = []
    elapsed = fixture["fixture"]["status"]["elapsed"] or 1
    gh = fixture["goals"]["home"] or 0
    ga = fixture["goals"]["away"] or 0
    lid = fixture["league"]["id"]
    haf = af_stats.get("home", {}); aaf = af_stats.get("away", {})

    # Tirs cadres — 30 pts
    son = haf.get("shots_on", 0) + aaf.get("shots_on", 0)
    st  = haf.get("shots_total", 0) + aaf.get("shots_total", 0)
    spm = son / elapsed
    if spm >= 0.20:    score += 30; signals.append(f"Rythme TRES intense : {son} cadres/{st} tirs")
    elif spm >= 0.12:  score += 22; signals.append(f"Rythme intense : {son}/{st} tirs")
    elif son >= 8:     score += 18; signals.append(f"Tirs cadres : {son}/{st}")
    elif son >= 5:     score += 12; signals.append(f"Tirs cadres : {son}/{st}")
    elif son >= 3:     score += 7;  signals.append(f"Tirs cadres : {son}")
    elif son >= 1:     score += 3

    # Possession — 10 pts
    ph = haf.get("possession", 50); pa = aaf.get("possession", 50)
    mxp = max(ph, pa)
    if mxp >= 70:
        score += 10; dom = "DOM" if ph > pa else "EXT"
        signals.append(f"Possession dominante : {mxp}% ({dom})")
    elif mxp >= 60:
        score += 5

    # Corners — 8 pts
    corners = haf.get("corners", 0) + aaf.get("corners", 0)
    if corners >= 10:  score += 8; signals.append(f"Beaucoup de corners : {corners}")
    elif corners >= 7: score += 5; signals.append(f"Corners : {corners}")
    elif corners >= 4: score += 3; signals.append(f"Corners : {corners}")
    elif corners >= 2: score += 1

    # Score serre — 7 pts
    diff = abs(gh - ga)
    if diff == 0:   score += 7; signals.append(f"Score nul ({gh}-{ga}) pression max")
    elif diff == 1: score += 4; signals.append(f"Score serre ({gh}-{ga})")

    # Scenario historique — 20 pts
    if scenario:
        conv = scenario["conversion"]; tot = scenario["total_matches"]
        if conv >= 85:   score += 20; signals.append(f"Scenario historique : {conv}% ({scenario['matches_with_goal']}/{tot})")
        elif conv >= 70: score += 15; signals.append(f"Scenario historique : {conv}% ({scenario['matches_with_goal']}/{tot})")
        elif conv >= 55: score += 9;  signals.append(f"Scenario historique : {conv}%")
        elif conv >= 40: score += 4

    # Pic de buts ligue — 10 pts
    profile = LEAGUE_PROFILES.get(lid)
    if profile:
        interval = get_interval(elapsed)
        peak_pct = profile.get("goal_peaks", {}).get(interval, 15)
        if peak_pct >= 22:
            score += 10; signals.append(f"Pic de buts max : {peak_pct}% en {interval}min")
        elif peak_pct >= 18:
            score += 7;  signals.append(f"Pic de buts eleve : {peak_pct}% en {interval}min")
        elif peak_pct >= 15:
            score += 4;  signals.append(f"Pic de buts : {peak_pct}% en {interval}min")

    # FORME DES EQUIPES — 15 pts max (nouveau v3.4)
    bonus_form = 0
    for hist in [hist_home, hist_away]:
        if not hist: continue
        if hist.get("over25_pct", 0) >= 70:    bonus_form += 3
        elif hist.get("over25_pct", 0) >= 50:  bonus_form += 1
        if hist.get("btts_pct", 0) >= 70:      bonus_form += 2
        elif hist.get("btts_pct", 0) >= 50:    bonus_form += 1
        wins = hist.get("wins", 0)
        if wins >= 4:   bonus_form += 2
        elif wins >= 3: bonus_form += 1
        if hist.get("scored_avg", 0) >= 2.0:   bonus_form += 2
        elif hist.get("scored_avg", 0) >= 1.5: bonus_form += 1
    if bonus_form > 0:
        bonus_form = min(bonus_form, 15)
        score += bonus_form
        parts = []
        if hist_home:
            parts.append(f"DOM {hist_home.get('form','')} o25={hist_home.get('over25_pct',0)}% btts={hist_home.get('btts_pct',0)}%")
        if hist_away:
            parts.append(f"EXT {hist_away.get('form','')} o25={hist_away.get('over25_pct',0)}% btts={hist_away.get('btts_pct',0)}%")
        signals.append(f"Forme equipes : {' | '.join(parts)} (+{bonus_form}pts)")

    # xG Understat ou FotMob — bonus 20 pts
    xg_data = xg_us
    if not xg_data and fm_data and fm_data.get("xg_total", 0) > 0:
        xg_data = {"home": fm_data["xg_home"], "away": fm_data["xg_away"],
                   "total": fm_data["xg_total"]}
    if xg_data:
        t = xg_data["total"]; src = "Understat" if xg_us else "FotMob"
        if t >= 3.0:   score += 20; signals.append(f"xG tres eleve : {t} ({src})")
        elif t >= 2.0: score += 15; signals.append(f"xG eleve : {t} ({src})")
        elif t >= 1.2: score += 10; signals.append(f"xG : {t} ({src})")
        elif t >= 0.5: score += 5;  signals.append(f"xG : {t} ({src})")
        if max(xg_data["home"], xg_data["away"]) >= 2.0:
            score = min(100, score + 3)
            signals.append(f"Domination xG : {xg_data['home']} vs {xg_data['away']}")

    # SofaScore — bonus 15 pts
    if sf_data:
        dh  = sf_data["home"].get("dangerous_attacks", 0)
        da  = sf_data["away"].get("dangerous_attacks", 0)
        bc  = sf_data["home"].get("big_chances", 0) + sf_data["away"].get("big_chances", 0)
        tb  = sf_data["home"].get("touches_box", 0) + sf_data["away"].get("touches_box", 0)
        dan = dh + da
        if dan >= 30:   score += 8; signals.append(f"Att. dang. : {dan} ({dh}/{da})")
        elif dan >= 20: score += 5; signals.append(f"Att. dang. : {dan}")
        elif dan >= 10: score += 2
        if bc >= 3:  score = min(100, score + 5); signals.append(f"Grosses occasions : {bc}")
        if tb >= 20: score = min(100, score + 2); signals.append(f"Touches surface : {tb}")

    # Momentum FotMob — bonus 10 pts
    if fm_data:
        mom = fm_data.get("momentum", "")
        if fm_data.get("home_dominant") or fm_data.get("away_dominant"):
            score += 10; signals.append(f"Momentum : {mom}")
        elif "legere" in mom.lower():
            score += 5; signals.append(f"Momentum : {mom}")

    # FBref PPDA — bonus 10 pts
    if fb_data:
        hp = fb_data.get("home_ppda"); ap = fb_data.get("away_ppda")
        if hp and hp < 8:    score += 5; signals.append(f"Pressing DOM PPDA {hp} ({fb_data['home_pressing']})")
        elif hp and hp < 11: score += 2
        if ap and ap < 8:    score += 5; signals.append(f"Pressing EXT PPDA {ap} ({fb_data['away_pressing']})")
        elif ap and ap < 11: score += 2

    # WhoScored — bonus 8 pts
    if ws_data:
        hr = ws_data.get("home_rating", 0); ar = ws_data.get("away_rating", 0)
        mx = max(hr, ar)
        if mx >= 7.5:   score += 8; signals.append(f"Ratings : {hr}/{ar}")
        elif mx >= 7.0: score += 5; signals.append(f"Ratings : {hr}/{ar}")
        elif mx >= 6.5: score += 2
        hf = ws_data.get("home_form", ""); af2 = ws_data.get("away_form", "")
        if hf.count("W") >= 3 or af2.count("W") >= 3:
            score = min(100, score + 2); signals.append(f"Forme WhoScored: {hf}/{af2}")

    return min(score, 100), signals

def entry_window(elapsed, ds):
    es = elapsed + 2; ee = elapsed + 5
    we = elapsed + 15 if ds >= 75 else elapsed + 20 if ds >= 55 else elapsed + 25
    return f"{es}-{ee}min", f"{es}-{we}min"

def calc_probs(ds, elapsed, scenario):
    b    = ds / 100
    p15  = min(85, int(b * 65))
    p30  = min(92, int(b * 78))
    ptot = min(96, int(b * 90 + ((90 - elapsed) / 90) * 15))
    if scenario and scenario["conversion"] > 0:
        ptot = min(96, int((ptot / 100 * 0.6 + scenario["conversion"] / 100 * 0.4) * 100))
    return {"15min": p15, "30min": p30, "total": ptot}

def target_odds(ds):
    if ds >= 75:   return "1.40-1.65"
    elif ds >= 60: return "1.55-1.80"
    elif ds >= 45: return "1.70-2.10"
    return "1.85-2.30"

def danger_level(s):
    if s >= 75:   return "TRES ELEVE"
    elif s >= 60: return "ELEVE"
    elif s >= 45: return "MODERE"
    return "FAIBLE"

def should_alert(fid, score):
    if score < DANGER_THRESHOLD: return False
    return (datetime.now().timestamp() - last_alert_time.get(fid, 0)) >= ALERT_COOLDOWN

def format_alert(fixture, af_stats, sf_data, xg_us, ws_data, fb_data,
                 fm_data, ds, signals, hist_home, hist_away, scenario):
    now     = datetime.now().strftime("%H:%M:%S")
    lid     = fixture["league"]["id"]
    elapsed = fixture["fixture"]["status"]["elapsed"] or 0
    home    = fixture["teams"]["home"]["name"]
    away    = fixture["teams"]["away"]["name"]
    gh      = fixture["goals"]["home"] or 0
    ga      = fixture["goals"]["away"] or 0
    league  = LEAGUES.get(lid, fixture["league"]["name"])
    label   = "MT" if fixture["fixture"]["status"]["short"] == "HT" else f"{elapsed}min"
    haf = af_stats.get("home", {}); aaf = af_stats.get("away", {})
    entry, window = entry_window(elapsed, ds)
    probs = calc_probs(ds, elapsed, scenario)

    if xg_us:
        xg_line = f"xG {xg_us['home']}-{xg_us['away']} = {xg_us['total']} (Understat)"
    elif fm_data and fm_data.get("xg_total", 0) > 0:
        xg_line = f"xG {fm_data['xg_home']}-{fm_data['xg_away']} = {fm_data['xg_total']} (FotMob)"
    else:
        xg_line = "xG : N/A"

    sf_line = ""
    if sf_data:
        dh = sf_data["home"].get("dangerous_attacks", 0); da = sf_data["away"].get("dangerous_attacks", 0)
        th = sf_data["home"].get("touches_box", 0); ta = sf_data["away"].get("touches_box", 0)
        bc = sf_data["home"].get("big_chances", 0) + sf_data["away"].get("big_chances", 0)
        sf_line = f"\nAtt.dang: {dh}-{da} | Surface: {th}-{ta} | Occ: {bc}"

    fb_line = ""
    if fb_data:
        fb_line = (f"\nPPDA: {fb_data.get('home_ppda','N/A')} "
                   f"({fb_data.get('home_pressing','')})-"
                   f"{fb_data.get('away_ppda','N/A')} ({fb_data.get('away_pressing','')})")

    ws_line = ""
    if ws_data:
        ws_line = (f"\nRatings: {ws_data.get('home_rating',0)}-"
                   f"{ws_data.get('away_rating',0)} | "
                   f"Forme WS: {ws_data.get('home_form','')} / {ws_data.get('away_form','')}")

    fm_line = f"\nMomentum: {fm_data['momentum']}" if fm_data else ""

    peak_line = ""
    if profile := LEAGUE_PROFILES.get(lid):
        interval = get_interval(elapsed)
        pct = profile.get("goal_peaks", {}).get(interval, 0)
        peak_line = f"\nPic de buts {interval}min : {pct}% des buts dans cette ligue"

    scenario_block = ""
    if scenario:
        scenario_block = (
            f"\n\nRECONSTRUCTION HISTORIQUE\n"
            f"Matchs analyses : {scenario['total_matches']}\n"
            f"Matchs avec but : {scenario['matches_with_goal']}\n"
            f"Conversion : {scenario['conversion']}%\n"
            f"Minute moyenne : {scenario['avg_goal_minute']}min\n"
            f"Intervalle dominant : {scenario['dominant_interval']}min"
        )

    def fmt_hist(h, label):
        if not h: return f"{label}: N/A"
        return (f"{label}: {h.get('form','?')} | "
                f"moy {h.get('avg5','?')} buts/match | "
                f"marques {h.get('scored_avg','?')} encaisses {h.get('conceded_avg','?')} | "
                f"over2.5={h.get('over25_pct',0)}% btts={h.get('btts_pct',0)}%")

    hist = fmt_hist(hist_home, "DOM") + "\n" + fmt_hist(hist_away, "EXT")
    signals_text = "\n".join([f"  - {s}" for s in signals]) if signals else "  - Analyse..."

    return f"""BUT IMMINENT DETECTE

{league}
{home} vs {away}
Score: {gh}-{ga} | {label}

SCORE : {ds}/100 - {danger_level(ds)}

SIGNAUX :
{signals_text}

STATS (6 sources)
{xg_line}
Tirs: {haf.get('shots_on',0)}/{haf.get('shots_total',0)}-{aaf.get('shots_on',0)}/{aaf.get('shots_total',0)}
Poss: {haf.get('possession',50)}%-{aaf.get('possession',50)}%
Corners: {haf.get('corners',0)}-{aaf.get('corners',0)}{sf_line}{fb_line}{ws_line}{fm_line}{peak_line}{scenario_block}

HISTORIQUE (5 derniers matchs)
{hist}

Point entree : {entry} | Cote cible : {target_odds(ds)}
Intervalle but attendu : {window}
Proba 70min : {probs['30min']}% | Fin match : {probs['total']}%

PARI OPTIMAL : Prochain but - entree {entry}
Responsable | {now}""".strip()

async def send_message(bot, text):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)
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

                    if status not in ["1H", "2H", "HT"]:
                        continue

                    (af_stats, xg_us, hist_home, hist_away,
                     sf_data, fm_data) = await asyncio.gather(
                        fetch_live_stats_af(session, fid),
                        fetch_xg_understat(session, lid, home_name, away_name),
                        fetch_team_history_af(session, home_id, lid, season),
                        fetch_team_history_af(session, away_id, lid, season),
                        fetch_sofascore_data(session, home_name, away_name),
                        fetch_fotmob_data(session, home_name, away_name),
                    )

                    ws_data  = await fetch_whoscored_data(session, home_name, away_name)
                    fb_data  = await fetch_fbref_pressing(session, lid, home_name, away_name)
                    scenario = await fetch_scenario_history(
                        session, lid, season, gh, ga, elapsed)

                    if not af_stats:
                        continue

                    ds, signals = calculate_danger_score(
                        fixture, af_stats, sf_data, xg_us, ws_data,
                        fb_data, fm_data, hist_home, hist_away, scenario)

                    son_total = af_stats.get("home",{}).get("shots_on",0) + af_stats.get("away",{}).get("shots_on",0)
                    st_total  = af_stats.get("home",{}).get("shots_total",0) + af_stats.get("away",{}).get("shots_total",0)
                    cor_total = af_stats.get("home",{}).get("corners",0) + af_stats.get("away",{}).get("corners",0)
                    pos_h     = af_stats.get("home",{}).get("possession",50)
                    h_form    = hist_home.get("form","?") if hist_home else "?"
                    a_form    = hist_away.get("form","?") if hist_away else "?"

                    log.info(
                        f"{home_name} vs {away_name} | {elapsed}min | {ds}/100\n"
                        f"   OK AF tirs={son_total}/{st_total} corners={cor_total} poss={pos_h}%\n"
                        f"   {'OK Understat xG='+str(xg_us['total']) if xg_us else '-- Understat'}\n"
                        f"   {'OK FotMob xG='+str(fm_data['xg_total']) if fm_data and fm_data.get('xg_total',0)>0 else ('OK FotMob momentum' if fm_data else '-- FotMob')}\n"
                        f"   {'OK SofaScore att='+str(sf_data['home'].get('dangerous_attacks',0)+sf_data['away'].get('dangerous_attacks',0)) if sf_data else '-- SofaScore'}\n"
                        f"   {'OK WhoScored '+str(ws_data.get('home_rating',0))+'/'+str(ws_data.get('away_rating',0)) if ws_data else '-- WhoScored'}\n"
                        f"   {'OK FBref PPDA='+str(fb_data.get('home_ppda','?'))+'/'+str(fb_data.get('away_ppda','?')) if fb_data else '-- FBref'}\n"
                        f"   {'OK Scenario '+str(scenario['conversion'])+'% ('+str(scenario['matches_with_goal'])+'/'+str(scenario['total_matches'])+')' if scenario else '-- Scenario'}\n"
                        f"   Forme DOM={h_form} EXT={a_form}"
                    )

                    if should_alert(fid, ds):
                        log.info(f"ALERTE : {home_name} vs {away_name} -- {ds}/100")
                        await send_message(bot, format_alert(
                            fixture, af_stats, sf_data, xg_us, ws_data,
                            fb_data, fm_data, ds, signals,
                            hist_home, hist_away, scenario))
                        last_alert_time[fid] = datetime.now().timestamp()

                    await asyncio.sleep(1)

            except Exception as e:
                log.error(f"Erreur boucle: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=(
            "BOT PREDICTION BUT v3.4\n\n"
            "7 sources actives :\n"
            "- API-Football (stats live + historique equipes)\n"
            "- Understat (xG top 5)\n"
            "- SofaScore (att. dang., occasions)\n"
            "- WhoScored (ratings, forme)\n"
            "- FBref (PPDA pressing)\n"
            "- FotMob (xG toutes ligues + momentum)\n\n"
            "Nouveautes v3.4 :\n"
            "- Forme des equipes : W/D/L sur 5 matchs\n"
            "- % over2.5 et BTTS sur 10 derniers matchs\n"
            "- Moyenne buts marques/encaisses\n"
            "- +15pts bonus si equipes en forme offensive\n"
            "- 63 ligues surveillees\n\n"
            f"Seuil : {DANGER_THRESHOLD}/100"
        )
    )
    await monitor_loop(bot)

if __name__ == "__main__":
    asyncio.run(main())
