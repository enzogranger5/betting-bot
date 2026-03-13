"""
⚽ BOT PRÉDICTION BUT IMMINENT - VERSION 3.1
Seuil abaissé à 45/100 — scoring réajusté pour fonctionner
même quand les sources de scraping échouent
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
    # Coupes européennes
    2:  {"name":"Champions League","avg_goals":2.9,"2nd_half_pct":56,
         "goal_peaks":{"0-15":12,"16-30":16,"31-45":18,"46-60":18,"61-75":20,"76-90":16}},
    3:  {"name":"Europa League","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    848:{"name":"Conference League","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    # Top 5 Europe
    39: {"name":"Premier League","avg_goals":2.8,"2nd_half_pct":58,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    40: {"name":"Championship","avg_goals":2.7,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":21,"76-90":19}},
    140:{"name":"La Liga","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    141:{"name":"La Liga 2","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    135:{"name":"Serie A","avg_goals":2.5,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    136:{"name":"Serie B","avg_goals":2.4,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    78: {"name":"Bundesliga","avg_goals":3.1,"2nd_half_pct":57,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    79: {"name":"2. Bundesliga","avg_goals":2.9,"2nd_half_pct":56,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    61: {"name":"Ligue 1","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    62: {"name":"Ligue 2","avg_goals":2.4,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    # Autres ligues Europe
    88: {"name":"Eredivisie","avg_goals":3.2,"2nd_half_pct":57,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
    94: {"name":"Primeira Liga","avg_goals":2.6,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    144:{"name":"Jupiler Pro League","avg_goals":2.9,"2nd_half_pct":57,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":16,"46-60":19,"61-75":21,"76-90":18}},
    119:{"name":"Superliga DK","avg_goals":2.7,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    103:{"name":"Eliteserien","avg_goals":2.8,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    113:{"name":"Allsvenskan","avg_goals":2.7,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    106:{"name":"Ekstraklasa","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":16,"46-60":20,"61-75":22,"76-90":19}},
    235:{"name":"Premier League RU","avg_goals":2.5,"2nd_half_pct":54,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":19}},
    98: {"name":"Super Lig","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    218:{"name":"Super League GR","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    207:{"name":"Premiership SCO","avg_goals":2.6,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    # Amérique du Sud
    71: {"name":"Brasileirao A","avg_goals":2.7,"2nd_half_pct":57,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    72: {"name":"Brasileirao B","avg_goals":2.5,"2nd_half_pct":56,
         "goal_peaks":{"0-15":10,"16-30":13,"31-45":15,"46-60":20,"61-75":23,"76-90":19}},
    128:{"name":"Liga Profesional AR","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    265:{"name":"Primera División CL","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    266:{"name":"Primera División CO","avg_goals":2.5,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    # Amérique du Nord
    262:{"name":"Liga MX","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    239:{"name":"MLS","avg_goals":2.9,"2nd_half_pct":57,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    # Moyen-Orient / Asie
    307:{"name":"Saudi Pro League","avg_goals":3.0,"2nd_half_pct":56,
         "goal_peaks":{"0-15":12,"16-30":15,"31-45":16,"46-60":19,"61-75":21,"76-90":17}},
    323:{"name":"UAE Pro League","avg_goals":2.8,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    318:{"name":"Qatar Stars","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    154:{"name":"Chinese Super League","avg_goals":2.8,"2nd_half_pct":56,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    # Coupes nationales
    45: {"name":"FA Cup","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    143:{"name":"Copa del Rey","avg_goals":2.8,"2nd_half_pct":56,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    137:{"name":"Coppa Italia","avg_goals":2.6,"2nd_half_pct":55,
         "goal_peaks":{"0-15":10,"16-30":14,"31-45":16,"46-60":20,"61-75":22,"76-90":18}},
    65: {"name":"Coupe de France","avg_goals":2.7,"2nd_half_pct":55,
         "goal_peaks":{"0-15":11,"16-30":14,"31-45":16,"46-60":19,"61-75":22,"76-90":18}},
    81: {"name":"DFB Pokal","avg_goals":3.0,"2nd_half_pct":57,
         "goal_peaks":{"0-15":11,"16-30":15,"31-45":17,"46-60":19,"61-75":21,"76-90":17}},
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
        goals = [(f["goals"]["home"] or 0) + (f["goals"]["away"] or 0) for f in fixtures]
        return {"avg": round(sum(goals) / len(goals), 1), "fixtures": fixtures}
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
            if p < 8:  return "🔥 Intense"
            if p < 11: return "💪 Bon"
            if p < 14: return "➡️ Moyen"
            return "⚠️ Faible"
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
                  "momentum": "⚖️ Équilibré", "home_dominant": False, "away_dominant": False}
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
            if hs >= 4:    result["momentum"] = "🏠 Domicile domine fortement"; result["home_dominant"] = True
            elif as_ >= 4: result["momentum"] = "✈️ Extérieur domine fortement"; result["away_dominant"] = True
            elif hs >= 3:  result["momentum"] = "🏠 Légère domination Domicile"
            elif as_ >= 3: result["momentum"] = "✈️ Légère domination Extérieur"
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
        total      = len(similar)
        with_goal  = sum(1 for m in similar if m["had_more_goals"])
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
        log.error(f"Erreur scénario: {e}"); return None

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

    # Tirs cadrés — 30 pts
    son = haf.get("shots_on", 0) + aaf.get("shots_on", 0)
    st  = haf.get("shots_total", 0) + aaf.get("shots_total", 0)
    spm = son / elapsed
    if spm >= 0.20:   score += 30; signals.append(f"🔫 Rythme TRÈS intense : {son} cadrés/{st} tirs")
    elif spm >= 0.12: score += 22; signals.append(f"🔫 Rythme intense : {son}/{st} tirs")
    elif son >= 8:    score += 18; signals.append(f"🔫 Tirs cadrés : {son}/{st}")
    elif son >= 5:    score += 12; signals.append(f"🔫 Tirs cadrés : {son}/{st}")
    elif son >= 3:    score += 7;  signals.append(f"🔫 Tirs cadrés : {son}")
    elif son >= 1:    score += 3

    # Possession — 10 pts
    ph = haf.get("possession", 50); pa = aaf.get("possession", 50)
    mxp = max(ph, pa)
    if mxp >= 70:
        score += 10; dom = "DOM" if ph > pa else "EXT"
        signals.append(f"⚡ Possession dominante : {mxp}% ({dom})")
    elif mxp >= 60:
        score += 5

    # Corners — 8 pts
    corners = haf.get("corners", 0) + aaf.get("corners", 0)
    if corners >= 10:  score += 8; signals.append(f"📐 Beaucoup de corners : {corners}")
    elif corners >= 7: score += 5; signals.append(f"📐 Corners : {corners}")
    elif corners >= 4: score += 3; signals.append(f"📐 Corners : {corners}")
    elif corners >= 2: score += 1

    # Score serré — 7 pts
    diff = abs(gh - ga)
    if diff == 0:   score += 7; signals.append(f"⚖️ Score nul ({gh}-{ga}) — pression max")
    elif diff == 1: score += 4; signals.append(f"⚖️ Score serré ({gh}-{ga})")

    # Historique scénario — 20 pts
    if scenario:
        conv = scenario["conversion"]; tot = scenario["total_matches"]
        if conv >= 85:   score += 20; signals.append(f"📊 Scénario historique : {conv}% ({scenario['matches_with_goal']}/{tot} matchs)")
        elif conv >= 70: score += 15; signals.append(f"📊 Scénario historique : {conv}% ({scenario['matches_with_goal']}/{tot})")
        elif conv >= 55: score += 9;  signals.append(f"📊 Scénario historique : {conv}%")
        elif conv >= 40: score += 4

    # Pic de buts ligue — 10 pts
    profile = LEAGUE_PROFILES.get(lid)
    if profile:
        interval = get_interval(elapsed)
        peak_pct = profile.get("goal_peaks", {}).get(interval, 15)
        if peak_pct >= 22:
            score += 10; signals.append(f"📈 Pic de buts maximum : {peak_pct}% en {interval}' dans cette ligue")
        elif peak_pct >= 18:
            score += 7;  signals.append(f"📈 Pic de buts élevé : {peak_pct}% en {interval}'")
        elif peak_pct >= 15:
            score += 4;  signals.append(f"📈 Pic de buts : {peak_pct}% en {interval}'")

    # BONUS — xG Understat/FotMob
    xg_data = xg_us
    if not xg_data and fm_data and fm_data.get("xg_total", 0) > 0:
        xg_data = {"home": fm_data["xg_home"], "away": fm_data["xg_away"],
                   "total": fm_data["xg_total"]}
    if xg_data:
        t = xg_data["total"]; src = "Understat" if xg_us else "FotMob"
        if t >= 3.0:   score += 20; signals.append(f"🎯 xG très élevé : {t} ({src})")
        elif t >= 2.0: score += 15; signals.append(f"🎯 xG élevé : {t} ({src})")
        elif t >= 1.2: score += 10; signals.append(f"🎯 xG : {t} ({src})")
        elif t >= 0.5: score += 5;  signals.append(f"🎯 xG : {t} ({src})")
        if max(xg_data["home"], xg_data["away"]) >= 2.0:
            score = min(100, score + 3)
            signals.append(f"⚡ Domination xG : {xg_data['home']} vs {xg_data['away']}")

    # BONUS — SofaScore
    if sf_data:
        dh  = sf_data["home"].get("dangerous_attacks", 0)
        da  = sf_data["away"].get("dangerous_attacks", 0)
        bc  = sf_data["home"].get("big_chances", 0) + sf_data["away"].get("big_chances", 0)
        tb  = sf_data["home"].get("touches_box", 0) + sf_data["away"].get("touches_box", 0)
        dan = dh + da
        if dan >= 30:   score += 8; signals.append(f"🔥 Att. dang. : {dan} ({dh}/{da})")
        elif dan >= 20: score += 5; signals.append(f"🔥 Att. dang. : {dan}")
        elif dan >= 10: score += 2
        if bc >= 3:  score = min(100, score + 5); signals.append(f"💥 Grosses occasions : {bc}")
        if tb >= 20: score = min(100, score + 2); signals.append(f"📦 Touches surface : {tb}")

    # BONUS — Momentum FotMob
    if fm_data:
        mom = fm_data.get("momentum", "")
        if fm_data.get("home_dominant") or fm_data.get("away_dominant"):
            score += 10; signals.append(f"🌊 {mom}")
        elif "légère" in mom.lower():
            score += 5; signals.append(f"🌊 {mom}")

    # BONUS — FBref PPDA
    if fb_data:
        hp = fb_data.get("home_ppda"); ap = fb_data.get("away_ppda")
        if hp and hp < 8:    score += 5; signals.append(f"⚡ Pressing DOM PPDA {hp} ({fb_data['home_pressing']})")
        elif hp and hp < 11: score += 2
        if ap and ap < 8:    score += 5; signals.append(f"⚡ Pressing EXT PPDA {ap} ({fb_data['away_pressing']})")
        elif ap and ap < 11: score += 2

    # BONUS — WhoScored
    if ws_data:
        hr = ws_data.get("home_rating", 0); ar = ws_data.get("away_rating", 0)
        mx = max(hr, ar)
        if mx >= 7.5:   score += 8; signals.append(f"⭐ Ratings : {hr}/{ar}")
        elif mx >= 7.0: score += 5; signals.append(f"⭐ Ratings : {hr}/{ar}")
        elif mx >= 6.5: score += 2
        hf = ws_data.get("home_form", ""); af2 = ws_data.get("away_form", "")
        if hf.count("W") >= 3 or af2.count("W") >= 3:
            score = min(100, score + 2); signals.append(f"📈 Forme : {hf}/{af2}")

    return min(score, 100), signals

def entry_window(elapsed, ds):
    es = elapsed + 2; ee = elapsed + 5
    we = elapsed + 15 if ds >= 75 else elapsed + 20 if ds >= 55 else elapsed + 25
    return f"{es}-{ee}'", f"{es}-{we}'"

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
    if s >= 75:   return "🔴 TRÈS ÉLEVÉ"
    elif s >= 60: return "🟠 ÉLEVÉ"
    elif s >= 45: return "🟡 MODÉRÉ"
    return "🟢 FAIBLE"

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
    label   = "MT" if fixture["fixture"]["status"]["short"] == "HT" else f"{elapsed}'"
    haf = af_stats.get("home", {}); aaf = af_stats.get("away", {})
    entry, window = entry_window(elapsed, ds)
    probs = calc_probs(ds, elapsed, scenario)

    if xg_us:
        xg_line = f"xG {xg_us['home']}—{xg_us['away']} = *{xg_us['total']}* (Understat)"
    elif fm_data and fm_data.get("xg_total", 0) > 0:
        xg_line = f"xG {fm_data['xg_home']}—{fm_data['xg_away']} = *{fm_data['xg_total']}* (FotMob)"
    else:
        xg_line = "xG : N/A"

    sf_line = ""
    if sf_data:
        dh = sf_data["home"].get("dangerous_attacks", 0); da = sf_data["away"].get("dangerous_attacks", 0)
        th = sf_data["home"].get("touches_box", 0); ta = sf_data["away"].get("touches_box", 0)
        bc = sf_data["home"].get("big_chances", 0) + sf_data["away"].get("big_chances", 0)
        sf_line = f"\n🔥 Att.dang: {dh}—{da} | 📦 Surface: {th}—{ta} | 💥 Occ: {bc}"

    fb_line = ""
    if fb_data:
        fb_line = (f"\n⚡ PPDA: {fb_data.get('home_ppda','N/A')} "
                   f"({fb_data.get('home_pressing','')})—"
                   f"{fb_data.get('away_ppda','N/A')} ({fb_data.get('away_pressing','')})")

    ws_line = ""
    if ws_data:
        ws_line = (f"\n⭐ Ratings: {ws_data.get('home_rating',0)}—"
                   f"{ws_data.get('away_rating',0)} | "
                   f"Forme: {ws_data.get('home_form','')} / {ws_data.get('away_form','')}")

    fm_line = f"\n🌊 {fm_data['momentum']}" if fm_data else ""

    peak_line = ""
    if profile := LEAGUE_PROFILES.get(lid):
        interval = get_interval(elapsed)
        pct = profile.get("goal_peaks", {}).get(interval, 0)
        peak_line = f"\n📈 Pic de buts {interval}' : {pct}% des buts dans cette ligue"

    scenario_block = ""
    if scenario:
        scenario_block = (
            f"\n\n📊 *RECONSTRUCTION HISTORIQUE*\n"
            f"Nombre total de matchs : {scenario['total_matches']}\n"
            f"Matchs avec but : {scenario['matches_with_goal']}\n"
            f"Conversion : *{scenario['conversion']}%*\n"
            f"Minute moyenne du premier but : {scenario['avg_goal_minute']}'\n"
            f"Intervalle dominant : {scenario['dominant_interval']}'"
        )

    signals_text = "\n".join([f"  • {s}" for s in signals]) if signals else "  • Analyse..."
    avg_h = hist_home["avg"] if hist_home else "N/A"
    avg_a = hist_away["avg"] if hist_away else "N/A"
    hist  = f"{avg_h} / {avg_a} buts/match" if avg_h != "N/A" else "N/A"

    return f"""⚡ *BUT IMMINENT DÉTECTÉ* ⚡

🏆 {league}
⚽ *{home} vs {away}*
📊 `{gh}-{ga}` | ⏱️ {label}

━━━━━━━━━━━━━━━━━━
🔥 *SCORE : {ds}/100* — {danger_level(ds)}

━━━━━━━━━━━━━━━━━━
📈 *SIGNAUX :*
{signals_text}

━━━━━━━━━━━━━━━━━━
📊 *STATS (6 sources)*
🎯 {xg_line}
🔫 Tirs: {haf.get('shots_on',0)}/{haf.get('shots_total',0)}—{aaf.get('shots_on',0)}/{aaf.get('shots_total',0)}
⚡ Poss: {haf.get('possession',50)}%—{aaf.get('possession',50)}%
📐 Corners: {haf.get('corners',0)}—{aaf.get('corners',0)}{sf_line}{fb_line}{ws_line}{fm_line}{peak_line}{scenario_block}

📊 Historique: {hist}

━━━━━━━━━━━━━━━━━━
🎯 Point d'entrée : {entry} | ⚡ Cote cible : {target_odds(ds)}
⏱️ Intervalle de but attendu : {window}
📈 Probabilités : jusqu'à la 70' → *{probs['30min']}%* | Jusqu'à la fin → *{probs['total']}%*

💰 *PARI OPTIMAL :* `Prochain but — entrée {entry}`
⚠️ _Responsable_ | 🕐 _{now}_""".strip()

async def send_message(bot, text):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text,
                               parse_mode=ParseMode.MARKDOWN)
        log.info("✅ Alerte envoyée")
    except Exception as e:
        log.error(f"Erreur Telegram: {e}")

async def monitor_loop(bot):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                log.info(f"🔍 Scan... {datetime.now().strftime('%H:%M:%S')}")
                fixtures = await fetch_live_fixtures(session)
                targets  = [f for f in fixtures if f["league"]["id"] in LEAGUES]
                log.info(f"📡 {len(targets)} matchs en direct")

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

                    # ── LOG DIAGNOSTIC ────────────────────────────────────────
                    son_total = af_stats.get("home",{}).get("shots_on",0) + af_stats.get("away",{}).get("shots_on",0)
                    st_total  = af_stats.get("home",{}).get("shots_total",0) + af_stats.get("away",{}).get("shots_total",0)
                    cor_total = af_stats.get("home",{}).get("corners",0) + af_stats.get("away",{}).get("corners",0)
                    pos_h     = af_stats.get("home",{}).get("possession",50)

                    af_line = f"✅ AF tirs={son_total}/{st_total} corners={cor_total} poss={pos_h}%"
                    us_line = f"✅ Understat xG={xg_us['total']}" if xg_us else "❌ Understat"
                    fm_line = f"✅ FotMob xG={fm_data['xg_total']} mom={fm_data['momentum']}" if fm_data and fm_data.get("xg_total",0)>0 else ("✅ FotMob (momentum only)" if fm_data else "❌ FotMob")
                    sf_line = f"✅ SofaScore att.dang={sf_data['home'].get('dangerous_attacks',0)+sf_data['away'].get('dangerous_attacks',0)}" if sf_data else "❌ SofaScore"
                    ws_line = f"✅ WhoScored ratings={ws_data.get('home_rating',0)}/{ws_data.get('away_rating',0)}" if ws_data else "❌ WhoScored"
                    fb_line = f"✅ FBref PPDA={fb_data.get('home_ppda','?')}/{fb_data.get('away_ppda','?')}" if fb_data else "❌ FBref"
                    sc_line = f"✅ Scénario conv={scenario['conversion']}% ({scenario['matches_with_goal']}/{scenario['total_matches']})" if scenario else "❌ Scénario historique"

                    log.info(
                        f"📊 {home_name} vs {away_name} | {elapsed}' | {ds}/100\n"
                        f"   {af_line}\n"
                        f"   {us_line}\n"
                        f"   {fm_line}\n"
                        f"   {sf_line}\n"
                        f"   {ws_line}\n"
                        f"   {fb_line}\n"
                        f"   {sc_line}"
                    )

                    if should_alert(fid, ds):
                        log.info(f"🚨 ALERTE : {home_name} vs {away_name} — {ds}/100")
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
            "🤖 *BOT PRÉDICTION BUT v3.2* ✅\n\n"
            "🧠 *6 sources :*\n"
            "  • API-Football (tirs, possession, corners)\n"
            "  • Understat (xG top 5 ligues)\n"
            "  • SofaScore (att. dang., touches surface)\n"
            "  • WhoScored (ratings, forme)\n"
            "  • FBref (PPDA pressing)\n"
            "  • FotMob (xG toutes ligues + momentum)\n\n"
            "🆕 *Nouveautés v3.2 :*\n"
            "  • 📊 Log diagnostic par match\n"
            "  • 🌍 37 ligues dans LEAGUE_PROFILES\n"
            "  • ⚙️ Seuil xG abaissé à 0.5\n\n"
            f"🎯 Seuil : {DANGER_THRESHOLD}/100\n"
            f"🏆 {len(LEAGUES)} ligues surveillées"
        ),
        parse_mode=ParseMode.MARKDOWN
    )
    await monitor_loop(bot)

if __name__ == "__main__":
    asyncio.run(main())
