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
DANGER_THRESHOLD = 70
ALERT_COOLDOWN   = 600
BASE_URL_AF = "https://v3.football.api-sports.io"
BASE_URL_US = "https://understat.com"

UNDERSTAT_LEAGUES = {
    39:"EPL",140:"La_liga",135:"Serie_A",78:"Bundesliga",61:"Ligue_1",235:"RFPL"
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

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO,
    handlers=[logging.FileHandler("goal_bot.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)
last_alert_time: dict = {}

def get_headers_af():
    return {"x-apisports-key": API_FOOTBALL_KEY}

async def fetch_live_fixtures(session):
    try:
        async with session.get(f"{BASE_URL_AF}/fixtures", headers=get_headers_af(),
                               params={"live":"all"}, timeout=10) as r:
            if r.status == 200:
                return (await r.json()).get("response", [])
            return []
    except Exception as e:
        log.error(f"Erreur fixtures: {e}")
        return []

async def fetch_live_stats(session, fixture_id):
    try:
        async with session.get(f"{BASE_URL_AF}/fixtures/statistics", headers=get_headers_af(),
                               params={"fixture": fixture_id}, timeout=10) as r:
            if r.status == 200:
                return parse_live_stats((await r.json()).get("response", []))
            return {}
    except Exception as e:
        log.error(f"Erreur stats {fixture_id}: {e}")
        return {}

async def fetch_team_history(session, team_id, league_id, season):
    try:
        params = {"team": team_id, "league": league_id, "season": season, "last": 5}
        async with session.get(f"{BASE_URL_AF}/fixtures", headers=get_headers_af(),
                               params=params, timeout=10) as r:
            if r.status != 200: return None
            fixtures = (await r.json()).get("response", [])
        if not fixtures: return None
        goals = [(f["goals"]["home"] or 0) + (f["goals"]["away"] or 0) for f in fixtures]
        return round(sum(goals) / len(goals), 1)
    except Exception as e:
        log.error(f"Erreur historique {team_id}: {e}")
        return None

def parse_live_stats(stats_response):
    result = {
        "home":{"shots_total":0,"shots_on":0,"possession":50,"corners":0,"fouls":0},
        "away":{"shots_total":0,"shots_on":0,"possession":50,"corners":0,"fouls":0}
    }
    mapping = {"Total Shots":"shots_total","Shots on Goal":"shots_on",
               "Ball Possession":"possession","Corner Kicks":"corners","Fouls":"fouls"}
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
    league_name = UNDERSTAT_LEAGUES.get(league_id)
    if not league_name: return None
    try:
        season = datetime.now().year if datetime.now().month >= 7 else datetime.now().year - 1
        url = f"{BASE_URL_US}/league/{league_name}/{season}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        async with session.get(url, headers=headers, timeout=15) as r:
            if r.status != 200: return None
            html = await r.text()
        match = re.search(r"datesData\s*=\s*JSON\.parse\('(.+?)'\)", html)
        if not match: return None
        raw = match.group(1).encode().decode("unicode_escape")
        matches = json.loads(raw)
        home_lower = home_team.lower()
        away_lower = away_team.lower()
        for m in matches:
            h = m.get("h", {}).get("title", "").lower()
            a = m.get("a", {}).get("title", "").lower()
            if (home_lower[:4] in h or h[:4] in home_lower) and \
               (away_lower[:4] in a or a[:4] in away_lower):
                xg_h = float(m.get("xG", {}).get("h", 0) or 0)
                xg_a = float(m.get("xG", {}).get("a", 0) or 0)
                return {"home": round(xg_h,2), "away": round(xg_a,2), "total": round(xg_h+xg_a,2)}
        return None
    except Exception as e:
        log.error(f"Erreur Understat: {e}")
        return None

def calculate_danger_score(fixture, live_stats, xg_data, avg_goals_home, avg_goals_away):
    score = 0
    signals = []
    elapsed = fixture["fixture"]["status"]["elapsed"] or 1
    home_s = live_stats.get("home", {})
    away_s = live_stats.get("away", {})

    # xG (25 pts)
    if xg_data:
        xg_total = xg_data["total"]
        if xg_total >= 3.0: score += 25; signals.append(f"🎯 xG total très élevé : {xg_total}")
        elif xg_total >= 2.0: score += 18; signals.append(f"🎯 xG total élevé : {xg_total}")
        elif xg_total >= 1.0: score += 10; signals.append(f"🎯 xG : {xg_total}")
        if max(xg_data["home"], xg_data["away"]) >= 2.0:
            score = min(score+5, score+5); signals.append(f"⚡ Domination xG : {xg_data['home']} vs {xg_data['away']}")

    # Tirs (20 pts)
    shots_on = home_s.get("shots_on",0) + away_s.get("shots_on",0)
    shots_total = home_s.get("shots_total",0) + away_s.get("shots_total",0)
    shots_per_min = shots_on / elapsed
    if shots_per_min >= 0.15: score += 20; signals.append(f"🔫 Pression tirs intense : {shots_on} cadrés/{shots_total} total")
    elif shots_on >= 8: score += 16; signals.append(f"🔫 Beaucoup de tirs : {shots_on} cadrés")
    elif shots_on >= 5: score += 10; signals.append(f"🔫 Tirs cadrés : {shots_on}")
    elif shots_on >= 3: score += 5

    # Possession (15 pts)
    pos_h = home_s.get("possession",50)
    pos_a = away_s.get("possession",50)
    max_pos = max(pos_h, pos_a)
    if max_pos >= 70:
        score += 15
        dom = "Domicile" if pos_h > pos_a else "Extérieur"
        signals.append(f"⚡ Domination possession : {max_pos}% ({dom})")
    elif max_pos >= 60: score += 8

    # Corners (15 pts)
    corners = home_s.get("corners",0) + away_s.get("corners",0)
    if corners >= 10: score += 15; signals.append(f"📐 Corners : {corners} (pression constante)")
    elif corners >= 7: score += 10; signals.append(f"📐 Corners : {corners}")
    elif corners >= 4: score += 5

    # Historique (15 pts)
    if avg_goals_home and avg_goals_away:
        avg = (avg_goals_home + avg_goals_away) / 2
        if avg >= 3.0: score += 15; signals.append(f"📊 Historique : {avg} buts/match en moyenne")
        elif avg >= 2.5: score += 10; signals.append(f"📊 Historique : {avg} buts/match")
        elif avg >= 2.0: score += 6

    # Score serré (10 pts)
    gh = fixture["goals"]["home"] or 0
    ga = fixture["goals"]["away"] or 0
    diff = abs(gh - ga)
    if diff == 0: score += 10; signals.append(f"⚖️ Match nul ({gh}-{ga}) → les deux équipes poussent")
    elif diff == 1: score += 6; signals.append(f"⚖️ Score serré ({gh}-{ga}) → pression de l'équipe qui perd")

    return min(score, 100), signals

def get_danger_level(score):
    if score >= 90: return "🔴 EXTRÊME"
    elif score >= 80: return "🟠 TRÈS ÉLEVÉ"
    elif score >= 70: return "🟡 ÉLEVÉ"
    else: return "🟢 MODÉRÉ"

def should_alert(fixture_id, score):
    if score < DANGER_THRESHOLD: return False
    now = datetime.now().timestamp()
    return (now - last_alert_time.get(fixture_id, 0)) >= ALERT_COOLDOWN

def format_goal_alert(fixture, live_stats, xg_data, danger_score, signals, avg_h, avg_a):
    now = datetime.now().strftime("%H:%M:%S")
    lid = fixture["league"]["id"]
    elapsed = fixture["fixture"]["status"]["elapsed"] or 0
    home = fixture["teams"]["home"]["name"]
    away = fixture["teams"]["away"]["name"]
    gh = fixture["goals"]["home"] or 0
    ga = fixture["goals"]["away"] or 0
    league = LEAGUES.get(lid, fixture["league"]["name"])
    level = get_danger_level(danger_score)
    home_s = live_stats.get("home", {})
    away_s = live_stats.get("away", {})
    xg_block = f"\n🎯 xG : {xg_data['home']} vs {xg_data['away']} = *{xg_data['total']} total*" if xg_data else "\n🎯 xG : _non disponible_"
    signals_text = "\n".join([f"  • {s}" for s in signals]) if signals else "  • Analyse en cours..."
    hist_text = f"{avg_h} (dom) | {avg_a} (ext)" if avg_h and avg_a else "Non disponible"
    return f"""⚡ *BUT IMMINENT DÉTECTÉ* ⚡

🏆 {league}
⚽ *{home} vs {away}*
📊 Score : `{gh} - {ga}` | ⏱️ {elapsed}'

━━━━━━━━━━━━━━━━━━
🔥 *SCORE DE DANGER : {danger_score}/100*
Niveau : {level}

━━━━━━━━━━━━━━━━━━
📈 *SIGNAUX DÉTECTÉS :*
{signals_text}

━━━━━━━━━━━━━━━━━━
📐 *STATS LIVE*{xg_block}
🔫 Tirs : {home_s.get('shots_on',0)}/{home_s.get('shots_total',0)} (dom) | {away_s.get('shots_on',0)}/{away_s.get('shots_total',0)} (ext)
⚡ Possession : {home_s.get('possession',50)}% vs {away_s.get('possession',50)}%
📐 Corners : {home_s.get('corners',0)} vs {away_s.get('corners',0)}

━━━━━━━━━━━━━━━━━━
📊 *HISTORIQUE BUTS* : {hist_text}

━━━━━━━━━━━━━━━━━━
💰 *PARI SUGGÉRÉ*
`Prochain but — 15 prochaines minutes`

⚠️ _Pariez de manière responsable_ | 🕐 _{now}_""".strip()

async def send_message(bot, text):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN)
        log.info("✅ Alerte envoyée")
    except Exception as e:
        log.error(f"Erreur Telegram: {e}")

async def monitor_loop(bot):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                log.info(f"🔍 Scan... {datetime.now().strftime('%H:%M:%S')}")
                fixtures = await fetch_live_fixtures(session)
                targets = [f for f in fixtures if f["league"]["id"] in LEAGUES]
                log.info(f"📡 {len(targets)} matchs dans nos ligues")
                for fixture in targets:
                    fid = fixture["fixture"]["id"]
                    status = fixture["fixture"]["status"]["short"]
                    if status not in ["1H", "2H"]: continue
                    lid = fixture["league"]["id"]
                    season = fixture["league"]["season"]
                    home_id = fixture["teams"]["home"]["id"]
                    away_id = fixture["teams"]["away"]["id"]
                    home_name = fixture["teams"]["home"]["name"]
                    away_name = fixture["teams"]["away"]["name"]
                    live_stats = await fetch_live_stats(session, fid)
                    if not live_stats: continue
                    xg_data = await fetch_xg_understat(session, lid, home_name, away_name)
                    avg_home, avg_away = await asyncio.gather(
                        fetch_team_history(session, home_id, lid, season),
                        fetch_team_history(session, away_id, lid, season),
                    )
                    danger_score, signals = calculate_danger_score(fixture, live_stats, xg_data, avg_home, avg_away)
                    log.info(f"📊 {home_name} vs {away_name} — Danger: {danger_score}/100")
                    if should_alert(fid, danger_score):
                        log.info(f"🚨 ALERTE BUT : {home_name} vs {away_name} — {danger_score}/100")
                        await send_message(bot, format_goal_alert(fixture, live_stats, xg_data, danger_score, signals, avg_home, avg_away))
                        last_alert_time[fid] = datetime.now().timestamp()
                    await asyncio.sleep(1)
            except Exception as e:
                log.error(f"Erreur boucle: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID,
        text=f"🤖 *BOT PRÉDICTION BUT DÉMARRÉ* ✅\n\n🧠 Algo : xG + Tirs + Possession + Corners + Historique\n🎯 Seuil : {DANGER_THRESHOLD}/100\n🏆 {len(LEAGUES)} ligues surveillées",
        parse_mode=ParseMode.MARKDOWN)
    await monitor_loop(bot)

if __name__ == "__main__":
    asyncio.run(main())