import asyncio
import logging
from datetime import datetime
from telegram import Bot
from telegram.constants import ParseMode
import aiohttp

TELEGRAM_TOKEN   = "8722101020:AAFPW-Pi7qtDLrOvb9YJu7MtxS27dFeM7lQ"
TELEGRAM_CHAT_ID = "457874923"
API_FOOTBALL_KEY = "47dff2948a07fdef4536532d267ace24"

CHECK_INTERVAL        = 60
MIN_CORNERS_HT        = 8
MAX_CORNERS_2H_TARGET = 6
LAST_N_MATCHES        = 5
BASE_URL = "https://v3.football.api-sports.io"

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
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)
alerted_matches: set = set()

def get_headers():
    return {"x-apisports-key": API_FOOTBALL_KEY}

async def fetch_live_fixtures(session):
    try:
        async with session.get(f"{BASE_URL}/fixtures", headers=get_headers(),
                               params={"live": "all"}, timeout=10) as r:
            if r.status == 200:
                return (await r.json()).get("response", [])
            return []
    except Exception as e:
        log.error(f"Erreur fixtures: {e}")
        return []

async def fetch_fixture_stats(session, fixture_id):
    try:
        async with session.get(f"{BASE_URL}/fixtures/statistics", headers=get_headers(),
                               params={"fixture": fixture_id}, timeout=10) as r:
            if r.status == 200:
                return parse_corners((await r.json()).get("response", []))
            return {}
    except Exception as e:
        log.error(f"Erreur stats {fixture_id}: {e}")
        return {}

async def fetch_team_corner_history(session, team_id, league_id, season):
    try:
        params = {"team": team_id, "league": league_id, "season": season, "last": LAST_N_MATCHES}
        async with session.get(f"{BASE_URL}/fixtures", headers=get_headers(),
                               params=params, timeout=10) as r:
            if r.status != 200:
                return None
            fixtures = (await r.json()).get("response", [])
        if not fixtures:
            return None
        corners_per_match = []
        for fix in fixtures:
            stats = await fetch_fixture_stats(session, fix["fixture"]["id"])
            await asyncio.sleep(0.5)
            if stats:
                corners_per_match.append(stats["total"])
        if not corners_per_match:
            return None
        avg = round(sum(corners_per_match) / len(corners_per_match), 1)
        if len(corners_per_match) >= 4:
            recent = sum(corners_per_match[:2]) / 2
            older  = sum(corners_per_match[-2:]) / 2
            if recent > older + 1: trend = "📈 En hausse"
            elif recent < older - 1: trend = "📉 En baisse"
            else: trend = "➡️ Stable"
        else:
            trend = "➡️ Stable"
        return {"avg": avg, "matches": len(corners_per_match), "trend": trend}
    except Exception as e:
        log.error(f"Erreur historique {team_id}: {e}")
        return None

def get_history_signal(home_avg, away_avg):
    if home_avg is None or away_avg is None:
        return None
    combined = home_avg + away_avg
    if combined >= 14: return "🔥 TRÈS FORT — ces équipes font beaucoup de corners"
    elif combined >= 11: return "💪 FORT — historique favorable"
    elif combined >= 8: return "✅ CORRECT — historique moyen"
    else: return "⚠️ FAIBLE — ces équipes font peu de corners habituellement"

def parse_corners(stats):
    home = away = 0
    for i, t in enumerate(stats):
        for s in t.get("statistics", []):
            if s.get("type") == "Corner Kicks":
                val = int(s.get("value") or 0)
                if i == 0: home = val
                else: away = val
    return {"total": home + away, "home": home, "away": away}

def get_confidence(n):
    if n >= 12: return "🔥 TRÈS HAUTE (12+ corners HT)"
    elif n >= 10: return "💪 HAUTE (10-11 corners HT)"
    else: return "✅ BONNE (8-9 corners HT)"

def analyze(fixture, corners):
    fid     = fixture["fixture"]["id"]
    status  = fixture["fixture"]["status"]["short"]
    elapsed = fixture["fixture"]["status"]["elapsed"] or 0
    if status not in ["HT", "2H"]: return None
    if status == "2H" and elapsed > 47: return None
    key = f"corner_ht_{fid}"
    if key in alerted_matches: return None
    total = corners.get("total", 0)
    if total < MIN_CORNERS_HT: return None
    lid = fixture["league"]["id"]
    return {
        "key": key, "league": LEAGUES.get(lid, fixture["league"]["name"]),
        "league_id": lid, "season": fixture["league"]["season"],
        "match": f"{fixture['teams']['home']['name']} vs {fixture['teams']['away']['name']}",
        "home_id": fixture["teams"]["home"]["id"], "away_id": fixture["teams"]["away"]["id"],
        "home_name": fixture["teams"]["home"]["name"], "away_name": fixture["teams"]["away"]["name"],
        "score": f"{fixture['goals']['home'] or 0} - {fixture['goals']['away'] or 0}",
        "corners_ht": total, "corners_home": corners["home"], "corners_away": corners["away"],
        "status": "MI-TEMPS" if status == "HT" else f"2ème mi-temps ({elapsed}')",
        "bet": f"Under {MAX_CORNERS_2H_TARGET} corners — 2ème mi-temps",
        "confidence": get_confidence(total),
    }

def format_alert(a, home_hist, away_hist):
    now = datetime.now().strftime("%H:%M:%S")
    if home_hist and away_hist:
        signal = get_history_signal(home_hist["avg"], away_hist["avg"])
        history_block = f"\n━━━━━━━━━━━━━━━━━━\n📊 *HISTORIQUE CORNERS ({LAST_N_MATCHES} derniers matchs)*\n🏠 {a['home_name']} : *{home_hist['avg']} corners/match* {home_hist['trend']}\n✈️ {a['away_name']} : *{away_hist['avg']} corners/match* {away_hist['trend']}\n🎯 Signal : {signal}"
    else:
        history_block = "\n━━━━━━━━━━━━━━━━━━\n📊 _Historique non disponible_"
    return f"""🚨 *ALERTE PARI CORNERS* 🚨

🏆 {a['league']}
⚽ *{a['match']}*
📊 Score : `{a['score']}`
⏱️ Statut : `{a['status']}`

━━━━━━━━━━━━━━━━━━
📐 *CORNERS 1ÈRE MI-TEMPS*
Total : *{a['corners_ht']} corners*
(Dom : {a['corners_home']} | Ext : {a['corners_away']}){history_block}

━━━━━━━━━━━━━━━━━━
💰 *PARI SUGGÉRÉ*
`{a['bet']}`
📈 Confiance : {a['confidence']}

━━━━━━━━━━━━━━━━━━
🧠 Pressing épuisé → 2ème mi-temps moins agressive

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
                targets  = [f for f in fixtures if f["league"]["id"] in LEAGUES]
                log.info(f"📡 {len(targets)} matchs dans nos ligues")
                for fixture in targets:
                    status  = fixture["fixture"]["status"]["short"]
                    elapsed = fixture["fixture"]["status"]["elapsed"] or 0
                    if status not in ["HT", "2H"]: continue
                    if status == "2H" and elapsed > 47: continue
                    corners = await fetch_fixture_stats(session, fixture["fixture"]["id"])
                    if not corners: continue
                    alert = analyze(fixture, corners)
                    if alert:
                        log.info(f"🚨 {alert['match']} — {alert['corners_ht']} corners HT")
                        home_hist, away_hist = await asyncio.gather(
                            fetch_team_corner_history(session, alert["home_id"], alert["league_id"], alert["season"]),
                            fetch_team_corner_history(session, alert["away_id"], alert["league_id"], alert["season"]),
                        )
                        await send_message(bot, format_alert(alert, home_hist, away_hist))
                        alerted_matches.add(alert["key"])
                    await asyncio.sleep(1)
            except Exception as e:
                log.error(f"Erreur boucle: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID,
        text=f"🤖 *BOT DÉMARRÉ* ✅\n\n📐 Stratégie : ≥{MIN_CORNERS_HT} corners HT → Under {MAX_CORNERS_2H_TARGET} corners 2ème MT\n📊 Historique : {LAST_N_MATCHES} derniers matchs\n🏆 {len(LEAGUES)} ligues surveillées",
        parse_mode=ParseMode.MARKDOWN)
    await monitor_loop(bot)

if __name__ == "__main__":
    asyncio.run(main())

if __name__ == "__main__":

    asyncio.run(main())

