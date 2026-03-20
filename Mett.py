import requests
import time
import random
import asyncio
import logging
import re
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ─────────────────────────── НАСТРОЙКИ ───────────────────────────

TOKEN = "ТВОЙ_ТОКЕН"
TEAM_NAME = "Металлург Мг"

CHECK_INTERVAL = 30
IDLE_INTERVAL = 120
REQUEST_TIMEOUT = 15

# ─────────────────────────── ИСТОЧНИКИ ───────────────────────────

@dataclass
class Source:
    name: str
    url: str
    source_type: str   # 'api', 'telegram', 'vk', 'web'
    priority: int       # чем выше — тем приоритетнее
    enabled: bool = True
    last_check: float = 0.0
    last_success: bool = True


SOURCES: Dict[str, Source] = {
    "khl_api": Source(
        name="КХЛ API",
        url="https://khl.ru/api/v1/",
        source_type="api",
        priority=5,
    ),
    "khl_site": Source(
        name="КХЛ Сайт",
        url="https://khl.ru",
        source_type="web",
        priority=4,
    ),
    "khl_telegram": Source(
        name="КХЛ Telegram",
        url="https://t.me/khl_official_telegram",
        source_type="telegram",
        priority=3,
    ),
    "metallurg_site": Source(
        name="Металлург Сайт",
        url="https://metallurg.ru",
        source_type="web",
        priority=10,
    ),
    "metallurg_telegram": Source(
        name="Металлург Telegram",
        url="https://t.me/metallurgmgn",
        source_type="telegram",
        priority=10,
    ),
    "metallurg_vk": Source(
        name="Металлург VK",
        url="https://vk.com/hcmetallurg",
        source_type="vk",
        priority=9,
    ),
}

# ─────────────────────────── ЛОГИРОВАНИЕ ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────── ИНИЦИАЛИЗАЦИЯ ───────────────────────

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ─────────────────────────── FSM ДЛЯ УСТАНОВКИ КАНАЛА ───────────

class SetChannelStates(StatesGroup):
    waiting_for_channel_id = State()

# ─────────────────────────── ШТРАФЫ ──────────────────────────────

@dataclass
class Penalty:
    """Один штраф."""
    team: str
    player: str
    minutes: int
    reason: str
    period: int
    game_time: str          # "12:34" — время в периоде
    start_timestamp: float  # unix-время начала штрафа
    is_active: bool = True

    @property
    def end_timestamp(self) -> float:
        return self.start_timestamp + self.minutes * 60

    def remaining_seconds(self) -> float:
        return max(0.0, self.end_timestamp - time.time())

    def remaining_str(self) -> str:
        rem = int(self.remaining_seconds())
        if rem <= 0:
            return "завершён"
        m, s = divmod(rem, 60)
        return f"{m}:{s:02d}"


@dataclass
class PenaltyTracker:
    """Отслеживание штрафов обеих команд."""
    home_penalties: List[Penalty] = field(default_factory=list)
    away_penalties: List[Penalty] = field(default_factory=list)
    notified_penalty_ids: set = field(default_factory=set)

    def add_penalty(
        self,
        team: str,
        player: str,
        minutes: int,
        reason: str,
        period: int,
        game_time: str,
        is_home: bool,
    ) -> Penalty:
        pen = Penalty(
            team=team,
            player=player,
            minutes=minutes,
            reason=reason,
            period=period,
            game_time=game_time,
            start_timestamp=time.time(),
        )
        if is_home:
            self.home_penalties.append(pen)
        else:
            self.away_penalties.append(pen)
        return pen

    def active_home(self) -> List[Penalty]:
        self._expire()
        return [p for p in self.home_penalties if p.is_active]

    def active_away(self) -> List[Penalty]:
        self._expire()
        return [p for p in self.away_penalties if p.is_active]

    def home_players_on_ice(self) -> int:
        return max(3, 5 - len(self.active_home()))

    def away_players_on_ice(self) -> int:
        return max(3, 5 - len(self.active_away()))

    def strength_str(self) -> str:
        h = self.home_players_on_ice()
        a = self.away_players_on_ice()
        return f"{h} на {a}"

    def is_powerplay_home(self) -> bool:
        return self.home_players_on_ice() > self.away_players_on_ice()

    def is_powerplay_away(self) -> bool:
        return self.away_players_on_ice() > self.home_players_on_ice()

    def is_equal_strength(self) -> bool:
        return self.home_players_on_ice() == self.away_players_on_ice()

    def cancel_penalty_on_goal(self, is_home_scored: bool):
        if is_home_scored:
            targets = self.away_penalties
        else:
            targets = self.home_penalties

        for pen in targets:
            if pen.is_active and pen.minutes == 2:
                pen.is_active = False
                break

    def _expire(self):
        now = time.time()
        for pen in self.home_penalties + self.away_penalties:
            if pen.is_active and now >= pen.end_timestamp:
                pen.is_active = False

    def clear(self):
        self.home_penalties.clear()
        self.away_penalties.clear()
        self.notified_penalty_ids.clear()

    def all_penalties_log(self) -> List[Penalty]:
        combined = self.home_penalties + self.away_penalties
        combined.sort(key=lambda p: p.start_timestamp)
        return combined


# ─────────────────────────── СОСТОЯНИЕ ───────────────────────────

@dataclass
class MatchState:
    match_id: Optional[int] = None
    channel_id: Optional[int] = None
    last_score: str = ""
    last_status: str = ""
    last_period: int = 0
    last_events: set = field(default_factory=set)
    notified_start: bool = False
    notified_end: bool = False
    home_team: str = ""
    away_team: str = ""
    source_data: Dict[str, Any] = field(default_factory=dict)
    penalty_tracker: PenaltyTracker = field(default_factory=PenaltyTracker)
    pp_goals_home: int = 0
    pp_goals_away: int = 0
    sh_goals_home: int = 0
    sh_goals_away: int = 0


state = MatchState()

# ─────────────────────────── ШАБЛОНЫ СООБЩЕНИЙ ──────────────────

GOAL_METALLURG_TEXTS = [
    "🥅🔥 МЕТАЛЛУРГ ЗАБИВАЕТ!",
    "⚡ ГОООЛ МЕТАЛЛУРГА!",
    "🚨 МАГНИТКА ЗАБИВАЕТ!",
    "💥 ГОЛ! Наши забили!",
]

GOAL_METALLURG_PP_TEXTS = [
    "🥅🔥⚡ МЕТАЛЛУРГ ЗАБИВАЕТ В БОЛЬШИНСТВЕ!",
    "💪🚨 ГОЛ В БОЛЬШИНСТВЕ! Реализовали!",
    "🔥 БОЛЬШИНСТВО РЕАЛИЗОВАНО! Магнитка забивает!",
]

GOAL_METALLURG_SH_TEXTS = [
    "🥅😱 МЕТАЛЛУРГ ЗАБИВАЕТ В МЕНЬШИНСТВЕ!",
    "🔥🛡 ГОЛ В МЕНЬШИНСТВЕ! Невероятно!",
    "💥 Магнитка забивает, играя в меньшинстве!",
]

CONCEDE_TEXTS = [
    "😤 Пропустили...",
    "😔 Гол в наши ворота...",
    "🥅 Соперник забил...",
]

CONCEDE_PP_TEXTS = [
    "😤 Соперник реализовал большинство...",
    "😔 Пропустили в меньшинстве...",
    "🥅 Не удержали меньшинство — пропустили.",
]

CONCEDE_SH_TEXTS = [
    "😱 Соперник забил, играя в меньшинстве!",
    "😤 Пропустили от команды в меньшинстве...",
]

START_TEXTS = [
    "🟢 Матч начался!",
    "🏒 Погнали, Магнитка!",
    "🏟 Шайба вброшена!",
]

PERIOD_TEXTS = {
    1: "1️⃣ Начался первый период",
    2: "2️⃣ Начался второй период",
    3: "3️⃣ Начался третий период",
    4: "⏱ Овертайм!",
    5: "🎯 Буллиты!",
}

END_TEXTS = [
    "🏁 Матч завершён!",
    "🔔 Финальная сирена!",
    "✅ Игра окончена!",
]

PENALTY_TEXTS = [
    "🟡 Удаление!",
    "⚠️ Штраф!",
    "❌ Нарушение!",
]

METALLURG_ALIASES = [
    "металлург мг", "металлург магнитогорск",
    "metallurg mg", "metallurg magnitogorsk", "магнитка",
]

# ─────────────────────────── ПАРСЕРЫ ИСТОЧНИКОВ ──────────────────

class SourceParser:
    @staticmethod
    def is_metallurg(team_name: str) -> bool:
        if not team_name:
            return False
        name_lower = team_name.lower().strip()
        return any(
            alias in name_lower or name_lower in alias
            for alias in METALLURG_ALIASES
        )


class KHLApiParser(SourceParser):
    @staticmethod
    def _request(url: str, params: Optional[Dict] = None) -> Dict:
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error("KHL API — запрос %s: %s", url, e)
            return {}

    @classmethod
    def get_today_matches(cls) -> Dict:
        return cls._request(
            "https://khl.ru/api/v1/matches",
            params={"date": time.strftime("%Y-%m-%d")},
        )

    @classmethod
    def get_match_data(cls, match_id: int) -> Dict:
        return cls._request(f"https://khl.ru/api/v1/game/{match_id}/")

    @classmethod
    def get_match_events(cls, match_id: int) -> List[Dict]:
        data = cls._request(f"https://khl.ru/api/v1/game/{match_id}/events/")
        return data.get("events", [])

    @classmethod
    def get_match_penalties(cls, match_id: int) -> List[Dict]:
        data = cls._request(f"https://khl.ru/api/v1/game/{match_id}/penalties/")
        return data.get("penalties", [])

    @classmethod
    def find_metallurg_match(cls) -> Optional[Dict]:
        data = cls.get_today_matches()
        for m in data.get("matches", []):
            home = m.get("team_a", {}).get("title", "")
            away = m.get("team_b", {}).get("title", "")
            if cls.is_metallurg(home) or cls.is_metallurg(away):
                return {
                    "id": m["id"],
                    "home": home,
                    "away": away,
                    "source": "khl_api",
                }
        return None


class KHLWebParser(SourceParser):
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    @classmethod
    def get_live_scores(cls) -> List[Dict]:
        try:
            resp = requests.get(
                "https://khl.ru/",
                headers=cls.HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            matches: List[Dict] = []
            match_blocks = soup.select(
                ".match-card, .game-card, [class*='match']"
            )

            for block in match_blocks:
                try:
                    teams = block.select(".team-name, .team__name")
                    scores = block.select(".score, .game-score")
                    if len(teams) < 2:
                        continue

                    home = teams[0].get_text(strip=True)
                    away = teams[1].get_text(strip=True)
                    score_a, score_b = "0", "0"

                    if scores:
                        score_text = scores[0].get_text(strip=True)
                        parts = re.split(r"[:\-]", score_text)
                        if len(parts) == 2:
                            score_a = parts[0].strip()
                            score_b = parts[1].strip()

                    pim_els = block.select(
                        ".pim, .penalty-minutes, [class*='penalty']"
                    )
                    pim_home = pim_els[0].get_text(strip=True) if len(pim_els) > 0 else "0"
                    pim_away = pim_els[1].get_text(strip=True) if len(pim_els) > 1 else "0"

                    matches.append({
                        "home": home,
                        "away": away,
                        "score_a": score_a,
                        "score_b": score_b,
                        "pim_home": pim_home,
                        "pim_away": pim_away,
                        "source": "khl_site",
                    })
                except Exception:
                    continue

            return matches
        except requests.RequestException as e:
            logger.error("KHL Web — ошибка: %s", e)
            return []


class MetallurgWebParser(SourceParser):
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36"
        ),
    }

    @classmethod
    def get_current_match(cls) -> Optional[Dict]:
        try:
            resp = requests.get(
                "https://metallurg.ru/",
                headers=cls.HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            live_block = soup.select_one(
                ".live-match, .current-game, "
                "[class*='live'], [class*='match-widget']"
            )

            if live_block:
                teams = live_block.select(".team, .team-name")
                scores = live_block.select(".score, .result")
                penalties = live_block.select(
                    ".penalty, .pim, [class*='penalty']"
                )

                if len(teams) >= 2:
                    result: Dict[str, Any] = {
                        "home": teams[0].get_text(strip=True),
                        "away": teams[1].get_text(strip=True),
                        "score": (
                            scores[0].get_text(strip=True)
                            if scores else "0:0"
                        ),
                        "source": "metallurg_site",
                        "is_live": (
                            "live"
                            in str(live_block.get("class", [])).lower()
                        ),
                    }

                    if penalties:
                        result["penalties_html"] = str(penalties)

                    return result

            today = datetime.now().strftime("%d.%m")
            for item in soup.select(".schedule-item, .game-item"):
                date_el = item.select_one(".date, .game-date")
                if date_el and today in date_el.get_text():
                    teams = item.select(".team, .team-name")
                    if len(teams) >= 2:
                        return {
                            "home": teams[0].get_text(strip=True),
                            "away": teams[1].get_text(strip=True),
                            "source": "metallurg_site",
                        }

            return None
        except requests.RequestException as e:
            logger.error("Metallurg Web — ошибка: %s", e)
            return None


class TelegramParser(SourceParser):
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36"
        ),
    }

    @classmethod
    def get_recent_posts(
        cls, channel: str, limit: int = 5
    ) -> List[Dict]:
        try:
            if "t.me/" in channel:
                username = channel.split("t.me/")[-1].strip("/")
            else:
                username = channel.lstrip("@")

            url = f"https://t.me/s/{username}"
            resp = requests.get(
                url, headers=cls.HEADERS, timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            posts: List[Dict] = []
            messages = soup.select(".tgme_widget_message")[-limit:]

            for msg in messages:
                text_el = msg.select_one(".tgme_widget_message_text")
                time_el = msg.select_one(
                    ".tgme_widget_message_date time"
                )
                if text_el:
                    post: Dict[str, Any] = {
                        "text": text_el.get_text(strip=True),
                        "html": str(text_el),
                        "source": f"telegram_{username}",
                    }
                    if time_el:
                        post["datetime"] = time_el.get("datetime", "")
                    posts.append(post)

            return posts
        except requests.RequestException as e:
            logger.error("Telegram %s — ошибка: %s", channel, e)
            return []

    @classmethod
    def parse_match_info(cls, posts: List[Dict]) -> Optional[Dict]:
        match_keywords = [
            r"матч\s+начал", r"шайба\s+вброшена", r"гол",
            r"счёт", r"счет", r"\d+[:\-]\d+", r"период",
            r"буллит", r"овертайм", r"удален", r"штраф",
        ]

        for post in reversed(posts):
            text = post.get("text", "").lower()
            for keyword in match_keywords:
                if re.search(keyword, text):
                    score_match = re.search(
                        r"(\d+)\s*[:\-]\s*(\d+)", post["text"]
                    )
                    return {
                        "text": post["text"],
                        "score": (
                            f"{score_match.group(1)}:{score_match.group(2)}"
                            if score_match
                            else None
                        ),
                        "source": post["source"],
                        "raw": post,
                    }
        return None


class VKParser(SourceParser):
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)"
        ),
    }

    @classmethod
    def get_recent_posts(
        cls, group_url: str, limit: int = 5
    ) -> List[Dict]:
        try:
            group_id = group_url.rstrip("/").split("/")[-1]
            url = f"https://m.vk.com/{group_id}"
            resp = requests.get(
                url, headers=cls.HEADERS, timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            posts: List[Dict] = []
            wall_posts = soup.select(
                ".wall_item, .post, [class*='wall_post']"
            )[:limit]

            for post in wall_posts:
                text_el = post.select_one(
                    ".wall_post_text, .pi_text"
                )
                if text_el:
                    posts.append({
                        "text": text_el.get_text(strip=True),
                        "source": f"vk_{group_id}",
                    })

            return posts
        except requests.RequestException as e:
            logger.error("VK %s — ошибка: %s", group_url, e)
            return []

    @classmethod
    def parse_match_info(cls, posts: List[Dict]) -> Optional[Dict]:
        return TelegramParser.parse_match_info(posts)


# ─────────────────────────── АГРЕГАТОР ИСТОЧНИКОВ ────────────────

class SourceAggregator:
    def __init__(self):
        self.last_results: Dict[str, Dict] = {}
        self.source_status: Dict[str, bool] = {
            k: True for k in SOURCES
        }

    async def check_all_sources(self) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        tasks: List[Tuple[str, asyncio.Task]] = []

        for source_id, source in SOURCES.items():
            if not source.enabled:
                continue
            task = asyncio.create_task(
                self._check_source(source_id, source)
            )
            tasks.append((source_id, task))

        for source_id, task in tasks:
            try:
                result = await asyncio.wait_for(
                    task, timeout=REQUEST_TIMEOUT + 5
                )
                if result:
                    results[source_id] = result
                    self.source_status[source_id] = True
                    SOURCES[source_id].last_success = True
                else:
                    self.source_status[source_id] = False
                    SOURCES[source_id].last_success = False
            except asyncio.TimeoutError:
                logger.warning("Источник %s — таймаут", source_id)
                self.source_status[source_id] = False
                SOURCES[source_id].last_success = False
            except Exception as e:
                logger.error("Источник %s — ошибка: %s", source_id, e)
                self.source_status[source_id] = False

            SOURCES[source_id].last_check = time.time()

        self.last_results = results
        return results

    async def _check_source(
        self, source_id: str, source: Source
    ) -> Optional[Dict]:
        loop = asyncio.get_running_loop()

        dispatch = {
            "khl_api": self._check_khl_api,
            "khl_site": self._check_khl_web,
            "metallurg_site": self._check_metallurg_web,
        }

        if source_id in dispatch:
            return await loop.run_in_executor(
                None, dispatch[source_id]
            )

        if source.source_type == "telegram":
            return await loop.run_in_executor(
                None, self._check_telegram, source.url, source_id
            )

        if source.source_type == "vk":
            return await loop.run_in_executor(
                None, self._check_vk, source.url, source_id
            )

        return None

    def _check_khl_api(self) -> Optional[Dict]:
        match_data = KHLApiParser.find_metallurg_match()
        if match_data:
            mid = match_data.get("id") or state.match_id
            if mid:
                state.match_id = mid
                full_data = KHLApiParser.get_match_data(mid)
                if full_data:
                    match_data["full_data"] = full_data
                events = KHLApiParser.get_match_events(mid)
                if events:
                    match_data["events"] = events
                penalties = KHLApiParser.get_match_penalties(mid)
                if penalties:
                    match_data["penalties"] = penalties
        return match_data

    def _check_khl_web(self) -> Optional[Dict]:
        for m in KHLWebParser.get_live_scores():
            if SourceParser.is_metallurg(
                m.get("home", "")
            ) or SourceParser.is_metallurg(m.get("away", "")):
                return m
        return None

    def _check_metallurg_web(self) -> Optional[Dict]:
        return MetallurgWebParser.get_current_match()

    def _check_telegram(
        self, url: str, source_id: str
    ) -> Optional[Dict]:
        posts = TelegramParser.get_recent_posts(url)
        if posts:
            info = TelegramParser.parse_match_info(posts)
            if info:
                info["all_posts"] = posts
                return info
        return None

    def _check_vk(
        self, url: str, source_id: str
    ) -> Optional[Dict]:
        posts = VKParser.get_recent_posts(url)
        if posts:
            info = VKParser.parse_match_info(posts)
            if info:
                info["all_posts"] = posts
                return info
        return None

    def select_source_for_message(
        self, event_type: str = "general"
    ) -> Optional[str]:
        available = [
            (sid, SOURCES[sid])
            for sid, data in self.last_results.items()
            if data and SOURCES[sid].enabled
        ]
        if not available:
            return None

        available.sort(key=lambda x: x[1].priority, reverse=True)

        if random.random() < 0.7 and len(available) >= 2:
            return random.choice(available[:2])[0]
        return random.choice(available)[0]

    def get_best_match_data(self) -> Optional[Dict]:
        if not self.last_results:
            return None

        priority_order = [
            "khl_api", "metallurg_site", "khl_site",
            "metallurg_telegram", "metallurg_vk", "khl_telegram",
        ]

        for sid in priority_order:
            if sid in self.last_results and self.last_results[sid]:
                data = self.last_results[sid]
                data["_source"] = sid
                return data

        for sid, data in self.last_results.items():
            if data:
                data["_source"] = sid
                return data

        return None


aggregator = SourceAggregator()

# ─────────────────────────── УТИЛИТЫ ─────────────────────────────

def penalty_id(pen_dict: Dict) -> str:
    return (
        f"{pen_dict.get('team','')}_"
        f"{pen_dict.get('player','')}_"
        f"{pen_dict.get('period','')}_"
        f"{pen_dict.get('time','')}"
    )


def format_penalties_block(
    home: str, away: str, tracker: PenaltyTracker
) -> str:
    lines: List[str] = []

    home_active = tracker.active_home()
    away_active = tracker.active_away()

    if home_active:
        lines.append(f"\n🟡 Штрафы <b>{home}</b>:")
        for p in home_active:
            lines.append(
                f"   • {p.player} — {p.minutes} мин "
                f"({p.reason}) [ост. {p.remaining_str()}]"
            )

    if away_active:
        lines.append(f"\n🟡 Штрафы <b>{away}</b>:")
        for p in away_active:
            lines.append(
                f"   • {p.player} — {p.minutes} мин "
                f"({p.reason}) [ост. {p.remaining_str()}]"
            )

    if home_active or away_active:
        lines.append(
            f"\n👥 На льду: <b>{tracker.strength_str()}</b>"
        )

    return "\n".join(lines)


def format_all_penalties_summary(
    home: str, away: str, tracker: PenaltyTracker
) -> str:
    all_pen = tracker.all_penalties_log()
    if not all_pen:
        return "🟢 Штрафов в матче пока нет."

    lines = ["📋 <b>Все штрафы матча:</b>\n"]

    for p in all_pen:
        status = "⏳" if p.is_active else "✅"
        lines.append(
            f"{status} {p.team} | {p.player} — "
            f"{p.minutes} мин ({p.reason}) "
            f"[{p.period}-й период, {p.game_time}]"
        )

    total_home = sum(
        p.minutes for p in tracker.home_penalties
    )
    total_away = sum(
        p.minutes for p in tracker.away_penalties
    )
    lines.append(
        f"\nИтого: {home} — {total_home} мин, "
        f"{away} — {total_away} мин"
    )

    return "\n".join(lines)


# ─────────────────────────── КОМАНДЫ БОТА ────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    text = (
        "🏒 <b>Бот ХК «Металлург» Магнитогорск</b>\n\n"
        "Отслеживаю матчи Магнитки из нескольких источников:\n"
        "• КХЛ (API + сайт + Telegram)\n"
        "• Металлург (сайт + Telegram + VK)\n\n"
        "<b>Команды:</b>\n"
        "/setchannel — установить канал\n"
        "/status — текущий статус\n"
        "/score — текущий счёт\n"
        "/penalties — штрафы матча\n"
        "/sources — статус источников\n"
        "/forcecheck — принудительная проверка\n"
        "/stop — остановить отслеживание\n"
    )
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("sources"))
async def cmd_sources(message: types.Message):
    lines = ["📡 <b>Статус источников:</b>\n"]
    for source_id, source in SOURCES.items():
        status_emoji = (
            "✅" if aggregator.source_status.get(source_id) else "❌"
        )
        enabled_emoji = "🟢" if source.enabled else "🔴"
        priority = "⭐" * min(source.priority // 3, 3)
        lines.append(
            f"{status_emoji} {enabled_emoji} "
            f"<b>{source.name}</b> {priority}\n"
            f"   └ {source.url[:50]}{'...' if len(source.url) > 50 else ''}"
        )
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    if state.channel_id:
        try:
            chat = await bot.get_chat(state.channel_id)
            channel_info = f"📺 {chat.title}"
        except Exception:
            channel_info = f"📺 <code>{state.channel_id}</code>"
    else:
        channel_info = "📺 <b>не установлен</b>"

    active_sources = sum(
        1 for s in aggregator.source_status.values() if s
    )
    total_sources = len(SOURCES)

    tracker = state.penalty_tracker
    strength = tracker.strength_str()

    lines = [
        "🏒 <b>ХК «Металлург» Магнитогорск</b>\n",
        channel_info,
        f"🆔 Матч: <code>{state.match_id or 'не найден'}</code>",
        f"📊 Счёт: <b>{state.last_score or '—'}</b>",
        f"👥 На льду: <b>{strength}</b>",
        f"📡 Источники: {active_sources}/{total_sources} активны",
    ]

    h_pim = sum(p.minutes for p in tracker.home_penalties)
    a_pim = sum(p.minutes for p in tracker.away_penalties)
    if h_pim or a_pim:
        lines.append(
            f"🟡 Штрафы: {state.home_team or 'Хозяева'} {h_pim} мин / "
            f"{state.away_team or 'Гости'} {a_pim} мин"
        )

    if any([
        state.pp_goals_home, state.pp_goals_away,
        state.sh_goals_home, state.sh_goals_away,
    ]):
        lines.append("\n<b>Голы в спецбригадах:</b>")
        if state.pp_goals_home or state.pp_goals_away:
            lines.append(
                f"💪 Бол-во: {state.home_team or 'Хоз.'} {state.pp_goals_home} / "
                f"{state.away_team or 'Гости'} {state.pp_goals_away}"
            )
        if state.sh_goals_home or state.sh_goals_away:
            lines.append(
                f"🛡 Мен-во: {state.home_team or 'Хоз.'} {state.sh_goals_home} / "
                f"{state.away_team or 'Гости'} {state.sh_goals_away}"
            )

    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("penalties"))
async def cmd_penalties(message: types.Message):
    home = state.home_team or "Хозяева"
    away = state.away_team or "Гости"
    text = format_all_penalties_summary(
        home, away, state.penalty_tracker
    )
    await message.answer(text, parse_mode="HTML")


# ─────────────────── УСТАНОВКА КАНАЛА (НОВАЯ ЛОГИКА) ─────────────

@dp.message(Command("setchannel"))
async def cmd_setchannel(message: types.Message, fsm_state: FSMContext):
    """
    Начало процесса привязки канала.
    Пользователь должен:
    1. Добавить бота в свой канал как администратора.
    2. Отправить ID канала боту.
    Бот проверит, что он является администратором этого канала,
    и только тогда привяжет канал.
    """
    args = message.text.split(maxsplit=1)

    # Если ID передан сразу в команде — обрабатываем напрямую
    if len(args) >= 2:
        await _try_set_channel(message, args[1].strip())
        return

    # Иначе — объясняем и просим ввести ID
    text = (
        "📺 <b>Установка канала для трансляции</b>\n\n"
        "<b>Инструкция:</b>\n"
        "1️⃣ Добавьте этого бота в свой Telegram-канал\n"
        "2️⃣ Назначьте бота <b>администратором</b> канала "
        "(с правом отправки сообщений)\n"
        "3️⃣ Узнайте ID канала — для этого перешлите любое "
        "сообщение из канала боту "
        "<a href='https://t.me/getmyid_bot'>@getmyid_bot</a>, "
        "или используйте <a href='https://t.me/RawDataBot'>@RawDataBot</a>\n"
        "4️⃣ Отправьте мне ID канала (начинается с <code>-100</code>)\n\n"
        "💡 <b>Пример:</b> <code>-1001234567890</code>\n\n"
        "⏳ Жду ID канала..."
    )
    await message.answer(text, parse_mode="HTML", disable_web_page_preview=True)
    await fsm_state.set_state(SetChannelStates.waiting_for_channel_id)


@dp.message(SetChannelStates.waiting_for_channel_id)
async def process_channel_id(message: types.Message, fsm_state: FSMContext):
    """Обработка введённого ID канала."""
    await _try_set_channel(message, message.text.strip())
    await fsm_state.clear()


async def _try_set_channel(message: types.Message, raw_input: str):
    """
    Пытаемся привязать канал по введённому ID.
    Проверяем:
    - Корректность ID (число, начинается с -100)
    - Что бот является администратором канала
    - Что бот может отправлять сообщения
    """

    # ── Валидация ID ─────────────────────────────────────────────
    raw_input = raw_input.strip()

    try:
        chat_id = int(raw_input)
    except ValueError:
        await message.answer(
            "❌ <b>Некорректный ID</b>\n\n"
            "ID канала должен быть числом, начинающимся с <code>-100</code>.\n"
            "Пример: <code>-1001234567890</code>\n\n"
            "Попробуйте снова: /setchannel",
            parse_mode="HTML",
        )
        return

    if not str(chat_id).startswith("-100"):
        await message.answer(
            "⚠️ <b>Это не похоже на ID канала</b>\n\n"
            "ID Telegram-канала всегда начинается с <code>-100</code>.\n"
            "Пример: <code>-1001234567890</code>\n\n"
            "Используйте @getmyid_bot или @RawDataBot, "
            "чтобы узнать правильный ID.\n\n"
            "Попробуйте снова: /setchannel",
            parse_mode="HTML",
        )
        return

    # ── Проверяем, что бот может получить информацию о канале ─────
    try:
        chat = await bot.get_chat(chat_id)
    except Exception as e:
        error_str = str(e).lower()
        if "not found" in error_str or "chat not found" in error_str:
            await message.answer(
                "❌ <b>Канал не найден</b>\n\n"
                "Бот не смог найти канал с таким ID.\n"
                "Убедитесь, что:\n"
                "• ID указан верно\n"
                "• Бот добавлен в канал\n\n"
                "Попробуйте снова: /setchannel",
                parse_mode="HTML",
            )
        elif "kicked" in error_str or "banned" in error_str:
            await message.answer(
                "❌ <b>Бот заблокирован в этом канале</b>\n\n"
                "Разблокируйте бота в настройках канала "
                "и добавьте его заново.\n\n"
                "Попробуйте снова: /setchannel",
                parse_mode="HTML",
            )
        else:
            await message.answer(
                f"❌ <b>Ошибка доступа к каналу</b>\n\n"
                f"<code>{e}</code>\n\n"
                "Убедитесь, что бот добавлен в канал "
                "и назначен администратором.\n\n"
                "Попробуйте снова: /setchannel",
                parse_mode="HTML",
            )
        return

    # ── Проверяем тип чата (должен быть канал) ───────────────────
    if chat.type != "channel":
        type_names = {
            "group": "группа",
            "supergroup": "супергруппа",
            "private": "личный чат",
        }
        type_name = type_names.get(chat.type, chat.type)
        await message.answer(
            f"⚠️ <b>Это не канал</b>\n\n"
            f"Указанный чат — <i>{type_name}</i> (<code>{chat.type}</code>).\n"
            f"Бот работает только с Telegram-каналами.\n\n"
            "Попробуйте снова: /setchannel",
            parse_mode="HTML",
        )
        return

    # ── Проверяем, что бот является администратором канала ────────
    try:
        me = await bot.get_me()
        bot_member = await bot.get_chat_member(chat_id, me.id)
    except Exception as e:
        await message.answer(
            f"❌ <b>Не удалось проверить права бота в канале</b>\n\n"
            f"<code>{e}</code>\n\n"
            "Убедитесь, что бот добавлен в канал "
            "и назначен администратором.\n\n"
            "Попробуйте снова: /setchannel",
            parse_mode="HTML",
        )
        return

    if bot_member.status not in ("administrator", "creator"):
        status_names = {
            "member": "обычный участник",
            "restricted": "ограниченный участник",
            "left": "не в канале",
            "kicked": "заблокирован",
        }
        status_name = status_names.get(bot_member.status, bot_member.status)
        await message.answer(
            f"⚠️ <b>Бот не является администратором канала</b>\n\n"
            f"Текущий статус бота: <i>{status_name}</i>\n\n"
            "<b>Что нужно сделать:</b>\n"
            "1. Откройте настройки канала\n"
            "2. Перейдите в «Администраторы»\n"
            "3. Добавьте бота как администратора\n"
            "4. Убедитесь, что у бота есть право «Отправка сообщений»\n\n"
            "После этого попробуйте снова: /setchannel",
            parse_mode="HTML",
        )
        return

    # ── Проверяем право отправки сообщений ───────────────────────
    can_post = getattr(bot_member, "can_post_messages", None)
    if can_post is False:
        await message.answer(
            "⚠️ <b>У бота нет права отправлять сообщения</b>\n\n"
            "Бот является администратором, но ему не разрешено "
            "отправлять сообщения в канал.\n\n"
            "<b>Что нужно сделать:</b>\n"
            "1. Откройте настройки канала → Администраторы\n"
            "2. Выберите бота\n"
            "3. Включите право «Отправка сообщений» "
            "(Post Messages)\n\n"
            "После этого попробуйте снова: /setchannel",
            parse_mode="HTML",
        )
        return

    # ── Отправляем тестовое сообщение ────────────────────────────
    try:
        test_msg = await bot.send_message(
            chat_id,
            "✅ <b>Бот подключён к каналу!</b>\n\n"
            "🏒 Теперь сюда будут приходить трансляции "
            "матчей ХК «Металлург» Магнитогорск.",
            parse_mode="HTML",
        )
        # Удаляем тестовое сообщение через 5 секунд
        await asyncio.sleep(5)
        try:
            await bot.delete_message(chat_id, test_msg.message_id)
        except Exception:
            pass  # Не страшно, если не удалось удалить

    except Exception as e:
        await message.answer(
            f"❌ <b>Не удалось отправить сообщение в канал</b>\n\n"
            f"<code>{e}</code>\n\n"
            "Проверьте права администратора бота в канале.\n\n"
            "Попробуйте снова: /setchannel",
            parse_mode="HTML",
        )
        return

    # ── Всё ОК — сохраняем канал ─────────────────────────────────
    state.channel_id = chat_id

    await message.answer(
        f"✅ <b>Канал успешно установлен!</b>\n\n"
        f"📺 <b>{chat.title}</b>\n"
        f"🆔 <code>{chat_id}</code>\n\n"
        "Теперь трансляции матчей будут отправляться в этот канал.\n"
        "Используйте /status для проверки текущего состояния.",
        parse_mode="HTML",
    )
    logger.info(
        "Канал установлен: %s (%s) пользователем %s",
        chat.title, chat_id, message.from_user.id,
    )


@dp.message(Command("score"))
async def cmd_score(message: types.Message):
    await aggregator.check_all_sources()
    data = aggregator.get_best_match_data()

    if not data:
        await message.answer("⚠️ Информация о матче не найдена.")
        return

    source_id = data.get("_source", "")
    source_name = SOURCES.get(
        source_id, Source("?", "", "", 0)
    ).name

    if "full_data" in data:
        game = data["full_data"].get("game", {})
        home = game.get("team_a", {}).get("title", "?")
        away = game.get("team_b", {}).get("title", "?")
        score = (
            f"{game.get('score_a', '?')}:{game.get('score_b', '?')}"
        )
    elif "score" in data and data["score"]:
        home = data.get("home", "?")
        away = data.get("away", "?")
        score = data["score"]
    else:
        home = data.get("home", "?")
        away = data.get("away", "?")
        score = "—"

    tracker = state.penalty_tracker
    penalties_block = format_penalties_block(home, away, tracker)

    text = (
        f"🏒 <b>{home}</b> {score} <b>{away}</b>\n"
        f"👥 На льду: <b>{tracker.strength_str()}</b>"
        f"{penalties_block}\n\n"
        f"📡 Источник: {source_name}"
    )
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("forcecheck"))
async def cmd_forcecheck(message: types.Message):
    await message.answer("🔄 Проверяю все источники...")
    results = await aggregator.check_all_sources()

    lines = ["📊 <b>Результаты проверки:</b>\n"]
    for source_id, data in results.items():
        source = SOURCES[source_id]
        lines.append(
            f"{'✅' if data else '❌'} {source.name}: "
            f"{'данные получены' if data else 'нет данных'}"
        )

    if not results:
        lines.append("\n⚠️ Ни один источник не вернул данных.")

    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    state.match_id = None
    state.last_score = ""
    state.last_status = ""
    state.last_period = 0
    state.notified_start = False
    state.notified_end = False
    state.home_team = ""
    state.away_team = ""
    state.pp_goals_home = 0
    state.pp_goals_away = 0
    state.sh_goals_home = 0
    state.sh_goals_away = 0
    state.penalty_tracker.clear()
    state.last_events.clear()
    await message.answer("🛑 Отслеживание остановлено.")


# ─────────────────────────── ОТПРАВКА В КАНАЛ ────────────────────

async def send_to_channel(
    text: str, source_id: Optional[str] = None
):
    if not state.channel_id:
        logger.warning("Канал не установлен, сообщение не отправлено.")
        return

    if source_id and source_id in SOURCES:
        source_name = SOURCES[source_id].name
        text += f"\n\n<i>📡 {source_name}</i>"

    try:
        await bot.send_message(
            state.channel_id, text, parse_mode="HTML"
        )
        logger.info("📤 Отправлено (источник: %s)", source_id or "?")
    except Exception as e:
        logger.error("Ошибка отправки в канал: %s", e)


# ─────────────────────────── ОБРАБОТКА ШТРАФОВ ───────────────────

async def process_penalties_from_api(
    penalties_data: List[Dict],
    home: str,
    away: str,
):
    tracker = state.penalty_tracker

    for pen in penalties_data:
        pid = penalty_id(pen)
        if pid in tracker.notified_penalty_ids:
            continue

        tracker.notified_penalty_ids.add(pid)

        team = pen.get("team", "")
        player = pen.get("player", "Неизвестный")
        minutes = int(pen.get("minutes", 2))
        reason = pen.get("reason", "нарушение правил")
        period = int(pen.get("period", state.last_period))
        game_time = pen.get("time", "??:??")
        is_home = team.lower() == home.lower()

        tracker.add_penalty(
            team=team,
            player=player,
            minutes=minutes,
            reason=reason,
            period=period,
            game_time=game_time,
            is_home=is_home,
        )

        metallurg_penalized = SourceParser.is_metallurg(team)
        emoji = "😤" if metallurg_penalized else "😏"

        selected_source = aggregator.select_source_for_message(
            "penalty"
        )

        strength = tracker.strength_str()
        situation = ""
        if not tracker.is_equal_strength():
            if tracker.is_powerplay_home():
                pp_team = home
            else:
                pp_team = away
            situation = (
                f"\n💪 Большинство: <b>{pp_team}</b>"
            )

        await send_to_channel(
            f"{random.choice(PENALTY_TEXTS)} {emoji}\n\n"
            f"🏒 {team} — <b>{player}</b>\n"
            f"⏱ {minutes} мин — {reason}\n"
            f"📍 {period}-й период, {game_time}\n"
            f"👥 На льду: <b>{strength}</b>"
            f"{situation}",
            selected_source,
        )


async def process_penalties_from_social(
    text: str, source_id: str, home: str, away: str
):
    penalty_patterns = [
        r"удал[её]н\w*\s+(\S+(?:\s+\S+)?)\s*"
        r"\(?(\d+)\s*мин",
        r"штраф\s+(\d+)\s*мин\w*\s*[—\-:]\s*(\S+(?:\s+\S+)?)",
        r"(\d+)\s*мин\w*\s+(\S+(?:\s+\S+)?)",
    ]

    for pattern in penalty_patterns:
        match = re.search(pattern, text.lower())
        if match:
            post_hash = hash(text[:100] + "penalty")
            if post_hash not in state.last_events:
                state.last_events.add(post_hash)

                await send_to_channel(
                    f"{random.choice(PENALTY_TEXTS)}\n\n"
                    f"📢 <i>{text[:300]}</i>",
                    source_id,
                )
            break


# ─────────────────────────── ОСНОВНОЙ ЦИКЛ ───────────────────────

async def watcher():
    logger.info(
        "🏒 Watcher запущен с %d источниками", len(SOURCES)
    )

    while True:
        try:
            results = await aggregator.check_all_sources()

            if not results:
                await asyncio.sleep(IDLE_INTERVAL)
                continue

            best_data = aggregator.get_best_match_data()
            if not best_data:
                await asyncio.sleep(IDLE_INTERVAL)
                continue

            current_source = best_data.get("_source", "unknown")

            if "full_data" in best_data:
                game = best_data["full_data"].get("game", {})

                home = game.get("team_a", {}).get("title", "?")
                away = game.get("team_b", {}).get("title", "?")
                score_a = game.get("score_a", 0)
                score_b = game.get("score_b", 0)
                score = f"{score_a}:{score_b}"
                status = game.get("state", "")
                period = game.get("current_period", 0)

                state.home_team = home
                state.away_team = away

                if "penalties" in best_data:
                    await process_penalties_from_api(
                        best_data["penalties"], home, away
                    )

                await process_match_events(
                    home, away, score, status, period,
                    current_source,
                )

            elif "score" in best_data and best_data["score"]:
                score = best_data["score"]
                home = best_data.get("home", state.home_team or "?")
                away = best_data.get("away", state.away_team or "?")

                state.home_team = state.home_team or home
                state.away_team = state.away_team or away

                if score != state.last_score and state.last_score:
                    selected_source = (
                        aggregator.select_source_for_message("goal")
                    )
                    await send_to_channel(
                        f"🥅 <b>Гол!</b>\n"
                        f"📊 Новый счёт: <b>{score}</b>",
                        selected_source,
                    )

                state.last_score = score

            for source_id in [
                "metallurg_telegram", "metallurg_vk",
                "khl_telegram",
            ]:
                if source_id in results and results[source_id]:
                    await check_social_for_goals(
                        results[source_id], source_id
                    )
                    await check_social_for_penalties(
                        results[source_id], source_id
                    )

        except Exception as e:
            logger.exception("Ошибка в watcher: %s", e)

        interval = (
            CHECK_INTERVAL
            if state.last_status == "live"
            else IDLE_INTERVAL
        )
        await asyncio.sleep(interval)


async def process_match_events(
    home: str,
    away: str,
    score: str,
    status: str,
    period: int,
    source_id: str,
):
    tracker = state.penalty_tracker

    if status == "live" and state.last_status != "live":
        selected = aggregator.select_source_for_message("start")
        await send_to_channel(
            f"{random.choice(START_TEXTS)}\n\n"
            f"🏒 <b>{home}</b> vs <b>{away}</b>",
            selected,
        )
        state.notified_start = True

    if period != state.last_period and period > 0:
        period_text = PERIOD_TEXTS.get(
            period, f"▶️ Период {period}"
        )
        selected = aggregator.select_source_for_message("period")
        await send_to_channel(
            f"{period_text}\n"
            f"🏒 {home} <b>{score}</b> {away}",
            selected,
        )

    if (
        score != state.last_score
        and state.last_score
        and status == "live"
    ):
        try:
            old_a, old_b = map(int, state.last_score.split(":"))
            new_a, new_b = map(int, score.split(":"))

            home_scored = new_a > old_a
            scorer_team = home if home_scored else away
            metallurg_scored = SourceParser.is_metallurg(
                scorer_team
            )

            is_pp_home = tracker.is_powerplay_home()
            is_pp_away = tracker.is_powerplay_away()
            is_equal = tracker.is_equal_strength()

            goal_type = "equal"
            goal_type_text = ""

            if home_scored:
                if is_pp_home:
                    goal_type = "pp"
                    state.pp_goals_home += 1
                    goal_type_text = "💪 <b>Гол в БОЛЬШИНСТВЕ!</b>"
                elif is_pp_away:
                    goal_type = "sh"
                    state.sh_goals_home += 1
                    goal_type_text = "🛡 <b>Гол в МЕНЬШИНСТВЕ!</b>"
            else:
                if is_pp_away:
                    goal_type = "pp"
                    state.pp_goals_away += 1
                    goal_type_text = "💪 <b>Гол в БОЛЬШИНСТВЕ!</b>"
                elif is_pp_home:
                    goal_type = "sh"
                    state.sh_goals_away += 1
                    goal_type_text = "🛡 <b>Гол в МЕНЬШИНСТВЕ!</b>"

            if metallurg_scored:
                if goal_type == "pp":
                    goal_text = random.choice(
                        GOAL_METALLURG_PP_TEXTS
                    )
                elif goal_type == "sh":
                    goal_text = random.choice(
                        GOAL_METALLURG_SH_TEXTS
                    )
                else:
                    goal_text = random.choice(
                        GOAL_METALLURG_TEXTS
                    )
            else:
                if goal_type == "pp":
                    goal_text = random.choice(CONCEDE_PP_TEXTS)
                elif goal_type == "sh":
                    goal_text = random.choice(CONCEDE_SH_TEXTS)
                else:
                    goal_text = random.choice(CONCEDE_TEXTS)

            if goal_type == "pp":
                tracker.cancel_penalty_on_goal(home_scored)

            selected = aggregator.select_source_for_message("goal")

            strength_info = ""
            if not is_equal:
                strength_info = (
                    f"\n👥 Формат: <b>{tracker.strength_str()}</b>"
                )

            penalties_block = format_penalties_block(
                home, away, tracker
            )

            msg_parts = [
                goal_text,
                "",
                f"🏒 {home} <b>{score}</b> {away}",
                f"⚽ Забил: <b>{scorer_team}</b>",
            ]
            if goal_type_text:
                msg_parts.insert(1, goal_type_text)
            if strength_info:
                msg_parts.append(strength_info)
            if penalties_block:
                msg_parts.append(penalties_block)

            await send_to_channel(
                "\n".join(msg_parts), selected
            )

        except ValueError:
            logger.warning(
                "Не удалось разобрать счёт: %s → %s",
                state.last_score, score,
            )

    if status == "finished" and not state.notified_end:
        selected = aggregator.select_source_for_message("end")

        h_pim = sum(p.minutes for p in tracker.home_penalties)
        a_pim = sum(p.minutes for p in tracker.away_penalties)

        penalty_summary = ""
        if h_pim or a_pim:
            penalty_summary = (
                f"\n\n🟡 <b>Штрафы:</b>\n"
                f"   {home} — {h_pim} мин "
                f"({len(tracker.home_penalties)} удал.)\n"
                f"   {away} — {a_pim} мин "
                f"({len(tracker.away_penalties)} удал.)"
            )

        pp_summary = ""
        if any([
            state.pp_goals_home, state.pp_goals_away,
            state.sh_goals_home, state.sh_goals_away,
        ]):
            pp_summary = (
                f"\n\n💪 <b>Голы в большинстве:</b> "
                f"{home} {state.pp_goals_home} / "
                f"{away} {state.pp_goals_away}"
                f"\n🛡 <b>Голы в меньшинстве:</b> "
                f"{home} {state.sh_goals_home} / "
                f"{away} {state.sh_goals_away}"
            )

        await send_to_channel(
            f"{random.choice(END_TEXTS)}\n\n"
            f"🏒 <b>{home}</b> {score} <b>{away}</b>"
            f"{penalty_summary}"
            f"{pp_summary}",
            selected,
        )
        state.notified_end = True

        await asyncio.sleep(300)

        state.match_id = None
        state.pp_goals_home = 0
        state.pp_goals_away = 0
        state.sh_goals_home = 0
        state.sh_goals_away = 0
        tracker.clear()

    state.last_score = score
    state.last_status = status
    state.last_period = period


async def check_social_for_goals(
    data: Dict, source_id: str
):
    posts = data.get("all_posts", [])

    goal_patterns = [
        r"гол[!]?\s", r"забил", r"шайба\s+в\s+ворот",
        r"\d+[:\-]\d+.*гол", r"счёт.*\d+[:\-]\d+",
    ]

    for post in posts[:3]:
        text = post.get("text", "").lower()
        for pattern in goal_patterns:
            if re.search(pattern, text):
                post_hash = hash(text[:100])
                if post_hash not in state.last_events:
                    state.last_events.add(post_hash)
                    if random.random() < 0.3:
                        await send_to_channel(
                            f"📢 <i>{post['text'][:200]}...</i>",
                            source_id,
                        )
                break


async def check_social_for_penalties(
    data: Dict, source_id: str
):
    posts = data.get("all_posts", [])

    penalty_keywords = [
        r"удал[её]н", r"штраф", r"\d+\s*мин",
        r"скамь[яе]", r"удаление",
    ]

    home = state.home_team or "Хозяева"
    away = state.away_team or "Гости"

    for post in posts[:3]:
        text = post.get("text", "")
        text_lower = text.lower()

        for kw in penalty_keywords:
            if re.search(kw, text_lower):
                post_hash = hash(text[:100] + "pen")
                if post_hash not in state.last_events:
                    state.last_events.add(post_hash)
                    await process_penalties_from_social(
                        text, source_id, home, away
                    )
                break


# ─────────────────────────── ЗАПУСК ──────────────────────────────

async def main():
    me = await bot.get_me()
    logger.info("🏒 Бот запущен: @%s", me.username)
    logger.info("📡 Источников: %d", len(SOURCES))

    watcher_task = asyncio.create_task(watcher())

    try:
        await dp.start_polling(bot)
    finally:
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
