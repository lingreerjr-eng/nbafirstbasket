import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class NBAScraper:
    """Scrape NBA play-by-play data while respecting site policies."""

    SCHEDULE_URL = "https://data.nba.com/data/10s/prod/v1/{season}/schedule.json"
    PBP_URL = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
    SCOREBOARD_URL = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
    ROBOTS_URL = "https://www.nba.com/robots.txt"

    def __init__(self) -> None:
        self.data_dir = "data"
        self.db_path = os.path.join(self.data_dir, "first_baskets.db")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0 Safari/537.36"
                )
            }
        )
        self.min_request_interval = 1.0
        self._last_request_timestamp: float = 0.0
        self._robots_checked = False

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

        self._initialise_database()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape_season_data(self) -> None:
        """Scrape play-by-play data for the current and previous NBA seasons."""

        self._check_robots_once()

        current_season_start = self._get_current_season_start_year()
        seasons = [current_season_start - 1, current_season_start]

        for season_start in seasons:
            season_label = self._format_season_label(season_start)
            logger.info("Scraping season %s", season_label)
            try:
                schedule_games = self._fetch_schedule(season_start)
            except requests.RequestException as exc:
                logger.error("Failed to download schedule for %s: %s", season_label, exc)
                continue

            processed_games = 0
            for game in schedule_games:
                if not self._should_process_game(game):
                    continue

                if self._process_game(game, season_label):
                    processed_games += 1

            logger.info("Processed %d games for season %s", processed_games, season_label)
            self._export_season_files(season_label)

    def get_todays_games(self) -> List[Dict]:
        """Return today's NBA games using the official live scoreboard feed."""

        try:
            data = self._fetch_json(self.SCOREBOARD_URL)
        except requests.RequestException as exc:
            logger.error("Unable to download today's scoreboard: %s", exc)
            return []

        games: List[Dict] = []
        for game in data.get("scoreboard", {}).get("games", []):
            games.append(
                {
                    "game_id": game.get("gameId"),
                    "date": game.get("gameTimeUTC"),
                    "home_team": game.get("homeTeam", {}).get("teamTricode"),
                    "away_team": game.get("awayTeam", {}).get("teamTricode"),
                    "time": game.get("gameStatusText"),
                }
            )

        return games

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _initialise_database(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS games (
                    game_id TEXT PRIMARY KEY,
                    season TEXT NOT NULL,
                    game_date TEXT NOT NULL,
                    home_team TEXT NOT NULL,
                    away_team TEXT NOT NULL,
                    first_scoring_team TEXT,
                    first_scoring_player TEXT,
                    first_scoring_player_id TEXT,
                    first_scoring_description TEXT,
                    first_scoring_elapsed REAL,
                    source_url TEXT,
                    last_updated TEXT NOT NULL
                )
                """
            )

    def _check_robots_once(self) -> None:
        if self._robots_checked:
            return

        try:
            response = self.session.get(self.ROBOTS_URL, timeout=15)
            response.raise_for_status()
            content = response.text
            disallowed_sections = [line.split(":", 1)[1].strip() for line in content.splitlines() if line.lower().startswith("disallow:")]
            for path in ("/static/json", "/data/10s"):
                if any(section.startswith(path) for section in disallowed_sections):
                    raise RuntimeError(
                        f"Scraping blocked by robots.txt rules for path '{path}'."
                    )
        except requests.RequestException as exc:
            logger.warning("Could not verify robots.txt. Proceeding cautiously: %s", exc)
        self._robots_checked = True

    def _get_current_season_start_year(self) -> int:
        today = datetime.now()
        if today.month >= 10:
            return today.year
        return today.year - 1

    def _format_season_label(self, season_start: int) -> str:
        return f"{season_start}-{(season_start + 1) % 100:02d}"

    def _fetch_schedule(self, season_start: int) -> List[Dict]:
        url = self.SCHEDULE_URL.format(season=season_start)
        payload = self._fetch_json(url)
        games = payload.get("league", {}).get("standard", [])
        if not isinstance(games, list):
            return []
        return games

    def _should_process_game(self, game: Dict) -> bool:
        try:
            status_num = int(game.get("statusNum", 0))
        except (TypeError, ValueError):
            status_num = 0

        if status_num < 2:  # 1 = Scheduled, 2 = Live, 3 = Final
            return False

        start_date = game.get("startDateEastern")
        if not start_date:
            return False

        try:
            game_date = datetime.strptime(start_date, "%Y%m%d").date()
        except ValueError:
            return False

        return game_date <= datetime.now().date()

    def _process_game(self, game: Dict, season_label: str) -> bool:
        game_id = game.get("gameId")
        if not game_id:
            return False

        if self._game_already_processed(game_id):
            return False

        pbp_url = self.PBP_URL.format(game_id=game_id)
        try:
            pbp_payload = self._fetch_json(pbp_url)
        except requests.RequestException as exc:
            logger.warning("Skipping game %s due to play-by-play download error: %s", game_id, exc)
            return False

        first_event = self._extract_first_scoring_event(pbp_payload)
        if not first_event:
            logger.debug("No scoring event found for game %s", game_id)
            return False

        record = self._build_game_record(game, season_label, first_event, pbp_url)
        self._upsert_game_record(record)
        return True

    def _game_already_processed(self, game_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT game_id FROM games WHERE game_id = ?",
                (game_id,),
            ).fetchone()
            return row is not None

    def _extract_first_scoring_event(self, payload: Dict) -> Optional[Dict]:
        game = payload.get("game")
        if not isinstance(game, dict):
            return None

        actions = game.get("actions", [])
        if not isinstance(actions, list):
            return None

        for action in actions:
            try:
                score_value = int(action.get("scoreValue", 0))
            except (TypeError, ValueError):
                score_value = 0

            if score_value <= 0:
                continue

            team = action.get("teamTricode")
            player = action.get("playerName") or action.get("playerNameI")
            if not team or not player:
                continue

            return {
                "team": team,
                "player": player,
                "player_id": str(action.get("personId")) if action.get("personId") else None,
                "description": action.get("description") or action.get("actionType"),
                "clock": action.get("clock", "12:00"),
                "period": int(action.get("period", 1)),
                "periodType": action.get("periodType", "REGULAR"),
            }

        return None

    def _build_game_record(
        self,
        game: Dict,
        season_label: str,
        first_event: Dict,
        source_url: str,
    ) -> Dict:
        start_date = game.get("startDateEastern", "")
        try:
            game_date = datetime.strptime(start_date, "%Y%m%d").date().isoformat()
        except ValueError:
            game_date = datetime.now().date().isoformat()

        elapsed = self._calculate_elapsed_seconds(
            first_event.get("clock", "12:00"),
            first_event.get("period", 1),
            first_event.get("periodType", "REGULAR"),
        )

        return {
            "game_id": game.get("gameId"),
            "season": season_label,
            "game_date": game_date,
            "home_team": game.get("hTeam", {}).get("triCode"),
            "away_team": game.get("vTeam", {}).get("triCode"),
            "first_scoring_team": first_event.get("team"),
            "first_scoring_player": first_event.get("player"),
            "first_scoring_player_id": first_event.get("player_id"),
            "first_scoring_description": first_event.get("description"),
            "first_scoring_elapsed": elapsed,
            "source_url": source_url,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    def _upsert_game_record(self, record: Dict) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO games (
                    game_id,
                    season,
                    game_date,
                    home_team,
                    away_team,
                    first_scoring_team,
                    first_scoring_player,
                    first_scoring_player_id,
                    first_scoring_description,
                    first_scoring_elapsed,
                    source_url,
                    last_updated
                ) VALUES (:game_id, :season, :game_date, :home_team, :away_team, :first_scoring_team,
                          :first_scoring_player, :first_scoring_player_id, :first_scoring_description,
                          :first_scoring_elapsed, :source_url, :last_updated)
                ON CONFLICT(game_id) DO UPDATE SET
                    season = excluded.season,
                    game_date = excluded.game_date,
                    home_team = excluded.home_team,
                    away_team = excluded.away_team,
                    first_scoring_team = excluded.first_scoring_team,
                    first_scoring_player = excluded.first_scoring_player,
                    first_scoring_player_id = excluded.first_scoring_player_id,
                    first_scoring_description = excluded.first_scoring_description,
                    first_scoring_elapsed = excluded.first_scoring_elapsed,
                    source_url = excluded.source_url,
                    last_updated = excluded.last_updated
                """,
                record,
            )

    def _export_season_files(self, season_label: str) -> None:
        games = self._load_games_from_db(season_label)
        games_path = os.path.join(self.data_dir, f"games_{season_label}.json")
        with open(games_path, "w", encoding="utf-8") as file:
            json.dump(games, file, indent=2)

        players = self._build_player_summary(season_label)
        players_path = os.path.join(self.data_dir, f"players_{season_label}.json")
        with open(players_path, "w", encoding="utf-8") as file:
            json.dump(players, file, indent=2)

    def _load_games_from_db(self, season_label: str) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM games WHERE season = ? ORDER BY game_date",
                (season_label,),
            ).fetchall()

        games: List[Dict] = []
        for row in rows:
            games.append(
                {
                    "game_id": row["game_id"],
                    "date": row["game_date"],
                    "home_team": row["home_team"],
                    "away_team": row["away_team"],
                    "first_basket_player": row["first_scoring_player"],
                    "first_basket_team": row["first_scoring_team"],
                    "first_basket_time": row["first_scoring_elapsed"],
                    "play_description": row["first_scoring_description"],
                    "source_url": row["source_url"],
                }
            )
        return games

    def _build_player_summary(self, season_label: str) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            player_rows = conn.execute(
                """
                SELECT first_scoring_player AS player,
                       first_scoring_player_id AS player_id,
                       first_scoring_team AS team,
                       COUNT(*) AS first_baskets,
                       AVG(first_scoring_elapsed) AS avg_elapsed
                FROM games
                WHERE season = ? AND first_scoring_team IS NOT NULL
                GROUP BY first_scoring_player, first_scoring_player_id, first_scoring_team
                ORDER BY team, first_baskets DESC
                """,
                (season_label,),
            ).fetchall()

            team_rows = conn.execute(
                """
                SELECT team, COUNT(*) AS games_played
                FROM (
                    SELECT home_team AS team FROM games WHERE season = ?
                    UNION ALL
                    SELECT away_team AS team FROM games WHERE season = ?
                )
                GROUP BY team
                """,
                (season_label, season_label),
            ).fetchall()

        team_games = {row["team"]: row["games_played"] for row in team_rows}

        players: List[Dict] = []
        for row in player_rows:
            team = row["team"]
            games_played = team_games.get(team, 0)
            if games_played == 0:
                continue

            probability = row["first_baskets"] / games_played
            player_id = row["player_id"] or f"{team}_{row['player']}"
            players.append(
                {
                    "player_id": str(player_id),
                    "name": row["player"],
                    "team": team,
                    "position": None,
                    "avg_first_basket_time": round(row["avg_elapsed"] or 0, 2),
                    "first_basket_probability": round(probability, 4),
                    "games_played": games_played,
                    "first_baskets": row["first_baskets"],
                }
            )

        return players

    def _calculate_elapsed_seconds(self, clock: str, period: int, period_type: str) -> float:
        try:
            minutes_str, seconds_str = clock.split(":")
            minutes = int(minutes_str)
            seconds = float(seconds_str)
        except (ValueError, AttributeError):
            minutes = 12
            seconds = 0.0

        period_length = 12 * 60
        if period > 4 or period_type.upper() == "OVERTIME":
            period_length = 5 * 60

        elapsed_in_period = period_length - (minutes * 60 + seconds)
        total_elapsed = elapsed_in_period + max(0, period - 1) * 12 * 60
        return round(total_elapsed, 2)

    def _fetch_json(self, url: str) -> Dict:
        self._respect_rate_limit()
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        return response.json()

    def _respect_rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_timestamp
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self._last_request_timestamp = time.time()
