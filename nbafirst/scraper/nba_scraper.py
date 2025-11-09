import json
import logging
import os
import sqlite3
import time
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

import requests
from nba_api.live.nba.endpoints import playbyplay as live_playbyplay  # type: ignore
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard  # type: ignore
from nba_api.stats.endpoints import leaguegamelog, playbyplayv2  # type: ignore

logger = logging.getLogger(__name__)


class NBAScraper:
    """Collect NBA play-by-play data using official and community APIs."""

    BALLDONTLIE_GAMES_ENDPOINT = "https://www.balldontlie.io/api/v1/games"

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
        self.min_request_interval = 0.75
        self._last_request_timestamp: float = 0.0

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

        self._initialise_database()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape_season_data(self) -> None:
        """Scrape play-by-play data for the current and previous NBA seasons."""

        current_season_start = self._get_current_season_start_year()
        seasons = [current_season_start - 1, current_season_start]

        for season_start in seasons:
            season_label = self._format_season_label(season_start)
            logger.info("Collecting schedule for season %s", season_label)

            try:
                schedule_games = self._fetch_schedule_from_nba_api(season_label)
            except Exception as exc:  # pragma: no cover - defensive
                logger.error("NBA API schedule retrieval failed for %s: %s", season_label, exc)
                schedule_games = []

            if not schedule_games:
                logger.warning(
                    "Falling back to balldontlie schedule for season %s", season_label
                )
                schedule_games = self._fetch_schedule_from_balldontlie(season_start)

            processed_games = 0
            for game in schedule_games:
                if not self._should_process_game(game):
                    continue

                if self._process_game(game, season_label):
                    processed_games += 1

            logger.info("Processed %d games for season %s", processed_games, season_label)
            self._export_season_files(season_label)

    def get_todays_games(self) -> List[Dict]:
        """Return today's NBA games using NBA and balldontlie APIs."""

        games = self._get_todays_games_from_nba_api()
        if games:
            return games

        return self._get_todays_games_from_balldontlie()

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

    def _get_current_season_start_year(self) -> int:
        today = datetime.now()
        if today.month >= 10:
            return today.year
        return today.year - 1

    def _format_season_label(self, season_start: int) -> str:
        return f"{season_start}-{(season_start + 1) % 100:02d}"

    def _respect_rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_timestamp
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self._last_request_timestamp = time.time()

    def _fetch_schedule_from_nba_api(self, season_label: str) -> List[Dict]:
        """Fetch a season schedule via the official nba_api client."""

        self._respect_rate_limit()
        log = leaguegamelog.LeagueGameLog(
            season=season_label,
            season_type_all_star="Regular Season",
        )
        frame = log.get_data_frames()[0]

        games: Dict[str, Dict] = {}
        for _, row in frame.iterrows():
            game_id = str(row.get("GAME_ID"))
            matchup = row.get("MATCHUP", "")
            team = row.get("TEAM_ABBREVIATION")
            game_date = row.get("GAME_DATE")

            if not game_id or not matchup or not team or not game_date:
                continue

            try:
                parsed_date = datetime.strptime(game_date, "%b %d, %Y").date()
            except ValueError:
                continue

            game_entry = games.setdefault(
                game_id,
                {
                    "game_id": game_id,
                    "game_date": parsed_date,
                    "home_team": None,
                    "away_team": None,
                },
            )

            if "vs." in matchup:
                game_entry["home_team"] = team
                game_entry["away_team"] = matchup.split(" vs. ", 1)[1]
            elif "@" in matchup:
                game_entry["home_team"] = matchup.split(" @ ", 1)[1]
                game_entry["away_team"] = team

        filtered_games = [game for game in games.values() if game["home_team"] and game["away_team"]]
        return sorted(filtered_games, key=lambda g: (g["game_date"], g["game_id"]))

    def _fetch_schedule_from_balldontlie(self, season_start: int) -> List[Dict]:
        """Fallback schedule retrieval using the balldontlie community API."""

        games: List[Dict] = []
        page = 1
        while True:
            params = {
                "seasons[]": season_start,
                "per_page": 100,
                "page": page,
                "postseason": "false",
            }
            try:
                self._respect_rate_limit()
                response = self.session.get(
                    self.BALLDONTLIE_GAMES_ENDPOINT,
                    params=params,
                    timeout=20,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.error("balldontlie schedule request failed: %s", exc)
                break

            payload = response.json()
            data = payload.get("data", [])
            for item in data:
                try:
                    parsed_date = datetime.fromisoformat(item["date"].replace("Z", "+00:00")).date()
                except (KeyError, ValueError, TypeError):
                    continue

                games.append(
                    {
                        "game_id": str(item.get("id")),
                        "game_date": parsed_date,
                        "home_team": item.get("home_team", {}).get("abbreviation"),
                        "away_team": item.get("visitor_team", {}).get("abbreviation"),
                    }
                )

            meta = payload.get("meta", {})
            if page >= int(meta.get("total_pages", page)):
                break
            page += 1

        filtered_games = [game for game in games if game["home_team"] and game["away_team"]]
        return sorted(filtered_games, key=lambda g: (g["game_date"], g["game_id"]))

    def _should_process_game(self, game: Dict) -> bool:
        game_date = game.get("game_date")
        if not isinstance(game_date, date):
            return False

        if game_date > datetime.now().date():
            return False

        game_id = game.get("game_id")
        if not game_id:
            return False

        return not self._game_already_processed(game_id)

    def _process_game(self, game: Dict, season_label: str) -> bool:
        game_id = game.get("game_id")
        if not game_id:
            return False

        first_event = self._fetch_first_event_from_nba_api(game_id)
        if not first_event:
            first_event = self._fetch_first_event_from_live_feed(game_id)

        if not first_event:
            logger.debug("No scoring event found for game %s", game_id)
            return False

        record = self._build_game_record(game, season_label, first_event)
        self._upsert_game_record(record)
        return True

    def _game_already_processed(self, game_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT game_id FROM games WHERE game_id = ?",
                (game_id,),
            ).fetchone()
            return row is not None

    def _fetch_first_event_from_nba_api(self, game_id: str) -> Optional[Dict]:
        """Fetch the first scoring event using nba_api.stats play-by-play."""

        try:
            self._respect_rate_limit()
            pbp = playbyplayv2.PlayByPlayV2(game_id=game_id, start_period=1, end_period=1)
            frame = pbp.get_data_frames()[0]
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("nba_api play-by-play request failed for %s: %s", game_id, exc)
            return None

        for _, row in frame.iterrows():
            score = row.get("SCORE")
            if not score or score.strip() in {"", "0 - 0"}:
                continue

            team = row.get("PLAYER1_TEAM_ABBREVIATION")
            player = row.get("PLAYER1_NAME")
            description = (
                row.get("HOMEDESCRIPTION")
                or row.get("VISITORDESCRIPTION")
                or row.get("NEUTRALDESCRIPTION")
            )

            if not team or not player or not description:
                continue

            return {
                "team": team,
                "player": player,
                "player_id": str(row.get("PLAYER1_ID")) if row.get("PLAYER1_ID") else None,
                "description": description,
                "clock": row.get("PCTIMESTRING", "12:00"),
                "period": int(row.get("PERIOD", 1)),
                "periodType": "REGULAR",
            }

        return None

    def _fetch_first_event_from_live_feed(self, game_id: str) -> Optional[Dict]:
        """Fallback to the nba_api live feed if stats API is unavailable."""

        if live_playbyplay is None:  # type: ignore[truthy-bool]
            return None

        try:
            self._respect_rate_limit()
            pbp = live_playbyplay.PlayByPlay(game_id=game_id)
            payload = pbp.get_dict()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Live play-by-play fallback failed for %s: %s", game_id, exc)
            return None

        game = payload.get("game", {})
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
            description = action.get("description") or action.get("actionType")

            if not team or not player or not description:
                continue

            return {
                "team": team,
                "player": player,
                "player_id": str(action.get("personId")) if action.get("personId") else None,
                "description": description,
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
    ) -> Dict:
        game_date = game.get("game_date")
        if isinstance(game_date, datetime):
            game_date_str = game_date.date().isoformat()
        elif hasattr(game_date, "isoformat"):
            game_date_str = game_date.isoformat()  # type: ignore[assignment]
        else:
            game_date_str = datetime.now().date().isoformat()

        elapsed = self._calculate_elapsed_seconds(
            first_event.get("clock", "12:00"),
            first_event.get("period", 1),
            first_event.get("periodType", "REGULAR"),
        )

        return {
            "game_id": game.get("game_id"),
            "season": season_label,
            "game_date": game_date_str,
            "home_team": game.get("home_team"),
            "away_team": game.get("away_team"),
            "first_scoring_team": first_event.get("team"),
            "first_scoring_player": first_event.get("player"),
            "first_scoring_player_id": first_event.get("player_id"),
            "first_scoring_description": first_event.get("description"),
            "first_scoring_elapsed": elapsed,
            "source_url": "nba_api.stats.playbyplayv2",
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
        if period > 4 or str(period_type).upper() == "OVERTIME":
            period_length = 5 * 60

        elapsed_in_period = period_length - (minutes * 60 + seconds)
        total_elapsed = elapsed_in_period + max(0, period - 1) * 12 * 60
        return round(total_elapsed, 2)

    # ------------------------------------------------------------------
    # Today's games helpers
    # ------------------------------------------------------------------
    def _get_todays_games_from_nba_api(self) -> List[Dict]:
        if live_scoreboard is None:  # type: ignore[truthy-bool]
            return []

        try:
            self._respect_rate_limit()
            board = live_scoreboard.ScoreBoard()
            payload = board.get_dict()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("nba_api live scoreboard request failed: %s", exc)
            return []

        games: List[Dict] = []
        for game in payload.get("scoreboard", {}).get("games", []):
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

    def _get_todays_games_from_balldontlie(self) -> List[Dict]:
        today = datetime.now().date().isoformat()
        params = {
            "dates[]": today,
            "per_page": 100,
        }
        try:
            self._respect_rate_limit()
            response = self.session.get(
                self.BALLDONTLIE_GAMES_ENDPOINT,
                params=params,
                timeout=15,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error("balldontlie today's games request failed: %s", exc)
            return []

        payload = response.json()
        games: List[Dict] = []
        for game in payload.get("data", []):
            games.append(
                {
                    "game_id": str(game.get("id")),
                    "date": game.get("date"),
                    "home_team": game.get("home_team", {}).get("abbreviation"),
                    "away_team": game.get("visitor_team", {}).get("abbreviation"),
                    "time": game.get("status"),
                }
            )
        return games
