from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from nba_api.stats.endpoints import CommonTeamRoster, PlayerGameLog
from nba_api.stats.static import players as static_players
from nba_api.stats.static import teams as static_teams

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="NBA Props Bar Chart App")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

STAT_MAP = {
    "PTS": "PTS",
    "REB": "REB",
    "AST": "AST",
    "3PM": "FG3M",
    "STL": "STL",
    "BLK": "BLK",
    "PRA": None,
    "PR": None,
    "PA": None,
    "RA": None,
}

PLAYER_POOL = static_players.get_players()
TEAM_POOL = sorted(static_teams.get_teams(), key=lambda team: team["full_name"])
CACHE_TTL_SECONDS = 600
GAME_LOG_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
ROSTER_CACHE: dict[tuple[int, str], dict[str, Any]] = {}
REQUEST_LOCK = Lock()
LAST_REQUEST_TIME = 0.0


def current_nba_season() -> str:
    now = datetime.now()
    year = now.year
    if now.month >= 10:
        start_year = year
    else:
        start_year = year - 1
    end_year_short = str(start_year + 1)[-2:]
    return f"{start_year}-{end_year_short}"


def normalize_name(name: str) -> str:
    return " ".join(name.lower().strip().split())


def throttle_request() -> None:
    global LAST_REQUEST_TIME

    with REQUEST_LOCK:
        elapsed = time.time() - LAST_REQUEST_TIME
        if elapsed < 0.7:
            time.sleep(0.7 - elapsed)
        LAST_REQUEST_TIME = time.time()


def compute_stat_value(row: dict[str, Any], stat: str) -> float:
    if stat == "PRA":
        return float(row.get("PTS", 0)) + float(row.get("REB", 0)) + float(row.get("AST", 0))
    if stat == "PR":
        return float(row.get("PTS", 0)) + float(row.get("REB", 0))
    if stat == "PA":
        return float(row.get("PTS", 0)) + float(row.get("AST", 0))
    if stat == "RA":
        return float(row.get("REB", 0)) + float(row.get("AST", 0))

    column = STAT_MAP.get(stat)
    if not column:
        raise ValueError(f"Unsupported stat: {stat}")
    return float(row.get(column, 0))


def fetch_player_game_log(player_id: int, season: str, season_type: str) -> list[dict[str, Any]]:
    cache_key = (player_id, season, season_type)
    cached = GAME_LOG_CACHE.get(cache_key)
    now_ts = time.time()

    if cached and now_ts - cached["timestamp"] < CACHE_TTL_SECONDS:
        return cached["rows"]

    throttle_request()

    try:
        response = PlayerGameLog(
            player_id=player_id,
            season=season,
            season_type_all_star=season_type,
            timeout=30,
        )
        df = response.get_data_frames()[0]
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                "NBA data request failed. This can happen when NBA stats throttles or times out. "
                f"Details: {exc}"
            ),
        ) from exc

    if df.empty:
        raise HTTPException(status_code=404, detail="No game logs found for this player and season.")

    df["GAME_DATE"] = df["GAME_DATE"].astype(str)
    rows = df.to_dict(orient="records")
    rows.sort(key=lambda row: datetime.strptime(row["GAME_DATE"], "%b %d, %Y"), reverse=True)

    GAME_LOG_CACHE[cache_key] = {"timestamp": time.time(), "rows": rows}
    return rows


def fetch_team_roster(team_id: int, season: str) -> list[dict[str, Any]]:
    cache_key = (team_id, season)
    cached = ROSTER_CACHE.get(cache_key)
    now_ts = time.time()

    if cached and now_ts - cached["timestamp"] < CACHE_TTL_SECONDS:
        return cached["rows"]

    throttle_request()

    try:
        response = CommonTeamRoster(team_id=team_id, season=season, timeout=30)
        df = response.get_data_frames()[0]
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                "Team roster request failed. This can happen when NBA stats throttles or times out. "
                f"Details: {exc}"
            ),
        ) from exc

    if df.empty:
        raise HTTPException(status_code=404, detail="No roster found for this team and season.")

    rows = df.to_dict(orient="records")

    def jersey_sort_key(row: dict[str, Any]) -> tuple[int, str]:
        raw_num = str(row.get("NUM", "")).strip()
        jersey_num = int(raw_num) if raw_num.isdigit() else 999
        return jersey_num, str(row.get("PLAYER", "")).lower()

    rows.sort(key=jersey_sort_key)
    ROSTER_CACHE[cache_key] = {"timestamp": time.time(), "rows": rows}
    return rows


@app.get("/")
def root() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/api/teams")
def get_teams() -> dict[str, Any]:
    return {
        "results": [
            {
                "id": team["id"],
                "full_name": team["full_name"],
                "abbreviation": team["abbreviation"],
                "nickname": team["nickname"],
                "city": team["city"],
            }
            for team in TEAM_POOL
        ]
    }


@app.get("/api/teams/{team_id}/roster")
def get_team_roster(team_id: int, season: str | None = None) -> dict[str, Any]:
    selected_season = season or current_nba_season()
    team = next((team for team in TEAM_POOL if team["id"] == team_id), None)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found.")

    rows = fetch_team_roster(team_id=team_id, season=selected_season)
    roster = []
    for row in rows:
        player_id = row.get("PLAYER_ID")
        if not player_id:
            continue
        roster.append(
            {
                "id": int(player_id),
                "full_name": str(row.get("PLAYER", "")).strip(),
                "jersey": str(row.get("NUM", "")).strip(),
                "position": str(row.get("POSITION", "")).strip(),
                "is_active": True,
                "team_id": team["id"],
                "team_name": team["full_name"],
                "team_abbreviation": team["abbreviation"],
            }
        )

    return {
        "team": {
            "id": team["id"],
            "full_name": team["full_name"],
            "abbreviation": team["abbreviation"],
        },
        "season": selected_season,
        "results": roster,
    }


@app.get("/api/players/search")
def search_players(q: str = Query(..., min_length=1, max_length=50)) -> dict[str, Any]:
    needle = normalize_name(q)
    matches: list[dict[str, Any]] = []

    for player in PLAYER_POOL:
        full_name = player.get("full_name", "")
        if needle in normalize_name(full_name):
            matches.append(
                {
                    "id": player["id"],
                    "full_name": full_name,
                    "is_active": player.get("is_active", False),
                }
            )

    matches.sort(key=lambda item: (not item["is_active"], item["full_name"]))
    return {"results": matches[:15]}


@app.get("/api/player-prop")
def player_prop(
    player_id: int,
    stat: str = Query(..., pattern="^(PTS|REB|AST|3PM|STL|BLK|PRA|PR|PA|RA)$"),
    line: float = Query(..., ge=0),
    last_n: int = Query(10, ge=3, le=30),
    season: str | None = None,
    season_type: str = Query("Regular Season"),
) -> dict[str, Any]:
    selected_season = season or current_nba_season()
    stat = stat.upper()

    player = next((p for p in PLAYER_POOL if p["id"] == player_id), None)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found.")

    rows = fetch_player_game_log(player_id=player_id, season=selected_season, season_type=season_type)
    rows = rows[:last_n]

    games: list[dict[str, Any]] = []
    hit_count = 0
    values: list[float] = []

    for row in rows:
        value = round(compute_stat_value(row, stat), 1)
        hit = value >= line
        if hit:
            hit_count += 1
        values.append(value)
        games.append(
            {
                "game_date": row["GAME_DATE"],
                "matchup": row.get("MATCHUP", ""),
                "value": value,
                "hit": hit,
            }
        )

    average = round(sum(values) / len(values), 2) if values else 0
    hit_rate = round((hit_count / len(values)) * 100, 1) if values else 0

    return {
        "player": {
            "id": player["id"],
            "full_name": player["full_name"],
            "is_active": player.get("is_active", False),
        },
        "season": selected_season,
        "season_type": season_type,
        "stat": stat,
        "line": line,
        "last_n": last_n,
        "average": average,
        "hit_count": hit_count,
        "games_count": len(values),
        "hit_rate": hit_rate,
        "games": list(reversed(games)),
    }
