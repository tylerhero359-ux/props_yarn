from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from nba_api.stats.endpoints import (
    CommonPlayerInfo,
    CommonTeamRoster,
    LeagueDashPlayerStats,
    PlayerGameLog,
    PlayerNextNGames,
)
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
POSITION_LABELS = {
    "G": "Guards",
    "F": "Forwards",
    "C": "Centers",
}

PLAYER_POOL = static_players.get_players()
TEAM_POOL = sorted(static_teams.get_teams(), key=lambda team: team["full_name"])
TEAM_LOOKUP = {team["id"]: team for team in TEAM_POOL}
CACHE_TTL_SECONDS = 600
PROFILE_TTL_SECONDS = 43200
POSITION_TTL_SECONDS = 21600
NEXT_GAME_TTL_SECONDS = 1800
GAME_LOG_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
ROSTER_CACHE: dict[tuple[int, str], dict[str, Any]] = {}
PLAYER_INFO_CACHE: dict[int, dict[str, Any]] = {}
NEXT_GAME_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
POSITION_DASH_CACHE: dict[tuple[str, str, str, int], dict[str, Any]] = {}
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
        if elapsed < 0.8:
            time.sleep(0.8 - elapsed)
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


def resolve_primary_position(raw_position: str | None) -> tuple[str | None, str | None]:
    if not raw_position:
        return None, None

    tokens = re.findall(r"[GFC]", str(raw_position).upper())
    if not tokens:
        return None, None

    primary = tokens[0]
    return primary, POSITION_LABELS.get(primary, primary)


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


def fetch_common_player_info(player_id: int) -> dict[str, Any]:
    cached = PLAYER_INFO_CACHE.get(player_id)
    now_ts = time.time()

    if cached and now_ts - cached["timestamp"] < PROFILE_TTL_SECONDS:
        return cached["row"]

    throttle_request()

    try:
        response = CommonPlayerInfo(player_id=player_id, timeout=30)
        df = response.get_data_frames()[0]
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                "Player info request failed. This can happen when NBA stats throttles or times out. "
                f"Details: {exc}"
            ),
        ) from exc

    if df.empty:
        raise HTTPException(status_code=404, detail="Player info not found.")

    row = df.to_dict(orient="records")[0]
    PLAYER_INFO_CACHE[player_id] = {"timestamp": time.time(), "row": row}
    return row


def fetch_next_game(player_id: int, season: str, season_type: str) -> dict[str, Any] | None:
    cache_key = (player_id, season, season_type)
    cached = NEXT_GAME_CACHE.get(cache_key)
    now_ts = time.time()

    if cached and now_ts - cached["timestamp"] < NEXT_GAME_TTL_SECONDS:
        return cached["row"]

    throttle_request()

    try:
        response = PlayerNextNGames(
            player_id=player_id,
            number_of_games=1,
            season_all=season,
            season_type_all_star=season_type,
            timeout=30,
        )
        df = response.get_data_frames()[0]
    except Exception:
        return None

    if df.empty:
        NEXT_GAME_CACHE[cache_key] = {"timestamp": time.time(), "row": None}
        return None

    row = df.to_dict(orient="records")[0]
    NEXT_GAME_CACHE[cache_key] = {"timestamp": time.time(), "row": row}
    return row


def fetch_position_dash(
    season: str,
    season_type: str,
    position_code: str,
    opponent_team_id: int = 0,
) -> list[dict[str, Any]]:
    cache_key = (season, season_type, position_code, opponent_team_id)
    cached = POSITION_DASH_CACHE.get(cache_key)
    now_ts = time.time()

    if cached and now_ts - cached["timestamp"] < POSITION_TTL_SECONDS:
        return cached["rows"]

    throttle_request()

    try:
        response = LeagueDashPlayerStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed="Totals",
            player_position_abbreviation_nullable=position_code,
            opponent_team_id=opponent_team_id,
            timeout=30,
        )
        df = response.get_data_frames()[0]
    except Exception:
        return []

    rows = df.to_dict(orient="records") if not df.empty else []
    POSITION_DASH_CACHE[cache_key] = {"timestamp": time.time(), "rows": rows}
    return rows


def summarize_position_environment(rows: list[dict[str, Any]], stat: str) -> dict[str, Any] | None:
    if not rows:
        return None

    total_gp = 0.0
    total_value = 0.0
    players_count = 0

    for row in rows:
        gp = float(row.get("GP") or 0)
        if gp <= 0:
            continue
        players_count += 1
        total_gp += gp
        total_value += compute_stat_value(row, stat)

    if total_gp <= 0:
        return None

    return {
        "players_count": players_count,
        "sample_gp": round(total_gp, 1),
        "per_player_game": round(total_value / total_gp, 2),
        "total_value": round(total_value, 1),
    }




def compute_recent_hit_streak(hit_flags: list[bool]) -> int:
    streak = 0
    for hit in hit_flags:
        if hit:
            streak += 1
        else:
            break
    return streak

def build_position_matchup(
    opponent_team_id: int,
    position_code: str,
    stat: str,
    season: str,
    season_type: str,
) -> dict[str, Any] | None:
    opponent_rows = fetch_position_dash(
        season=season,
        season_type=season_type,
        position_code=position_code,
        opponent_team_id=opponent_team_id,
    )
    league_rows = fetch_position_dash(
        season=season,
        season_type=season_type,
        position_code=position_code,
        opponent_team_id=0,
    )

    opponent_summary = summarize_position_environment(opponent_rows, stat)
    league_summary = summarize_position_environment(league_rows, stat)
    if not opponent_summary or not league_summary:
        return None

    opponent_value = opponent_summary["per_player_game"]
    league_average = league_summary["per_player_game"]
    delta = round(opponent_value - league_average, 2)
    delta_pct = round((delta / league_average) * 100, 1) if league_average else 0.0

    if delta_pct >= 12:
        lean = "Very favorable"
        lean_tone = "good"
    elif delta_pct >= 5:
        lean = "Favorable"
        lean_tone = "good"
    elif delta_pct <= -12:
        lean = "Very tough"
        lean_tone = "bad"
    elif delta_pct <= -5:
        lean = "Tough"
        lean_tone = "bad"
    else:
        lean = "Neutral"
        lean_tone = "neutral"

    return {
        "position_code": position_code,
        "position_label": POSITION_LABELS.get(position_code, position_code),
        "stat": stat,
        "opponent_value": opponent_value,
        "league_average": league_average,
        "delta": delta,
        "delta_pct": delta_pct,
        "lean": lean,
        "lean_tone": lean_tone,
        "sample_gp": opponent_summary["sample_gp"],
        "players_count": opponent_summary["players_count"],
    }


def build_next_game_payload(
    next_game_row: dict[str, Any] | None,
    player_team_id: int | None,
) -> dict[str, Any] | None:
    if not next_game_row or not player_team_id:
        return None

    home_team_id = int(next_game_row.get("HOME_TEAM_ID") or 0)
    visitor_team_id = int(next_game_row.get("VISITOR_TEAM_ID") or 0)

    if player_team_id == home_team_id:
        is_home = True
        opponent_team_id = visitor_team_id
        opponent_name = next_game_row.get("VISITOR_TEAM_NAME")
        opponent_abbreviation = next_game_row.get("VISITOR_TEAM_ABBREVIATION")
        player_team_abbreviation = next_game_row.get("HOME_TEAM_ABBREVIATION")
        matchup_label = f"vs {opponent_abbreviation}"
    elif player_team_id == visitor_team_id:
        is_home = False
        opponent_team_id = home_team_id
        opponent_name = next_game_row.get("HOME_TEAM_NAME")
        opponent_abbreviation = next_game_row.get("HOME_TEAM_ABBREVIATION")
        player_team_abbreviation = next_game_row.get("VISITOR_TEAM_ABBREVIATION")
        matchup_label = f"@ {opponent_abbreviation}"
    else:
        return None

    return {
        "game_date": str(next_game_row.get("GAME_DATE", "")).strip(),
        "game_time": str(next_game_row.get("GAME_TIME", "")).strip(),
        "is_home": is_home,
        "matchup_label": matchup_label,
        "opponent_team_id": opponent_team_id,
        "opponent_name": str(opponent_name or "").strip(),
        "opponent_abbreviation": str(opponent_abbreviation or "").strip(),
        "player_team_abbreviation": str(player_team_abbreviation or "").strip(),
    }


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
    team = TEAM_LOOKUP.get(team_id)
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




@app.get("/api/bet-finder")
def bet_finder(
    team_id: int,
    stat: str = Query(..., pattern="^(PTS|REB|AST|3PM|STL|BLK|PRA|PR|PA|RA)$"),
    line: float = Query(..., ge=0),
    last_n: int = Query(10, ge=3, le=30),
    season: str | None = None,
    season_type: str = Query("Regular Season"),
    min_games: int = Query(5, ge=1, le=30),
    limit: int = Query(8, ge=1, le=20),
) -> dict[str, Any]:
    selected_season = season or current_nba_season()
    stat = stat.upper()

    team = TEAM_LOOKUP.get(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found.")

    roster_rows = fetch_team_roster(team_id=team_id, season=selected_season)
    results: list[dict[str, Any]] = []

    for row in roster_rows:
        raw_player_id = row.get("PLAYER_ID")
        if raw_player_id in (None, ""):
            continue

        player_id = int(raw_player_id)
        try:
            game_rows = fetch_player_game_log(
                player_id=player_id,
                season=selected_season,
                season_type=season_type,
            )
        except HTTPException:
            continue

        sample_rows = game_rows[:last_n]
        if len(sample_rows) < min_games:
            continue

        values: list[float] = []
        hit_flags: list[bool] = []
        for game_row in sample_rows:
            value = round(compute_stat_value(game_row, stat), 1)
            hit = value >= line
            values.append(value)
            hit_flags.append(hit)

        if not values:
            continue

        hit_count = sum(1 for hit in hit_flags if hit)
        games_count = len(values)
        hit_rate = round((hit_count / games_count) * 100, 1)
        average = round(sum(values) / games_count, 2)
        avg_edge = round(average - line, 2)
        hit_streak = compute_recent_hit_streak(hit_flags)
        last_value = values[0]

        results.append(
            {
                "player": {
                    "id": player_id,
                    "full_name": str(row.get("PLAYER", "")).strip(),
                    "team_id": team["id"],
                    "team_name": team["full_name"],
                    "team_abbreviation": team["abbreviation"],
                    "position": str(row.get("POSITION", "")).strip(),
                    "jersey": str(row.get("NUM", "")).strip(),
                    "is_active": True,
                },
                "stat": stat,
                "line": line,
                "games_count": games_count,
                "hit_count": hit_count,
                "hit_rate": hit_rate,
                "average": average,
                "avg_edge": avg_edge,
                "last_value": last_value,
                "hit_streak": hit_streak,
            }
        )

    results.sort(
        key=lambda item: (
            item["hit_rate"],
            item["avg_edge"],
            item["hit_count"],
            item["average"],
        ),
        reverse=True,
    )

    return {
        "team": {
            "id": team["id"],
            "full_name": team["full_name"],
            "abbreviation": team["abbreviation"],
        },
        "season": selected_season,
        "season_type": season_type,
        "stat": stat,
        "line": line,
        "last_n": last_n,
        "min_games": min_games,
        "results": results[:limit],
    }


@app.get("/api/player-prop")
def player_prop(
    player_id: int,
    stat: str = Query(..., pattern="^(PTS|REB|AST|3PM|STL|BLK|PRA|PR|PA|RA)$"),
    line: float = Query(..., ge=0),
    last_n: int = Query(10, ge=3, le=30),
    season: str | None = None,
    season_type: str = Query("Regular Season"),
    team_id: int | None = Query(None),
    player_position: str | None = Query(None),
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

    profile_row = None
    resolved_team_id = team_id
    resolved_position = player_position

    if resolved_team_id is None or not resolved_position:
        try:
            profile_row = fetch_common_player_info(player_id)
        except HTTPException:
            profile_row = None
        if profile_row:
            if resolved_team_id is None:
                raw_team_id = profile_row.get("TEAM_ID")
                resolved_team_id = int(raw_team_id) if raw_team_id not in (None, "") else None
            if not resolved_position:
                resolved_position = str(profile_row.get("POSITION", "")).strip()

    position_code, position_label = resolve_primary_position(resolved_position)
    next_game_row = fetch_next_game(player_id=player_id, season=selected_season, season_type=season_type)
    next_game = build_next_game_payload(next_game_row, resolved_team_id)
    vs_position = None

    if next_game and position_code:
        vs_position = build_position_matchup(
            opponent_team_id=next_game["opponent_team_id"],
            position_code=position_code,
            stat=stat,
            season=selected_season,
            season_type=season_type,
        )

    return {
        "player": {
            "id": player["id"],
            "full_name": player["full_name"],
            "is_active": player.get("is_active", False),
            "team_id": resolved_team_id,
            "position": resolved_position or "",
            "position_group": position_code or "",
            "position_group_label": position_label or "",
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
        "matchup": {
            "next_game": next_game,
            "vs_position": vs_position,
        },
    }
