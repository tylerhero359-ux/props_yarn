from __future__ import annotations

import re
import time
import unicodedata
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Any

import requests
from pypdf import PdfReader

try:
    import pdfplumber  # type: ignore
except Exception:
    pdfplumber = None

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from nba_api.stats.endpoints import (
    CommonPlayerInfo,
    CommonTeamRoster,
    LeagueDashPlayerStats,
    PlayerGameLog,
    PlayerNextNGames,
    ScoreboardV2,
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


def current_nba_game_date() -> str:
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

PLAYER_POOL = static_players.get_players()
TEAM_POOL = sorted(static_teams.get_teams(), key=lambda team: team["full_name"])
TEAM_LOOKUP = {team["id"]: team for team in TEAM_POOL}


def build_team_alias_lookup() -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for team in TEAM_POOL:
        keys = {
            str(team.get("abbreviation") or "").strip().lower(),
            str(team.get("nickname") or "").strip().lower(),
            str(team.get("city") or "").strip().lower(),
            str(team.get("full_name") or "").strip().lower(),
        }
        for key in keys:
            if key:
                lookup[key] = team
    return lookup


TEAM_ALIAS_LOOKUP = build_team_alias_lookup()
CACHE_TTL_SECONDS = 600
PROFILE_TTL_SECONDS = 43200
POSITION_TTL_SECONDS = 21600
NEXT_GAME_TTL_SECONDS = 1800
GAME_LOG_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
ROSTER_CACHE: dict[tuple[int, str], dict[str, Any]] = {}
PLAYER_INFO_CACHE: dict[int, dict[str, Any]] = {}
NEXT_GAME_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
TEAM_NEXT_GAME_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
POSITION_DASH_CACHE: dict[tuple[str, str, str, int], dict[str, Any]] = {}
SCOREBOARD_CACHE: dict[str, dict[str, Any]] = {}
INJURY_REPORT_PAGE_URL = "https://official.nba.com/nba-injury-report-2025-26-season/"
INJURY_REPORT_TTL_SECONDS = 300
INJURY_REPORT_CACHE: dict[str, Any] = {"timestamp": 0.0, "payload": None}
INJURY_REPORT_LINKS_CACHE: dict[str, Any] = {"timestamp": 0.0, "links": []}
INJURY_REPORT_URL_CACHE: dict[str, dict[str, Any]] = {}
INJURY_MATCH_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
INJURY_STATUS_ORDER = {"Out": 0, "Ineligible": 0, "Suspended": 0, "Doubtful": 1, "Questionable": 2, "Pending report": 2, "Not listed": 3, "Available": 4, "Probable": 4}
UNAVAILABLE_STATUSES = {"Out", "Ineligible", "Suspended"}
RISKY_STATUSES = {"Doubtful", "Questionable", "Pending report"}
GOOD_STATUSES = {"Available", "Probable"}
REPORT_STATUSES = ["Questionable", "Ineligible", "Suspended", "Doubtful", "Probable", "Available", "Out"]
STATUS_PATTERN = "|".join(re.escape(status) for status in sorted(REPORT_STATUSES, key=len, reverse=True))
REQUEST_LOCK = Lock()
LAST_REQUEST_TIME = 0.0


def current_nba_season() -> str:
    now = datetime.now(ZoneInfo("America/New_York"))
    year = now.year
    if now.month >= 10:
        start_year = year
    else:
        start_year = year - 1
    end_year_short = str(start_year + 1)[-2:]
    return f"{start_year}-{end_year_short}"


def normalize_name(name: str) -> str:
    return " ".join(name.lower().strip().split())


def normalize_compact_text(value: str) -> str:
    raw = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    raw = re.sub(r"[^A-Za-z0-9]", "", raw)
    return raw.lower()


def normalize_report_person_name(name: str) -> str:
    raw = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii")
    raw = raw.replace("-", " ")
    raw = raw.replace("'", "")
    raw = raw.replace(".", " ")
    raw = raw.strip()

    if "," in raw:
        last, first = [part.strip() for part in raw.split(",", 1)]
        raw = f"{first} {last}".strip()

    raw = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", " ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"[^A-Za-z\s]", " ", raw)
    return " ".join(raw.lower().split())


def build_player_name_variants(name: str) -> set[str]:
    variants: set[str] = set()
    raw = str(name or "").strip()
    if not raw:
        return variants

    canonical = normalize_report_person_name(raw)
    if canonical:
        variants.add(canonical)

    ascii_raw = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z,\s'-]", " ", ascii_raw)
    cleaned = " ".join(cleaned.split())
    if cleaned:
        variants.add(normalize_report_person_name(cleaned))

    if "," in ascii_raw:
        parts = [part.strip() for part in ascii_raw.split(",", 1)]
        if len(parts) == 2:
            reversed_name = f"{parts[1]} {parts[0]}".strip()
            if reversed_name:
                variants.add(normalize_report_person_name(reversed_name))
    else:
        pieces = [piece for piece in re.split(r"\s+", ascii_raw.strip()) if piece]
        if len(pieces) >= 2:
            reversed_name = f"{pieces[-1]}, {' '.join(pieces[:-1])}".strip()
            variants.add(normalize_report_person_name(reversed_name))

    return {variant for variant in variants if variant}


def report_name_variants(name: str) -> set[str]:
    """Backward-compatible alias for player name variant generation."""
    return build_player_name_variants(name)


def parse_injury_report_timestamp(url: str) -> datetime:
    match = re.search(r"Injury-Report_(\d{4}-\d{2}-\d{2})_(\d{2})_(\d{2})(AM|PM)\.pdf", url)
    if not match:
        return datetime.min
    date_part, hour, minute, meridiem = match.groups()
    return datetime.strptime(f"{date_part} {hour}:{minute}{meridiem}", "%Y-%m-%d %I:%M%p")


def format_injury_report_timestamp(report_dt: datetime | None) -> str:
    if not report_dt or report_dt == datetime.min:
        return ""
    return report_dt.strftime("%b %d, %Y %I:%M %p ET")


def extract_team_prefix(text_line: str) -> tuple[str | None, str]:
    compact_line = normalize_compact_text(text_line)
    for team in sorted(TEAM_POOL, key=lambda item: len(item["full_name"]), reverse=True):
        team_name = str(team["full_name"])
        if text_line.startswith(team_name):
            return team_name, text_line[len(team_name):].strip()

        compact_team = normalize_compact_text(team_name)
        if compact_line.startswith(compact_team):
            consumed = len(compact_team)
            remainder_start = 0
            alnum_seen = 0
            for idx, ch in enumerate(text_line):
                if ch.isalnum():
                    alnum_seen += 1
                if alnum_seen >= consumed:
                    remainder_start = idx + 1
                    break
            return team_name, text_line[remainder_start:].strip()

    return None, text_line


def parse_injury_report_rows(report_text: str) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    pending_teams: set[str] = set()
    current_team: str | None = None
    last_row: dict[str, Any] | None = None
    for raw_line in report_text.splitlines():
        line = " ".join(str(raw_line or "").split())
        if not line:
            continue
        compact_line = normalize_compact_text(line)
        if line.startswith("Injury Report:") or line.startswith("Page ") or compact_line.startswith("page"):
            continue
        if line.startswith("Game Date ") or line.startswith("Current Status") or compact_line.startswith("gamedate"):
            continue

        full_row_match = re.match(r"^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}\s*\(ET\)\s+[A-Z]{2,4}@[A-Z]{2,4}\s+(.*)$", line)
        if full_row_match:
            line = full_row_match.group(1).strip()

        team_name, remainder = extract_team_prefix(line)
        if team_name:
            current_team = team_name
        else:
            remainder = line

        if remainder == "NOT YET SUBMITTED":
            if current_team:
                pending_teams.add(current_team)
            last_row = None
            continue

        row_match = re.match(
            rf"^(?P<player>.+?)\s+(?P<status>{STATUS_PATTERN})\b(?:\s+(?P<reason>.*))?$",
            remainder,
        )
        if row_match and current_team:
            player_display = row_match.group("player").strip()
            status = row_match.group("status").strip()
            reason = (row_match.group("reason") or "").strip()
            row_payload = {
                "team_name": current_team,
                "player_display": player_display,
                "player_key": normalize_report_person_name(player_display),
                "status": status,
                "reason": reason,
            }
            rows.append(row_payload)
            last_row = row_payload
            continue

        if last_row and not extract_team_prefix(line)[0] and not re.match(r"^\d{2}/\d{2}/\d{4}", line):
            continuation = line.strip()
            if continuation and not continuation.startswith("Game Date"):
                last_row["reason"] = f"{last_row.get('reason', '').strip()} {continuation}".strip()

    return {"rows": rows, "pending_teams": sorted(pending_teams)}


def extract_injury_report_text_candidates(pdf_bytes: bytes) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []

    if pdfplumber is not None:
        try:
            plumber_pages: list[str] = []
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:  # type: ignore[arg-type]
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    plumber_pages.append(page_text)
            plumber_text = "\n".join(plumber_pages).strip()
            if plumber_text:
                candidates.append({"method": "pdfplumber", "text": plumber_text})
        except Exception:
            pass

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        pypdf_text = "\n".join((page.extract_text() or "") for page in reader.pages).strip()
        if pypdf_text:
            candidates.append({"method": "pypdf", "text": pypdf_text})
    except Exception:
        pass

    unique: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates:
        text_value = candidate["text"]
        if text_value not in seen:
            seen.add(text_value)
            unique.append(candidate)
    return unique


def choose_best_injury_report_parse(pdf_bytes: bytes) -> dict[str, Any]:
    candidates = extract_injury_report_text_candidates(pdf_bytes)
    best_payload = {"rows": [], "pending_teams": [], "raw_text": "", "method": "none"}

    for candidate in candidates:
        parsed = parse_injury_report_rows(candidate["text"])
        score = len(parsed.get("rows") or [])
        best_score = len(best_payload.get("rows") or [])
        if score > best_score:
            best_payload = {
                "rows": parsed.get("rows") or [],
                "pending_teams": parsed.get("pending_teams") or [],
                "raw_text": candidate["text"],
                "method": candidate["method"],
            }

    if not best_payload["rows"] and candidates:
        fallback = candidates[0]
        parsed = parse_injury_report_rows(fallback["text"])
        best_payload = {
            "rows": parsed.get("rows") or [],
            "pending_teams": parsed.get("pending_teams") or [],
            "raw_text": fallback["text"],
            "method": fallback["method"],
        }

    return best_payload


def list_recent_injury_report_links(limit: int = 12) -> list[str]:
    now_ts = time.time()
    cached_links = INJURY_REPORT_LINKS_CACHE.get("links") or []
    cached_ts = float(INJURY_REPORT_LINKS_CACHE.get("timestamp") or 0.0)
    if cached_links and now_ts - cached_ts < INJURY_REPORT_TTL_SECONDS:
        return list(cached_links)[:limit]

    headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
    page_response = requests.get(INJURY_REPORT_PAGE_URL, headers=headers, timeout=20)
    page_response.raise_for_status()
    html = page_response.text
    links = set(re.findall(r"https://ak-static\.cms\.nba\.com/referee/injury/Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html))
    if not links:
        relative_links = re.findall(r"/wp-content/uploads/.+?Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html)
        links = {f"https://official.nba.com{match}" for match in relative_links}
    if not links:
        raise RuntimeError("No injury report PDF links found on the official page.")

    sorted_links = sorted(links, key=parse_injury_report_timestamp, reverse=True)
    INJURY_REPORT_LINKS_CACHE["timestamp"] = now_ts
    INJURY_REPORT_LINKS_CACHE["links"] = sorted_links
    return sorted_links[:limit]


def fetch_injury_report_payload_for_url(report_url: str) -> dict[str, Any]:
    cached = INJURY_REPORT_URL_CACHE.get(report_url)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < INJURY_REPORT_TTL_SECONDS:
        return dict(cached.get("payload") or {})

    headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
    latest_dt = parse_injury_report_timestamp(report_url)
    pdf_response = requests.get(report_url, headers=headers, timeout=30)
    pdf_response.raise_for_status()
    parsed_choice = choose_best_injury_report_parse(pdf_response.content)
    payload = {
        "ok": True,
        "report_url": report_url,
        "report_timestamp": latest_dt.isoformat() if latest_dt != datetime.min else "",
        "report_label": format_injury_report_timestamp(latest_dt),
        "rows": parsed_choice.get("rows") or [],
        "pending_teams": parsed_choice.get("pending_teams") or [],
        "raw_text": parsed_choice.get("raw_text") or "",
        "parse_method": parsed_choice.get("method") or "unknown",
        "error": None,
    }
    INJURY_REPORT_URL_CACHE[report_url] = {"timestamp": now_ts, "payload": payload}
    return payload


def search_report_payload_for_player(report_payload: dict[str, Any], player_name: str, team_name: str | None = None) -> dict[str, Any] | None:
    player_key = normalize_report_person_name(player_name)
    wanted_team = str(team_name or "").strip()
    rows = report_payload.get("rows") or []
    candidates = [row for row in rows if row.get("player_key") == player_key]

    if wanted_team:
        team_filtered = [row for row in candidates if row.get("team_name") == wanted_team]
        if team_filtered:
            candidates = team_filtered

    if not candidates:
        token_set = set(player_key.split())
        fuzzy_matches = []
        for row in rows:
            row_tokens = set(str(row.get("player_key") or "").split())
            if token_set and token_set.issubset(row_tokens):
                fuzzy_matches.append(row)
        if wanted_team:
            team_fuzzy = [row for row in fuzzy_matches if row.get("team_name") == wanted_team]
            if team_fuzzy:
                fuzzy_matches = team_fuzzy
        if fuzzy_matches:
            candidates = fuzzy_matches

    if not candidates:
        direct_row = try_direct_report_match(
            report_text=str(report_payload.get("raw_text") or ""),
            player_name=player_name,
            team_name=team_name or None,
        )
        if direct_row:
            candidates = [direct_row]

    if not candidates:
        raw_text = str(report_payload.get("raw_text") or "")
        name_variants = [
            player_name.strip(),
            " ".join(reversed(player_name.strip().split(" ", 1))) if " " in player_name.strip() else player_name.strip(),
        ]
        last_first = ""
        parts = [part for part in player_name.strip().split() if part]
        if len(parts) >= 2:
            last_first = f"{parts[-1]}, {' '.join(parts[:-1])}"
            name_variants.append(last_first)
        lowered_lines = [" ".join(line.split()) for line in raw_text.splitlines() if str(line or "").strip()]
        current_team: str | None = None
        for line in lowered_lines:
            team_hit, remainder = extract_team_prefix(line)
            if team_hit:
                current_team = team_hit
            else:
                remainder = line
            if wanted_team and current_team != wanted_team:
                continue
            for variant in name_variants:
                if variant and variant.lower() in remainder.lower():
                    row_match = re.match(rf"^(?P<player>.+?)\s+(?P<status>{STATUS_PATTERN})\b(?:\s+(?P<reason>.*))?$", remainder)
                    if row_match:
                        return {
                            "team_name": current_team or wanted_team,
                            "player_display": row_match.group("player").strip(),
                            "player_key": normalize_report_person_name(row_match.group("player").strip()),
                            "status": row_match.group("status").strip(),
                            "reason": (row_match.group("reason") or "").strip(),
                        }

    return candidates[0] if candidates else None


def find_player_in_recent_reports(player_name: str, team_name: str | None = None, max_reports: int = 8) -> dict[str, Any] | None:
    cache_key = (normalize_report_person_name(player_name), str(team_name or "").strip())
    cached = INJURY_MATCH_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < INJURY_REPORT_TTL_SECONDS:
        result = cached.get("result")
        return dict(result) if isinstance(result, dict) else None

    try:
        links = list_recent_injury_report_links(limit=max_reports)
    except Exception:
        return None

    for link in links:
        try:
            payload = fetch_injury_report_payload_for_url(link)
        except Exception:
            continue
        matched_row = search_report_payload_for_player(payload, player_name=player_name, team_name=team_name)
        if matched_row:
            result = {
                "row": matched_row,
                "report_label": payload.get("report_label") or "",
                "report_url": payload.get("report_url") or link,
                "pending_teams": payload.get("pending_teams") or [],
            }
            INJURY_MATCH_CACHE[cache_key] = {"timestamp": now_ts, "result": result}
            return result

    INJURY_MATCH_CACHE[cache_key] = {"timestamp": now_ts, "result": None}
    return None


def try_direct_report_match(report_text: str, player_name: str, team_name: str | None = None) -> dict[str, Any] | None:
    if not report_text.strip():
        return None

    parsed = parse_injury_report_rows(report_text)
    rows = parsed.get("rows") or []
    player_key = normalize_report_person_name(player_name)
    wanted_team = str(team_name or "").strip()

    exact = [row for row in rows if row.get("player_key") == player_key]
    if wanted_team:
        team_exact = [row for row in exact if row.get("team_name") == wanted_team]
        if team_exact:
            exact = team_exact
    if exact:
        return exact[0]

    token_set = set(player_key.split())
    fuzzy: list[dict[str, Any]] = []
    for row in rows:
        row_tokens = set(str(row.get("player_key") or "").split())
        if token_set and token_set.issubset(row_tokens):
            fuzzy.append(row)
    if wanted_team:
        team_fuzzy = [row for row in fuzzy if row.get("team_name") == wanted_team]
        if team_fuzzy:
            fuzzy = team_fuzzy
    if fuzzy:
        return fuzzy[0]

    return None


def fetch_latest_injury_report_payload() -> dict[str, Any]:
    now_ts = time.time()
    cached = INJURY_REPORT_CACHE.get("payload")
    if cached and now_ts - float(INJURY_REPORT_CACHE.get("timestamp") or 0.0) < INJURY_REPORT_TTL_SECONDS:
        return cached

    headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
    try:
        page_response = requests.get(INJURY_REPORT_PAGE_URL, headers=headers, timeout=20)
        page_response.raise_for_status()
        html = page_response.text
        links = set(re.findall(r"https://ak-static\.cms\.nba\.com/referee/injury/Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html))
        if not links:
            relative_links = re.findall(r"/wp-content/uploads/.+?Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html)
            links = {f"https://official.nba.com{match}" for match in relative_links}
        if not links:
            raise RuntimeError("No injury report PDF links found on the official page.")

        latest_url = max(links, key=parse_injury_report_timestamp)
        latest_dt = parse_injury_report_timestamp(latest_url)
        pdf_response = requests.get(latest_url, headers=headers, timeout=30)
        pdf_response.raise_for_status()

        parsed_choice = choose_best_injury_report_parse(pdf_response.content)
        payload = {
            "ok": True,
            "report_url": latest_url,
            "report_timestamp": latest_dt.isoformat() if latest_dt != datetime.min else "",
            "report_label": format_injury_report_timestamp(latest_dt),
            "rows": parsed_choice.get("rows") or [],
            "pending_teams": parsed_choice.get("pending_teams") or [],
            "raw_text": parsed_choice.get("raw_text") or "",
            "parse_method": parsed_choice.get("method") or "unknown",
            "error": None,
        }
    except Exception as exc:
        payload = {
            "ok": False,
            "report_url": "",
            "report_timestamp": "",
            "report_label": "",
            "rows": [],
            "pending_teams": [],
            "raw_text": "",
            "parse_method": "error",
            "error": str(exc),
        }

    INJURY_REPORT_CACHE["timestamp"] = now_ts
    INJURY_REPORT_CACHE["payload"] = payload
    return payload


def build_availability_payload(player_name: str, team_name: str | None = None) -> dict[str, Any]:
    report_payload = fetch_latest_injury_report_payload()
    if not report_payload.get("ok"):
        return {
            "status": "Unknown",
            "tone": "neutral",
            "reason": "Official injury report unavailable right now.",
            "note": "Could not fetch the latest official injury report.",
            "source": "Official NBA injury report",
            "report_label": "",
            "report_url": "",
            "is_unavailable": False,
            "is_risky": False,
            "sort_rank": 3,
        }

    player_key = normalize_report_person_name(player_name)
    team_name = str(team_name or "").strip()
    rows = report_payload.get("rows") or []
    candidates = [row for row in rows if row.get("player_key") == player_key]

    if team_name:
        team_filtered = [row for row in candidates if row.get("team_name") == team_name]
        if team_filtered:
            candidates = team_filtered

    if not candidates:
        token_set = set(player_key.split())
        fuzzy_matches = []
        for row in rows:
            row_tokens = set(str(row.get("player_key") or "").split())
            if token_set and token_set.issubset(row_tokens):
                fuzzy_matches.append(row)
        if team_name:
            team_fuzzy = [row for row in fuzzy_matches if row.get("team_name") == team_name]
            if team_fuzzy:
                fuzzy_matches = team_fuzzy
        if fuzzy_matches:
            candidates = fuzzy_matches

    if not candidates:
        direct_row = try_direct_report_match(
            report_text=str(report_payload.get("raw_text") or ""),
            player_name=player_name,
            team_name=team_name or None,
        )
        if direct_row:
            candidates = [direct_row]

    if candidates:
        row = candidates[0]
        status = str(row.get("status") or "Unknown").strip()
        if status in GOOD_STATUSES:
            tone = "good"
        elif status in UNAVAILABLE_STATUSES:
            tone = "bad"
        elif status in RISKY_STATUSES:
            tone = "warning"
        else:
            tone = "neutral"

        reason = str(row.get("reason") or "").strip()
        note = reason or "Official status found on the latest NBA injury report."
        return {
            "status": status,
            "tone": tone,
            "reason": reason,
            "note": note,
            "source": "Official NBA injury report",
            "report_label": report_payload.get("report_label") or "",
            "report_url": report_payload.get("report_url") or "",
            "is_unavailable": status in UNAVAILABLE_STATUSES,
            "is_risky": status in RISKY_STATUSES,
            "sort_rank": INJURY_STATUS_ORDER.get(status, 3),
        }

    if team_name and team_name in set(report_payload.get("pending_teams") or []):
        return {
            "status": "Pending report",
            "tone": "warning",
            "reason": "Team report not yet submitted on the latest official injury report.",
            "note": "The team has not yet submitted its latest official report.",
            "source": "Official NBA injury report",
            "report_label": report_payload.get("report_label") or "",
            "report_url": report_payload.get("report_url") or "",
            "is_unavailable": False,
            "is_risky": True,
            "sort_rank": INJURY_STATUS_ORDER.get("Pending report", 2),
        }

    recent_match = find_player_in_recent_reports(player_name=player_name, team_name=team_name or None, max_reports=12)
    if recent_match and recent_match.get("row"):
        row = dict(recent_match["row"])
        status = str(row.get("status") or "Unknown").strip()
        if status in GOOD_STATUSES:
            tone = "good"
        elif status in UNAVAILABLE_STATUSES:
            tone = "bad"
        elif status in RISKY_STATUSES:
            tone = "warning"
        else:
            tone = "neutral"

        reason = str(row.get("reason") or "").strip()
        note = reason or "Official status found on a recent NBA injury report."
        return {
            "status": status,
            "tone": tone,
            "reason": reason,
            "note": note,
            "source": "Official NBA injury report",
            "report_label": recent_match.get("report_label") or report_payload.get("report_label") or "",
            "report_url": recent_match.get("report_url") or report_payload.get("report_url") or "",
            "is_unavailable": status in UNAVAILABLE_STATUSES,
            "is_risky": status in RISKY_STATUSES,
            "sort_rank": INJURY_STATUS_ORDER.get(status, 3),
        }

    return {
        "status": "Not listed",
        "tone": "neutral",
        "reason": "Player was not listed on the latest official report.",
        "note": f"Not listed on the latest official injury report ({report_payload.get('report_label') or 'latest update'}).",
        "source": "Official NBA injury report",
        "report_label": report_payload.get("report_label") or "",
        "report_url": report_payload.get("report_url") or "",
        "is_unavailable": False,
        "is_risky": False,
        "sort_rank": INJURY_STATUS_ORDER.get("Not listed", 3),
    }


def build_team_availability_summary(team_name: str | None, report_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    team_name = str(team_name or "").strip()
    payload = report_payload or fetch_latest_injury_report_payload()
    if not payload.get("ok"):
        return {
            "team_name": team_name,
            "headline": "Report unavailable",
            "tone": "neutral",
            "listed_count": 0,
            "out_count": 0,
            "questionable_count": 0,
            "probable_count": 0,
            "players": [],
        }

    pending_teams = set(payload.get("pending_teams") or [])
    if team_name and team_name in pending_teams:
        return {
            "team_name": team_name,
            "headline": "Pending report",
            "tone": "warning",
            "listed_count": 0,
            "out_count": 0,
            "questionable_count": 0,
            "probable_count": 0,
            "players": [],
        }

    rows = [row for row in (payload.get("rows") or []) if row.get("team_name") == team_name]
    out_count = sum(1 for row in rows if str(row.get("status") or "") in UNAVAILABLE_STATUSES)
    questionable_count = sum(1 for row in rows if str(row.get("status") or "") in RISKY_STATUSES)
    probable_count = sum(1 for row in rows if str(row.get("status") or "") in GOOD_STATUSES)

    if out_count:
        headline = f"{out_count} out"
        if questionable_count:
            headline += f" • {questionable_count} questionable"
        tone = "bad"
    elif questionable_count:
        headline = f"{questionable_count} questionable"
        tone = "warning"
    elif rows:
        headline = f"{len(rows)} listed"
        tone = "neutral"
    else:
        headline = "Clean report"
        tone = "good"

    return {
        "team_name": team_name,
        "headline": headline,
        "tone": tone,
        "listed_count": len(rows),
        "out_count": out_count,
        "questionable_count": questionable_count,
        "probable_count": probable_count,
        "players": [
            {
                "name": str(row.get("player_display") or "").strip(),
                "status": str(row.get("status") or "").strip(),
            }
            for row in rows[:4]
        ],
    }


def build_confidence_engine(
    *,
    side: str,
    hit_rate: float,
    games_count: int,
    edge: float | None,
    ev: float,
    matchup_delta_pct: float | None,
    availability: dict[str, Any],
) -> dict[str, Any]:
    if availability.get("is_unavailable"):
        return {
            "grade": "X",
            "score": 0,
            "tone": "out",
            "summary": "Officially unavailable on the latest report.",
        }

    edge_value = float(edge or 0.0)
    matchup_value = float(matchup_delta_pct or 0.0)
    if side == "UNDER":
        matchup_value *= -1

    score = 44.0
    score += max(-14.0, min(20.0, ev * 110.0))
    score += max(-12.0, min(18.0, edge_value * 1.1))
    score += max(-9.0, min(15.0, (hit_rate - 50.0) * 0.48))
    score += max(-5.0, min(7.0, (games_count - 5) * 0.7))

    matchup_penalty_applied = False
    matchup_bonus_applied = False
    if matchup_delta_pct is not None:
        if matchup_value >= 0:
            score += max(0.0, min(8.0, matchup_value * 0.32))
            matchup_bonus_applied = matchup_value >= 5
        else:
            score -= max(0.0, min(22.0, abs(matchup_value) * 0.9))
            matchup_penalty_applied = abs(matchup_value) >= 5
            if abs(matchup_value) >= 12:
                score = min(score, 74.0)
            elif abs(matchup_value) >= 5:
                score = min(score, 83.0)

    if availability.get("is_risky"):
        score -= 14.0

    score = int(max(0, min(99, round(score))))

    if score >= 84:
        grade = "A"
        tone = "elite"
    elif score >= 72:
        grade = "B"
        tone = "good"
    elif score >= 60:
        grade = "C"
        tone = "warm"
    elif score >= 48:
        grade = "D"
        tone = "neutral"
    else:
        grade = "F"
        tone = "bad"

    summary_parts: list[str] = []
    if edge_value >= 8:
        summary_parts.append("strong edge")
    elif edge_value >= 3:
        summary_parts.append("modest edge")
    else:
        summary_parts.append("thin edge")

    if ev >= 0.08:
        summary_parts.append("healthy EV")
    elif ev >= 0.02:
        summary_parts.append("positive EV")
    else:
        summary_parts.append("limited EV")

    if matchup_bonus_applied:
        summary_parts.append("supportive matchup")
    elif matchup_penalty_applied:
        summary_parts.append("tough matchup penalty")

    if availability.get("is_risky"):
        summary_parts.append("watch availability")

    return {
        "grade": grade,
        "score": score,
        "tone": tone,
        "summary": " • ".join(summary_parts).capitalize(),
    }


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


def parse_minutes_to_decimal(raw_minutes: Any) -> float:
    raw = str(raw_minutes or '').strip()
    if not raw:
        return 0.0
    if ':' in raw:
        try:
            mins, secs = raw.split(':', 1)
            return round(int(mins) + int(secs) / 60.0, 1)
        except Exception:
            return 0.0
    try:
        return round(float(raw), 1)
    except Exception:
        return 0.0


def safe_stat_number(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key, 0) or 0)
    except Exception:
        return 0.0


def average_or_zero(values: list[float], digits: int = 1) -> float:
    return round(sum(values) / len(values), digits) if values else 0.0


def build_game_log_entry(row: dict[str, Any], stat: str, line: float) -> dict[str, Any]:
    value = round(compute_stat_value(row, stat), 1)
    return {
        "game_date": row["GAME_DATE"],
        "matchup": row.get("MATCHUP", ""),
        "value": value,
        "hit": value >= line,
        "minutes": parse_minutes_to_decimal(row.get("MIN")),
        "fga": round(safe_stat_number(row, "FGA"), 1),
        "fg3a": round(safe_stat_number(row, "FG3A"), 1),
        "fta": round(safe_stat_number(row, "FTA"), 1),
    }


def build_opportunity_context(season_rows: list[dict[str, Any]], last_n: int) -> dict[str, Any]:
    recent_rows = season_rows[: max(1, min(last_n, len(season_rows)))]
    short_rows = season_rows[: max(1, min(5, len(season_rows)))]
    older_rows = season_rows[5:10] if len(season_rows) >= 10 else season_rows[len(short_rows):]

    short_minutes = [parse_minutes_to_decimal(row.get("MIN")) for row in short_rows]
    recent_minutes = [parse_minutes_to_decimal(row.get("MIN")) for row in recent_rows]
    older_minutes = [parse_minutes_to_decimal(row.get("MIN")) for row in older_rows]

    short_fga = [safe_stat_number(row, "FGA") for row in short_rows]
    recent_fga = [safe_stat_number(row, "FGA") for row in recent_rows]
    older_fga = [safe_stat_number(row, "FGA") for row in older_rows]

    short_fg3a = [safe_stat_number(row, "FG3A") for row in short_rows]
    recent_fg3a = [safe_stat_number(row, "FG3A") for row in recent_rows]
    short_fta = [safe_stat_number(row, "FTA") for row in short_rows]
    recent_fta = [safe_stat_number(row, "FTA") for row in recent_rows]

    mins_last5 = average_or_zero(short_minutes)
    mins_sample = average_or_zero(recent_minutes)
    fga_last5 = average_or_zero(short_fga)
    fga_sample = average_or_zero(recent_fga)
    fg3a_last5 = average_or_zero(short_fg3a)
    fg3a_sample = average_or_zero(recent_fg3a)
    fta_last5 = average_or_zero(short_fta)
    fta_sample = average_or_zero(recent_fta)

    older_min_avg = average_or_zero(older_minutes) if older_minutes else mins_sample
    older_fga_avg = average_or_zero(older_fga) if older_fga else fga_sample

    minute_delta = round(mins_last5 - older_min_avg, 1)
    fga_delta = round(fga_last5 - older_fga_avg, 1)

    if minute_delta >= 2.0:
        minutes_trend = 'up'
        minutes_label = 'Minutes rising'
    elif minute_delta <= -2.0:
        minutes_trend = 'down'
        minutes_label = 'Minutes dipping'
    else:
        minutes_trend = 'steady'
        minutes_label = 'Minutes stable'

    if fga_delta >= 2.0:
        volume_trend = 'up'
        volume_label = 'Shot volume rising'
    elif fga_delta <= -2.0:
        volume_trend = 'down'
        volume_label = 'Shot volume dipping'
    else:
        volume_trend = 'steady'
        volume_label = 'Shot volume stable'

    return {
        'minutes_last5': mins_last5,
        'minutes_sample': mins_sample,
        'minutes_delta': minute_delta,
        'minutes_trend': minutes_trend,
        'minutes_label': minutes_label,
        'fga_last5': fga_last5,
        'fga_sample': fga_sample,
        'fga_delta': fga_delta,
        'volume_trend': volume_trend,
        'volume_label': volume_label,
        'fg3a_last5': fg3a_last5,
        'fg3a_sample': fg3a_sample,
        'fta_last5': fta_last5,
        'fta_sample': fta_sample,
        'summary': f'{minutes_label} • {mins_last5:.1f} MIN lately. {volume_label} • {fga_last5:.1f} FGA lately.',
    }


def build_team_opportunity_context(team_name: str | None, player_name: str, stat: str) -> dict[str, Any]:
    payload = fetch_latest_injury_report_payload()
    team_name = str(team_name or '').strip()
    empty = {
        'headline': 'No major same-team absences flagged',
        'tone': 'neutral',
        'summary': 'No major same-team absences are flagged on the latest report.',
        'listed_count': 0,
        'impact_count': 0,
        'players': [],
    }
    if not payload.get('ok') or not team_name:
        return empty

    rows = [row for row in (payload.get('rows') or []) if row.get('team_name') == team_name]
    if not rows:
        return empty

    player_keys = build_player_name_variants(player_name) or set()
    filtered = [row for row in rows if row.get('player_key') not in player_keys]
    impacted = [row for row in filtered if str(row.get('status') or '') in UNAVAILABLE_STATUSES.union(RISKY_STATUSES)]
    impacted.sort(key=lambda row: INJURY_STATUS_ORDER.get(str(row.get('status') or ''), 9))

    if not impacted:
        return {
            **empty,
            'listed_count': len(filtered),
        }

    out_count = sum(1 for row in impacted if str(row.get('status') or '') in UNAVAILABLE_STATUSES)
    risky_count = sum(1 for row in impacted if str(row.get('status') or '') in RISKY_STATUSES)

    if stat in {'PTS', '3PM', 'PRA', 'PR', 'PA'}:
        angle = 'That can open more shots and on-ball work for available teammates.'
    elif stat in {'REB', 'RA'}:
        angle = 'That can open more rebound chances or a bigger floor role.'
    elif stat in {'AST'}:
        angle = 'That can open more playmaking and possession control.'
    else:
        angle = 'That can shift role and opportunity on this roster.'

    headline_parts = []
    if out_count:
        headline_parts.append(f'{out_count} out')
    if risky_count:
        headline_parts.append(f'{risky_count} questionable/doubtful')
    headline = ' • '.join(headline_parts)

    return {
        'headline': headline or 'Team absences flagged',
        'tone': 'warning' if out_count or risky_count else 'neutral',
        'summary': angle,
        'listed_count': len(filtered),
        'impact_count': len(impacted),
        'players': [
            {
                'name': str(row.get('player_display') or '').strip(),
                'status': str(row.get('status') or '').strip(),
            }
            for row in impacted[:4]
        ],
    }


def build_analyzer_interpretation(
    *,
    stat: str,
    line: float,
    hit_rate: float,
    average: float,
    availability: dict[str, Any],
    matchup: dict[str, Any] | None,
    opportunity: dict[str, Any],
    team_context: dict[str, Any],
    h2h: dict[str, Any],
) -> dict[str, Any]:
    avg_edge = round(average - line, 1)
    matchup = matchup or {}
    lean = str((matchup.get('vs_position') or {}).get('lean') or 'Neutral')
    tone = 'neutral'

    if availability.get('is_unavailable'):
        return {
            'headline': 'Avoid for now',
            'tone': 'bad',
            'summary': 'The player is officially unavailable, so this prop should stay off the card until the status changes.',
            'bullets': [
                availability.get('status') or 'Unavailable',
                availability.get('reason') or 'Official report says he will not play.',
            ],
            'market_takeaway': 'Avoid this prop because the player is not expected to suit up.',
        }

    bullets: list[str] = []
    if availability.get('is_risky'):
        tone = 'warning'
        headline = 'Status check first'
        summary = 'There is injury risk on the player, so wait for a final status check before treating this as a live play.'
        bullets.append(f"{availability.get('status')}: {availability.get('reason') or availability.get('note')}")
    elif hit_rate >= 75 and avg_edge >= 1.0 and lean.lower() in {'good matchup', 'favorable', 'strong over environment'}:
        tone = 'good'
        headline = 'Strong over case'
        summary = f'Recent form, line clearance, and matchup support all point in the same direction for this {get_stat_label_for_copy(stat).lower()} over.'
    elif hit_rate >= 65 and avg_edge >= 0.5:
        tone = 'good'
        headline = 'Over lean'
        summary = f'The recent sample leans over this {get_stat_label_for_copy(stat).lower()} line, but it still looks more like a lean than a lock.'
    elif hit_rate <= 35 and avg_edge <= -1.0 and lean.lower() in {'tough environment', 'tough matchup', 'bad matchup'}:
        tone = 'bad'
        headline = 'Strong under case'
        summary = f'The trend and matchup both lean against this {get_stat_label_for_copy(stat).lower()} line, which makes the under look cleaner.'
    elif hit_rate <= 45 and avg_edge <= -0.5:
        tone = 'bad'
        headline = 'Under lean'
        summary = f'The player has been finishing below this {get_stat_label_for_copy(stat).lower()} line often enough to keep the under in front.'
    elif lean.lower() in {'tough environment', 'tough matchup', 'bad matchup'}:
        tone = 'warning'
        headline = 'Caution spot'
        summary = 'The recent numbers are playable, but the matchup is working against the prop enough to keep this in the caution tier.'
    elif team_context.get('impact_count') and opportunity.get('minutes_trend') == 'up':
        tone = 'good'
        headline = 'Opportunity building'
        summary = 'Minutes and team context both point to a slightly better role, which keeps this prop interesting.'
    elif h2h and h2h.get('games_count') and h2h.get('hit_count', 0) >= max(1, h2h.get('games_count', 0) - 1):
        tone = 'neutral'
        headline = 'Opponent history matters'
        summary = 'The recent sample is mixed, but this opponent history keeps the prop on the radar.'
    else:
        headline = 'Balanced spot'
        summary = 'Nothing here looks broken, but the signals are mixed enough that this reads as a lean instead of an automatic play.'

    bullets.append(f'Cleared the line in {hit_rate:.1f}% of the recent sample with a {average:.1f} average against a {line:.1f} line.')
    bullets.append(f"Recent role: {opportunity.get('minutes_last5', 0):.1f} MIN, {opportunity.get('fga_last5', 0):.1f} FGA, {opportunity.get('fg3a_last5', 0):.1f} 3PA, {opportunity.get('fta_last5', 0):.1f} FTA.")

    if lean.lower() != 'neutral':
        bullets.append(f"Matchup read: {lean} versus the next opponent.")
    if team_context.get('impact_count'):
        bullets.append(f"Team context: {team_context.get('headline')}. {team_context.get('summary')}")
    if h2h and h2h.get('games_count'):
        bullets.append(f"H2H: {h2h.get('hit_count')}/{h2h.get('games_count')} over this line versus {h2h.get('opponent_abbreviation') or h2h.get('opponent_name')}.")

    market_takeaway = summary
    if tone == 'good' and 'over' in headline.lower():
        market_takeaway = 'Plug-and-play lean: the over has enough support to stay in the shortlist.'
    elif tone == 'bad' and 'under' in headline.lower():
        market_takeaway = 'Plug-and-play lean: the under reads cleaner than the over here.'
    elif tone == 'warning':
        market_takeaway = 'Plug-and-play lean: usable, but not clean enough to force.'

    return {
        'headline': headline,
        'tone': tone,
        'summary': summary,
        'bullets': bullets[:4],
        'market_takeaway': market_takeaway,
    }


def get_stat_label_for_copy(stat: str) -> str:
    labels = {
        'PTS': 'Points', 'REB': 'Rebounds', 'AST': 'Assists', '3PM': 'Threes', 'STL': 'Steals', 'BLK': 'Blocks',
        'PRA': 'Points + rebounds + assists', 'PR': 'Points + rebounds', 'PA': 'Points + assists', 'RA': 'Rebounds + assists',
    }
    return labels.get(stat, stat)


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


def fetch_scoreboard_games(game_date: str) -> list[dict[str, Any]]:
    cached = SCOREBOARD_CACHE.get(game_date)
    now_ts = time.time()

    if cached and now_ts - cached["timestamp"] < NEXT_GAME_TTL_SECONDS:
        return cached["rows"]

    throttle_request()

    try:
        response = ScoreboardV2(game_date=game_date, day_offset=0, league_id="00", timeout=30)
        header_df = response.game_header.get_data_frame()
        try:
            line_score_df = response.line_score.get_data_frame()
        except Exception:
            line_score_df = None
    except Exception:
        return []

    rows = header_df.to_dict(orient="records") if not header_df.empty else []
    if not rows:
        SCOREBOARD_CACHE[game_date] = {"timestamp": time.time(), "rows": []}
        return []

    line_score_lookup: dict[tuple[str, int], dict[str, Any]] = {}
    if line_score_df is not None and not line_score_df.empty:
        for score_row in line_score_df.to_dict(orient="records"):
            game_id = str(score_row.get("GAME_ID") or "").strip()
            team_id = int(score_row.get("TEAM_ID") or 0)
            if game_id and team_id:
                line_score_lookup[(game_id, team_id)] = score_row

    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        game_id = str(row.get("GAME_ID") or "").strip()
        home_team_id = int(row.get("HOME_TEAM_ID") or 0)
        away_team_id = int(row.get("VISITOR_TEAM_ID") or 0)
        home_score_row = line_score_lookup.get((game_id, home_team_id), {})
        away_score_row = line_score_lookup.get((game_id, away_team_id), {})

        enriched_row = dict(row)
        enriched_row["PTS_HOME"] = int(home_score_row.get("PTS") or row.get("PTS_HOME") or 0)
        enriched_row["PTS_AWAY"] = int(away_score_row.get("PTS") or row.get("PTS_AWAY") or 0)
        enriched_rows.append(enriched_row)

    SCOREBOARD_CACHE[game_date] = {"timestamp": time.time(), "rows": enriched_rows}
    return enriched_rows


def build_scoreboard_next_game_payload(
    game_row: dict[str, Any],
    player_team_id: int | None,
) -> dict[str, Any] | None:
    if not player_team_id:
        return None

    home_team_id = int(game_row.get("HOME_TEAM_ID") or 0)
    visitor_team_id = int(game_row.get("VISITOR_TEAM_ID") or 0)

    if player_team_id == home_team_id:
        is_home = True
        opponent_team_id = visitor_team_id
        opponent = TEAM_LOOKUP.get(visitor_team_id, {})
        player_team = TEAM_LOOKUP.get(home_team_id, {})
        opponent_abbreviation = str(opponent.get("abbreviation") or "").strip()
        matchup_label = f"vs {opponent_abbreviation}" if opponent_abbreviation else "vs Opponent"
    elif player_team_id == visitor_team_id:
        is_home = False
        opponent_team_id = home_team_id
        opponent = TEAM_LOOKUP.get(home_team_id, {})
        player_team = TEAM_LOOKUP.get(visitor_team_id, {})
        opponent_abbreviation = str(opponent.get("abbreviation") or "").strip()
        matchup_label = f"@ {opponent_abbreviation}" if opponent_abbreviation else "@ Opponent"
    else:
        return None

    game_date = str(game_row.get("GAME_DATE_EST") or "").strip()

    return {
        "game_date": game_date,
        "game_time": "",
        "is_home": is_home,
        "matchup_label": matchup_label,
        "opponent_team_id": opponent_team_id,
        "opponent_name": str(opponent.get("full_name") or "").strip(),
        "opponent_abbreviation": opponent_abbreviation,
        "player_team_abbreviation": str(player_team.get("abbreviation") or "").strip(),
    }


def find_team_next_game_via_scoreboard(team_id: int | None, lookahead_days: int = 10) -> dict[str, Any] | None:
    if not team_id:
        return None

    start_date = datetime.now(ZoneInfo("America/New_York")).date()
    for offset in range(lookahead_days + 1):
        game_date = (start_date + timedelta(days=offset)).strftime("%Y-%m-%d")
        rows = fetch_scoreboard_games(game_date)
        for row in rows:
            payload = build_scoreboard_next_game_payload(row, team_id)
            if payload:
                return payload

    return None


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




def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def decimal_implied_probability(odds: float | None) -> float | None:
    if odds in (None, 0):
        return None
    try:
        odds_value = float(odds)
    except (TypeError, ValueError):
        return None
    if odds_value <= 1:
        return None
    return 1 / odds_value


def resolve_team_from_text(team_text: str | None) -> dict[str, Any] | None:
    if not team_text:
        return None
    key = normalize_name(team_text)
    return TEAM_ALIAS_LOOKUP.get(key)


def find_player_by_name(player_name: str, team_id: int | None = None) -> dict[str, Any] | None:
    needles = build_player_name_variants(player_name)
    if not needles:
        return None

    roster_ids: set[int] | None = None
    if team_id is not None:
        try:
            roster = fetch_team_roster(team_id=team_id, season=current_nba_season())
            roster_ids = {int(row.get("PLAYER_ID")) for row in roster if row.get("PLAYER_ID") not in (None, "")}
        except HTTPException:
            roster_ids = None

    def player_sort_key(item: dict[str, Any]) -> tuple[bool, str]:
        return (not item.get("is_active", False), str(item.get("full_name", "")))

    exact_matches: list[dict[str, Any]] = []
    partial_matches: list[dict[str, Any]] = []

    for player in PLAYER_POOL:
        player_variants = build_player_name_variants(str(player.get("full_name", "")))
        if not player_variants:
            continue
        if needles & player_variants:
            exact_matches.append(player)
            continue
        if any(
            needle in candidate or candidate in needle
            for needle in needles
            for candidate in player_variants
        ):
            partial_matches.append(player)

    if roster_ids is not None:
        for collection in (exact_matches, partial_matches):
            roster_matches = [player for player in collection if int(player["id"]) in roster_ids]
            if roster_matches:
                return sorted(roster_matches, key=player_sort_key)[0]

    if exact_matches:
        return sorted(exact_matches, key=player_sort_key)[0]
    if partial_matches:
        return sorted(partial_matches, key=player_sort_key)[0]
    return None


def estimate_model_probabilities(
    hit_rate_pct: float,
    average: float,
    line: float,
    matchup_delta_pct: float | None = None,
) -> tuple[float, float]:
    base = hit_rate_pct / 100.0
    edge_term = 0.0
    scale = max(1.0, max(line, 1.0) * 0.18)
    edge_term += clamp((average - line) / scale, -0.18, 0.18)
    matchup_term = 0.0
    if matchup_delta_pct is not None:
        matchup_term = clamp(matchup_delta_pct / 100.0 * 0.2, -0.12, 0.12)
    model_over = clamp(base + edge_term + matchup_term, 0.02, 0.98)
    return round(model_over, 4), round(1 - model_over, 4)


def build_prop_analysis_payload(
    player_id: int,
    stat: str,
    line: float,
    last_n: int,
    season: str,
    season_type: str,
    team_id: int | None = None,
    player_position: str | None = None,
) -> dict[str, Any]:
    player = next((p for p in PLAYER_POOL if p["id"] == player_id), None)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found.")

    season_rows = fetch_player_game_log(player_id=player_id, season=season, season_type=season_type)
    rows = season_rows[:last_n]

    games: list[dict[str, Any]] = []
    hit_count = 0
    values: list[float] = []

    for row in rows:
        game_entry = build_game_log_entry(row, stat, line)
        if game_entry["hit"]:
            hit_count += 1
        values.append(game_entry["value"])
        games.append(game_entry)

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
    next_game = resolve_team_next_game(
        team_id=resolved_team_id,
        primary_player_id=player_id,
        season=season,
        season_type=season_type,
    )
    vs_position = None

    if next_game and position_code:
        vs_position = build_position_matchup(
            opponent_team_id=next_game["opponent_team_id"],
            position_code=position_code,
            stat=stat,
            season=season,
            season_type=season_type,
        )

    team_name = TEAM_LOOKUP.get(resolved_team_id, {}).get("full_name") if resolved_team_id else None
    availability = build_availability_payload(player_name=player["full_name"], team_name=team_name)

    h2h_payload = {
        "opponent_name": next_game.get("opponent_name") if next_game else "",
        "opponent_abbreviation": next_game.get("opponent_abbreviation") if next_game else "",
        "games_count": 0,
        "hit_count": 0,
        "hit_rate": 0.0,
        "average": 0.0,
        "games": [],
    }
    if next_game and next_game.get("opponent_abbreviation"):
        opponent_abbreviation = str(next_game.get("opponent_abbreviation") or "").upper().strip()
        h2h_rows = [
            row for row in season_rows
            if opponent_abbreviation and opponent_abbreviation in str(row.get("MATCHUP") or "").upper()
        ]
        h2h_games: list[dict[str, Any]] = []
        h2h_values: list[float] = []
        h2h_hits = 0
        for row in h2h_rows:
            game_entry = build_game_log_entry(row, stat, line)
            if game_entry["hit"]:
                h2h_hits += 1
            h2h_values.append(game_entry["value"])
            h2h_games.append(game_entry)
        if h2h_games:
            h2h_payload = {
                "opponent_name": next_game.get("opponent_name") or "",
                "opponent_abbreviation": opponent_abbreviation,
                "games_count": len(h2h_games),
                "hit_count": h2h_hits,
                "hit_rate": round((h2h_hits / len(h2h_games)) * 100, 1),
                "average": round(sum(h2h_values) / len(h2h_values), 2),
                "games": list(reversed(h2h_games)),
            }

    opportunity = build_opportunity_context(season_rows, last_n)
    team_context = build_team_opportunity_context(team_name=team_name, player_name=player["full_name"], stat=stat)
    interpretation = build_analyzer_interpretation(
        stat=stat,
        line=line,
        hit_rate=hit_rate,
        average=average,
        availability=availability,
        matchup={"next_game": next_game, "vs_position": vs_position},
        opportunity=opportunity,
        team_context=team_context,
        h2h=h2h_payload,
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
        "season": season,
        "season_type": season_type,
        "stat": stat,
        "line": line,
        "last_n": last_n,
        "average": average,
        "hit_count": hit_count,
        "games_count": len(values),
        "hit_rate": hit_rate,
        "games": list(reversed(games)),
        "availability": availability,
        "matchup": {
            "next_game": next_game,
            "vs_position": vs_position,
        },
        "h2h": h2h_payload,
        "opportunity": opportunity,
        "team_context": team_context,
        "interpretation": interpretation,
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


def resolve_team_next_game(
    team_id: int | None,
    primary_player_id: int,
    season: str,
    season_type: str,
) -> dict[str, Any] | None:
    if not team_id:
        return None

    cache_key = (team_id, season, season_type)
    cached = TEAM_NEXT_GAME_CACHE.get(cache_key)
    now_ts = time.time()

    if cached and now_ts - cached["timestamp"] < NEXT_GAME_TTL_SECONDS:
        return cached["row"]

    candidate_player_ids: list[int] = [primary_player_id]

    try:
        roster_rows = fetch_team_roster(team_id=team_id, season=season)
        teammate_ids = [
            int(row.get("PLAYER_ID"))
            for row in roster_rows
            if row.get("PLAYER_ID") and int(row.get("PLAYER_ID")) != primary_player_id
        ]
        candidate_player_ids.extend(teammate_ids[:4])
    except HTTPException:
        pass

    seen: set[int] = set()
    for candidate_player_id in candidate_player_ids:
        if candidate_player_id in seen:
            continue
        seen.add(candidate_player_id)

        next_game_row = fetch_next_game(candidate_player_id, season, season_type)
        next_game = build_next_game_payload(next_game_row, team_id)
        if next_game:
            TEAM_NEXT_GAME_CACHE[cache_key] = {"timestamp": time.time(), "row": next_game}
            return next_game

    fallback_next_game = find_team_next_game_via_scoreboard(team_id=team_id, lookahead_days=10)
    if fallback_next_game:
        TEAM_NEXT_GAME_CACHE[cache_key] = {"timestamp": time.time(), "row": fallback_next_game}
        return fallback_next_game

    TEAM_NEXT_GAME_CACHE[cache_key] = {"timestamp": time.time(), "row": None}
    return None


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
    needles = build_player_name_variants(q)
    matches: list[dict[str, Any]] = []

    for player in PLAYER_POOL:
        full_name = str(player.get("full_name", ""))
        variants = build_player_name_variants(full_name)
        if not variants:
            continue
        if any(
            needle in candidate or candidate in needle
            for needle in needles
            for candidate in variants
        ):
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


@app.post("/api/market-scan")
def market_scan(
    payload: dict[str, Any] = Body(...),
) -> dict[str, Any]:
    rows = payload.get("rows") or []
    default_last_n = int(payload.get("last_n") or 10)
    selected_season = str(payload.get("season") or current_nba_season())
    season_type = str(payload.get("season_type") or "Regular Season")

    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="Please provide at least one market row.")

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            errors.append({"row": index, "reason": "Invalid row format."})
            continue

        player_name = str(row.get("player_name") or "").strip()
        stat = str(row.get("stat") or "").upper().strip()
        team_text = str(row.get("team") or "").strip()
        opponent_text = str(row.get("opponent") or "").strip()
        if stat not in STAT_MAP:
            errors.append({"row": index, "player_name": player_name, "reason": f"Unsupported stat: {stat}"})
            continue
        try:
            line = float(row.get("line"))
            over_odds = float(row.get("over_odds"))
            under_odds = float(row.get("under_odds"))
        except (TypeError, ValueError):
            errors.append({"row": index, "player_name": player_name, "reason": "Line and odds must be numeric."})
            continue

        team = resolve_team_from_text(team_text) if team_text else None
        opponent = resolve_team_from_text(opponent_text) if opponent_text else None
        team_id = int(team["id"]) if team else None
        player = find_player_by_name(player_name, team_id=team_id)
        if not player:
            errors.append({"row": index, "player_name": player_name, "reason": "Player not found."})
            continue

        try:
            analysis = build_prop_analysis_payload(
                player_id=int(player["id"]),
                stat=stat,
                line=line,
                last_n=default_last_n,
                season=selected_season,
                season_type=season_type,
                team_id=team_id,
                player_position=None,
            )
        except HTTPException as exc:
            errors.append({"row": index, "player_name": player_name, "reason": exc.detail})
            continue

        matchup = analysis.get("matchup", {})
        next_game = matchup.get("next_game") or {}
        vs_position = matchup.get("vs_position") or {}
        availability = analysis.get("availability") or {}
        matchup_delta_pct = vs_position.get("delta_pct") if isinstance(vs_position, dict) else None
        model_over, model_under = estimate_model_probabilities(
            hit_rate_pct=float(analysis["hit_rate"]),
            average=float(analysis["average"]),
            line=line,
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
        )
        implied_over = decimal_implied_probability(over_odds)
        implied_under = decimal_implied_probability(under_odds)
        over_edge = round((model_over - implied_over) * 100, 1) if implied_over is not None else None
        under_edge = round((model_under - implied_under) * 100, 1) if implied_under is not None else None
        over_ev = round(model_over * over_odds - 1, 3)
        under_ev = round(model_under * under_odds - 1, 3)

        if under_ev > over_ev:
            best_side = "UNDER"
            best_edge = under_edge if under_edge is not None else round(under_ev * 100, 1)
            best_ev = under_ev
            best_model = model_under
            best_implied = implied_under
            market_odds = under_odds
        else:
            best_side = "OVER"
            best_edge = over_edge if over_edge is not None else round(over_ev * 100, 1)
            best_ev = over_ev
            best_model = model_over
            best_implied = implied_over
            market_odds = over_odds

        confidence_engine = build_confidence_engine(
            side=best_side,
            hit_rate=float(analysis["hit_rate"]),
            games_count=int(analysis["games_count"]),
            edge=best_edge,
            ev=best_ev,
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            availability=availability,
        )

        display_side = best_side
        if availability.get("is_unavailable"):
            display_side = "AVOID"
        elif availability.get("is_risky"):
            display_side = f"{best_side}?"

        resolved_team_id = analysis["player"].get("team_id") or team_id
        resolved_team = TEAM_LOOKUP.get(int(resolved_team_id)) if resolved_team_id else None
        resolved_team_abbreviation = (
            (resolved_team or {}).get("abbreviation")
            or next_game.get("player_team_abbreviation")
            or (team.get("abbreviation") if team else team_text)
        )
        resolved_opponent_abbreviation = (
            next_game.get("opponent_abbreviation")
            or (opponent.get("abbreviation") if opponent else opponent_text)
        )

        results.append({
            "row": index,
            "player": {
                "id": analysis["player"]["id"],
                "full_name": analysis["player"]["full_name"],
                "team_id": resolved_team_id,
                "team": resolved_team_abbreviation,
                "opponent": resolved_opponent_abbreviation,
                "position": analysis["player"].get("position") or "",
            },
            "market": {
                "stat": stat,
                "line": line,
                "over_odds": over_odds,
                "under_odds": under_odds,
            },
            "analysis": {
                "average": analysis["average"],
                "hit_rate": analysis["hit_rate"],
                "hit_count": analysis["hit_count"],
                "games_count": analysis["games_count"],
                "last_n": analysis["last_n"],
                "over_streak": compute_recent_hit_streak([game.get("hit") for game in reversed(analysis["games"])]),
                "last_value": analysis["games"][-1]["value"] if analysis["games"] else None,
                "availability": availability,
                "matchup": {
                    "next_game": next_game,
                    "vs_position": vs_position,
                },
                "h2h": analysis.get("h2h") or {},
                "opportunity": analysis.get("opportunity") or {},
                "team_context": analysis.get("team_context") or {},
                "interpretation": analysis.get("interpretation") or {},
            },
            "model": {
                "over_probability": round(model_over * 100, 1),
                "under_probability": round(model_under * 100, 1),
                "over_implied": round(implied_over * 100, 1) if implied_over is not None else None,
                "under_implied": round(implied_under * 100, 1) if implied_under is not None else None,
                "over_edge": over_edge,
                "under_edge": under_edge,
                "over_ev": round(over_ev * 100, 1),
                "under_ev": round(under_ev * 100, 1),
            },
            "best_bet": {
                "side": best_side,
                "display_side": display_side,
                "edge": round(best_edge, 1) if best_edge is not None else None,
                "ev": round(best_ev * 100, 1),
                "model_probability": round(best_model * 100, 1),
                "implied_probability": round(best_implied * 100, 1) if best_implied is not None else None,
                "odds": market_odds,
                "confidence": confidence_engine["grade"],
                "confidence_score": confidence_engine["score"],
                "confidence_summary": confidence_engine["summary"],
                "confidence_tone": confidence_engine["tone"],
                "playable": not availability.get("is_unavailable", False),
                "user_read": analysis.get("interpretation", {}).get("market_takeaway") or confidence_engine["summary"],
            },
            "availability": availability,
            "matchup": {
                "next_game": next_game,
                "vs_position": vs_position,
            },
        })

    results.sort(
        key=lambda item: (
            item["best_bet"].get("confidence_score", 0),
            item.get("availability", {}).get("sort_rank", 3),
            item["best_bet"]["ev"],
            item["best_bet"]["edge"] or -999,
            item["analysis"]["hit_rate"],
        ),
        reverse=True,
    )

    return {
        "season": selected_season,
        "season_type": season_type,
        "last_n": default_last_n,
        "template": "player_name,stat,line,over_odds,under_odds",
        "results": results,
        "errors": errors,
    }


@app.get("/api/todays-games")
def todays_games(game_date: str | None = None) -> dict[str, Any]:
    requested_date = game_date or current_nba_game_date()
    resolved_date = requested_date
    rows = fetch_scoreboard_games(requested_date)
    fallback_used = False

    if not rows:
        base_date = datetime.strptime(requested_date, "%Y-%m-%d").date()
        for offset in range(1, 4):
            probe_date = (base_date + timedelta(days=offset)).strftime("%Y-%m-%d")
            probe_rows = fetch_scoreboard_games(probe_date)
            if probe_rows:
                rows = probe_rows
                resolved_date = probe_date
                fallback_used = True
                break

    report_payload = fetch_latest_injury_report_payload()
    games: list[dict[str, Any]] = []

    for row in rows:
        home_team_id = int(row.get("HOME_TEAM_ID") or 0)
        away_team_id = int(row.get("VISITOR_TEAM_ID") or 0)
        home_team = TEAM_LOOKUP.get(home_team_id, {})
        away_team = TEAM_LOOKUP.get(away_team_id, {})
        game_status = str(row.get("GAME_STATUS_TEXT") or "").strip()
        home_score = int(row.get("PTS_HOME") or 0)
        away_score = int(row.get("PTS_AWAY") or 0)
        home_summary = build_team_availability_summary(str(home_team.get("full_name") or ""), report_payload)
        away_summary = build_team_availability_summary(str(away_team.get("full_name") or ""), report_payload)

        games.append({
            "game_id": str(row.get("GAME_ID") or "").strip(),
            "game_date": resolved_date,
            "status_text": game_status or "TBD",
            "status_category": "final" if "Final" in game_status else ("live" if "Q" in game_status or "Halftime" in game_status else "scheduled"),
            "game_label": f"{away_team.get('abbreviation', '')} @ {home_team.get('abbreviation', '')}",
            "home": {
                "team_id": home_team_id,
                "full_name": str(home_team.get("full_name") or "").strip(),
                "abbreviation": str(home_team.get("abbreviation") or "").strip(),
                "score": home_score,
                "availability": home_summary,
            },
            "away": {
                "team_id": away_team_id,
                "full_name": str(away_team.get("full_name") or "").strip(),
                "abbreviation": str(away_team.get("abbreviation") or "").strip(),
                "score": away_score,
                "availability": away_summary,
            },
        })

    return {
        "requested_date": requested_date,
        "resolved_date": resolved_date,
        "fallback_used": fallback_used,
        "report_label": report_payload.get("report_label") or "",
        "games": games,
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
    return build_prop_analysis_payload(
        player_id=player_id,
        stat=stat,
        line=line,
        last_n=last_n,
        season=selected_season,
        season_type=season_type,
        team_id=team_id,
        player_position=player_position,
    )
