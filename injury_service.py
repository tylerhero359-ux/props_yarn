from __future__ import annotations

import copy
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import Any, Callable
from zoneinfo import ZoneInfo

from pypdf import PdfReader

from identity_utils import normalize_compact_text, normalize_report_person_name, build_player_name_variants, normalize_name


@dataclass
class InjuryReportService:
    team_pool: list[dict[str, Any]]
    http_get: Callable[..., Any]
    timed_call: Callable[[str], Callable]
    team_alias_lookup: dict[str, dict[str, Any]] = field(default_factory=dict)
    pdfplumber_module: Any | None = None
    page_url: str = "https://official.nba.com/nba-injury-report-2025-26-season/"
    report_ttl_seconds: int = 600
    links_ttl_seconds: int = 300
    failure_cooldown_seconds: int = 180
    max_stale_seconds: int = 6 * 60 * 60
    page_timeout: tuple[int, int] = (3, 6)
    pdf_timeout: tuple[int, int] = (3, 8)
    status_order: dict[str, int] = field(
        default_factory=lambda: {
            "Out": 0,
            "Ineligible": 0,
            "Suspended": 0,
            "Doubtful": 1,
            "Questionable": 2,
            "Pending report": 2,
            "Not listed": 3,
            "Available": 4,
            "Probable": 4,
        }
    )
    unavailable_statuses: set[str] = field(default_factory=lambda: {"Out", "Ineligible", "Suspended"})
    risky_statuses: set[str] = field(default_factory=lambda: {"Doubtful", "Questionable", "Pending report"})
    good_statuses: set[str] = field(default_factory=lambda: {"Available", "Probable"})
    report_statuses: list[str] = field(
        default_factory=lambda: ["Questionable", "Ineligible", "Suspended", "Doubtful", "Probable", "Available", "Out"]
    )
    report_cache: dict[str, Any] = field(default_factory=lambda: {"timestamp": 0.0, "payload": None})
    links_cache: dict[str, Any] = field(default_factory=lambda: {"timestamp": 0.0, "links": []})
    url_cache: dict[str, dict[str, Any]] = field(default_factory=dict)
    meta: dict[str, float] = field(default_factory=lambda: {"last_attempt": 0.0, "last_failure": 0.0})
    match_cache: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    availability_cache: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.status_pattern = "|".join(re.escape(status) for status in sorted(self.report_statuses, key=len, reverse=True))
        self.compact_team_lookup: dict[str, str] = {}
        for team in self.team_pool:
            full_name = str(team.get("full_name") or "")
            compact_full_name = normalize_compact_text(full_name)
            if compact_full_name:
                self.compact_team_lookup[compact_full_name] = full_name
            abbreviation = str(team.get("abbreviation") or "").strip()
            nickname = str(team.get("nickname") or "").strip()
            city = str(team.get("city") or "").strip()
            if abbreviation and nickname:
                compact_abbr_nickname = normalize_compact_text(f"{abbreviation} {nickname}")
                if compact_abbr_nickname:
                    self.compact_team_lookup[compact_abbr_nickname] = full_name
            if city and nickname:
                city_initials = "".join(part[:1] for part in city.split() if part)
                compact_city_initials = normalize_compact_text(f"{city_initials} {nickname}")
                if compact_city_initials:
                    self.compact_team_lookup[compact_city_initials] = full_name
        for alias, team in self.team_alias_lookup.items():
            compact_alias = normalize_compact_text(alias)
            full_name = str(team.get("full_name") or "")
            if compact_alias and full_name:
                self.compact_team_lookup[compact_alias] = full_name

    def _strip_matchup_prefix(self, text_line: str) -> str:
        cleaned = " ".join(str(text_line or "").split())
        patterns = (
            r"^[A-Z]{2,4}\s*@\s*[A-Z]{2,4}\s+",
            r"^[A-Z]{2,4}@[A-Z]{2,4}\s+",
        )
        for pattern in patterns:
            updated = re.sub(pattern, "", cleaned)
            if updated != cleaned:
                cleaned = updated.strip()
        return cleaned

    def _extract_team_remainder(self, text_line: str, compact_team: str) -> str:
        compact_seen = ""
        for idx, char in enumerate(text_line):
            if char.isspace():
                continue
            compact_seen += char
            if compact_seen.lower() == compact_team.lower():
                return text_line[idx + 1 :].strip()
            if not compact_team.lower().startswith(compact_seen.lower()):
                break
        return text_line.strip()

    def _format_player_display(self, player_display: str) -> str:
        cleaned = re.sub(r",(?!\s)", ", ", str(player_display or "").strip())
        cleaned = re.sub(r"(?<=[a-z])(?=(?:Jr\.|Sr\.|II|III|IV|V)\b)", " ", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        return cleaned.strip()

    def _is_plausible_player_display(self, player_display: str) -> bool:
        candidate = str(player_display or "").strip()
        if not candidate or "@" in candidate:
            return False

        compact_candidate = normalize_compact_text(candidate)
        if not compact_candidate:
            return False

        for team in self.team_pool:
            compact_team = normalize_compact_text(str(team.get("full_name") or ""))
            if compact_team and compact_team in compact_candidate:
                return False
        return True

    def _team_name_by_compact_text(self, compact_text: str) -> str | None:
        return self.compact_team_lookup.get(compact_text)

    def parse_report_timestamp(self, url: str) -> datetime:
        match = re.search(r"Injury-Report_(\d{4}-\d{2}-\d{2})_(\d{2})_(\d{2})(AM|PM)\.pdf", url)
        if not match:
            return datetime.min.replace(tzinfo=ZoneInfo("America/New_York"))
        date_part, hour_part, minute_part, meridiem = match.groups()
        hour = int(hour_part)
        minute = int(minute_part)
        if meridiem == "PM" and hour != 12:
            hour += 12
        if meridiem == "AM" and hour == 12:
            hour = 0
        parsed = datetime.fromisoformat(f"{date_part}T{hour:02d}:{minute:02d}:00")
        return parsed.replace(tzinfo=ZoneInfo("America/New_York"))

    def format_report_timestamp(self, report_dt: datetime | None) -> str:
        if not report_dt or report_dt == datetime.min.replace(tzinfo=ZoneInfo("America/New_York")):
            return ""
        local_dt = report_dt.astimezone(ZoneInfo("America/New_York"))
        return local_dt.strftime("%b %d, %Y %I:%M %p ET")

    def extract_team_prefix(self, text_line: str) -> tuple[str | None, str]:
        cleaned_line = self._strip_matchup_prefix(text_line)
        compact_line = normalize_compact_text(cleaned_line)
        for team in sorted(self.team_pool, key=lambda item: len(item["full_name"]), reverse=True):
            team_name = str(team.get("full_name") or "")
            compact_team = normalize_compact_text(team_name)
            if compact_team and compact_line.startswith(compact_team):
                if cleaned_line.startswith(team_name):
                    remainder = cleaned_line[len(team_name):].strip()
                else:
                    remainder = self._extract_team_remainder(cleaned_line, compact_team)
                return team_name, remainder
        return None, cleaned_line.strip()

    def parse_report_rows(self, report_text: str) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        pending_teams: list[str] = []
        current_team: str | None = None

        lines = [" ".join(line.split()) for line in report_text.splitlines() if str(line or "").strip()]

        for line in lines:
            lower_line = line.lower()
            if (
                "nba injury report" in lower_line
                or lower_line.startswith("game date game time matchup team player current status")
                or lower_line.startswith("game date")
                or lower_line.startswith("matchup team player")
            ):
                continue

            team_hit, remainder = self.extract_team_prefix(line)
            if team_hit:
                current_team = team_hit
            else:
                remainder = line

            if re.search(r"\b(pending report|not\s*yet\s*submitted)\b", remainder, flags=re.IGNORECASE):
                if current_team and current_team not in pending_teams:
                    pending_teams.append(current_team)
                continue

            row_match = re.match(
                rf"^(?P<player>.+?)\s+(?P<status>{self.status_pattern})\b(?:\s+(?P<reason>.*))?$",
                remainder,
                flags=re.IGNORECASE,
            )
            if not row_match or not current_team:
                continue

            player_display = self._format_player_display(row_match.group("player").strip())
            if not self._is_plausible_player_display(player_display):
                continue

            status = row_match.group("status").strip()
            canonical_status = next((candidate for candidate in self.report_statuses if candidate.lower() == status.lower()), status)
            reason = (row_match.group("reason") or "").strip()

            rows.append(
                {
                    "team_name": current_team,
                    "player_display": player_display,
                    "player_key": normalize_report_person_name(player_display),
                    "status": canonical_status,
                    "reason": reason,
                }
            )

        return {"rows": rows, "pending_teams": pending_teams}

    def extract_report_rows_from_table(self, pdf_bytes: bytes) -> list[dict[str, Any]] | None:
        if self.pdfplumber_module is None:
            return None
        rows: list[dict[str, Any]] = []
        try:
            last_team_name: str | None = None
            with self.pdfplumber_module.open(BytesIO(pdf_bytes)) as pdf:  # type: ignore[arg-type]
                for page in pdf.pages:
                    tables = page.extract_tables() or []
                    for table in tables:
                        for row in table:
                            if not row:
                                continue
                            cells = [str(c or "").strip() for c in row]
                            if len(cells) < 5:
                                continue
                            if cells[0].lower() in ("game date", "date") or cells[3].lower() in ("team",):
                                continue
                            if not any(cells):
                                continue

                            team_col_idx = None
                            for ci in range(2, min(6, len(cells))):
                                team_name = self._team_name_by_compact_text(normalize_compact_text(cells[ci]))
                                if team_name:
                                    team_col_idx = ci
                                    break

                            if team_col_idx is not None:
                                last_team_name = self._team_name_by_compact_text(normalize_compact_text(cells[team_col_idx]))
                                player_col_idx = team_col_idx + 1
                                status_col_idx = team_col_idx + 2
                                reason_col_idx = team_col_idx + 3
                            else:
                                if not last_team_name:
                                    continue
                                player_col_idx = 4
                                status_col_idx = 5
                                reason_col_idx = 6

                            player_cell = cells[player_col_idx] if player_col_idx < len(cells) else ""
                            status_cell = cells[status_col_idx] if status_col_idx < len(cells) else ""
                            reason_cell = cells[reason_col_idx] if reason_col_idx < len(cells) else ""
                            status_cell = status_cell.strip()

                            if status_cell not in self.report_statuses:
                                found = None
                                for status in sorted(self.report_statuses, key=len, reverse=True):
                                    if status.lower() in status_cell.lower():
                                        found = status
                                        break
                                if not found:
                                    continue
                                status_cell = found

                            player_display = self._format_player_display(player_cell.strip())
                            if not last_team_name or not self._is_plausible_player_display(player_display):
                                continue

                            rows.append(
                                {
                                    "team_name": last_team_name,
                                    "player_display": player_display,
                                    "player_key": normalize_report_person_name(player_display),
                                    "status": status_cell,
                                    "reason": reason_cell.strip(),
                                }
                            )
            return rows if rows else None
        except Exception:
            return None

    def extract_report_rows_from_layout(self, pdf_bytes: bytes) -> dict[str, Any] | None:
        if self.pdfplumber_module is None:
            return None

        def group_lines(words: list[dict[str, Any]], tolerance: float = 4.0) -> list[list[dict[str, Any]]]:
            lines: list[list[dict[str, Any]]] = []
            current: list[dict[str, Any]] = []
            current_top: float | None = None
            for word in sorted(words, key=lambda item: (float(item.get("top") or 0.0), float(item.get("x0") or 0.0))):
                top = float(word.get("top") or 0.0)
                if current_top is None or abs(top - current_top) <= tolerance:
                    current.append(word)
                    if current_top is None:
                        current_top = top
                else:
                    lines.append(current)
                    current = [word]
                    current_top = top
            if current:
                lines.append(current)
            return lines

        def join_words(items: list[dict[str, Any]]) -> str:
            return " ".join(str(item.get("text") or "").strip() for item in sorted(items, key=lambda item: float(item.get("x0") or 0.0)) if str(item.get("text") or "").strip()).strip()

        rows: list[dict[str, Any]] = []
        pending_teams: list[str] = []
        pending_entries: list[dict[str, Any]] = []
        current_team: str | None = None
        last_row: dict[str, Any] | None = None
        current_game_date: str | None = None
        current_matchup: str | None = None

        try:
            with self.pdfplumber_module.open(BytesIO(pdf_bytes)) as pdf:  # type: ignore[arg-type]
                for page in pdf.pages:
                    words = page.extract_words(use_text_flow=False, keep_blank_chars=False) or []
                    for line in group_lines(words):
                        team_words = [word for word in line if 250 <= float(word.get("x0") or 0.0) < 410]
                        player_words = [word for word in line if 410 <= float(word.get("x0") or 0.0) < 580]
                        status_words = [word for word in line if 580 <= float(word.get("x0") or 0.0) < 660]
                        reason_words = [word for word in line if float(word.get("x0") or 0.0) >= 660]
                        lead_words = [word for word in line if float(word.get("x0") or 0.0) < 250]

                        lead_text = join_words(lead_words)
                        team_text = join_words(team_words)
                        player_text = join_words(player_words)
                        status_text = join_words(status_words)
                        reason_text = join_words(reason_words)
                        full_line = " ".join(part for part in (lead_text, team_text, player_text, status_text, reason_text) if part).strip()
                        lower_line = full_line.lower()

                        if (
                            not full_line
                            or lower_line.startswith("injury report:")
                            or lower_line.startswith("page ")
                            or lower_line == "page"
                            or "game date game time matchup team player name current status reason" in lower_line
                        ):
                            continue

                        date_match = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", full_line)
                        if date_match:
                            try:
                                current_game_date = datetime.strptime(date_match.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                            except Exception:
                                current_game_date = date_match.group(1)

                        matchup_match = re.search(r"\b([A-Z]{2,4}@[A-Z]{2,4})\b", full_line)
                        if matchup_match:
                            current_matchup = matchup_match.group(1)

                        team_name = self._team_name_by_compact_text(normalize_compact_text(team_text)) if team_text else None
                        if team_name:
                            current_team = team_name

                        pending_text = " ".join(part for part in (player_text, status_text, reason_text) if part).strip()
                        if re.search(r"\bnot\s*yet\s*submitted\b", pending_text, flags=re.IGNORECASE):
                            if current_team and current_team not in pending_teams:
                                pending_teams.append(current_team)
                            if current_team:
                                pending_entries.append(
                                    {
                                        "team_name": current_team,
                                        "game_date": current_game_date or "",
                                        "matchup": current_matchup or "",
                                    }
                                )
                            last_row = None
                            continue

                        status_value = next((status for status in self.report_statuses if status.lower() == status_text.lower()), "")
                        if player_text and status_value and current_team:
                            player_display = self._format_player_display(player_text)
                            if self._is_plausible_player_display(player_display):
                                row = {
                                    "team_name": current_team,
                                    "game_date": current_game_date or "",
                                    "matchup": current_matchup or "",
                                    "player_display": player_display,
                                    "player_key": normalize_report_person_name(player_display),
                                    "status": status_value,
                                    "reason": reason_text.strip(),
                                }
                                rows.append(row)
                                last_row = row
                                continue

                        if reason_text and last_row is not None and not player_text and not status_text:
                            previous_reason = str(last_row.get("reason") or "").strip()
                            last_row["reason"] = f"{previous_reason} {reason_text.strip()}".strip()

            return {"rows": rows, "pending_teams": pending_teams, "pending_entries": pending_entries} if rows or pending_teams else None
        except Exception:
            return None

    def extract_report_text_candidates(self, pdf_bytes: bytes) -> list[dict[str, str]]:
        candidates: list[dict[str, str]] = []

        if self.pdfplumber_module is not None:
            try:
                plumber_pages: list[str] = []
                with self.pdfplumber_module.open(BytesIO(pdf_bytes)) as pdf:  # type: ignore[arg-type]
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

    def choose_best_report_parse(self, pdf_bytes: bytes) -> dict[str, Any]:
        layout_parsed = self.extract_report_rows_from_layout(pdf_bytes)
        if layout_parsed:
            return {
                "rows": layout_parsed.get("rows") or [],
                "pending_teams": layout_parsed.get("pending_teams") or [],
                "pending_entries": layout_parsed.get("pending_entries") or [],
                "raw_text": "",
                "method": "layout",
            }

        candidates = self.extract_report_text_candidates(pdf_bytes)
        best_payload: dict[str, Any] = {"rows": [], "pending_teams": [], "pending_entries": [], "raw_text": "", "method": "none"}

        table_rows = self.extract_report_rows_from_table(pdf_bytes)
        if table_rows:
            best_payload = {
                "rows": table_rows,
                "pending_teams": [],
                "pending_entries": [],
                "raw_text": "",
                "method": "table",
            }

        for candidate in candidates:
            parsed = self.parse_report_rows(candidate["text"])
            if len(parsed.get("rows") or []) > len(best_payload.get("rows") or []):
                best_payload = {
                    "rows": parsed.get("rows") or [],
                    "pending_teams": parsed.get("pending_teams") or [],
                    "pending_entries": parsed.get("pending_entries") or [],
                    "raw_text": candidate["text"],
                    "method": candidate["method"],
                }

        if not best_payload["rows"] and candidates:
            fallback = candidates[0]
            parsed = self.parse_report_rows(fallback["text"])
            best_payload = {
                "rows": parsed.get("rows") or [],
                "pending_teams": parsed.get("pending_teams") or [],
                "pending_entries": parsed.get("pending_entries") or [],
                "raw_text": fallback["text"],
                "method": fallback["method"],
            }

        return best_payload

    def list_recent_report_links(self, limit: int = 12) -> list[str]:
        now_ts = time.time()
        cached_links = self.links_cache.get("links") or []
        cached_ts = float(self.links_cache.get("timestamp") or 0.0)
        if cached_links and now_ts - cached_ts < self.links_ttl_seconds:
            return list(cached_links)[:limit]

        headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
        page_response = self.http_get(self.page_url, headers=headers, timeout=self.page_timeout)
        page_response.raise_for_status()
        html = page_response.text
        links = set(re.findall(r"https://ak-static\.cms\.nba\.com/referee/injury/Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html))
        if not links:
            relative_links = re.findall(r"/wp-content/uploads/.+?Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html)
            links = {f"https://official.nba.com{match}" for match in relative_links}
        if not links:
            raise RuntimeError("No injury report PDF links found on the official page.")

        sorted_links = sorted(links, key=self.parse_report_timestamp, reverse=True)
        self.links_cache["timestamp"] = now_ts
        self.links_cache["links"] = sorted_links
        return sorted_links[:limit]

    def fetch_report_payload_for_url(self, report_url: str) -> dict[str, Any]:
        cached = self.url_cache.get(report_url)
        now_ts = time.time()
        if cached and now_ts - float(cached.get("timestamp") or 0.0) < self.report_ttl_seconds:
            return dict(cached.get("payload") or {})

        headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
        latest_dt = self.parse_report_timestamp(report_url)
        pdf_response = self.http_get(report_url, headers=headers, timeout=(5, 30))
        pdf_response.raise_for_status()
        parsed_choice = self.choose_best_report_parse(pdf_response.content)
        payload = {
            "ok": True,
            "report_url": report_url,
            "report_timestamp": latest_dt.isoformat() if latest_dt != datetime.min.replace(tzinfo=ZoneInfo("America/New_York")) else "",
            "report_label": self.format_report_timestamp(latest_dt),
            "rows": parsed_choice.get("rows") or [],
            "pending_teams": parsed_choice.get("pending_teams") or [],
            "pending_entries": parsed_choice.get("pending_entries") or [],
            "raw_text": parsed_choice.get("raw_text") or "",
            "parse_method": parsed_choice.get("method") or "unknown",
            "error": None,
        }
        self.url_cache[report_url] = {"timestamp": now_ts, "payload": payload}
        return payload

    def try_direct_report_match(self, report_text: str, player_name: str, team_name: str | None = None) -> dict[str, Any] | None:
        if not report_text.strip():
            return None

        parsed = self.parse_report_rows(report_text)
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
        return fuzzy[0] if fuzzy else None

    def search_report_payload_for_player(
        self,
        report_payload: dict[str, Any],
        player_name: str,
        team_name: str | None = None,
        game_date: str | None = None,
    ) -> dict[str, Any] | None:
        player_key = normalize_report_person_name(player_name)
        wanted_team = str(team_name or "").strip()
        wanted_team_norm = normalize_compact_text(wanted_team) if wanted_team else ""
        rows = self.get_team_rows(report_payload, wanted_team, game_date=game_date) if wanted_team and game_date else (report_payload.get("rows") or [])
        name_variants = build_player_name_variants(player_name)

        def team_matches(row_team: str | None) -> bool:
            if not wanted_team_norm:
                return True
            return normalize_compact_text(str(row_team or "")) == wanted_team_norm

        candidates: list[dict[str, Any]] = []
        for row in rows:
            row_key = str(row.get("player_key") or "")
            if row_key == player_key or (name_variants and row_key in name_variants):
                candidates.append(row)

        if wanted_team:
            team_filtered = [row for row in candidates if team_matches(row.get("team_name"))]
            if team_filtered:
                candidates = team_filtered

        if not candidates:
            token_set = set(player_key.split())
            fuzzy_matches: list[tuple[int, dict[str, Any]]] = []
            for row in rows:
                row_tokens = set(str(row.get("player_key") or "").split())
                if not token_set or not row_tokens:
                    continue
                overlap = len(token_set.intersection(row_tokens))
                if overlap <= 0:
                    continue
                score = overlap * 10
                if token_set.issubset(row_tokens):
                    score += 25
                if team_matches(row.get("team_name")):
                    score += 20
                fuzzy_matches.append((score, row))
            if fuzzy_matches:
                fuzzy_matches.sort(key=lambda item: item[0], reverse=True)
                candidates = [row for _, row in fuzzy_matches[:3]]

        if not candidates:
            direct_row = self.try_direct_report_match(str(report_payload.get("raw_text") or ""), player_name, team_name)
            if direct_row:
                candidates = [direct_row]

        if not candidates:
            raw_text = str(report_payload.get("raw_text") or "")
            name_variants = [
                player_name.strip(),
                " ".join(reversed(player_name.strip().split(" ", 1))) if " " in player_name.strip() else player_name.strip(),
            ]
            parts = [part for part in player_name.strip().split() if part]
            if len(parts) >= 2:
                name_variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
            lowered_lines = [" ".join(line.split()) for line in raw_text.splitlines() if str(line or "").strip()]
            current_team: str | None = None
            for line in lowered_lines:
                team_hit, remainder = self.extract_team_prefix(line)
                if team_hit:
                    current_team = team_hit
                else:
                    remainder = line
                if wanted_team and normalize_compact_text(current_team or "") != wanted_team_norm:
                    continue
                for variant in name_variants:
                    if variant and variant.lower() in remainder.lower():
                        row_match = re.match(
                            rf"^(?P<player>.+?)\s+(?P<status>{self.status_pattern})\b(?:\s+(?P<reason>.*))?$",
                            remainder,
                        )
                        if row_match:
                            raw_disp = re.sub(r",(?!\s)", ", ", row_match.group("player").strip())
                            return {
                                "team_name": current_team or wanted_team,
                                "player_display": raw_disp,
                                "player_key": normalize_report_person_name(raw_disp),
                                "status": row_match.group("status").strip(),
                                "reason": (row_match.group("reason") or "").strip(),
                            }
        if not candidates:
            return None

        # Final alignment pass: prefer best team match + longest key overlap.
        scored: list[tuple[int, dict[str, Any]]] = []
        for row in candidates:
            row_key = str(row.get("player_key") or "")
            row_tokens = set(row_key.split())
            base_score = 5 * len(row_tokens.intersection(set(player_key.split())))
            if row_key == player_key:
                base_score += 40
            if row_key in name_variants:
                base_score += 20
            if team_matches(row.get("team_name")):
                base_score += 15
            scored.append((base_score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1] if scored else None

    def find_player_in_recent_reports(self, player_name: str, team_name: str | None = None, max_reports: int = 8) -> dict[str, Any] | None:
        cache_key = (normalize_report_person_name(player_name), str(team_name or "").strip())
        cached = self.match_cache.get(cache_key)
        now_ts = time.time()
        if cached and now_ts - float(cached.get("timestamp") or 0.0) < self.report_ttl_seconds:
            result = cached.get("result")
            return dict(result) if isinstance(result, dict) else None

        try:
            links = self.list_recent_report_links(limit=max_reports)
        except Exception:
            return None

        for link in links:
            try:
                payload = self.fetch_report_payload_for_url(link)
            except Exception:
                continue
            matched_row = self.search_report_payload_for_player(payload, player_name=player_name, team_name=team_name)
            if matched_row:
                result = {
                    "row": matched_row,
                    "report_label": payload.get("report_label") or "",
                    "report_url": payload.get("report_url") or link,
                    "pending_teams": payload.get("pending_teams") or [],
                }
                self.match_cache[cache_key] = {"timestamp": now_ts, "result": result}
                return result

        self.match_cache[cache_key] = {"timestamp": now_ts, "result": None}
        return None

    def _parse_iso_datetime(self, value: Any) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except Exception:
            return None

    def payload_is_reliable_stale(self, payload: dict[str, Any] | None, cache_timestamp: float | None = None) -> bool:
        if not payload or not payload.get("ok"):
            return False

        now_et = datetime.now(ZoneInfo("America/New_York"))
        report_dt = self._parse_iso_datetime(payload.get("report_timestamp"))
        if report_dt is not None:
            try:
                report_dt = report_dt.astimezone(ZoneInfo("America/New_York")) if report_dt.tzinfo else report_dt.replace(tzinfo=ZoneInfo("America/New_York"))
            except Exception:
                pass
            if report_dt.date() != now_et.date():
                return False

        fetched_at = payload.get("fetched_at")
        fetched_dt = self._parse_iso_datetime(fetched_at)
        age_seconds = None
        if fetched_dt is not None:
            age_seconds = max(0.0, (datetime.now(fetched_dt.tzinfo or ZoneInfo("America/New_York")) - fetched_dt).total_seconds())
        elif cache_timestamp:
            age_seconds = max(0.0, time.time() - float(cache_timestamp))

        return age_seconds is not None and age_seconds <= self.max_stale_seconds

    def _ensure_payload_indexes(self, payload: dict[str, Any]) -> dict[str, Any]:
        cached = payload.get("_injury_indexes")
        if isinstance(cached, dict):
            return cached

        rows = payload.get("rows") or []
        rows_by_team_date: dict[tuple[str, str], list[dict[str, Any]]] = {}
        rows_by_team: dict[str, list[dict[str, Any]]] = {}
        rows_by_team_date_norm: dict[tuple[str, str], list[dict[str, Any]]] = {}
        rows_by_team_norm: dict[str, list[dict[str, Any]]] = {}
        pending_by_date: dict[str, set[str]] = {}
        pending_by_date_norm: dict[str, set[str]] = {}

        for row in rows:
            team_name = str(row.get("team_name") or "").strip()
            if not team_name:
                continue
            game_date = str(row.get("game_date") or "").strip()
            rows_by_team.setdefault(team_name, []).append(row)
            rows_by_team_date.setdefault((team_name, game_date), []).append(row)
            norm_team = normalize_compact_text(team_name)
            if norm_team:
                rows_by_team_norm.setdefault(norm_team, []).append(row)
                rows_by_team_date_norm.setdefault((norm_team, game_date), []).append(row)

        for entry in (payload.get("pending_entries") or []):
            team_name = str(entry.get("team_name") or "").strip()
            game_date = str(entry.get("game_date") or "").strip()
            if team_name:
                pending_by_date.setdefault(game_date, set()).add(team_name)
                norm_team = normalize_compact_text(team_name)
                if norm_team:
                    pending_by_date_norm.setdefault(game_date, set()).add(norm_team)

        indexes = {
            "rows_by_team_date": rows_by_team_date,
            "rows_by_team": rows_by_team,
            "rows_by_team_date_norm": rows_by_team_date_norm,
            "rows_by_team_norm": rows_by_team_norm,
            "pending_by_date": pending_by_date,
            "pending_by_date_norm": pending_by_date_norm,
        }
        payload["_injury_indexes"] = indexes
        return indexes

    def get_team_rows(self, payload: dict[str, Any], team_name: str, game_date: str | None = None) -> list[dict[str, Any]]:
        indexes = self._ensure_payload_indexes(payload)
        normalized_team_name = str(team_name or "").strip()
        if game_date is None:
            direct = list(indexes["rows_by_team"].get(normalized_team_name, []))
            if direct:
                return direct
            norm_key = normalize_compact_text(normalized_team_name)
            return list(indexes["rows_by_team_norm"].get(norm_key, []))
        date_key = str(game_date or "").strip()
        direct = list(indexes["rows_by_team_date"].get((normalized_team_name, date_key), []))
        if direct:
            return direct
        norm_key = normalize_compact_text(normalized_team_name)
        normalized_date_rows = list(indexes["rows_by_team_date_norm"].get((norm_key, date_key), []))
        if normalized_date_rows:
            return normalized_date_rows
        # Fallback: some scoreboards/report sources can differ by one day due to timezone
        # boundaries. If no date-scoped match exists, return team-scoped rows.
        direct_team_rows = list(indexes["rows_by_team"].get(normalized_team_name, []))
        if direct_team_rows:
            return direct_team_rows
        return list(indexes["rows_by_team_norm"].get(norm_key, []))

    def get_pending_teams(self, payload: dict[str, Any], game_date: str | None = None) -> set[str]:
        indexes = self._ensure_payload_indexes(payload)
        if game_date is None:
            raw = set(payload.get("pending_teams") or [])
            return {normalize_compact_text(name) for name in raw if name} if raw else set()
        date_pending = set(indexes["pending_by_date_norm"].get(str(game_date or "").strip(), set()))
        if date_pending:
            return date_pending
        raw = set(payload.get("pending_teams") or [])
        return {normalize_compact_text(name) for name in raw if name} if raw else set()

    def _build_error_payload(self, exc: Exception) -> dict[str, Any]:
        return {
            "ok": False,
            "report_url": "",
            "report_timestamp": "",
            "report_label": "",
            "rows": [],
            "pending_teams": [],
            "raw_text": "",
            "parse_method": "error",
            "error": str(exc),
            "source": "error",
            "fetched_at": datetime.now(ZoneInfo("America/New_York")).isoformat(),
        }

    def _fetch_report_pdf_payload(self, report_url: str, report_dt: datetime, headers: dict[str, str]) -> dict[str, Any]:
        cached_url_payload = self.url_cache.get(report_url)
        if cached_url_payload and cached_url_payload.get("payload"):
            return copy.deepcopy(cached_url_payload["payload"])

        pdf_response = self.http_get(report_url, headers=headers, timeout=self.pdf_timeout)
        pdf_response.raise_for_status()
        parsed_choice = self.choose_best_report_parse(pdf_response.content)
        payload = {
            "ok": True,
            "report_url": report_url,
            "report_timestamp": report_dt.isoformat() if report_dt != datetime.min.replace(tzinfo=ZoneInfo("America/New_York")) else "",
            "report_label": self.format_report_timestamp(report_dt),
            "rows": parsed_choice.get("rows") or [],
            "pending_teams": parsed_choice.get("pending_teams") or [],
            "raw_text": parsed_choice.get("raw_text") or "",
            "parse_method": parsed_choice.get("method") or "unknown",
            "error": None,
            "source": "fresh",
            "fetched_at": datetime.now(ZoneInfo("America/New_York")).isoformat(),
        }
        self.url_cache[report_url] = {"timestamp": time.time(), "payload": copy.deepcopy(payload)}
        return payload

    def _fetch_latest_report_payload_impl(self) -> dict[str, Any]:
        now_ts = time.time()
        cached = self.report_cache.get("payload")
        cached_ts = float(self.report_cache.get("timestamp") or 0.0)
        if cached and now_ts - cached_ts < self.report_ttl_seconds:
            try:
                links = self.list_recent_report_links(limit=12)
                latest_url = max(links, key=self.parse_report_timestamp)
                latest_dt = self.parse_report_timestamp(latest_url)
                cached_dt = self.parse_report_timestamp(str(cached.get("report_url") or ""))
                if latest_url and latest_dt > cached_dt and latest_url != cached.get("report_url"):
                    headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
                    payload = self._fetch_report_pdf_payload(latest_url, latest_dt, headers)
                    self.report_cache["timestamp"] = now_ts
                    self.report_cache["payload"] = payload
                    return payload
            except Exception:
                pass
            return cached

        if (
            cached
            and self.payload_is_reliable_stale(cached, cached_ts)
            and now_ts - float(self.meta.get("last_failure") or 0.0) < self.failure_cooldown_seconds
        ):
            stale_payload = dict(cached)
            stale_payload["source"] = stale_payload.get("source") or "stale"
            return stale_payload

        headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
        self.meta["last_attempt"] = now_ts
        try:
            links = self.list_recent_report_links(limit=12)
            latest_url = max(links, key=self.parse_report_timestamp)
            latest_dt = self.parse_report_timestamp(latest_url)

            if cached and cached.get("ok") and cached.get("report_url") == latest_url:
                verified_payload = dict(cached)
                verified_payload["source"] = "verified-cache"
                self.report_cache["timestamp"] = now_ts
                self.report_cache["payload"] = verified_payload
                return verified_payload

            payload = self._fetch_report_pdf_payload(latest_url, latest_dt, headers)
            self.report_cache["timestamp"] = now_ts
            self.report_cache["payload"] = payload
            return payload
        except Exception as exc:
            self.meta["last_failure"] = now_ts
            if cached and self.payload_is_reliable_stale(cached, cached_ts):
                stale_payload = dict(cached)
                stale_payload["source"] = "stale-fallback"
                stale_payload["error"] = str(exc)
                self.report_cache["timestamp"] = now_ts
                self.report_cache["payload"] = stale_payload
                return stale_payload

            payload = self._build_error_payload(exc)
            self.report_cache["timestamp"] = now_ts
            self.report_cache["payload"] = payload
            return payload

    def fetch_latest_report_payload(self) -> dict[str, Any]:
        return self.timed_call("fetch_latest_injury_report_payload")(self._fetch_latest_report_payload_impl)()

    def build_availability_payload(self, player_name: str, team_name: str | None = None, game_date: str | None = None) -> dict[str, Any]:
        report_payload = self.fetch_latest_report_payload()
        report_label = str(report_payload.get("report_label") or "")
        team_name = str(team_name or "").strip()
        cache_team_key = normalize_name(team_name) if team_name else ""
        cache_key = (normalize_report_person_name(player_name), cache_team_key, str(game_date or "").strip(), report_label)
        cached = self.availability_cache.get(cache_key)
        if cached:
            return dict(cached)

        if not report_payload.get("ok"):
            result = {
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
            self.availability_cache[cache_key] = dict(result)
            return result

        matched_row = self.search_report_payload_for_player(
            report_payload,
            player_name=player_name,
            team_name=team_name or None,
            game_date=game_date,
        )
        if matched_row:
            status = str(matched_row.get("status") or "Unknown").strip()
            tone = "good" if status in self.good_statuses else "bad" if status in self.unavailable_statuses else "warning" if status in self.risky_statuses else "neutral"
            reason = str(matched_row.get("reason") or "").strip()
            result = {
                "status": status,
                "tone": tone,
                "reason": reason,
                "note": reason or "Official status found on the latest NBA injury report.",
                "source": "Official NBA injury report",
                "report_label": report_payload.get("report_label") or "",
                "report_url": report_payload.get("report_url") or "",
                "is_unavailable": status in self.unavailable_statuses,
                "is_risky": status in self.risky_statuses,
                "sort_rank": self.status_order.get(status, 3),
            }
            self.availability_cache[cache_key] = dict(result)
            return result

        pending_teams = self.get_pending_teams(report_payload, game_date=game_date) if team_name else set()
        if team_name and normalize_compact_text(team_name) in pending_teams:
            result = {
                "status": "Pending report",
                "tone": "warning",
                "reason": "Team report not yet submitted on the latest official injury report.",
                "note": "The team has not yet submitted its latest official report.",
                "source": "Official NBA injury report",
                "report_label": report_payload.get("report_label") or "",
                "report_url": report_payload.get("report_url") or "",
                "is_unavailable": False,
                "is_risky": True,
                "sort_rank": self.status_order.get("Pending report", 2),
            }
            self.availability_cache[cache_key] = dict(result)
            return result

        recent_match = self.find_player_in_recent_reports(player_name=player_name, team_name=team_name or None, max_reports=12)
        if recent_match and recent_match.get("row"):
            row = dict(recent_match["row"])
            status = str(row.get("status") or "Unknown").strip()
            tone = "good" if status in self.good_statuses else "bad" if status in self.unavailable_statuses else "warning" if status in self.risky_statuses else "neutral"
            reason = str(row.get("reason") or "").strip()
            result = {
                "status": status,
                "tone": tone,
                "reason": reason,
                "note": reason or "Official status found on a recent NBA injury report.",
                "source": "Official NBA injury report",
                "report_label": recent_match.get("report_label") or report_payload.get("report_label") or "",
                "report_url": recent_match.get("report_url") or report_payload.get("report_url") or "",
                "is_unavailable": status in self.unavailable_statuses,
                "is_risky": status in self.risky_statuses,
                "sort_rank": self.status_order.get(status, 3),
            }
            self.availability_cache[cache_key] = dict(result)
            return result

        result = {
            "status": "Not listed",
            "tone": "good",
            "reason": "Player is not listed on the latest official injury report.",
            "note": "No official injury designation found on the latest report.",
            "source": "Official NBA injury report",
            "report_label": report_payload.get("report_label") or "",
            "report_url": report_payload.get("report_url") or "",
            "is_unavailable": False,
            "is_risky": False,
            "sort_rank": self.status_order.get("Not listed", 3),
        }
        self.availability_cache[cache_key] = dict(result)
        return result

    def build_team_availability_summary(self, team_name: str | None, report_payload: dict[str, Any] | None = None, game_date: str | None = None) -> dict[str, Any]:
        team_name = str(team_name or "").strip()
        payload = report_payload or self.fetch_latest_report_payload()
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

        pending_teams = self.get_pending_teams(payload, game_date=game_date)
        if team_name and normalize_compact_text(team_name) in pending_teams:
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

        rows = self.get_team_rows(payload, team_name, game_date=game_date)
        out_count = sum(1 for row in rows if str(row.get("status") or "") in self.unavailable_statuses)
        questionable_count = sum(1 for row in rows if str(row.get("status") or "") in self.risky_statuses)
        probable_count = sum(1 for row in rows if str(row.get("status") or "") in self.good_statuses)

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
                {"name": re.sub(r",(?!\s)", ", ", str(row.get("player_display") or "").strip()), "status": str(row.get("status") or "").strip()}
                for row in rows[:4]
            ],
        }

    def report_cache_age_seconds(self) -> float | None:
        timestamp = float(self.report_cache.get("timestamp") or 0.0)
        if not timestamp:
            return None
        return round(time.time() - timestamp, 2)
