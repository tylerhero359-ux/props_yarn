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

from identity_utils import normalize_compact_text, normalize_report_person_name


@dataclass
class InjuryReportService:
    team_pool: list[dict[str, Any]]
    http_get: Callable[..., Any]
    timed_call: Callable[[str], Callable]
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

    def _team_name_by_compact_text(self, compact_text: str) -> str | None:
        for team in self.team_pool:
            if normalize_compact_text(str(team.get("full_name") or "")) == compact_text:
                return str(team.get("full_name") or "")
        return None

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
        compact_line = normalize_compact_text(text_line)
        for team in sorted(self.team_pool, key=lambda item: len(item["full_name"]), reverse=True):
            team_name = str(team.get("full_name") or "")
            compact_team = normalize_compact_text(team_name)
            if compact_team and compact_line.startswith(compact_team):
                remainder = text_line[len(team_name):].strip()
                return team_name, remainder
        return None, text_line.strip()

    def parse_report_rows(self, report_text: str) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        pending_teams: list[str] = []
        current_team: str | None = None

        for raw_line in report_text.splitlines():
            line = " ".join(str(raw_line or "").split())
            if not line:
                continue

            team_name, remainder = self.extract_team_prefix(line)
            if team_name:
                current_team = team_name
            else:
                remainder = line

            pending_match = re.search(r"pending\s+report", remainder, flags=re.IGNORECASE)
            if team_name and pending_match:
                pending_teams.append(team_name)
                continue

            row_match = re.match(
                rf"^(?P<player>.+?)\s+(?P<status>{self.status_pattern})\b(?:\s+(?P<reason>.*))?$",
                remainder,
                flags=re.IGNORECASE,
            )
            if not row_match or not current_team:
                continue

            player_display = re.sub(r",(?!\s)", ", ", row_match.group("player").strip())
            status = row_match.group("status").strip()
            canonical_status = next((s for s in self.report_statuses if s.lower() == status.lower()), status)
            rows.append(
                {
                    "team_name": current_team,
                    "player_display": player_display,
                    "player_key": normalize_report_person_name(player_display),
                    "status": canonical_status,
                    "reason": (row_match.group("reason") or "").strip(),
                }
            )

        return {"rows": rows, "pending_teams": list(dict.fromkeys(pending_teams))}

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

                            player_display = re.sub(r",(?!\s)", ", ", player_cell.strip())
                            if not player_display or not last_team_name:
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
        candidates = self.extract_report_text_candidates(pdf_bytes)
        best_payload: dict[str, Any] = {"rows": [], "pending_teams": [], "raw_text": "", "method": "none"}

        for candidate in candidates:
            parsed = self.parse_report_rows(candidate["text"])
            if len(parsed.get("rows") or []) > len(best_payload.get("rows") or []):
                best_payload = {
                    "rows": parsed.get("rows") or [],
                    "pending_teams": parsed.get("pending_teams") or [],
                    "raw_text": candidate["text"],
                    "method": candidate["method"],
                }

        if not best_payload["rows"] and candidates:
            fallback = candidates[0]
            parsed = self.parse_report_rows(fallback["text"])
            best_payload = {
                "rows": parsed.get("rows") or [],
                "pending_teams": parsed.get("pending_teams") or [],
                "raw_text": fallback["text"],
                "method": fallback["method"],
            }

        return best_payload

    def list_recent_report_links(self, limit: int = 12) -> list[str]:
        now_ts = time.time()
        cached_links = self.links_cache.get("links") or []
        cached_ts = float(self.links_cache.get("timestamp") or 0.0)
        if cached_links and now_ts - cached_ts < self.report_ttl_seconds:
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

    def search_report_payload_for_player(self, report_payload: dict[str, Any], player_name: str, team_name: str | None = None) -> dict[str, Any] | None:
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
                if wanted_team and current_team != wanted_team:
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
        return candidates[0] if candidates else None

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

    def build_availability_payload(self, player_name: str, team_name: str | None = None) -> dict[str, Any]:
        report_payload = self.fetch_latest_report_payload()
        report_label = str(report_payload.get("report_label") or "")
        team_name = str(team_name or "").strip()
        cache_key = (normalize_report_person_name(player_name), team_name, report_label)
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

        matched_row = self.search_report_payload_for_player(report_payload, player_name=player_name, team_name=team_name or None)
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

        if team_name and team_name in set(report_payload.get("pending_teams") or []):
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

    def build_team_availability_summary(self, team_name: str | None, report_payload: dict[str, Any] | None = None) -> dict[str, Any]:
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
