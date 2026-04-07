from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any


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
        last = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", last)
        raw = f"{first} {last}".strip()
    else:
        raw = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", raw)

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


def build_team_alias_lookup(team_pool: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for team in team_pool:
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


@dataclass
class PlayerSearchIndex:
    player_lookup: dict[int, dict[str, Any]]
    variant_lookup: dict[int, set[str]]
    exact_lookup: dict[str, tuple[int, ...]]
    token_lookup: dict[str, tuple[int, ...]]
    search_text: dict[int, str]
    sort_rank: dict[int, int]
    query_cache: dict[str, tuple[tuple[int, ...], tuple[int, ...]]] = field(default_factory=dict)

    @classmethod
    def build(cls, player_pool: list[dict[str, Any]]) -> "PlayerSearchIndex":
        player_lookup = {int(player["id"]): player for player in player_pool}
        sorted_players = sorted(
            player_pool,
            key=lambda item: (not item.get("is_active", False), str(item.get("full_name", ""))),
        )
        sort_rank = {int(player["id"]): index for index, player in enumerate(sorted_players)}

        variant_lookup: dict[int, set[str]] = {}
        exact_sets: dict[str, set[int]] = {}
        token_sets: dict[str, set[int]] = {}
        search_text: dict[int, str] = {}

        for player in sorted_players:
            player_id = int(player["id"])
            variants = build_player_name_variants(str(player.get("full_name", "")))
            variant_lookup[player_id] = variants
            search_text[player_id] = " ".join(sorted(variants))
            for variant in variants:
                exact_sets.setdefault(variant, set()).add(player_id)
                for token in variant.split():
                    if token:
                        token_sets.setdefault(token, set()).add(player_id)

        exact_lookup = {
            key: tuple(sorted(ids, key=lambda player_id: sort_rank.get(player_id, math.inf)))
            for key, ids in exact_sets.items()
        }
        token_lookup = {
            key: tuple(sorted(ids, key=lambda player_id: sort_rank.get(player_id, math.inf)))
            for key, ids in token_sets.items()
        }

        return cls(
            player_lookup=player_lookup,
            variant_lookup=variant_lookup,
            exact_lookup=exact_lookup,
            token_lookup=token_lookup,
            search_text=search_text,
            sort_rank=sort_rank,
        )

    def sorted_player_ids(self, player_ids: set[int]) -> list[int]:
        return sorted(player_ids, key=lambda player_id: self.sort_rank.get(player_id, math.inf))

    def candidate_player_ids_for_needles(self, needles: set[str]) -> set[int]:
        token_candidates: set[int] = set()
        for needle in needles:
            for token in needle.split():
                token_candidates.update(self.token_lookup.get(token, ()))
        return token_candidates

    def resolve_query_ids(self, query: str) -> tuple[tuple[int, ...], tuple[int, ...]]:
        cached = self.query_cache.get(query)
        if cached is not None:
            return cached

        needles = build_player_name_variants(query)
        if not needles:
            return (), ()

        exact_ids: set[int] = set()
        for needle in needles:
            exact_ids.update(self.exact_lookup.get(needle, ()))

        candidate_ids = self.candidate_player_ids_for_needles(needles)
        if not candidate_ids:
            candidate_ids = set(self.player_lookup)
        candidate_ids.difference_update(exact_ids)

        partial_ids: set[int] = set()
        for player_id in candidate_ids:
            search_text = self.search_text.get(player_id, "")
            if any(needle in search_text for needle in needles):
                partial_ids.add(player_id)
                continue
            player_variants = self.variant_lookup.get(player_id, set())
            if any(
                needle in candidate or candidate in needle
                for needle in needles
                for candidate in player_variants
            ):
                partial_ids.add(player_id)

        resolved = (tuple(self.sorted_player_ids(exact_ids)), tuple(self.sorted_player_ids(partial_ids)))
        if len(self.query_cache) >= 512:
            self.query_cache.pop(next(iter(self.query_cache)))
        self.query_cache[query] = resolved
        return resolved

    def find_player(self, player_name: str, roster_ids: set[int] | None = None) -> dict[str, Any] | None:
        exact_ids, partial_ids = self.resolve_query_ids(player_name)
        if not exact_ids and not partial_ids:
            return None

        exact_matches = [self.player_lookup[player_id] for player_id in exact_ids if player_id in self.player_lookup]
        partial_matches = [self.player_lookup[player_id] for player_id in partial_ids if player_id in self.player_lookup]

        if roster_ids is not None:
            for collection in (exact_matches, partial_matches):
                roster_matches = [player for player in collection if int(player["id"]) in roster_ids]
                if roster_matches:
                    return roster_matches[0]

        if exact_matches:
            return exact_matches[0]
        if partial_matches:
            return partial_matches[0]
        return None

    def search(self, query: str, limit: int = 15) -> list[dict[str, Any]]:
        exact_ids, partial_ids = self.resolve_query_ids(query)
        matches: list[dict[str, Any]] = []

        for player_id in (*exact_ids, *partial_ids):
            player = self.player_lookup.get(player_id)
            if not player:
                continue
            matches.append(
                {
                    "id": player_id,
                    "full_name": str(player.get("full_name", "")),
                    "is_active": player.get("is_active", False),
                }
            )
            if len(matches) >= limit:
                break

        return matches
