"""Legacy identity data-hygiëne: pure classificatie- en correctielogica.

Alle functies zijn read-only ten opzichte van de store: ze produceren
correctietabellen die de aanroeper via de PATCH-route persisteert. Een
correctie bevat uitsluitend item-level externe IDs (imdb_id/tmdb_id/
tvdb_id) — nooit show_*, status, generation of source-state.

Splitsing van ID-lagen (FASE identity-split):
  show-level   -> show_imdb_id / show_tmdb_id / show_tvdb_id (series-searchkey)
  item-level   -> imdb_id / tmdb_id / tvdb_id  (episode-level metadata)
Legacy pollutie: pre-split ingest zette show-ID's op item-level velden.
"""
from __future__ import annotations

ITEM_FIELDS = ("imdb_id", "tmdb_id", "tvdb_id")
SHOW_MAP = {"imdb_id": "show_imdb_id", "tmdb_id": "show_tmdb_id", "tvdb_id": "show_tvdb_id"}
PLEX_KEY = {"imdb_id": "imdb", "tmdb_id": "tmdb", "tvdb_id": "tvdb"}

ITEM_ID_VALID = "ITEM_ID_VALID"
ITEM_ID_EQUALS_SHOW_ID = "ITEM_ID_EQUALS_SHOW_ID"
ITEM_ID_MISSING = "ITEM_ID_MISSING"
ITEM_ID_CONFLICT = "ITEM_ID_CONFLICT"
UNKNOWN = "UNKNOWN"

# NO_SOURCE-finaalmodel (DEEL H): één exacte reden per item, nooit "generic".
PROVIDER_NO_MATCH = "PROVIDER_NO_MATCH"          # 0 candidates bij complete identiteit
NO_USABLE_CANDIDATE = "NO_USABLE_CANDIDATE"      # candidates > 0, geen valideerbaar
BACKEND_UNAVAILABLE = "BACKEND_UNAVAILABLE"      # provider/back-end faalt (transient)
PLEX_ORPHAN = "PLEX_ORPHAN"                      # pad/broken symlink, niet de provider
STALE_DUPLICATE = "STALE_DUPLICATE"              # legacy rij naast gezond canoniek item
TRUE_NO_SOURCE = "TRUE_NO_SOURCE"                # niets anders aantoonbaar


def classify_item_ids(item: dict, plex_episode: dict | None = None) -> str:
    """Classificeer item-level IDs tegen show-IDs en optioneel Plex episode-GUIDs.

    plex_episode: {"imdb": "tt...", "tmdb": int, "tvdb": int} of None.
    Gelijkheid aan het show-ID is pollutie-verdacht; pas bij een bekende
    authoritative episode-GUID die ook afwijkt is het een conflict. Zonder
    authoritative bron blijft het (bewust) UNKNOWN — nooit gokken.
    """
    item_vals = {f: item.get(f) for f in ITEM_FIELDS}
    show_vals = {f: item.get(SHOW_MAP[f]) for f in ITEM_FIELDS}

    if all(v is None for v in item_vals.values()):
        return ITEM_ID_MISSING

    equal = [f for f in ITEM_FIELDS
             if item_vals[f] is not None and show_vals[f] is not None
             and str(item_vals[f]) == str(show_vals[f])]
    conflict = [f for f in ITEM_FIELDS
                if item_vals[f] is not None and f not in equal
                and plex_episode is not None and plex_episode.get(PLEX_KEY[f]) is not None
                and str(item_vals[f]) != str(plex_episode[PLEX_KEY[f]])]

    if conflict:
        return ITEM_ID_CONFLICT
    if equal:
        return ITEM_ID_EQUALS_SHOW_ID
    if plex_episode:
        ok = all(item_vals[f] is None or str(item_vals[f]) == str(plex_episode.get(PLEX_KEY[f]))
                 for f in ITEM_FIELDS)
        return ITEM_ID_VALID if ok else ITEM_ID_CONFLICT
    return UNKNOWN


def build_correction(item: dict, authoritative: dict | None) -> dict:
    """Correctietabel-entry: uitsluitend afwijkende item-level IDs.

    - alleen velden waarvoor een authoritative waarde bestaat (nooit NULL-en);
    - alleen als de huidige waarde afwijkt (idempotent: tweede run is leeg);
    - nooit show_*, status, generation, desired of path-keys.
    """
    if not authoritative:
        return {}
    fix: dict = {}
    for f in ITEM_FIELDS:
        av = authoritative.get(PLEX_KEY[f])
        cur = item.get(f)
        if av is not None and (cur is None or str(cur) != str(av)):
            fix[f] = av
    return fix


def find_duplicate_episodes(items: list[dict]) -> dict[tuple, list[str]]:
    """Dubbele resolver-items op (series, season, episode), casefold onafhankelijk."""
    groups: dict[tuple, list[str]] = {}
    for it in items:
        if it.get("kind") != "episode" or not it.get("series"):
            continue
        key = (it["series"].casefold(), it.get("season"), it.get("episode"))
        groups.setdefault(key, []).append(it["id"])
    return {k: v for k, v in groups.items() if len(v) > 1}


def decide_stale_duplicate(stale: dict, canonical: dict) -> dict:
    """Bewijs-gedreven retirement-beslissing voor een duplicaat-rij.

    Eis: het canonieke item is gezond (READY, generation >= 1) én het
    stale-item heeft geen actieve source (generation 0, geen sources).
    Retourneert besluit + exacte redenen; veranderd niets zelf.
    """
    reasons: list[str] = []
    canonical_ready = canonical.get("status") == "READY"
    canonical_proven = (canonical.get("generation") or 0) >= 1
    stale_unproven = (stale.get("generation") or 0) == 0 and not stale.get("has_sources")
    if canonical_ready:
        reasons.append("canonical READY")
    if canonical_proven:
        reasons.append("canonical generation >= 1 (resolve bewezen)")
    if stale_unproven:
        reasons.append("stale generation 0 zonder sources")
    retire = canonical_ready and canonical_proven and stale_unproven
    return {"retire": retire, "reasons": reasons}


def classify_no_source(candidates: int, *, validation_failures: int = 0,
                       identity_rejections: int = 0, budget_exhausted: bool = False,
                       provider_error: bool = False,
                       path_readable: bool | None = None) -> str:
    """NO_SOURCE -> exacte reden (D6/D7): 0 candidates vs onbruikbaar vs orphan.

    candidates == 0 en pad gezond          -> PROVIDER_NO_MATCH
    candidates > 0, niets valideert        -> NO_USABLE_CANDIDATE
    provider/back-end error overheerst     -> BACKEND_UNAVAILABLE
    pad niet leesbaar (.ids/broken link)   -> PLEX_ORPHAN (niet de provider)
    """
    if path_readable is False:
        return PLEX_ORPHAN
    if candidates <= 0 and not provider_error:
        return PROVIDER_NO_MATCH
    if provider_error and validation_failures == 0 and identity_rejections == 0 \
            and candidates <= 0:
        return BACKEND_UNAVAILABLE
    return NO_USABLE_CANDIDATE


def audit_report(rows: list[dict]) -> dict:
    """Rapport met uitsluitend allowlisted keys (geen secrets/tokens/paths met creds)."""
    allowed = {"total_episodes", "item_ids_valid", "item_id_equals_show_id",
               "correctable_from_plex", "correctable_from_tautulli", "unresolved",
               "no_source_states", "duplicates"}
    return {k: v for k, v in rows.items() if k in allowed}
