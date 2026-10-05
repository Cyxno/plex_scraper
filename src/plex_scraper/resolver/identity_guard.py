"""Identity-conflict guard (D/B/C): wrong identity must fail closed before search.

Pure beslisfunctie: incoming identity vs exact Plex-authoritative identity vs
een reeds geverifieerde resolver-identity. Title/year is NOOIT authority.
Geen auto-correctie: conflict => flag + event + search-block; herstel gaat
via authoritative reconciliation (operator), niet via stil overschrijven.
"""
from __future__ import annotations

IDENTITY_OK = "IDENTITY_OK"
IDENTITY_ENRICHED = "IDENTITY_ENRICHED"
IDENTITY_CONFLICT = "IDENTITY_CONFLICT"
IDENTITY_INCOMPLETE = "IDENTITY_INCOMPLETE"


def _norm_imdb(v) -> str | None:
    v = (v or "").strip().lower()
    return v if v.startswith("tt") and v[2:].isdigit() else None


def _norm_tmdb(v) -> str | None:
    if v is None or v == "":
        return None
    s = str(v).strip()
    return s if s.isdigit() else None


def normalize_ids(ids: dict) -> dict:
    return {"imdb_id": _norm_imdb(ids.get("imdb_id")),
            "tmdb_id": _norm_tmdb(ids.get("tmdb_id"))}


def decide_identity(incoming: dict, authoritative: dict,
                    existing: dict | None = None) -> tuple[str, dict]:
    """B/C: beslis (decision, details).

    - beide aanwezig en verschillend (imdb óf tmdb) -> IDENTITY_CONFLICT
    - agreement -> IDENTITY_OK
    - incoming leeg, authoritative aanwezig -> IDENTITY_ENRICHED (bestaande
      safe-enrichment policy; nooit bij conflict)
    - geen authoritative bron of beide leeg -> IDENTITY_INCOMPLETE
    - bestaande verified identity wordt nooit als conflict-bron gebruikt,
      alleen ter bescherming (C5): een conflict t.o.v. authoritative blijft
      een conflict, ook als existing toevallig met incoming overeenkomt.
    """
    inc = normalize_ids(incoming)
    auth = normalize_ids(authoritative)
    exi = normalize_ids(existing or {})
    conflicts = []
    for field in ("imdb_id", "tmdb_id"):
        if inc[field] and auth[field] and inc[field] != auth[field]:
            conflicts.append(field)
    detail = {"incoming": inc, "authoritative": auth, "existing": exi,
              "conflicting_fields": conflicts}
    if conflicts:
        return IDENTITY_CONFLICT, detail
    if not any(auth.values()):
        return IDENTITY_INCOMPLETE, detail   # geen authoritative bron: geen oordeel (C3)
    if not any(inc.values()):
        return IDENTITY_ENRICHED, detail
    return IDENTITY_OK, detail
