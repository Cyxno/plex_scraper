"""Transparent preference-driven scoring (FASE 3).

Points are positional: the first preferred entry earns full points, later
entries decay. The full breakdown is returned so a candidate can say *why*
it scored what it scored, and rejects always carry reasons.
"""
from __future__ import annotations

import yaml
from dataclasses import dataclass

from ..domain.models import ScoreBreakdown, ScoreLine
from .release_parser import ParsedRelease, parse_release, parse_size_bytes

_POSITIONAL = [30.0, 20.0, 10.0]          # resolution
_DECAY = [15.0, 10.0, 6.0, 4.0, 2.0]      # video
_AUDIO_DECAY = [10.0, 8.0, 6.0, 4.0, 2.0]  # audio (brief example: Atmos +10)
_RELEASE_DECAY = [10.0, 7.0, 4.0, 2.0, 1.0]  # release type (brief example: WEB-DL +10)


def _positional_points(prefer: list, value) -> float | None:
    if value is None:
        return None
    try:
        idx = [str(p).lower() for p in prefer].index(str(value).lower())
    except ValueError:
        return 0.0
    return _POSITIONAL[idx] if len(_POSITIONAL) > idx else 2.0


def _decayed_points(prefer: list, value, curve: list | None = None) -> float | None:
    if value is None:
        return None
    try:
        idx = [str(p).lower() for p in prefer].index(str(value).lower())
    except ValueError:
        return 0.0
    curve = curve if curve is not None else _DECAY
    return curve[idx] if len(curve) > idx else 1.0


@dataclass
class Scorer:
    prefs: dict

    @classmethod
    def from_yaml(cls, path: str) -> "Scorer":
        with open(path, "r", encoding="utf-8") as fh:
            return cls(yaml.safe_load(fh) or {})

    def _list(self, section: str, key: str = "prefer") -> list:
        return (self.prefs.get(section) or {}).get(key) or []

    def score(
        self,
        release_name: str,
        *,
        cached: bool = False,
        seeders: int | None = None,
        size: int | None = None,
        parsed: ParsedRelease | None = None,
    ) -> ScoreBreakdown:
        parsed = parsed or parse_release(release_name)
        out = ScoreBreakdown()

        # ---- hard rejects first -------------------------------------------
        exclusions = [str(e).lower() for e in self.prefs.get("exclude") or []]
        for flag in sorted(parsed.flags):
            if flag in exclusions:
                out.rejects.append(f"excluded: {flag}")
        limits = self.prefs.get("limits") or {}
        max_gb = limits.get("max_size_gb")
        size = size if size is not None else parse_size_bytes(release_name)
        if max_gb and size and size > float(max_gb) * (1 << 30):
            out.rejects.append(f"size {size / (1 << 30):.1f}GB > max {max_gb}GB")
        if out.rejected:
            return out

        # ---- preference points --------------------------------------------
        pts = _positional_points(self._list("resolution"), parsed.resolution)
        if pts is not None:
            out.lines.append(ScoreLine(f"resolution {parsed.resolution}", pts))

        pts = _decayed_points(self._list("video"), parsed.video)
        if pts is not None:
            out.lines.append(ScoreLine(f"video {parsed.video}", pts))

        pts = _decayed_points(self._list("audio"), parsed.audio, _AUDIO_DECAY)
        if pts is not None:
            out.lines.append(ScoreLine(f"audio {parsed.audio}", pts))

        lang_prefer = [str(l).lower() for l in self._list("language")]
        if parsed.language == "foreign":
            if "english" in lang_prefer:
                out.lines.append(ScoreLine("language foreign (no english track marker)", 0.0))
        else:
            if "english" in lang_prefer:
                out.lines.append(ScoreLine("language english", 15.0))

        pts = _decayed_points(self._list("release"), parsed.release_type, _RELEASE_DECAY)
        if pts is not None:
            out.lines.append(ScoreLine(f"release {parsed.release_type}", pts))

        bonuses = self.prefs.get("bonuses") or {}
        if cached:
            out.lines.append(ScoreLine("cached/ready", float(bonuses.get("cached", 18))))
        if seeders is not None and seeders >= 10 and bonuses.get("seeders_top10"):
            out.lines.append(ScoreLine(f"seeders {seeders}>=10", float(bonuses["seeders_top10"])))

        out.total = round(sum(l.points for l in out.lines), 2)
        return out
