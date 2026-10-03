"""Media-aware throughput requirement (adaptive-throughput fase).

required_throughput = media_bitrate x safety_margin

Bitrate-bron, in volgorde van betrouwbaarheid:
  1. `media_bitrate_mbit` expliciet gezet (Plex metadata / operator)  → 'plex'
  2. afgeleid uit file_size / duration_s                              → 'derived'
  3. veilige globale floor (PLAYBACK_MIN_MBIT)                        → 'floor'

Bij 'floor' is de waarde een schatting: health mag daarmee waarschuwen
maar geen agressieve actie (repair/upgrade) nemen.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MediaProfile:
    bitrate_mbit: float
    confidence: str            # 'plex' | 'derived' | 'floor'
    duration_s: float | None = None
    size_bytes: int | None = None
    risk: str = "normal"       # 'normal' | 'HIGH_RISK' (FASE 10)

    def required_mbit(self, margin: float) -> float:
        """Vereiste sustain-throughput incl. burst-headroom (FASE 3): de
        safety-margin is ook de piekvoorraad; Plex stelt geen peak-bitrate
        beschikbaar via de bestaande koppeling, dus de marge dekt pieken."""
        return self.bitrate_mbit * margin


HEAVY_SIZE_BYTES = 40 * 10**9


def build_profile_risk(size_bytes: int | None, confidence: str) -> str:
    """FASE 10: unknown-metadata + zware file = HIGH_RISK — zo'n item mag
    nooit via de lage floor als 'veilig' worden behandeld."""
    if confidence == "floor" and size_bytes and size_bytes > HEAVY_SIZE_BYTES:
        return "HIGH_RISK"
    return "normal"

    def required_mbit(self, margin: float) -> float:
        """Vereiste sustain-throughput incl. burst-headroom (FASE 3): de
        safety-margin is ook de piekvoorraad; Plex stelt geen peak-bitrate
        beschikbaar via de bestaande koppeling, dus de marge dekt pieken."""
        return self.bitrate_mbit * margin


def build_profile(size_bytes: int | None,
                  media_bitrate_mbit: float | None,
                  duration_s: float | None,
                  floor_mbit: float) -> MediaProfile:
    if media_bitrate_mbit and media_bitrate_mbit > 0:
        return MediaProfile(bitrate_mbit=float(media_bitrate_mbit),
                            confidence="plex",
                            duration_s=duration_s, size_bytes=size_bytes,
                            risk=build_profile_risk(size_bytes, "plex"))
    if duration_s and duration_s > 0 and size_bytes and size_bytes > 0:
        # totale container-bitrate (video+audio+overhead) — exact wat de
        # speler sequentieel moet trekken
        mbit = size_bytes * 8 / 1e6 / float(duration_s)
        return MediaProfile(bitrate_mbit=round(mbit, 1),
                            confidence="derived",
                            duration_s=duration_s, size_bytes=size_bytes,
                            risk=build_profile_risk(size_bytes, "derived"))
    return MediaProfile(bitrate_mbit=float(floor_mbit), confidence="floor",
                        duration_s=duration_s, size_bytes=size_bytes,
                        risk=build_profile_risk(size_bytes, "floor"))
