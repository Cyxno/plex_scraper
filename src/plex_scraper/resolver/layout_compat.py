"""Source-layout compatibiliteit voor source-switches (incident Pirates
2026-10-10 19:50 CEST).

Bewezen probleem: een mid-session switch van de 54,6 GB HYBRID REMUX naar de
26,5 GB x265-E-encode liet Plex geloven dat een inhoudelijk ánder bestand
nog steeds dezelfde byte/stream-identiteit had. Het stabiele pad bleef
gelijk, de bytes daaronder wisselden; de eerste seek startte nieuwe ffmpeg-
jobs tegen een gemixte metadata-state (part size/duration van de nieuwe bron,
streamlijst van de oude) en playback herstelde niet.

Contract:
  * classify(old, new) → IDENTICAL_LAYOUT | MATERIAL_LAYOUT_CHANGE, op basis
    van meetbare velden (size, codec, resolution, hdr, audio-familie,
    container-extensie) — nooit op titel/hash alleen;
  * MATERIAL → de switch mag alleen met een gecontroleerde overgang:
    metadata-coherentie vóór bytes (zie JitController._activate);
  * kleine verschillen (release/re-encode binnen ±25% size, zelfde
    codec/resolutie/HDR/audio-familie) zijn IDENTICAL_LAYOUT.
"""
from __future__ import annotations

import os

IDENTICAL_LAYOUT = "IDENTICAL_LAYOUT"
MATERIAL_LAYOUT_CHANGE = "MATERIAL_LAYOUT_CHANGE"

# drempels (bewust conservatief: een Pirates-achtige REMUX→encode wissel is
# −51% size en móét materiaal zijn)
SIZE_REL_THRESHOLD = 0.25          # >25% relatief verschil
SIZE_ABS_THRESHOLD = 2 * 10**9     # of >2 GB absoluut

_VIDEO_FAMILIES = {"hevc": "hevc", "h265": "hevc", "x265": "hevc",
                   "h264": "h264", "x264": "h264", "avc": "h264",
                   "av1": "av1", "vp9": "vp9"}

_AUDIO_FAMILIES = {"truehd": "truehd", "truehd_atmos": "truehd",
                   "dts_hd_ma": "dts", "dts": "dts", "eac3": "dd+",
                   "ac3": "dd", "flac": "flac", "opus": "opus",
                   "aac": "aac"}


def _norm(value) -> str:
    return (value or "").strip().lower()


def _container(file_name: str | None) -> str:
    return os.path.splitext(file_name or "")[1].lower().lstrip(".")


def classify(old, new) -> tuple[str, list[str]]:
    """Vergelijk twee bronnen; retourneert (verdict, redenen).

    Alle vergelijkbare velden worden gerapporteerd; één materiaal
    verschil is voldoende voor MATERIAL_LAYOUT_CHANGE.
    """
    reasons: list[str] = []
    if old is None:
        return IDENTICAL_LAYOUT, []          # eerste bron: geen wissel

    # 1) size — de hardste indicator (Pirates: 54,6 → 26,5 GB)
    if old.size and new.size:
        delta_rel = abs(new.size - old.size) / max(old.size, new.size)
        delta_abs = abs(new.size - old.size)
        if delta_rel > SIZE_REL_THRESHOLD or delta_abs > SIZE_ABS_THRESHOLD:
            reasons.append(
                f"size {old.size}->{new.size} "
                f"(rel {delta_rel:.0%}, abs {delta_abs / 10**9:.1f} GB)")

    # 2) video codec (familie)
    if _norm(old.codec) and _norm(new.codec) and \
            _VIDEO_FAMILIES.get(_norm(old.codec), _norm(old.codec)) != \
            _VIDEO_FAMILIES.get(_norm(new.codec), _norm(new.codec)):
        reasons.append(f"codec {old.codec}->{new.codec}")

    # 3) resolutie
    if _norm(old.resolution) and _norm(new.resolution) and \
            _norm(old.resolution) != _norm(new.resolution):
        reasons.append(f"resolution {old.resolution}->{new.resolution}")

    # 4) HDR/DV profiel
    if _norm(old.hdr) and _norm(new.hdr) and _norm(old.hdr) != _norm(new.hdr):
        reasons.append(f"hdr {old.hdr}->{new.hdr}")

    # 5) audio-familie (codec-klasse; truehd_atmos vs truehd is gelijk)
    a_old = _AUDIO_FAMILIES.get(_norm(old.audio), _norm(old.audio))
    a_new = _AUDIO_FAMILIES.get(_norm(new.audio), _norm(new.audio))
    if a_old and a_new and a_old != a_new:
        reasons.append(f"audio {old.audio}->{new.audio}")

    # 6) container
    c_old, c_new = _container(old.file_name), _container(new.file_name)
    if c_old and c_new and c_old != c_new:
        reasons.append(f"container {c_old}->{c_new}")

    if reasons:
        return MATERIAL_LAYOUT_CHANGE, reasons
    return IDENTICAL_LAYOUT, []
