"""Release-name parsing: unstructured torrent names -> canonical tokens.

Canonical vocabulary matches config/preferences.example.yaml exactly.
Deliberately conservative: unknown -> None -> "no preference points", never a
wrong classification.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

_RESOLUTIONS = [
    ("2160p", re.compile(r"\b2160p?\b|\b4k\b", re.I)),
    ("1080p", re.compile(r"\b1080[pi]?\b", re.I)),
    ("720p", re.compile(r"\b720p?\b", re.I)),
    ("480p", re.compile(r"\b480p?\b|\bDVD(Rip)?\b|\bPAL\b|\bNTSC\b", re.I)),
]

_VIDEO = [
    ("dolby_vision", re.compile(r"DOLBY[\s.]?VISION|\bDOVI\b|\bDVI\b(?![a-z])|\bDV\b(?![a-z])|DV[\s.]?HDR10", re.I)),
    ("hdr10", re.compile(r"\bHDR10\+?\b|\bHDR\b|\bPQ\b|\bHLG\b", re.I)),
    # default when nothing found
    ("sdr", re.compile(r"\bSDR\b", re.I)),
]

_AUDIO = [
    ("truehd_atmos", re.compile(r"TRUE[\s.]?HD.*ATMOS|ATMOS.*TRUE[\s.]?HD", re.I)),
    ("ddp_atmos", re.compile(r"(E-?AC-?3|DDP|DD\+|DOLBY[\s.]?DIGITAL[\s.]?PLUS).*(ATMOS)|ATMOS.*(E-?AC-?3|DDP|DD\+)", re.I)),
    ("truehd", re.compile(r"TRUE[\s.]?HD", re.I)),
    ("ddp", re.compile(r"\bE-?AC-?3\b|\bDDP\b|\bDD\+?\b|\bDD5[\s.]?1\b|DOLBY[\s.]?DIGITAL(\s*PLUS)?\b|AC-?3\b", re.I)),
    ("dts_hd", re.compile(r"DTS[\s.]?(HD|MA|X)(?!.*ATMOS)", re.I)),
    ("flac", re.compile(r"\bFLAC\b", re.I)),
    ("pcm", re.compile(r"\bPCM\b|\bLPCM\b", re.I)),
    ("aac", re.compile(r"\bAAC(?:[\s.]?\d)?\b", re.I)),
    ("dts", re.compile(r"\bDTS\b", re.I)),
]

_RELEASE_TYPES = [
    ("web-dl", re.compile(r"\bWEB-?DL\b|\bWEB\s?DL\b|\bWEBDL\b", re.I)),
    ("remux", re.compile(r"\bREMUX\b", re.I)),
    ("bluray", re.compile(r"\bBLU-?RAY\b|\bB[DR](?:Rip)?\b|\bBD\b(?![a-z])", re.I)),
    ("webrip", re.compile(r"\bWEB-?RIP\b", re.I)),
    ("hdtv", re.compile(r"\bHDTV\b", re.I)),
]

_LANG_FOREIGN = re.compile(r"\bMULTI\b|\bMULTi\b|\bFRENCH\b|\bTRUEFRENCH\b|\bGERMAN\b|\bDUTCH\b|\bSPANISH\b|\bITALIAN\b|\bVOSTFR\b|\bSUBFRENCH\b", re.I)
_LANG_ENGLISH = re.compile(r"\bENG\b|\bENGLISH\b", re.I)

_FLAG_PATTERNS = [
    ("3d", re.compile(r"\b3D\b", re.I)),
    ("cam", re.compile(r"\bCAM\b|\bHDCAM\b|\bCAMRIP\b|\bHDTS\b", re.I)),
    ("telesync", re.compile(r"\bTELESYNC\b|\bTELECINE\b|\bTS\b|\bTC\b", re.I)),
    ("hardcoded_subs", re.compile(r"\bHC\b|\bHARDSUB\w*\b|\bSUBBED\b", re.I)),
]

_SIZE = re.compile(r"([\d.]+)\s*(TB|GB|MB|G|M)\b", re.I)


@dataclass
class ParsedRelease:
    resolution: str | None = None
    video: str | None = None      # dolby_vision | hdr10 | sdr
    audio: str | None = None
    release_type: str | None = None
    language: str = "english"     # approximated: english unless explicit foreign marker
    flags: set[str] = field(default_factory=set)


def parse_release(name: str) -> ParsedRelease:
    parsed = ParsedRelease()
    for token, pattern in _RESOLUTIONS:
        if pattern.search(name):
            parsed.resolution = token
            break
    for token, pattern in _VIDEO:
        if pattern.search(name):
            parsed.video = token
            break
    if parsed.video is None:
        parsed.video = "sdr"
    for token, pattern in _AUDIO:
        if pattern.search(name):
            parsed.audio = token
            break
    for token, pattern in _RELEASE_TYPES:
        if pattern.search(name):
            parsed.release_type = token
            break
    if _LANG_FOREIGN.search(name) and not _LANG_ENGLISH.search(name):
        parsed.language = "foreign"
    for flag, pattern in _FLAG_PATTERNS:
        if pattern.search(name):
            parsed.flags.add(flag)
    return parsed


def parse_size_bytes(name: str) -> int | None:
    m = _SIZE.search(name or "")
    if not m:
        return None
    value = float(m.group(1))
    unit = m.group(2).upper()
    factor = {"TB": 1 << 40, "GB": 1 << 30, "G": 1 << 30, "MB": 1 << 20, "M": 1 << 20}[unit]
    return int(value * factor)
