"""Environment-driven settings. Secrets never enter the codebase."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _read_secret_file(path: str) -> str:
    """Docker-secrets / Unraid keyfile support: token never lives in env."""
    if not path:
        return ""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read().strip()
    except OSError:
        return ""


@dataclass
class Settings:
    # resolver
    resolver_bind: str = "0.0.0.0:8282"
    debug: bool = False
    log_level: str = "info"
    torbox_api_token: str = ""
    torbox_base_url: str = "https://api.torbox.app/v1/api"
    scraper_torrentio_base: str = (
        "https://torrentio.strem.fun/providers=yts,eztv,rarbg,1337x,"
        "thepiratebay,kickasstorrents,torrentgalaxy,magnetdl,itorrent"
    )
    db_path: str = "/data/state.db"
    config_dir: str = "/config"

    # caches (seconds)
    cache_candidates_ttl: int = 1800
    cache_checkcached_ttl: int = 600
    cache_link_ttl: int = 9000          # requestdl validity ~3h
    cache_bad_ttl: int = 1800
    cache_bad_ttl_max: int = 21600

    # provider hygiene
    torbox_timeout_connect: float = 10.0
    torbox_timeout_read: float = 30.0
    torbox_max_retries: int = 3
    upstream_concurrency: int = 8
    validation_probe_bytes: int = 65536

    # vfs
    vfs_mountpoint: str = "/mnt/plex-scraper"
    # diagnostics web GUI
    mig_db_path: str = "/mnt/user/appdata/plex-scraper/migration/migration-state.sqlite"
    resolver_url: str = "http://127.0.0.1:8282"
    stream_readahead_bytes: int = 8388608

    # state machine budgets
    max_provider_adds_per_resolve: int = 1
    # release-size sanity (guards pack .nfo picks + mislabeled fake releases)
    min_media_movie_mb: int = 268
    min_media_episode_mb: int = 64
    resolve_candidate_timeout: float = 120.0
    torrent_ready_poll_interval: float = 8.0
    torrent_ready_max_polls: int = 10
    # health sweeper
    sweeper_items_per_hour: int = 100
    sweeper_enabled: bool = False
    sweeper_shadow_mode: bool = True
    sweeper_upgrade_enabled: bool = False
    sweeper_upgrade_min_score_delta: float = 5.0
    sweeper_min_source_age_s: float = 3600.0
    sweeper_cooldown_repair_s: float = 3600.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            resolver_bind=os.environ.get("RESOLVER_BIND", cls.resolver_bind),
            debug=_bool("DEBUG", False),
            log_level=os.environ.get("LOG_LEVEL", cls.log_level),
            torbox_api_token=(
                os.environ.get("TORBOX_API_TOKEN", "").strip()
                or _read_secret_file(os.environ.get("TORBOX_API_TOKEN_FILE", ""))),
            torbox_base_url=os.environ.get("TORBOX_BASE_URL", cls.torbox_base_url),
            scraper_torrentio_base=os.environ.get(
                "SCRAPER_TORRENTIO_BASE", cls.scraper_torrentio_base
            ),
            db_path=os.environ.get("DB_PATH", cls.db_path),
            config_dir=os.environ.get("CONFIG_DIR", cls.config_dir),
            cache_candidates_ttl=_int("CACHE_CANDIDATES_TTL", cls.cache_candidates_ttl),
            cache_checkcached_ttl=_int("CACHE_CHECKCACHED_TTL", cls.cache_checkcached_ttl),
            cache_link_ttl=_int("CACHE_LINK_TTL", cls.cache_link_ttl),
            cache_bad_ttl=_int("CACHE_BAD_TTL", cls.cache_bad_ttl),
            cache_bad_ttl_max=_int("CACHE_BAD_TTL_MAX", cls.cache_bad_ttl_max),
            torbox_timeout_connect=float(
                os.environ.get("TORBOX_TIMEOUT_CONNECT", cls.torbox_timeout_connect)
            ),
            torbox_timeout_read=float(os.environ.get("TORBOX_TIMEOUT_READ", cls.torbox_timeout_read)),
            torbox_max_retries=_int("TORBOX_MAX_RETRIES", cls.torbox_max_retries),
            upstream_concurrency=_int("UPSTREAM_CONCURRENCY", cls.upstream_concurrency),
            validation_probe_bytes=_int("VALIDATION_PROBE_BYTES", cls.validation_probe_bytes),
            vfs_mountpoint=os.environ.get("VFS_MOUNTPOINT", cls.vfs_mountpoint),
            mig_db_path=os.environ.get("MIG_DB", cls.mig_db_path),
            resolver_url=os.environ.get("RESOLVER_URL", cls.resolver_url),
            stream_readahead_bytes=_int("STREAM_READAHEAD_BYTES", cls.stream_readahead_bytes),
            torrent_ready_poll_interval=float(
                os.environ.get("TORBOX_POLL_INTERVAL", cls.torrent_ready_poll_interval)
            ),
            sweeper_items_per_hour=_int("SWEEPER_ITEMS_PER_HOUR", cls.sweeper_items_per_hour),
            sweeper_enabled=_bool("SWEEPER_ENABLED", cls.sweeper_enabled),
            sweeper_shadow_mode=_bool("SWEEPER_SHADOW_MODE", cls.sweeper_shadow_mode),
            sweeper_upgrade_enabled=_bool("SWEEPER_UPGRADE_ENABLED", cls.sweeper_upgrade_enabled),
            sweeper_upgrade_min_score_delta=float(
                os.environ.get("SWEEPER_UPGRADE_MIN_DELTA", cls.sweeper_upgrade_min_score_delta)),
            sweeper_min_source_age_s=float(
                os.environ.get("SWEEPER_MIN_SOURCE_AGE_S", cls.sweeper_min_source_age_s)),
            sweeper_cooldown_repair_s=float(
                os.environ.get("SWEEPER_COOLDOWN_REPAIR_S", cls.sweeper_cooldown_repair_s)),
            torrent_ready_max_polls=_int("TORBOX_MAX_POLLS", cls.torrent_ready_max_polls),
            max_provider_adds_per_resolve=_int(
                "MAX_PROVIDER_ADDS_PER_RESOLVE", cls.max_provider_adds_per_resolve),
            min_media_movie_mb=_int("MIN_MEDIA_MOVIE_MB", cls.min_media_movie_mb),
            min_media_episode_mb=_int("MIN_MEDIA_EPISODE_MB", cls.min_media_episode_mb),
        )
