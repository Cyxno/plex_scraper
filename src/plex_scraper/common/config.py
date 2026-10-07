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
        # kale stream-endpoint — de providers=-gefilterde addon-variant wordt
        # door Torrentio streng per-IP gelimiteerd (429 vrijwel altijd), de
        # kale URL niet (ingest-hardening 2026-10-06, empirisch bewezen)
        "https://torrentio.strem.fun"
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
    # TorBox-cap is 60 uncached adds/uur account-breed; 3 begrensde adds per
    # resolve houdt geldige alternatieven bereikbaar zonder die cap te raken
    # (cached candidates verbruiken nooit budget).
    max_provider_adds_per_resolve: int = 3
    # release-size sanity (guards pack .nfo picks + mislabeled fake releases)
    min_media_movie_mb: int = 268
    min_media_episode_mb: int = 64
    resolve_candidate_timeout: float = 120.0
    torrent_ready_poll_interval: float = 8.0
    torrent_ready_max_polls: int = 10
    # health sweeper
    sweeper_items_per_hour: int = 100
    sweeper_enabled: bool = False
    sweeper_autostart: bool = True
    sweeper_shadow_mode: bool = True
    sweeper_upgrade_enabled: bool = False
    sweeper_upgrade_min_score_delta: float = 5.0
    sweeper_min_source_age_s: float = 3600.0
    sweeper_cooldown_repair_s: float = 3600.0
    sweeper_max_repairs_per_item_per_day: int = 3
    sweeper_no_source_base_s: float = 3600.0
    sweeper_no_source_max_s: float = 86400.0
    sweeper_fail_strikes: int = 2
    sweeper_playback_pause: bool = True
    sweeper_stale_reconcile_s: float = 900.0
    playback_min_mbit: float = 25.0
    sweeper_throughput_margin: float = 1.5
    sweeper_throughput_strikes: int = 3
    sweeper_throughput_probe_interval_s: float = 3600.0
    stream_two_way_enabled: bool = True
    adaptive_two_way_min_mbit: float = 40.0
    adaptive_fallback_errors: int = 2
    candidate_min_improvement: float = 1.5
    candidate_headroom: float = 1.2
    # JIT playback preflight (FASE 3-15)
    jit_enabled: bool = True
    jit_preflight_min_mbit: float = 40.0
    jit_fast_ratio: float = 1.2
    jit_degraded_ratio: float = 0.8
    jit_ttfb_max_s: float = 5.0
    jit_max_wait_s: float = 12.0
    jit_probe_candidates: int = 3
    jit_min_gain: float = 1.5
    jit_allow_minor_deviation: bool = True
    jit_allow_quality_downgrade: bool = False
    jit_delivery_bad_ttl_s: float = 3600.0
    jit_stall_s: float = 6.0
    jit_max_failovers_per_session: int = 2
    jit_reconnect_on_failover: bool = True
    jit_probe_parallel: int = 2
    jit_confirm_cached_fast: bool = True
    jit_rescue_margin: float = 1.2
    jit_startup_first_byte_s: float = 8.0
    tautulli_url: str = ""
    tautulli_apikey: str = ""

    # ------------------------------------------------------------- ingest
    # persistente arr→resolver wanted-bridge (ingest-hardening 2026-10-06)
    ingest_enabled: bool = False
    ingest_reconcile_interval_s: float = 1200.0        # 20 min (15-30 min venster)
    ingest_worker_interval_s: float = 20.0
    ingest_batch_size: int = 1                         # catch-up: bewust klein houden
    ingest_job_backoff_base_s: float = 300.0           # 5m → 10m → 20m → ... cap
    ingest_job_backoff_max_s: float = 86400.0
    ingest_job_max_attempts: int = 8
    ingest_plex_probe_retries: int = 6                  # verse .ids-node materialiseert even in de FUSE
    ingest_plex_probe_wait_s: float = 5.0
    ingest_delivery_probe_retries: int = 3
    ingest_delivery_probe_wait_s: float = 20.0
    sonarr_enabled: bool = False
    sonarr_url: str = "http://192.168.1.2:7854"
    sonarr_api_key: str = ""
    radarr_enabled: bool = False
    radarr_url: str = "http://192.168.1.2:7878"
    radarr_api_key: str = ""
    ingest_webhook_token: str = ""                     # optioneel shared secret
    plex_container: str = "plex"
    plex_section_tv: int = 2
    plex_section_movies: int = 1
    symlink_root: str = "/mnt/vm_storage/symlinks"
    # hoe de symlink-tree BINNEN de plex-container heet (autoritatieve
    # namespace voor leesprobes/scans; host-prefix verschilt!)
    plex_symlink_root: str = "/symlinks"
    canonical_root: str = "/mnt/remote/nzbdav"
    sonarr_root_map: str = "/media=TV Shows"           # arrroot=subtree
    radarr_root_map: str = "/media-movies=Movies"
    arr_reconcile_enabled: bool = True

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
            sweeper_autostart=_bool("SWEEPER_AUTOSTART", cls.sweeper_autostart),
            sweeper_shadow_mode=_bool("SWEEPER_SHADOW_MODE", cls.sweeper_shadow_mode),
            sweeper_upgrade_enabled=_bool("SWEEPER_UPGRADE_ENABLED", cls.sweeper_upgrade_enabled),
            sweeper_upgrade_min_score_delta=float(
                os.environ.get("SWEEPER_UPGRADE_MIN_DELTA", cls.sweeper_upgrade_min_score_delta)),
            sweeper_min_source_age_s=float(
                os.environ.get("SWEEPER_MIN_SOURCE_AGE_S", cls.sweeper_min_source_age_s)),
            sweeper_cooldown_repair_s=float(
                os.environ.get("SWEEPER_COOLDOWN_REPAIR_S", cls.sweeper_cooldown_repair_s)),
            sweeper_max_repairs_per_item_per_day=_int(
                "SWEEPER_MAX_REPAIRS_PER_DAY", cls.sweeper_max_repairs_per_item_per_day),
            sweeper_no_source_base_s=float(
                os.environ.get("SWEEPER_NO_SOURCE_BASE_S", cls.sweeper_no_source_base_s)),
            sweeper_no_source_max_s=float(
                os.environ.get("SWEEPER_NO_SOURCE_MAX_S", cls.sweeper_no_source_max_s)),
            sweeper_fail_strikes=_int("SWEEPER_FAIL_STRIKES", cls.sweeper_fail_strikes),
            sweeper_playback_pause=_bool("SWEEPER_PLAYBACK_PAUSE", cls.sweeper_playback_pause),
            sweeper_stale_reconcile_s=float(
                os.environ.get("SWEEPER_STALE_RECONCILE_S", cls.sweeper_stale_reconcile_s)),
            playback_min_mbit=float(os.environ.get("PLAYBACK_MIN_MBIT", cls.playback_min_mbit)),
            sweeper_throughput_margin=float(
                os.environ.get("SWEEPER_THROUGHPUT_MARGIN", cls.sweeper_throughput_margin)),
            sweeper_throughput_strikes=_int(
                "SWEEPER_THROUGHPUT_STRIKES", cls.sweeper_throughput_strikes),
            sweeper_throughput_probe_interval_s=float(os.environ.get(
                "SWEEPER_THROUGHPUT_PROBE_INTERVAL_S",
                cls.sweeper_throughput_probe_interval_s)),
            stream_two_way_enabled=_bool("STREAM_TWO_WAY_ENABLED", cls.stream_two_way_enabled),
            adaptive_two_way_min_mbit=float(os.environ.get(
                "ADAPTIVE_TWO_WAY_MIN_MBIT", cls.adaptive_two_way_min_mbit)),
            adaptive_fallback_errors=_int(
                "ADAPTIVE_FALLBACK_ERRORS", cls.adaptive_fallback_errors),
            candidate_min_improvement=float(
                os.environ.get("CANDIDATE_MIN_IMPROVEMENT", cls.candidate_min_improvement)),
            candidate_headroom=float(
                os.environ.get("CANDIDATE_HEADROOM", cls.candidate_headroom)),
            jit_enabled=_bool("JIT_ENABLED", cls.jit_enabled),
            jit_preflight_min_mbit=float(
                os.environ.get("JIT_PREFLIGHT_MIN_MBIT", cls.jit_preflight_min_mbit)),
            jit_fast_ratio=float(os.environ.get("JIT_FAST_RATIO", cls.jit_fast_ratio)),
            jit_degraded_ratio=float(
                os.environ.get("JIT_DEGRADED_RATIO", cls.jit_degraded_ratio)),
            jit_ttfb_max_s=float(os.environ.get("JIT_TTFB_MAX_S", cls.jit_ttfb_max_s)),
            jit_max_wait_s=float(os.environ.get("JIT_MAX_WAIT_S", cls.jit_max_wait_s)),
            jit_probe_candidates=_int("JIT_PROBE_CANDIDATES", cls.jit_probe_candidates),
            jit_min_gain=float(os.environ.get("JIT_MIN_GAIN", cls.jit_min_gain)),
            jit_allow_minor_deviation=_bool(
                "JIT_ALLOW_MINOR_DEVIATION", cls.jit_allow_minor_deviation),
            jit_allow_quality_downgrade=_bool(
                "JIT_ALLOW_QUALITY_DOWNGRADE", cls.jit_allow_quality_downgrade),
            jit_delivery_bad_ttl_s=float(
                os.environ.get("JIT_DELIVERY_BAD_TTL_S", cls.jit_delivery_bad_ttl_s)),
            jit_stall_s=float(os.environ.get("JIT_STALL_S", cls.jit_stall_s)),
            jit_max_failovers_per_session=_int(
                "JIT_MAX_FAILOVERS_PER_SESSION", cls.jit_max_failovers_per_session),
            jit_reconnect_on_failover=_bool(
                "JIT_RECONNECT_ON_FAILOVER", cls.jit_reconnect_on_failover),
            jit_probe_parallel=_int("JIT_PROBE_PARALLEL", cls.jit_probe_parallel),
            jit_confirm_cached_fast=_bool(
                "JIT_CONFIRM_CACHED_FAST", cls.jit_confirm_cached_fast),
            jit_rescue_margin=float(
                os.environ.get("JIT_RESCUE_MARGIN", cls.jit_rescue_margin)),
            jit_startup_first_byte_s=float(
                os.environ.get("JIT_STARTUP_FIRST_BYTE_S", cls.jit_startup_first_byte_s)),
            tautulli_url=os.environ.get("TAUTULLI_URL", cls.tautulli_url),
            tautulli_apikey=os.environ.get("TAUTULLI_APIKEY", cls.tautulli_apikey),
            torrent_ready_max_polls=_int("TORBOX_MAX_POLLS", cls.torrent_ready_max_polls),
            max_provider_adds_per_resolve=_int(
                "MAX_PROVIDER_ADDS_PER_RESOLVE", cls.max_provider_adds_per_resolve),
            min_media_movie_mb=_int("MIN_MEDIA_MOVIE_MB", cls.min_media_movie_mb),
            min_media_episode_mb=_int("MIN_MEDIA_EPISODE_MB", cls.min_media_episode_mb),
            ingest_enabled=_bool("INGEST_ENABLED", False),
            ingest_reconcile_interval_s=float(
                os.environ.get("INGEST_RECONCILE_INTERVAL_S",
                               cls.ingest_reconcile_interval_s)),
            ingest_worker_interval_s=float(
                os.environ.get("INGEST_WORKER_INTERVAL_S",
                               cls.ingest_worker_interval_s)),
            ingest_batch_size=_int("INGEST_BATCH_SIZE", cls.ingest_batch_size),
            ingest_job_backoff_base_s=float(
                os.environ.get("INGEST_JOB_BACKOFF_BASE_S",
                               cls.ingest_job_backoff_base_s)),
            ingest_job_backoff_max_s=float(
                os.environ.get("INGEST_JOB_BACKOFF_MAX_S",
                               cls.ingest_job_backoff_max_s)),
            ingest_job_max_attempts=_int(
                "INGEST_JOB_MAX_ATTEMPTS", cls.ingest_job_max_attempts),
            ingest_delivery_probe_retries=_int(
                "INGEST_DELIVERY_PROBE_RETRIES", cls.ingest_delivery_probe_retries),
            ingest_delivery_probe_wait_s=float(
                os.environ.get("INGEST_DELIVERY_PROBE_WAIT_S",
                               cls.ingest_delivery_probe_wait_s)),
            ingest_plex_probe_retries=_int(
                "INGEST_PLEX_PROBE_RETRIES", cls.ingest_plex_probe_retries),
            ingest_plex_probe_wait_s=float(
                os.environ.get("INGEST_PLEX_PROBE_WAIT_S",
                               cls.ingest_plex_probe_wait_s)),
            sonarr_enabled=_bool("SONARR_ENABLED", False),
            sonarr_url=os.environ.get("SONARR_URL", cls.sonarr_url),
            sonarr_api_key=(
                os.environ.get("SONARR_API_KEY", "").strip()
                or _read_secret_file(os.environ.get("SONARR_API_KEY_FILE", ""))),
            radarr_enabled=_bool("RADARR_ENABLED", False),
            radarr_url=os.environ.get("RADARR_URL", cls.radarr_url),
            radarr_api_key=(
                os.environ.get("RADARR_API_KEY", "").strip()
                or _read_secret_file(os.environ.get("RADARR_API_KEY_FILE", ""))),
            ingest_webhook_token=(
                os.environ.get("INGEST_WEBHOOK_TOKEN", "").strip()
                or _read_secret_file(
                    os.environ.get("INGEST_WEBHOOK_TOKEN_FILE", ""))),
            plex_container=os.environ.get("PLEX_CONTAINER", cls.plex_container),
            plex_section_tv=_int("PLEX_SECTION_TV", cls.plex_section_tv),
            plex_section_movies=_int("PLEX_SECTION_MOVIES", cls.plex_section_movies),
            symlink_root=os.environ.get("SYMLINK_ROOT", cls.symlink_root),
            plex_symlink_root=os.environ.get(
                "PLEX_SYMLINK_ROOT", cls.plex_symlink_root),
            canonical_root=os.environ.get("CANONICAL_ROOT", cls.canonical_root),
            sonarr_root_map=os.environ.get("SONARR_ROOT_MAP", cls.sonarr_root_map),
            radarr_root_map=os.environ.get("RADARR_ROOT_MAP", cls.radarr_root_map),
            arr_reconcile_enabled=_bool(
                "ARR_RECONCILE_ENABLED", cls.arr_reconcile_enabled),
        )
