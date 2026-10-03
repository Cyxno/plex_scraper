"""Control/observability API (FASE 16) + internal session/stream endpoints.

External (documented):  /health /status /media...
Internal (used by vfs): /media/{id}/open, /stream/{handle}, /open/{handle}
Debug (DEBUG=true only): /debug/sources/{id}/fail, /debug/media/{id}/fail-current
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import time

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from plex_scraper import __version__
from plex_scraper.common.domain import models as m
from plex_scraper.resolver.engine import Resolver, UnresolvedError
from .schemas import item_out, source_out

START_TIME = time.time()


def process_rss_kb() -> int:
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return -1


def process_cpu_seconds() -> float:
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_utime + resource.getrusage(resource.RUSAGE_SELF).ru_stime


def create_app(resolver: Resolver, settings) -> FastAPI:
    app = FastAPI(title="plex_scraper resolver", version=__version__, docs_url=None, redoc_url=None)

    # health sweeper (optional, disabled by default)
    sweeper_task = None
    if getattr(settings, "sweeper_enabled", False):
        from plex_scraper.resolver.health import HealthSweeper
        db_dir = os.path.dirname(settings.db_path) or "/data"
        sweeper = HealthSweeper(
            resolver,
            db_path=os.path.join(db_dir, "health-sweeper.sqlite"),
            items_per_hour=getattr(settings, "sweeper_items_per_hour", 100),
            upgrade_enabled=getattr(settings, "sweeper_upgrade_enabled", False),
            upgrade_min_score_delta=getattr(settings, "sweeper_upgrade_min_score_delta", 5.0),
            shadow_mode=getattr(settings, "sweeper_shadow_mode", True),
            max_repairs_per_item_per_day=getattr(
                settings, "sweeper_max_repairs_per_item_per_day", 3),
            cooldown_after_repair_s=getattr(settings, "sweeper_cooldown_repair_s", 3600.0),
            min_source_age_s=getattr(settings, "sweeper_min_source_age_s", 3600.0),
            no_source_base_s=getattr(settings, "sweeper_no_source_base_s", 3600.0),
            no_source_max_s=getattr(settings, "sweeper_no_source_max_s", 86400.0),
            fail_strikes=getattr(settings, "sweeper_fail_strikes", 2),
            playback_pause=getattr(settings, "sweeper_playback_pause", True),
            playback_min_mbit=getattr(settings, "playback_min_mbit", 25.0),
            throughput_margin=getattr(settings, "sweeper_throughput_margin", 1.5),
            throughput_strikes=getattr(settings, "sweeper_throughput_strikes", 3),
            throughput_probe_interval_s=getattr(
                settings, "sweeper_throughput_probe_interval_s", 3600.0),
        )

        @app.on_event("startup")
        async def _start_sweeper():
            nonlocal sweeper_task
            if getattr(settings, "sweeper_autostart", True):
                sweeper_task = asyncio.get_event_loop().create_task(sweeper.run())

        @app.on_event("shutdown")
        async def _stop_sweeper():
            sweeper.stop()
            if sweeper_task:
                sweeper_task.cancel()

        resolver._sweeper = sweeper
        app.state.sweeper = sweeper

    @app.exception_handler(KeyError)
    async def _not_found(_req: Request, exc: KeyError):
        return JSONResponse(status_code=404, content={"error": str(exc)})

    @app.exception_handler(UnresolvedError)
    async def _unresolved(_req: Request, exc: UnresolvedError):
        return JSONResponse(status_code=503, content={"error": str(exc)})

    from plex_scraper.scraper.providers.base import ProviderError

    @app.exception_handler(ProviderError)
    async def _provider(_req: Request, exc: ProviderError):
        return JSONResponse(status_code=502, content={"error": str(exc)})

    # ------------------------------------------------------------- health
    @app.get("/health")
    async def health():
        result: dict = {"status": "ok", "version": __version__,
                        "uptime_s": round(time.time() - START_TIME, 1)}
        sweeper = app.state.sweeper if hasattr(app.state, "sweeper") else None
        if sweeper:
            result["sweeper"] = {"running": sweeper._running,
                                 "shadow_mode": sweeper.shadow_mode}
        return result

    # --------------------------------------------------- self-healing API
    @app.get("/api/jit/status")
    async def jit_status():
        """FASE 26/28: JIT-state + metrics voor GUI/dashboard."""
        jit = resolver.jit
        return {"enabled": jit.cfg.enabled,
                "preflight_min_mbit": jit.cfg.preflight_min_mbit,
                "bands": {"fast_ratio": jit.cfg.fast_ratio,
                          "degraded_ratio": jit.cfg.degraded_ratio},
                "allow_minor_deviation": jit.cfg.allow_minor_deviation,
                "allow_quality_downgrade": jit.cfg.allow_quality_downgrade,
                "metrics": dict(sorted(jit.metrics.items())),
                "cached_decisions": {k: {"band": v[1].band,
                                         "mbit": v[1].measured_mbit,
                                         "expires_in_s": round(v[0] - time.time())}
                                     for k, v in jit._cache.items()},
                "inflight": sorted(jit._inflight)}

    @app.get("/api/playback/active")
    async def playback_active():
        """Sessies met recente reads = actieve playback (voor sweeper-pauze)."""
        cutoff = time.time() - 90.0
        out = []
        for handle, ctx in resolver.sessions.items():
            last = ctx.last_read_at or 0
            if last >= cutoff:
                out.append({"handle": handle,
                            "item_id": ctx.session.media_item_id,
                            "read_count": ctx.session.read_count,
                            "size": ctx.session.size,
                            "idle_s": round(time.time() - last, 1),
                            "read_mode": "2way" if getattr(ctx.reader, "two_way", False) else "single",
                            "required_mbit": getattr(ctx, "required_mbit", 0.0),
                            "delivery": (ctx.monitor.snapshot() if ctx.monitor else None),
                            "hot_spare": resolver.jit._hot_spares.get(ctx.session.media_item_id)})
        return {"active": len(out), "streams": out,
                "threshold_s": 90}

    @app.get("/api/selfheal/status")
    async def selfheal_status():
        sweeper = app.state.sweeper if hasattr(app.state, "sweeper") else None
        if sweeper is None:
            return {"enabled": False}
        c = sqlite3.connect(sweeper.db_path)
        evs = []
        counts: dict[str, int] = {}
        if c:
            try:
                c.row_factory = sqlite3.Row
                evs = [dict(r) for r in c.execute(
                    "SELECT * FROM health_events ORDER BY id DESC LIMIT 20").fetchall()]
                counts = {r["event"]: r["n"] for r in c.execute(
                    "SELECT event, COUNT(*) n FROM health_events WHERE ts >= ? "
                    "GROUP BY event ORDER BY n DESC",
                    (time.time() - 86400,)).fetchall()}
            except Exception:
                pass
            c.close()
        return {"enabled": True, "shadow_mode": sweeper.shadow_mode,
                "upgrade_enabled": sweeper.upgrade_enabled,
                "items_per_hour": sweeper.items_per_hour,
                "no_source_tracked": len(sweeper.no_source_retry._fail_count),
                "playback_pause": sweeper.playback_pause,
                "active_playback": await sweeper.playback_active_count(),
                "degraded_throughput": sorted(sweeper.degraded_throughput),
                "health_states": {
                    "HEALTHY_FOR_MEDIA": sum(
                        1 for v in sweeper._tp_state.values() if v == "HEALTHY_FOR_MEDIA"),
                    "DEGRADED_THROUGHPUT": len(sweeper._degraded),
                    "BROKEN": sum(1 for v in sweeper._tp_state.values() if v == "BROKEN")},
                "counts_24h": counts,
                "events": evs}

    @app.post("/api/selfheal/check-now")
    async def selfheal_check_now():
        sweeper = app.state.sweeper if hasattr(app.state, "sweeper") else None
        if sweeper is None:
            return {"error": "sweeper not enabled"}
        await sweeper.sweep()
        return {"result": "sweep complete"}

    @app.post("/api/selfheal/check-item/{item_id}")
    async def selfheal_check_item(item_id: str):
        """Gerichte health-check + (auto-mode) repair van één item."""
        sweeper = app.state.sweeper if hasattr(app.state, "sweeper") else None
        if sweeper is None:
            return {"error": "sweeper not enabled"}
        item = await resolver.store.get_item(item_id)
        if item is None:
            raise KeyError(f"unknown media item {item_id}")
        result = await sweeper.check_source(item)
        out = {"check": result}
        if item.status == "NO_SOURCE":
            out["no_source"] = await sweeper.handle_no_source(item)
            fresh = await resolver.store.get_item(item_id)
            out["status_now"] = fresh.status if fresh else None
        elif result.get("repair_needed"):
            if sweeper.shadow_mode:
                shadow = await sweeper._shadow_evaluate(item)
                out["shadow"] = shadow
            else:
                out["repair_done"] = await sweeper._repair(item)
        return out

    @app.get("/status")
    async def status():
        items = await resolver.store.list_items()
        by_status: dict[str, int] = {}
        for it in items:
            by_status[it.status] = by_status.get(it.status, 0) + 1
        r = resolver.metrics
        req = r["request_count"] or 1
        return {
            "version": __version__,
            "uptime_s": round(time.time() - START_TIME, 1),
            "items": {"total": len(items), "by_status": by_status},
            "sessions_open": len(resolver.sessions),
            "resolutions": r["resolutions"],
            "generation_switches": r["generation_switches"],
            "resolve_latency_avg_s": round(r["resolve_latency_sum"] / max(1, r["resolutions"]), 3),
            "request_latency_avg_ms": round(1000 * r["request_latency_sum"] / req, 2),
            "reads": r["reads"],
            "read_bytes": r["read_bytes"],
            "caches": resolver.caches.stats(),
            "adaptive": {k: resolver.metrics[k] for k in
                         ("prefetch_bytes", "prefetch_cancelled_bytes",
                          "prefetch_hits", "prefetch_errors",
                          "adaptive_fallbacks", "two_way_sessions")},
            "resources": {"rss_kb": process_rss_kb(), "cpu_seconds": round(process_cpu_seconds(), 3)},
            "recent_events": (await resolver.store.recent_events(20))[::-1],
        }

    # -------------------------------------------------------------- media
    @app.get("/media")
    async def list_media():
        out = []
        for i in await resolver.store.list_items():
            view = item_out(i)
            active = await resolver._active_source(i.id)
            view["size"] = active.size if active else 0
            out.append(view)
        return out

    @app.post("/media", status_code=201)
    async def register_media(payload: dict, response: Response):
        required = ("plex_path",)
        missing = [k for k in required if not payload.get(k)]
        if missing:
            return JSONResponse(status_code=422, content={"error": f"missing {missing}"})
        try:
            item = await resolver.register_item(payload)
        except ValueError as exc:
            return JSONResponse(status_code=422, content={"error": str(exc)})
        response.headers["Location"] = f"/media/{item.id}"
        return item_out(item)

    @app.get("/media/{item_id}")
    async def get_media(item_id: str):
        item = await resolver.store.get_item(item_id)
        if item is None:
            raise KeyError(f"unknown media item {item_id}")
        out = item_out(item)
        out["sources"] = [source_out(s) for s in await resolver.store.list_sources(item_id)]
        return out

    @app.patch("/media/{item_id}")
    async def patch_media(item_id: str, payload: dict):
        item = await resolver.update_desired(item_id, payload.get("desired") or {})
        if item is None:
            raise KeyError(f"unknown media item {item_id}")
        # FASE 2: media-aware bitrate/kENNIS kunnen per item gezet worden
        for key in ("duration_s", "media_bitrate_mbit"):
            if key in payload and payload[key] is not None:
                setattr(item, key, float(payload[key]))
        if any(key in payload for key in ("duration_s", "media_bitrate_mbit")):
            await resolver.store.update_item(item)
        return item_out(item)

    @app.get("/media/{item_id}/throughput")
    async def media_throughput(item_id: str):
        """FASE 19: media-aware throughput-weergave voor één item."""
        item = await resolver.store.get_item(item_id)
        if item is None:
            raise KeyError(f"unknown media item {item_id}")
        active = await resolver._active_source(item_id)
        size = active.size if active else 0
        profile = resolver.media_profile(item, size)
        required = profile.required_mbit(resolver.s.sweeper_throughput_margin)
        sweeper = getattr(resolver, "_sweeper", None)
        state, samples = "UNKNOWN", []
        if sweeper is not None:
            state = sweeper.throughput_state(item.plex_path)
            samples = sweeper.throughput_samples(item.plex_path)
        # read-mode zoals de sessies die nu openstaan hem kiezen
        two_way = resolver.two_way_for(item, required)
        return {"item_id": item_id,
                "title": item.title,
                "media_bitrate_mbit": round(profile.bitrate_mbit, 1),
                "bitrate_confidence": profile.confidence,
                "duration_s": item.duration_s,
                "size_bytes": size,
                "required_mbit": round(required, 1),
                "state": state,
                "read_mode": "ADAPTIVE_2WAY" if two_way else "SINGLE",
                "samples": samples}

    @app.delete("/media/{item_id}")
    async def delete_media(item_id: str):
        if not await resolver.delete_item(item_id):
            raise KeyError(f"unknown media item {item_id}")
        return {"deleted": item_id}

    @app.get("/media/{item_id}/sources")
    async def media_sources(item_id: str):
        if await resolver.store.get_item(item_id) is None:
            raise KeyError(f"unknown media item {item_id}")
        return [source_out(s) for s in await resolver.store.list_sources(item_id)]

    @app.post("/media/{item_id}/resolve")
    async def resolve_media(item_id: str):
        item = await resolver.store.get_item(item_id)
        if item is None:
            raise KeyError(f"unknown media item {item_id}")
        resolver.caches.candidates.invalidate(
            f"{item.kind}:{item.imdb_id}:{item.season}:{item.episode}")
        source = await resolver.resolve_item(item, reason="forced")
        return {"item": item_out(item),
                "resolved": source_out(source) if source else None}

    # ------------------------------------------- internal session/stream
    @app.post("/media/{item_id}/open")
    async def open_media(item_id: str, two_way: int | None = None):
        # two_way=0: health-checks/achtergrond willen géén prefetch
        ctx = await resolver.open_handle(
            item_id, two_way=None if two_way is None else bool(two_way))
        jd = getattr(ctx, "jit_decision", None)
        return {"handle": ctx.session.handle, "size": ctx.session.size,
                "generation": ctx.session.generation,
                "read_mode": "2way" if getattr(ctx.reader, "two_way", False) else "single",
                "required_mbit": getattr(ctx, "required_mbit", 0.0),
                "jit": None if jd is None else {
                    "band": jd.band, "measured_mbit": jd.measured_mbit,
                    "ttfb_s": jd.ttfb_s, "switched": jd.switched,
                    "switched_to": jd.switched_to, "note": jd.note},
                "source": source_out(ctx.source)}

    @app.get("/stream/{handle}")
    async def stream(handle: str, offset: int = 0, length: int = 262144):
        data = await resolver.read(handle, offset, length)
        return Response(content=data, media_type="application/octet-stream")

    @app.delete("/open/{handle}")
    async def close_open(handle: str):
        await resolver.release(handle)
        return {"closed": handle}

    # -------------------------------------------------------------- debug
    def _debug_guard():
        if not settings.debug:
            return JSONResponse(status_code=404, content={"error": "not found"})
        return None

    @app.post("/debug/sources/{source_id}/fail")
    async def debug_fail_source(source_id: str):
        if (resp := _debug_guard()) is not None:
            return resp
        if not await resolver.fail_source(source_id):
            raise KeyError(f"unknown source {source_id}")
        return {"failed": source_id}

    @app.post("/debug/media/{item_id}/fail-current")
    async def debug_fail_current(item_id: str):
        if (resp := _debug_guard()) is not None:
            return resp
        if not await resolver.fail_current(item_id):
            raise KeyError(f"no active source for {item_id}")
        return {"failed_current_source_of": item_id}

    return app
