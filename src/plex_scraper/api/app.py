"""Control/observability API (FASE 16) + internal session/stream endpoints.

External (documented):  /health /status /media...
Internal (used by vfs): /media/{id}/open, /stream/{handle}, /open/{handle}
Debug (DEBUG=true only): /debug/sources/{id}/fail, /debug/media/{id}/fail-current
"""
from __future__ import annotations

import time

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from .. import __version__
from ..domain import models as m
from ..resolver.engine import Resolver, UnresolvedError
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

    @app.exception_handler(KeyError)
    async def _not_found(_req: Request, exc: KeyError):
        return JSONResponse(status_code=404, content={"error": str(exc)})

    @app.exception_handler(UnresolvedError)
    async def _unresolved(_req: Request, exc: UnresolvedError):
        return JSONResponse(status_code=503, content={"error": str(exc)})

    from ..providers.base import ProviderError

    @app.exception_handler(ProviderError)
    async def _provider(_req: Request, exc: ProviderError):
        return JSONResponse(status_code=502, content={"error": str(exc)})

    # ------------------------------------------------------------- health
    @app.get("/health")
    async def health():
        return {"status": "ok", "version": __version__,
                "uptime_s": round(time.time() - START_TIME, 1)}

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
        return item_out(item)

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
    async def open_media(item_id: str):
        ctx = await resolver.open_handle(item_id)
        return {"handle": ctx.session.handle, "size": ctx.session.size,
                "generation": ctx.session.generation,
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
