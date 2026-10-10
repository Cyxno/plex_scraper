"""Gededupliceerde, gebonden Plex library-refresh na geslaagde ingest.

Bewezen gat (MobLand S02E04, 2026-10-09): de hele ingest-keten (Sonarr →
ingest → resolver READY → canonical .ids → stable symlink → plex-namespace
leesprobe) slaagde, maar de library-scan was een fire-and-forget waarvan een
stil falen (docker-exec retourneert {"error": ...} i.p.v. een exception)
nooit zichtbaar werd — Plex kende de episode niet tot een handmatige scan.

Contract:
  * refresh alleen nádat de stable symlink bestaat én leesbaar is gebleken
    (de aanroeper garandeert dat; de coordinator checkt het pad nog eens);
  * debounce/batch: meerdere ingests binnen het debouncvenster (~45 s)
    voor dezelfde section → één refresh (één path-scan, anders section-scan);
  * geen server-wide scan: hoogstens section-scope;
  * bounded retry bij tijdelijke onbereikbaarheid van Plex;
  * een faalende scan keert NOOIT terug in de ingest-status: READY blijft
    READY — alleen observability (plex_scan_failed).
"""
from __future__ import annotations

import asyncio
import os
import time

# toestanden (observerbaar in trace/cockpit)
QUEUED = "plex_scan_queued"
TRIGGERED = "plex_scan_triggered"
SUCCEEDED = "plex_scan_succeeded"
FAILED = "plex_scan_failed"


class PlexScanCoordinator:
    def __init__(self, plex_client, store_event=None,
                 debounce_s: float = 45.0, retry_max: int = 3,
                 retry_backoff_s: float = 15.0):
        # plex_client mag een object of een zero-arg callable zijn (de bridge
        # kan de client runtime vervangen — tests/productie-hotswap)
        self.plex = plex_client
        self._emit = store_event or (lambda kind, **kw: None)
        self.debounce_s = debounce_s
        self.retry_max = max(1, retry_max)
        self.retry_backoff_s = retry_backoff_s
        # section → {path-set}; laatste wint bij coalesce
        self._pending: dict[int, set[str]] = {}
        self._item_ids: list[str] = []
        self._part_files: dict[str, str] = {}      # item_id → plex-part-pad
        self._timer: asyncio.Task | None = None
        self._worker: asyncio.Task | None = None
        self._last_scan_s: dict[int, float] = {}
        self.metrics = {"queued": 0, "deduped": 0, "triggered": 0,
                        "succeeded": 0, "failed": 0}

    # -------------------------------------------------------------- request
    async def request(self, section: int, path: str, item_id: str = "",
                      kind: str = "", link_path: str | None = None,
                      part_file: str | None = None) -> bool:
        """Nadat symlink + leesprobe OK zijn: plan een section-refresh.
        Retourneert direct — een scanfaal kan de ingest-flow nooit blokkeren.
        Als link_path is meegegeven en daar staat géén symlink wordt er
        géén refresh gepland (lexists: een verse link mag nog dangling zijn —
        de leesprobe in de plex-container is het readability-bewijs)."""
        if link_path and not os.path.lexists(link_path):
            self._emit("plex_scan_skipped", section=int(section), path=path,
                       item_id=item_id, media_kind=kind,
                       reason="symlink_bestaat_niet")
            return False
        if item_id and part_file:
            self._part_files[item_id] = part_file
        paths = self._pending.setdefault(int(section), set())
        if path in paths and self._timer is not None:
            self.metrics["deduped"] += 1
            self._emit(QUEUED, section=int(section), path=path,
                       item_id=item_id, media_kind=kind, deduped=True)
            return True
        paths.add(path)
        if item_id:
            self._item_ids.append(item_id)
        self.metrics["queued"] += 1
        self._emit(QUEUED, section=int(section), path=path,
                   item_id=item_id, media_kind=kind, deduped=False)
        if self._timer is not None:
            self._timer.cancel()
        self._timer = asyncio.get_event_loop().create_task(self._debounce())
        return True

    async def _debounce(self) -> None:
        try:
            await asyncio.sleep(self.debounce_s)
        except asyncio.CancelledError:
            return
        self._timer = None
        pending = self._pending
        items = self._item_ids
        self._pending = {}
        self._item_ids = []
        self._worker = asyncio.get_event_loop().create_task(
            self._flush(pending, items))

    async def _flush(self, pending: dict[int, set[str]],
                     item_ids: list[str]) -> None:
        for section, paths in pending.items():
            # laatste succesvolle scan op deze section binnen het venster?
            # → alsnog één verse scan (nieuwe items zijn binnen gekomen)
            if len(paths) == 1:
                scan_path = next(iter(paths))
            else:
                scan_path = None            # section-scope: batch van paths
            self._emit(TRIGGERED, section=int(section), path=scan_path,
                       item_ids=item_ids[:8], batch=len(paths))
            self.metrics["triggered"] += 1
            ok = False
            last_err = ""
            client = self.plex() if callable(self.plex) else self.plex
            for attempt in range(1, self.retry_max + 1):
                try:
                    res = await client.scan_section(section, scan_path)
                except Exception as exc:            # noqa: BLE001
                    res = {"ok": False, "error": repr(exc)[:160]}
                # docker-exec-fouten komen als {"error": ...} binnen — dat is
                # óók een faal (bewezen stil-faal-venster MobLand)
                if res.get("ok") or (res.get("status") == 200
                                     and not res.get("error")):
                    ok = True
                    break
                last_err = str(res.get("error") or res)[:160]
                if attempt < self.retry_max:
                    await asyncio.sleep(self.retry_backoff_s * (2 ** (attempt - 1)))
            self._last_scan_s[section] = time.time()
            if ok:
                self.metrics["succeeded"] += 1
                self._emit(SUCCEEDED, section=int(section), path=scan_path,
                           item_ids=item_ids[:8])
                # observability voor de eerstvolgende echte import: ratingKey
                # per item opzoeken (read-only DB) en rapporteren — non-fatal
                await self._report_rating_keys(item_ids)
            else:
                self.metrics["failed"] += 1
                self._emit(FAILED, section=int(section), path=scan_path,
                           item_ids=item_ids[:8], error=last_err,
                           attempts=self.retry_max)

    async def _report_rating_keys(self, item_ids: list[str]) -> None:
        """Observability-only: na een geslaagde scan het Plex-item per item
        opzoeken (read-only) en als event rapporteren. Faalt dit, dan is dat
        een waarneming — het raakt de ingest-state nooit."""
        client = self.plex() if callable(self.plex) else self.plex
        lookup = getattr(client, "find_rating_key_via_db", None)
        if lookup is None:
            return
        for iid in item_ids:
            part = self._part_files.get(iid)
            if not part:
                self._emit("plex_scan_ratingkey_found", item_id=iid,
                           rating_key=None, reason="geen part-pad bekend")
                continue
            try:
                rk = await lookup(part)
            except Exception as exc:                # noqa: BLE001
                self._emit("plex_scan_ratingkey_found", item_id=iid,
                           rating_key=None, error=repr(exc)[:120])
                continue
            self._emit("plex_scan_ratingkey_found", item_id=iid,
                       rating_key=rk, part=part[:120])
            if rk:
                self._part_files.pop(iid, None)

    # ---------------------------------------------------------- lifecycle
    async def flush_now(self) -> None:
        """Test/onderhoud: debounce-venster overslaan en pending direct flushen."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        pending = self._pending
        items = self._item_ids
        self._pending = {}
        self._item_ids = []
        if pending:
            await self._flush(pending, items)

    def stop(self) -> None:
        for t in (self._timer, self._worker):
            if t is not None:
                t.cancel()
        self._timer = self._worker = None

    def snapshot(self) -> dict:
        return {"metrics": dict(self.metrics),
                "pending": {str(k): sorted(v) for k, v in self._pending.items()}}
