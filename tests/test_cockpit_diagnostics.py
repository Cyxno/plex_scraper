"""Regressie: Diagnostics/Inventory-rendering in cockpit.html.

Extraheert diagnosticsHtml + inventoryHtml letterlijk uit de template en
draait asserts onder Node, zodat template-semantiek en tests nooit uit
elkaar kunnen groeien:
  * Diagnostics bevat alleen live-signalen (geen coverage/legacy/snapshot);
  * Coverage snapshot staat in Inventory, muted, zonder percentage/status;
  * "recount pending" bestaat niet — alleen een echte draaiende audit-job;
  * een historische INTERRUPTED sweep krijgt géén alarmkleur en wijkt voor
    een nieuwere SUCCESS.
"""
from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

TEMPLATE = (Path(__file__).resolve().parents[1]
            / "src" / "plex_scraper" / "web" / "templates" / "cockpit.html")

JS_ASSERTS = r"""
const assert = require('node:assert/strict');

const NOW = 1791397248;
const base = (over) => Object.assign({
  physical: {status: 'HEALTHY', checked_at: NOW - 60},
  provider_availability: {state: 'HEALTHY', blocked: false, retry_in_s: 0,
                          last_success_at: NOW - 30},
  sweeper: {last_runs: [{id: 1, status: 'SUCCESS', started_at: NOW - 600,
                         finished_at: NOW - 500, processed: 12,
                         recovered: 1}]},
  ingest: {enabled: true, counts_by_state: {QUEUED: 2, RESOLVING: 1},
           provider: {blocked: []}},
  coverage: {managed: 1955, logical_total: 2022, age_s: 44 * 3600},
  now: {jobs_running: []},
}, over);

// ---- 1) Diagnostics = alleen live-signalen -------------------------------
const dg = diagnosticsHtml(base());
assert.match(dg, /Physical library/);
assert.match(dg, /Provider availability/);
assert.match(dg, /Last sweep/);
assert.match(dg, /Ingest queue/);
assert.ok(!/Coverage/i.test(dg), 'coverage hoort niet in Diagnostics');
assert.ok(!/Legacy audit/i.test(dg), 'legacy audit hoort niet in Diagnostics');
assert.ok(!/snapshot/i.test(dg), 'geen snapshot-status in Diagnostics');
assert.ok(!/not live health/i.test(dg), 'geen snapshot-disclaimer in Diagnostics');
assert.ok(!/recount pending/i.test(dg), 'geen fictieve recount-pending');

// ---- 2) Inventory = historisch, muted, zonder health-kleur ---------------
const inv = inventoryHtml(base());
assert.match(inv, /Coverage snapshot/);
assert.match(inv, /1,955 \/ 2,022/);
assert.match(inv, /informational only/);
assert.match(inv, /Legacy audit/);
assert.match(inv, /no current recount/);
assert.ok(!/%/.test(inv), 'geen percentage als live health');
assert.ok(!/diaglist"/.test(inv.replace('diaglist inv', '')) ?
           true : inv.includes('diaglist inv'),
           'inventory gebruikt de muted inv-lijst');
assert.ok(!/st-dot err/.test(inv) && !/st-dot warn/.test(inv),
          'geen health-kleuren in Inventory');

// ---- 3) recount alléén bij een ECHTE draaiende job -----------------------
assert.ok(!/recount running/i.test(inventoryHtml(base())));
const withJob = inventoryHtml(base({now: {jobs_running: [
  {id: 9, job_type: 'library_audit'}]}}));
assert.match(withJob, /recount running/);
assert.ok(!/recount pending/i.test(withJob), 'nooit de tekst recount pending');

// ---- 4) Last sweep: nieuwe SUCCESS wint van oude INTERRUPTED -------------
const recovered = diagnosticsHtml(base({sweeper: {last_runs: [
  {id: 2, status: 'INTERRUPTED', started_at: NOW - 100, processed: 3},
  {id: 1, status: 'SUCCESS', started_at: NOW - 3600, finished_at: NOW - 3500,
   processed: 42}]}}));
assert.match(recovered, /SUCCESS/);
assert.match(recovered, /na interrupted run/);
assert.ok(!/INTERRUPTED/.test(recovered), 'oude interrupted run verbergen');

// historische interrupted zónder nieuwere success: neutraal, geen alarmkleur
const historic = diagnosticsHtml(base({sweeper: {last_runs: [
  {id: 2, status: 'INTERRUPTED', started_at: NOW - 100, processed: 3}]}}));
assert.match(historic, /INTERRUPTED/);
assert.match(historic, /historisch/);
const sweepRow = historic.split('Last sweep')[1] || '';
assert.ok(!/st-dot (err|warn)/.test(sweepRow),
          'geen alarmkleur voor historische interrupted run');

// RUNNende sweep toont RUNNING
const running = diagnosticsHtml(base({sweeper: {last_runs: [
  {id: 3, status: 'RUNNING', started_at: NOW - 30}]}}));
assert.match(running, /RUNNING/);

// ---- 5) Provider-blackout is een live Diagnostics-signaal ----------------
const blocked = diagnosticsHtml(base({provider_availability: {
  state: 'DAILY_LIMIT', blocked: true, retry_in_s: 3600,
  last_success_at: NOW - 7200}}));
assert.match(blocked, /DAILY_LIMIT/);
assert.match(blocked, /cooldown/);
assert.match(blocked, /st-dot err/, 'DAILY_LIMIT = rood in Diagnostics');
"""

STUBS = r"""
// stubs voor template-helpers die buiten het geëxtraheerde blok staan
const esc = (v) => String(v == null ? '' : v)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const DOT = (cls='') => `<span class="st-dot ${cls}"></span>`;
const badge = (t,c)=>`<span class="badge b-${esc(c||t)}">${esc(t)}</span>`;
const fmtIn = s => s < 60 ? `${s}s` : s < 3600 ? `${Math.round(s/60)}m` : `${Math.round(s/3600)}h`;
"""


def _extract_render_fns() -> str:
    html = TEMPLATE.read_text(encoding="utf-8")
    start = html.index("function diagnosticsHtml")
    end = html.index("function overviewHtml")
    return html[start:end]


def test_diagnostics_and_inventory_rendering():
    html = TEMPLATE.read_text(encoding="utf-8")
    fmt = re.search(r"/\* UI-FMT-BEGIN[^*]*\*/(.*?)/\* UI-FMT-END \*/",
                    html, re.S)
    assert fmt, "UI-FMT-blok ontbreekt in cockpit.html"
    code = (fmt.group(1) + "\n" + STUBS + "\n" + _extract_render_fns()
            + "\n" + JS_ASSERTS)
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "diag_test.cjs"
        path.write_text(code, encoding="utf-8")
        proc = subprocess.run(["node", str(path)], capture_output=True,
                              text=True, timeout=60)
    assert proc.returncode == 0, f"node faalde:\n{proc.stdout}\n{proc.stderr}"
