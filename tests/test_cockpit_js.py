"""Regressie: de JS-formatters in cockpit.html (audit 2026-10-07).

Het gemarkeerde UI-FMT-blok in de template is de enige plek waar
relatieve tijd wordt geformatteerd. Deze test extraheert dat blok
letterlijk en draait asserts onder Node, zodat template en tests nooit
uiteen kunnen groeien:
  - timestamp seconds/ms/ISO/invalid parsing;
  - nooit 'ago ago';
  - absurde leeftijden (20733d) onmogelijk → 'timestamp unavailable';
  - duur-formatter ("25m 9s" i.p.v. "1509s").
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

TEMPLATE = (Path(__file__).resolve().parents[1]
            / "src" / "plex_scraper" / "web" / "templates" / "cockpit.html")

JS_ASSERTS = r"""
import { parseTs, ageSeconds, fmtAgeLabel, fmtAgo, fmtDur, fmtNum }
  from './fmt.mjs';
import assert from 'node:assert/strict';

const NOW = 1791397248;                       // vaste referentie, geen flakiness

// ---- timestamp parsing: seconds / ms / ISO / num-string / garbage ----
assert.equal(parseTs(NOW), NOW);
assert.equal(parseTs(NOW * 1000), NOW);                              // ms → s
assert.equal(parseTs(String(NOW)), NOW);                             // "179..."
assert.equal(parseTs(new Date(NOW * 1000).toISOString()), NOW);      // ISO
assert.equal(parseTs(0), null);
assert.equal(parseTs(''), null);
assert.equal(parseTs(null), null);
assert.equal(parseTs(true), null);
assert.equal(parseTs('garbage'), null);
assert.equal(parseTs('not-a-timestamp 123abc'), null);
assert.ok(Number.isNaN(parseTs(NaN)) === false && parseTs(NaN) === null);

// ---- nooit 'ago ago', ongeacht input ----
for (const v of [NOW - 20 * 3600, (NOW - 20 * 3600) * 1000, 73328, 0, null, 'x']) {
  const out = fmtAgo(v, NOW);
  assert.match(out, /ago|timestamp unavailable/);
  assert.ok(!/ago\s+ago/i.test(out), `dubbele ago voor ${v}: ${out}`);
}

// ---- de bug zelf: leeftijd als duur gaf '20733d ago ago' ----
assert.equal(fmtAgeLabel(73328), '20h ago');
assert.equal(fmtAgo(NOW - 73328, NOW), '20h ago');
assert.equal(fmtAgeLabel(45), '45s ago');
assert.equal(fmtAgeLabel(125), '2 min ago');
assert.equal(fmtAgeLabel(3 * 3600), '3h ago');
assert.equal(fmtAgeLabel(2 * 86400 + 3600), '2d ago');
assert.equal(fmtAgo((NOW - 90) * 1000, NOW), '2 min ago');           // ms

// ---- absurde leeftijden onmogelijk ----
assert.equal(fmtAgeLabel(20733 * 86400), 'timestamp unavailable');
assert.equal(fmtAgo(0, NOW), 'timestamp unavailable');
assert.equal(fmtAgo(NOW + 365 * 86400, NOW), 'timestamp unavailable'); // toekomst
assert.equal(fmtAgeLabel(null), 'timestamp unavailable');
assert.equal(fmtAgeLabel('garbage'), 'timestamp unavailable');

// ---- duur-formatter: '25m 9s' i.p.v. '1509s' ----
assert.equal(fmtDur(1509), '25m 9s');
assert.equal(fmtDur(42), '42s');
assert.equal(fmtDur(60), '1m');
assert.equal(fmtDur(3600), '1h');
assert.equal(fmtDur(5400), '1h 30m');
assert.equal(fmtDur(90000), '1d 1h');
assert.equal(fmtDur(null), '–');

// ---- getallen ----
assert.equal(fmtNum(2038), '2,038');
assert.equal(fmtNum(0), '0');
assert.equal(fmtNum(null), '–');

console.log('ui-fmt ok');
"""


def _extract_fmt_block() -> str:
    src = TEMPLATE.read_text(encoding="utf-8")
    m = re.search(r"/\* UI-FMT-BEGIN.*?\*/(.*?)/\* UI-FMT-END \*/", src, re.S)
    assert m, "UI-FMT-blok niet gevonden in cockpit.html"
    return m.group(1)


def test_template_has_no_stray_ago_formatter():
    """De oude helper `ago(` (absolute epoch, plakte zelf 'ago' erachter)
    is volledig vervangen; call sites gebruiken fmtAgo/fmtAgeLabel."""
    src = TEMPLATE.read_text(encoding="utf-8")
    assert "ago ago" not in src
    assert re.search(r"(?<![\wA-Za-z])ago\(", src) is None


def test_ui_fmt_block_is_dependency_free():
    block = _extract_fmt_block()
    for banned in ("document", "window", "fetch", "esc(", "$("):
        assert banned not in block, f"UI-FMT-blok gebruikt '{banned}'"


@pytest.mark.skipif(shutil.which("node") is None, reason="node niet beschikbaar")
def test_ui_fmt_js_semantics():
    block = _extract_fmt_block()
    exports = ("\nexport { parseTs, ageSeconds, fmtAgeLabel, fmtAgo, "
               "fmtDur, fmtNum };\n")
    with tempfile.TemporaryDirectory() as td:
        mod = Path(td) / "fmt.mjs"
        mod.write_text(block + exports, encoding="utf-8")
        test = Path(td) / "assert.mjs"
        test.write_text(JS_ASSERTS, encoding="utf-8")
        r = subprocess.run(["node", str(test)], capture_output=True,
                           text=True, timeout=60, cwd=td)
        assert r.returncode == 0, \
            f"JS-formatter asserts faalden:\n{r.stdout}\n{r.stderr}"
        assert "ui-fmt ok" in r.stdout
