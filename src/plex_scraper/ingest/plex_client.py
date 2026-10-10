"""Plex-consumer-verificatie + scan-trigger via docker-exec (Phase 19).

Plex-container namespace is autoritatief. De resolver-container heeft zelf
géén Plex-token; de sentinel-orchestratie (resolver/physical.py) exec'ed al
python3 IN de plex-container — hier hergebruikt voor:

  * canonical .ids head+seek leesprobe (in Plex-namespace)
  * symlink head+seek leesprobe (in Plex-namespace)
  * library-section scan trigger (specific path)
  * episode/movie-lookup (aanwezig in library? juist part-bestand?)

Alle uitvoer is JSON één-regel; errors worden als {"error": ...} gemeld.
"""
from __future__ import annotations

import asyncio
import json

DOCKER_SOCK = "/var/run/docker.sock"
PLEX_CONTAINER = "plex"


class PlexExecClient:
    def __init__(self, container: str = PLEX_CONTAINER,
                 docker_sock: str = DOCKER_SOCK, section_tv: int = 2,
                 section_movies: int = 1, sock_timeout: float = 120.0):
        self.container = container
        self.sock = docker_sock
        self.section_tv = section_tv
        self.section_movies = section_movies
        self.sock_timeout = sock_timeout

    # ------------------------------------------------------------ plumbing
    def _docker(self, method: str, path: str, body: dict | None = None) -> dict:
        """Zelfde unix-socket-protocol als resolver/physical.py."""
        import http.client
        import socket as _s
        s = _s.socket(_s.AF_UNIX, _s.SOCK_STREAM)
        s.settimeout(self.sock_timeout)
        s.connect(self.sock)
        payload = json.dumps(body or {}).encode()
        req = (f"{method} /v1.41{path} HTTP/1.1\r\nHost: docker\r\n"
               f"Content-Type: application/json\r\n"
               f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n")
        s.sendall(req.encode() + payload)
        chunks = []
        while True:
            b = s.recv(65536)
            if not b:
                break
            chunks.append(b)
        s.close()
        raw = b"".join(chunks)
        head, _, rest = raw.partition(b"\r\n\r\n")
        status = int(head.split(b" ")[1])
        body_bytes = rest
        if b"Transfer-Encoding: chunked" in head:
            out, i = bytearray(), 0
            while True:
                j = rest.find(b"\r\n", i)
                if j < 0:
                    break
                n = int(rest[i:j], 16)
                if n == 0:
                    break
                out += rest[j + 2:j + 2 + n]
                i = j + 2 + n + 2
            body_bytes = bytes(out)
        if method == "POST" and "/exec" in path and "start" in path:
            out, i = bytearray(), 0
            while i + 8 <= len(body_bytes):
                n = int.from_bytes(body_bytes[i + 4:i + 8], "big")
                if body_bytes[i] in (0, 1, 2):
                    out += body_bytes[i + 8:i + 8 + n]
                i += 8 + n
            return {"status": status, "output": out.decode("utf-8", "replace")}
        try:
            return {"status": status, "json": json.loads(body_bytes or b"{}")}
        except ValueError:
            return {"status": status, "json": {}}

    async def _exec(self, script: str, arg: str | None = None) -> dict:
        cmd = ["python3", "-c", script] + ([arg] if arg is not None else [])
        created = await asyncio.to_thread(
            self._docker, "POST", f"/containers/{self.container}/exec",
            {"AttachStdout": True, "AttachStderr": True, "Cmd": cmd})
        eid = created["json"]["Id"]
        started = await asyncio.to_thread(
            self._docker, "POST", f"/exec/{eid}/start",
            {"Detach": False, "Tty": False})
        insp = await asyncio.to_thread(self._docker, "GET", f"/exec/{eid}/json")
        exit_code = insp["json"].get("ExitCode", 1)
        raw = (started.get("output") or "").strip()
        line = raw.splitlines()[-1] if raw else "{}"
        try:
            out = json.loads(line or "{}")
        except ValueError:
            out = {"error": f"unparsable plex-exec output: {raw[:160]}"}
        out["exit_code"] = exit_code
        return out

    # ------------------------------------------------------------ probes
    async def read_probe(self, path: str) -> dict:
        """Head + mid seek leesprobe in Plex-namespace (geen streamstart)."""
        script = (
            "import json,sys\n"
            "p = sys.argv[1]\n"
            "out = {}\n"
            "try:\n"
            "    import os\n"
            "    size = os.path.getsize(p)\n"
            "    with open(p, 'rb') as fh:\n"
            "        head = fh.read(65536)\n"
            "    mid = max(0, size // 2)\n"
            "    with open(p, 'rb') as fh:\n"
            "        fh.seek(mid); midb = fh.read(65536)\n"
            "    out = {'ok': bool(head) and bool(midb), 'size': size,\n"
            "           'head': len(head), 'mid': len(midb)}\n"
            "except OSError as e:\n"
            "    out = {'ok': False, 'error': repr(e)[:120]}\n"
            "print(json.dumps(out))\n")
        return await self._exec(script, path)

    async def scan_section(self, section: int, path: str | None = None) -> dict:
        """Library-scan trigger (specifiek pad, of hele section)."""
        script = (
            "import json,sys,re,urllib.request\n"
            "section, path = sys.argv[1].split('|', 1)\n"
            "section = int(section); path = path or None\n"
            "tok = re.search(r'PlexOnlineToken=\"([^\"]+)\"',\n"
            "    open('/config/Plex Media Server/Preferences.xml').read()).group(1)\n"
            "url = f'http://127.0.0.1:32400/library/sections/{section}/refresh'\n"
            "if path:\n"
            "    import urllib.parse\n"
            "    url += '?' + urllib.parse.urlencode({'path': path, 'X-Plex-Token': tok})\n"
            "else:\n"
            "    url += '?X-Plex-Token=' + tok\n"
            "req = urllib.request.Request(url, method='GET')\n"
            "try:\n"
            "    with urllib.request.urlopen(req, timeout=30) as r:\n"
            "        print(json.dumps({'scanned': section, 'status': r.status}))\n"
            "except Exception as e:\n"
            "    print(json.dumps({'scanned': section, 'status': 0,\n"
            "                      'error': repr(e)[:120]}))\n")
        return await self._exec(script, f"{section}|{path or ''}")

    async def find_episode(self, show_title: str, season: int, episode: int,
                           guid_imdb: str | None = None,
                           file_suffix: str | None = None) -> dict:
        """Zoekt een episode in de TV-section (title+index; GUID als extra
        bewijs; part-bestand met file_suffix = onze symlink)."""
        script = (
            "import json,sys,re,urllib.request\n"
            "show, season, episode, guid, suffix = sys.argv[1].split('|', 4)\n"
            "season = int(season); episode = int(episode)\n"
            "want = re.sub(r'\\s*\\(\\d{4}\\)\\s*$', '', show).casefold()\n"
            "tok = re.search(r'PlexOnlineToken=\"([^\"]+)\"',\n"
            "    open('/config/Plex Media Server/Preferences.xml').read()).group(1)\n"
            "req = urllib.request.Request(\n"
            "    f'http://127.0.0.1:32400/library/sections/2/all?includeGuids=1',\n"
            "    headers={'Accept':'application/json','X-Plex-Token':tok})\n"
            "mds = json.loads(urllib.request.urlopen(req, timeout=60).read())['MediaContainer'].get('Metadata') or []\n"
            "shows = [x for x in mds if x.get('title','').casefold()==want]\n"
            "found = {'present': False}\n"
            "for s in shows:\n"
            "    req2 = urllib.request.Request(\n"
            "        f\"http://127.0.0.1:32400/library/metadata/{s['ratingKey']}/allLeaves\",\n"
            "        headers={'Accept':'application/json','X-Plex-Token':tok})\n"
            "    leaves = json.loads(urllib.request.urlopen(req2, timeout=60).read())['MediaContainer'].get('Metadata') or []\n"
            "    for ep in leaves:\n"
            "        if ep.get('parentIndex')==season and ep.get('index')==episode:\n"
            "            found['present'] = True\n"
            "            found['title'] = ep.get('title')\n"
            "            parts = []\n"
            "            for med in ep.get('Media') or []:\n"
            "                for pt in med.get('Part') or []:\n"
            "                    parts.append(pt.get('file'))\n"
            "            found['files'] = parts\n"
            "            if suffix and any((f or '').endswith(suffix) for f in parts):\n"
            "                found['file_match'] = True\n"
            "print(json.dumps(found))\n")
        return await self._exec(script, "|".join(
            [show_title, str(season), str(episode), guid_imdb or "",
              file_suffix or ""]))

    # ---------------------------------------------- media-revalidatie
    async def analyze_item(self, rating_key: int | str) -> dict:
        """Item-gerichte media-analyse: herbouwt media_parts/media_streams.

        Dit is de bewezen kleinste supported repair na een bron-generatie-
        wissel (audit 2026-10-09): scope = exact dit item, geen section-scan.
        """
        script = (
            "import json,sys,re,urllib.request\n"
            "rk = sys.argv[1]\n"
            "tok = re.search(r'PlexOnlineToken=\"([^\"]+)\"',\n"
            "    open('/config/Plex Media Server/Preferences.xml').read()).group(1)\n"
            "url = (f'http://127.0.0.1:32400/library/metadata/{rk}/analyze'\n"
            "       f'?X-Plex-Token={tok}')\n"
            "req = urllib.request.Request(url, method='PUT')\n"
            "with urllib.request.urlopen(req, timeout=30) as r:\n"
            "    print(json.dumps({'analyzed': rk, 'status': r.status}))\n")
        return await self._exec(script, str(rating_key))

    async def get_media_info(self, rating_key: int | str) -> dict:
        """Actuele media-metadata (parts/streams) zoals Plex die ziet."""
        script = (
            "import json,sys,re,urllib.request\n"
            "rk = sys.argv[1]\n"
            "tok = re.search(r'PlexOnlineToken=\"([^\"]+)\"',\n"
            "    open('/config/Plex Media Server/Preferences.xml').read()).group(1)\n"
            "req = urllib.request.Request(\n"
            "    f'http://127.0.0.1:32400/library/metadata/{rk}',\n"
            "    headers={'Accept':'application/json','X-Plex-Token':tok})\n"
            "out = {'ok': False}\n"
            "try:\n"
            "    md = json.loads(urllib.request.urlopen(req, timeout=60).read())\n"
            "    mt = (md['MediaContainer'].get('Metadata') or [{}])[0]\n"
            "    med = (mt.get('Media') or [{}])[0]\n"
            "    part = (med.get('Part') or [{}])[0]\n"
            "    streams = []\n"
            "    for st in part.get('Stream') or []:\n"
            "        streams.append({'index': st.get('index'),\n"
            "                        'type': st.get('streamType'),\n"
            "                        'codec': st.get('codec')})\n"
            "    vst = next((s for s in streams if str(s['type'])=='1'), {})\n"
            "    ast = next((s for s in streams if str(s['type'])=='2'), {})\n"
            "    out = {'ok': True,\n"
            "           'container': med.get('container') or part.get('container'),\n"
            "           'video_codec': vst.get('codec'),\n"
            "           'audio_codec': ast.get('codec'),\n"
            "           'width': med.get('width'), 'height': med.get('height'),\n"
            "           'duration_s': (float(med['duration'])/1000.0\n"
            "                           if med.get('duration') else None),\n"
            "           'size': part.get('size'),\n"
            "           'streams': streams}\n"
            "except Exception as e:\n"
            "    out = {'ok': False, 'error': repr(e)[:120]}\n"
            "print(json.dumps(out))\n")
        return await self._exec(script, str(rating_key))

    async def find_rating_key_via_db(self, part_file: str) -> int | None:
        """ratingKey via read-only SQLite-lookup op het exacte part-bestand.

        ALLEEN-LEZEN (mode=ro): muteren van de Plex-DB blijft expliciet
        verboden. Dit is de snelste exacte match (geen per-show traversal)
        en valt weg bij een tijdelijke DB-lock — de HTTP-lookup is fallback.
        """
        script = (
            "import json,sys,sqlite3\n"
            "part = sys.argv[1]\n"
            "P = '/config/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db'\n"
            "out = {'rating_key': None}\n"
            "try:\n"
            "    db = sqlite3.connect(f'file:{P}?mode=ro', uri=True,\n"
            "                         timeout=5.0)\n"
            "    row = db.execute(\n"
            "        'SELECT mi.metadata_item_id FROM media_parts mp '\n"
            "        'JOIN media_items mi ON mi.id=mp.media_item_id '\n"
            "        'WHERE mp.file = ? LIMIT 1', (part,)).fetchone()\n"
            "    if row is None:\n"
            "        row = db.execute(\n"
            "            'SELECT mi.metadata_item_id FROM media_parts mp '\n"
            "            'JOIN media_items mi ON mi.id=mp.media_item_id '\n"
            "            'WHERE mp.file LIKE ? LIMIT 1', ('%'+part,)).fetchone()\n"
            "    out['rating_key'] = row[0] if row else None\n"
            "except Exception as e:\n"
            "    out = {'rating_key': None, 'error': repr(e)[:120]}\n"
            "print(json.dumps(out))\n")
        return (await self._exec(script, part_file)).get("rating_key")

    async def find_rating_key_by_target(self, plex_path: str) -> int | None:
        """ratingKey via symlink-target: Plex-parts onder /symlinks zijn
        symlinks naar ons canonical .ids-pad — readlink levert dus een exacte
        item-identiteit, onafhankelijk van de release-bestandsnaam.

        ALLEEN-LEZEN (readlink + ro-DB); werkt voor gemigreerde items zonder
        geregistreerd part-pad. Bounded: stopt bij de eerste match.
        """
        script = (
            "import json,sys,os,sqlite3\n"
            "want = sys.argv[1]\n"
            "P = '/config/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db'\n"
            "out = {'rating_key': None}\n"
            "try:\n"
            "    db = sqlite3.connect(f'file:{P}?mode=ro', uri=True, timeout=5.0)\n"
            "    rows = db.execute(\n"
            "        \"SELECT mp.file, mi.id FROM media_parts mp \"\n"
            "        \"JOIN media_items mi ON mi.id=mp.media_item_id \"\n"
            "        \"WHERE mp.file LIKE '/symlinks/%'\").fetchall()\n"
            "    leaf = want.rstrip('/').split('/')[-1]\n"
            "    for f, mid in rows:\n"
            "        try:\n"
            "            t = os.readlink(f)\n"
            "        except OSError:\n"
            "            continue\n"
            "        if t.rstrip('/').split('/')[-1] == leaf:\n"
            "            out['rating_key'] = mid\n"
            "            break\n"
            "except Exception as e:\n"
            "    out = {'rating_key': None, 'error': repr(e)[:120]}\n"
            "print(json.dumps(out))\n")
        return (await self._exec(script, plex_path)).get("rating_key")

    async def find_rating_key_by_path(self, plex_path: str,
                                      exact_path: str | None = None) -> int | None:
        """ratingKey van het metadata-item dat dit part-bestand bezit.

        plex_path is ons stabiele VFS-pad (/mnt/remote/...); in Plex-namespace
        is dat /symlinks/... Zoekt in beide TV- en movie-sections.

        Bewezen faalwijzen (incident 2026-10-09/10, 99 revalidaties faalden
        hierop):
          1. `/library/sections/N/all` geeft XML tenzij Accept: application/json
             — zonder die header: JSONDecodeError → stilletjes None;
          2. TV-sections geven op /all alléén shows; Media/Part zit in de
             allLeaves per show — die worden hier meteen meegetraverseerd;
          3. Plex-parts dragen de symlink-releasenaam, niet de .ids-uuid —
             match daarom op exact part-pad (exact_path, voorkeur) óf op de
             basename van een écht part-bestand.
        """
        script = (
            "import json,sys,re,urllib.request\n"
            "suffix, exact = sys.argv[1].split('|', 1)\n"
            "tok = re.search(r'PlexOnlineToken=\"([^\"]+)\"',\n"
            "    open('/config/Plex Media Server/Preferences.xml').read()).group(1)\n"
            "H = {'Accept': 'application/json', 'X-Plex-Token': tok}\n"
            "def leaves_parts(show_rk):\n"
            "    u = f'http://127.0.0.1:32400/library/metadata/{show_rk}/allLeaves'\n"
            "    try:\n"
            "        return (json.loads(urllib.request.urlopen(\n"
            "            urllib.request.Request(u, headers=H), timeout=60).read())\n"
            "            ['MediaContainer'].get('Metadata') or [])\n"
            "    except Exception:\n"
            "        return []\n"
            "def match(mds):\n"
            "    for mt in mds:\n"
            "        leaves = (mt.get('Media') and [mt]) or leaves_parts(mt['ratingKey'])\n"
            "        for it in leaves:\n"
            "            for med in it.get('Media') or []:\n"
            "                for pt in med.get('Part') or []:\n"
            "                    f = pt.get('file') or ''\n"
            "                    if (exact and f == exact) or (suffix and f.endswith(suffix)):\n"
            "                        # TV: leaf-ratingKey (episode), niet de show\n"
            "                        return it.get('ratingKey') or mt['ratingKey']\n"
            "    return None\n"
            "rk = None\n"
            "for section in (2, 1):\n"
            "    if rk: break\n"
            "    url = (f'http://127.0.0.1:32400/library/sections/{section}/all'\n"
            "           f'?includeFields=file&X-Plex-Token={tok}')\n"
            "    try:\n"
            "        mds = json.loads(urllib.request.urlopen(\n"
            "            urllib.request.Request(url, headers=H), timeout=60).read())\n"
            "    except Exception:\n"
            "        continue\n"
            "    rk = match(mds['MediaContainer'].get('Metadata') or [])\n"
            "print(json.dumps({'rating_key': rk}))\n")
        import os
        suffix = os.path.basename(plex_path)
        out = await self._exec(script, f"{suffix}|{exact_path or ''}")
        return out.get("rating_key")
