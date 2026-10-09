# plex_scraper

[![tests](https://github.com/Cyxno/plex_scraper/actions/workflows/test.yml/badge.svg)](https://github.com/Cyxno/plex_scraper/actions/workflows/test.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/)

**Plex-first dynamic media resolver with stable library paths and disposable backing sources.**

`plex_scraper` keeps the logical media identity and Plex-visible path stable while the backing source can be replaced when it degrades or disappears.

> Current release line: **1.0.x** · Current version: **1.0.4**

## What it does

```text
Sonarr / Radarr
      ↓
logical media identity
      ↓
resolver  → scrape → rank → validate → pin / fail over
      ↓
canonical .ids source
      ↓
stable symlink + VFS namespace
      ↓
Plex
```

Key capabilities:

- TorBox-backed source resolution with Torrentio discovery.
- Quality-preserving JIT failover and playback rescue.
- Multi-file torrent-safe probing: provider file selection is independent from scraper file indexes.
- Stable VFS paths with POSIX read/seek semantics.
- Arr-authoritative ingest queue with persistent SQLite/WAL state.
- Provider-aware backoff/circuit handling; 429/5xx never become false `NO_SOURCE`.
- Physical Plex-namespace health checks and self-healing.
- Operator cockpit with live Ready / Issues / Pending semantics.
- Health sweeper, soak monitoring, queue invariants and maintenance tooling.

## Security model

Secrets are read from environment variables or secret files and **must not be committed**. The repository ignores `.env`, `secrets/`, databases and common key formats.

The control/API surfaces do not provide a general authentication layer. Keep them bound to localhost or a trusted private network/reverse proxy.

See [SECURITY.md](SECURITY.md).

## Quick start

```bash
cp .env.example .env
cp config/preferences.example.yaml config/preferences.yaml
# Add your TorBox token to .env or use a Docker secret file.
docker compose up -d --build
```

Without a TorBox token the project can run against the seeded demo provider for local testing.

The default compose file is intentionally development-friendly and reads credentials from `.env`. For production, prefer secret files/Docker secrets and keep the control surfaces private.

### Unraid / FUSE

The VFS bind source must live on a shared mount so the FUSE mount can propagate to Plex:

```bash
mkdir -p /mnt/cache/appdata/plex-scraper/vfs
```

The example compose file binds the control services to localhost by default. Adapt paths and mount propagation to your host.

## Configuration

Primary configuration is environment-driven. See:

- [`.env.example`](.env.example)
- [`config/preferences.example.yaml`](config/preferences.example.yaml)
- [`ARCHITECTURE.md`](ARCHITECTURE.md)
- [`ARCHITECTURE_DECISION.md`](ARCHITECTURE_DECISION.md)
- [`docs/RUNBOOKS.md`](docs/RUNBOOKS.md)

Never put real API keys, Plex tokens, webhook secrets, internal hostnames, or private LAN addresses in committed config.

## Testing

Portable suite:

```bash
pytest -q --ignore=tests/test_vfs_integration.py
```

The VFS integration suite requires privileged, real FUSE mount semantics and is intentionally kept out of GitHub-hosted CI. Run it on a suitable Linux/Unraid host:

```bash
pytest -q tests/test_vfs_integration.py
```

## Diagnostics

The optional web role exposes the operations cockpit (default container port `8285`) with resolver health, ingest state, jobs, provider status, physical-library checks and playback traces.

Keep it private; do not expose it directly to the public internet.

## Project status

The original proof-of-concept has evolved into a production-oriented 1.0.x stack. Historical PoC reports remain in the repository as design/test records.

## Project scope & affiliation

This is an independent community project. It is not affiliated with, endorsed by, or maintained by Plex, TorBox, Torrentio, Sonarr or Radarr. Users are responsible for complying with the terms of the services they connect and with applicable law.

## Contributing

Bug reports and focused pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md). For security issues, follow [SECURITY.md](SECURITY.md) and do not post credentials or private infrastructure details in public issues.

## License

<<<<<<< HEAD
MIT — see [LICENSE](LICENSE).
=======
AGPL-3.0-only (GNU Affero General Public License v3.0) — see [LICENSE](LICENSE)

## Rollen & diagnostics GUI (oct 2026)

Eén image, vier process-roles: `resolver`, `vfs`, `scraper` (optionele
standalone scraper-API), `web` (read-only diagnostics GUI). Zie
[README-ROLES.md](README-ROLES.md). Diagnostics GUI: `http://<host>:8285/` —
health, resolver totals, queue, failures en per-item playback-trace
(resolver → symlink → VFS → backend → read-test) met retry-acties.
>>>>>>> 7575306 (Relicense MIT -> AGPL-3.0-only)
