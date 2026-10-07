# plex_scraper

**Plex-first dynamic media resolver with stable library paths and disposable backing sources.**

`plex_scraper` keeps the logical media identity and Plex-visible path stable while the backing source can be replaced when it degrades or disappears.

> Current release line: **1.0.x**

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

## License

MIT — see [LICENSE](LICENSE).
