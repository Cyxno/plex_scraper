# Contributing

Thanks for considering a contribution to `plex_scraper`.

## Before opening an issue

- Search existing issues first.
- Reproduce against the current `main` branch when practical.
- Remove API keys, tokens, private hostnames, LAN addresses, media-library paths and other personal infrastructure details from logs/screenshots.
- For security-sensitive findings, follow [SECURITY.md](SECURITY.md) instead of posting details publicly.

## Bug reports

Please include:

- `plex_scraper` version or commit SHA
- host/runtime (for example Linux/Unraid, Docker version)
- affected component: resolver, VFS, ingest, JIT, provider, cockpit
- expected vs actual behavior
- minimal redacted logs or events
- whether the issue reproduces in the portable test suite or only with real FUSE

Do not include live credentials.

## Pull requests

Keep changes focused and explain the root cause, not only the symptom.

Before opening a PR:

```bash
pytest -q --ignore=tests/test_vfs_integration.py
```

If your change touches VFS behavior, also run on a host with real FUSE support:

```bash
pytest -q tests/test_vfs_integration.py
```

Update tests and documentation when behavior or configuration changes.

## Design principles

Contributions should preserve the core contracts:

- logical media identity stays stable while backing sources are replaceable;
- provider outages/rate limits are not `NO_SOURCE`;
- Plex-visible paths remain stable;
- failover must not silently downgrade quality unless policy explicitly allows it;
- secrets and site-specific infrastructure do not belong in committed defaults.
