# Security

## Secrets

Never commit real API keys, Plex tokens, webhook secrets, private keys, or
credential-bearing URLs.

Use environment variables or secret files. The repository intentionally
ignores `.env`, `secrets/`, databases, key files and common local backup
artifacts.

If a credential is ever committed, removing it from the current tree is not
enough: rotate/revoke it immediately and, when appropriate, purge it from Git
history.

## Network exposure

The resolver/control API and diagnostics UI are intended for localhost or a
trusted private network. They do not provide a general-purpose authentication
layer.

Do not expose the resolver, scraper, diagnostics UI, Docker socket, or secret
files directly to the public internet. Put remote access behind an
authenticated reverse proxy/VPN and apply least-privilege network rules.

## Reporting

Do not publish live tokens, private LAN details, personal data, or credential
material in a public issue. Redact logs and configuration before sharing them.
