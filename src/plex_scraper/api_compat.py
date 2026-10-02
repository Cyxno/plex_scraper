"""Compatibility shim: builds the resolver FastAPI app from the refactored
modules. Kept separate so the CLI role dispatcher stays small."""
from __future__ import annotations

from .common.config import Settings


def resolver_app(settings: Settings):
    from .resolver.api.app import create_app
    from .cli import build_resolver
    return create_app(build_resolver(settings), settings)
