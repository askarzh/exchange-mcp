"""The EWS gateway: connection lifecycle + the one Account per daemon.

Nothing is re-exported at package level on purpose. ``client.py`` imports
exchangelib at module top, and the thin MCP imports ``gateway.wellknown``
(exchangelib-free) for the well-known folder keys — a package-level
``from .client import ...`` would drag exchangelib into that process.
Import the submodules explicitly: ``from ewsmcp.gateway.client import
EWSGateway``.
"""
