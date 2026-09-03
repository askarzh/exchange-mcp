# Dead-code inventory of /home/askar/src/exchange-mcp (2026-09-03, post-extraction)

## 1. Defined but never referenced
- `ewsmcp/server.py:119 run_stdio` — main.py uses mcp.server.run_stdio; zero refs (high)
- `ewsmcp/server.py:89 build_mcp_server` — only caller is dead run_stdio (high)
- `ewsmcp/tools/base.py:237 mint_token` — no caller (high)
- `ewsmcp/tools/mail_read.py:45 _cache_folder_key`, `:50 _cache_watermark` — passthroughs, no callers (high)
- `ewsmcp/errors.py:47 ToolError.http_status` — http.py maps via HTTP_BY_CODE directly (high)
- `ewsmcp/gateway/connection.py:74 ConnectionManager.is_warm` — consumers use status()["state"] (high)
- `ewsmcp/ids.py:252 NullAliaser` — only a noqa import in tests/test_ids.py:16 (high)
- `ewsmcp/ids.py:70 kind_for_key` — tests only (medium)
- `ewsmcp/cache/store.py:192 CacheStore.purge` — tests only; EWS_CACHE_PURGE_ON_BOOT removed (high)
- `ewsmcp/cache/__init__.py:6 __all__` — harmless (low)
- `tests/test_daemon_api.py:6` unused `make_settings` import kept with noqa (high)
- NOT dead: `CacheStore.tombstone_messages` reached by string dispatch in tools/writes.py:551,831 via `_write_through`.

## 2. Modules/scripts that no longer fit
- `scripts/live_smoke.py` — hits /api/tools/*, SMOKE_AR_QUERY; zero refs (high) → delete
- `ewsmcp/http.py:144` default `tools_prefix="/api/tools"`, `mount_mcp=True`, `streamable` param — only tests use defaults; production passes /v1/tools and mount_mcp=False (high)
- `ewsmcp/server.py:33 _NullAudit` — used by mcp/server.py and tests; forces mcp.server to import the daemon module (exchangelib). Move to audit.py (medium)
- `.github/workflows/tests.yml:65-68` "Boot smoke (cache disabled)" job sets removed EWS_CACHE_ENABLED; duplicate of tier=full (high)
- `scripts/verify_audit_chain.py` — KEEP (referenced by DESIGN.md, README, audit.py, tests/test_audit_persistence.py)

## 3. Dead branches / shims
- `tools/base.py:280 _resolve_ids = resolve_ids` — zero call sites (high)
- `cache/store.py:84 close()` no-op — no caller (high)
- `tools/mail_read.py:33-34` re-exports `_row_body,_row_card,_row_full,_stamp` — only `_stamp` used inside mail_read; nothing imports the others from mail_read (high)
- `http.py:214-219` /mcp branch — unreachable in production (medium)

## 4. Docs/comments describing removed behaviour
- `tools/mail_read.py:10` "reads come from SQLite"; `:104` "alias mints are SQLite write transactions" (high)
- `cache/sync.py:54` "700× paid once" (medium)
- `bodyclean.py:6-7,417` Arabic/bidi claims (high)
- `docs/API.md:466-472` advertises mode=semantic/find_similar; `:508` rename map lists find_similar; `:83` generated mode enum still offers "semantic"; `:471-472` says spec is at repo root (it is docs/superpowers/specs/)
- `README.md:171-175` "Removed from the 4.5 line" paragraph (belongs in CHANGELOG)
- `DESIGN.md:20,140`, `README.md:84`, `docs/API.md:474-479` v3/4.5 back-references and the 67-tool rename map
- `Dockerfile:26-28` "v3 Dockerfile" comment
- `docs/superpowers/plans/2026-09-03-phase1-postgres-daemon.md` full of v5/ paths (historical artifact; keep as history)

## 5. Redundant tests/fixtures
- Seven files hand-roll Context instead of conftest.make_context: test_dispatcher.py:24, test_cache_first_reads.py:71, test_http_shim.py:23, test_envelope_contract.py:86, test_mail_read.py:105, test_calendar_people.py:37, test_surface_completion.py:29
- Eight near-identical gateway fakes: _Gateway (test_dispatcher.py:19), NoTouchGateway+RecordingGateway (test_cache_first_reads.py:21,31), CountingGateway (test_north_star.py:31), _FakeGateway ×3 (test_envelope_contract.py:42, test_mail_read.py:66, test_calendar_people.py:29), Gateway+DeadGateway (test_surface_completion.py:19,182), FakeGateway ×2 (test_writes.py:43, test_sync_engine.py:17), BoomGateway (test_sync_engine.py:115)
- `make_row` defined in tests/test_pg_store.py:12, imported by 5 test modules → move to conftest
- `tests/test_http_shim.py` pins the legacy /api/tools default
- `tests/test_cache_reads.py` overlaps test_cache_first_reads.py (low)

## 6. bodyclean.py Arabic/bidi surface
- `:6-7` docstring; `:27-31 _BIDI_RE`; `:36-38 _AR_TRANS`; `:48` docstring; `:65-67 _ORIG_AR_RE`; `:73-76 _FROM_AR_RE,_PAIR_AR_RE`; `:82 _AR_WROTE_RE`; `:130-137` outlook_ar branch; `:142-151` gmail_ar branch; `:211-217 _CLOSERS_AR`; `:417` docstring.
- Tests depending on it in tests/test_bodyclean.py: lines 222, 230, 234, 244, 286, 290, 384 (+ fixtures 104, 245).
