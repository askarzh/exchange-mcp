# Phase 3: Search Quality, Mirror Completeness, Archive Hygiene — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make semantic and keyword search rank on message content rather than boilerplate and structure, complete the mirror (item class, attachment inventory), and close the four archive hygiene gaps parked in Phase 2.

**Architecture:** All work lands in the existing two-process layout: `ewsd` (daemon: sync hydration, cleaning, embedding, archive lanes) and `ewsmcp` (thin MCP reading Postgres). Cleaning stays deterministic in `bodyclean`; two log-only boilerplate detectors (embedding similarity, Gemini boundary call) hang off `SemanticIndex.index_messages` and write to one hits table. Migration 004 adds columns and tables; no data is dropped.

**Tech Stack:** Python 3.11, exchangelib 5.0.3, psycopg 3 + pgvector, Gemini `gemini-embedding-2` (768 dims) and `gemini-2.5-flash-lite` over httpx, pytest against a real Postgres (`pg_dsn` fixture / `docker` pgvector container).

**Spec:** `docs/superpowers/specs/2026-09-08-phase3-search-quality-design.md`

## Global Constraints

- Every new module-level import sits at the top of the file (`tests/test_no_lazy_imports.py`); the MCP process never imports `exchangelib` (`tests/test_mcp_import_boundary.py`).
- `EMBED_DIMS` stays 768; `chunks.embedding` is `vector(768)`.
- The stored `body_clean` is changed only by `bodyclean` rules (§1); detectors (§2, §2b) affect the index only.
- The first non-empty paragraph of a message is never cut or dropped by any rule or detector.
- `ARCHIVE_BOILERPLATE_DROP` default `off`; `ARCHIVE_GC_INTERVAL_HOURS` 168; `ARCHIVE_MAX_ITEM_MB` 50; `DB_POOL_MAX` 8; `EMBED_BOILERPLATE_THRESHOLD` 0.80; `GEMINI_CLEAN_MODEL` `gemini-2.5-flash-lite`.
- Delete rails, grace and policy are untouched.
- ruff config is pinned (`E,F,W,I`, ignore `E731`, line length 100); run `.venv/bin/python -m ruff check .` before every commit.
- Run pytest in the foreground with `timeout 300`: `.venv/bin/python -m pytest tests/<file> -q`. Never background it.
- `docs/API.md` is generated: after any tool schema change run `.venv/bin/python scripts/dump_tool_table.py --write`; CI runs `--check`.
- Commit trailers: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01DLm4hbGrGpRbAygG5Jqj6v`.
- Version becomes `5.2.0a1` in `pyproject.toml` and `ewsmcp/__init__.py` (Task 12), with a CHANGELOG entry.

---

## File map

| File | Responsibility in this phase |
|---|---|
| `ewsmcp/migrations/004_phase3.sql` | item_class, attachments_json, boilerplate_refs, boilerplate_hits, conv/date index, re-queue short replies |
| `ewsmcp/db.py` | `SCHEMA_VERSION = 4`; `max_size` from settings |
| `ewsmcp/config.py` | new settings (§8 of spec) |
| `ewsmcp/bodyclean.py` | disclaimer tail cut; `tail_paragraphs()` helper shared by detectors |
| `ewsmcp/cache/sync.py` | `HYDRATE_FIELDS` += `item_class`, `attachments`; `attachments_json()`; `row_from_message` stores both |
| `ewsmcp/cache/store.py` | new columns in upsert; `parent_in_thread`; `search_messages(include_calendar_items)`; `similar_message_ids` excludes calendar; boilerplate refs/hits methods; `update_bodies` accepts `item_class`/`attachments_json` |
| `ewsmcp/embeddings.py` | `chunk_text(..., context=None)` |
| `ewsmcp/semantic.py` | thread context; boilerplate harness calling both detectors |
| `ewsmcp/boilerplate.py` (new) | `EmbeddingDetector`, `LlmDetector`, `GeminiCleaner` client, `Hit` dataclass, drop policy |
| `ewsmcp/archive/gc.py` (new) | `GcWorker` |
| `ewsmcp/archive/capture.py` | size cap |
| `ewsmcp/archive/delete.py` | audit line for skipped items |
| `ewsmcp/archive/runner.py` | `gc` kind, gc lane, status blocks (`boilerplate`, `gc`, `skipped_too_large`) |
| `ewsmcp/tools/cache_reads.py` | `_row_full` attachments from mirror |
| `ewsmcp/tools/mail_read.py` | `include_calendar_items` on `search_messages` |
| `ewsmcp/tools/archive.py` | `find_similar` excludes calendar; `archive_run` accepts `gc`; status block |
| `ewsmcp/server.py` | pool size + consumer warning; runner gets index/cleaner |
| `scripts/backfill_bodies.py` | fills item_class/attachments_json; `--all` re-clean |
| `scripts/seed_boilerplate.py` (new) | refs from `scripts/boilerplate_refs.txt` |
| `scripts/boilerplate_report.py` (new) | two-detector comparison |
| `tests/test_bodyclean.py`, `tests/test_sync_engine.py`, `tests/test_pg_store.py`, `tests/test_semantic.py`, `tests/test_boilerplate.py` (new), `tests/test_archive_gc.py` (new), `tests/test_archive_capture.py`, `tests/test_archive_verify_delete.py`, `tests/test_archive_runner.py`, `tests/test_archive_tools.py`, `tests/test_mail_read.py`, `tests/test_migrations.py` | tests |

Existing helpers to reuse: `tests/conftest.py` — `db`, `make_settings(**overrides)`, `make_row(ews_id, ...)`, `make_context(db, **over)`, `FakeGateway`, `FakeEmbedder`; `tests/test_archive_capture.py` — `FakeAccount`, `FakeItem`, `FakeGatewayFor`; `tests/test_archive_verify_delete.py` — `DeletableItem`, `RecordingAudit`.

---

### Task 0: Spike — `mail-parser-reply` against the mirror

**Files:**
- Create: `.superpowers/sdd/2026-09-08-phase3-search-quality/spike-mail-parser-reply.md` (report; the directory is git-ignored)
- Create (throwaway, not committed): `/tmp/claude-1000/-home-askar-src-ews-mcp/*/scratchpad/spike_mpr.py`

**Interfaces:**
- Consumes: `ews.messages.body_clean` via `EWS_TEST_DATABASE_URL` or the production DSN read-only (`docker exec postgres-ews psql`).
- Produces: a report only. Any regex worth porting becomes a bullet under "Port to bodyclean" and is implemented in Task 2.

- [ ] **Step 1: Install into a throwaway venv and confirm language support**

```bash
python3 -m venv /tmp/claude-1000/mpr && /tmp/claude-1000/mpr/bin/pip install -q mail-parser-reply
/tmp/claude-1000/mpr/bin/python -c "import mailparser_reply, pkgutil, os; p=os.path.dirname(mailparser_reply.__file__); print(sorted(os.listdir(os.path.join(p,'languages'))) if os.path.isdir(os.path.join(p,'languages')) else 'no languages dir')"
```

Record the supported language list in the report. If `ru` and `kk` are absent, the spike's conclusion for RU/KZ is "no coverage"; still run the diff for EN mail.

- [ ] **Step 2: Dump bodies**

```bash
docker exec postgres-ews psql -U ews -d ews -Atc "select ews_id || E'\t' || replace(coalesce(body_clean,''), E'\n', '\\n') from ews.messages where length(coalesce(body_clean,''))>0" > /tmp/claude-1000/bodies.tsv
wc -l /tmp/claude-1000/bodies.tsv
```

- [ ] **Step 3: Diff script**

```python
# spike_mpr.py — throwaway
import sys
from mailparser_reply import EmailReplyParser
parser = EmailReplyParser(languages=["en", "de", "fr"])  # whatever Step 1 lists
we_keep_they_drop = []
for line in open(sys.argv[1], encoding="utf-8"):
    ews_id, body = line.rstrip("\n").split("\t", 1)
    body = body.replace("\\n", "\n")
    r = parser.read(text=body)
    theirs = (r.replies[0].body if r.replies else body).strip()
    if len(theirs) < len(body.strip()) - 40:
        we_keep_they_drop.append((ews_id, body.strip()[len(theirs):][:200]))
print(len(we_keep_they_drop))
for ews_id, tail in we_keep_they_drop[:40]:
    print("----", ews_id); print(tail)
```

Run: `/tmp/claude-1000/mpr/bin/python spike_mpr.py /tmp/claude-1000/bodies.tsv > /tmp/claude-1000/spike.out`

- [ ] **Step 4: Write the report**

The report has three sections: supported languages; count and 20 examples of text the library removes that we keep, each labelled `boilerplate` / `content` / `unsure` by reading it; "Port to bodyclean" listing at most five concrete patterns (regex + example) if the `boilerplate` label appears more than five times. End with a one-line recommendation: `port N patterns` or `nothing to port`.

- [ ] **Step 5: Ledger entry** — `Task 0: complete — <recommendation>` in the SDD ledger. No commit (nothing under version control changed).

---

### Task 1: Migration 004, schema version, new settings

**Files:**
- Create: `ewsmcp/migrations/004_phase3.sql`
- Modify: `ewsmcp/db.py` (`SCHEMA_VERSION`, `Database.__init__` signature unchanged)
- Modify: `ewsmcp/config.py` (new fields after `archive_cycle_seconds`)
- Test: `tests/test_migrations.py`, `tests/test_config.py`

**Interfaces:**
- Produces: columns `ews.messages.item_class text`, `ews.messages.attachments_json text`; tables `ews.boilerplate_refs`, `ews.boilerplate_hits`; settings `embed_boilerplate_threshold: float = 0.80`, `archive_boilerplate_drop: str = "off"`, `archive_boilerplate_llm: bool = True`, `gemini_clean_model: str = "gemini-2.5-flash-lite"`, `archive_gc_interval_hours: int = 168`, `archive_max_item_mb: int = 50`, `db_pool_max: int = 8`.

- [ ] **Step 1: Failing migration test**

Add to `tests/test_migrations.py` (follow the file's existing pattern for applying migrations to a fresh DB; the `db` fixture yields a migrated `Database`):

```python
def test_004_adds_phase3_columns_and_tables_and_requeues_short_replies(db):
    store = CacheStore(db)
    store.upsert_messages([
        make_row("P1", conv="C1", body="x" * 700, date_ts=1000),
        make_row("R1", conv="C1", body="short reply", date_ts=2000),
        make_row("S1", conv=None, body="short", date_ts=3000),
    ])
    store.mark_embedded(["P1", "R1", "S1"])
    db.reapply_last_migration_for_tests()  # see Step 3
    with db.conn() as c:
        cols = {r["column_name"] for r in c.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='ews' AND table_name='messages'")}
        assert {"item_class", "attachments_json"} <= cols
        tables = {r["table_name"] for r in c.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='ews'")}
        assert {"boilerplate_refs", "boilerplate_hits"} <= tables
        requeued = {r["ews_id"] for r in c.execute(
            "SELECT ews_id FROM ews.messages WHERE embedded_at IS NULL")}
    assert requeued == {"R1"}          # short AND in a conversation
    assert db.schema_version() == 4
```

If `tests/test_migrations.py` has no helper to re-run a single migration, implement Step 3's helper and use it; do not weaken the assertion.

- [ ] **Step 2: Run it** — `.venv/bin/python -m pytest tests/test_migrations.py -q -k 004` → FAIL (no such columns / helper).

- [ ] **Step 3: Write the migration and bump the version**

`ewsmcp/migrations/004_phase3.sql`:

```sql
-- ews schema v4 (Phase 3): item class + attachment inventory in the mirror,
-- boilerplate detector tables, and a one-time re-queue of short replies so
-- their chunk 0 gains thread context (see the Phase 3 design, §3).
ALTER TABLE ews.messages ADD COLUMN IF NOT EXISTS item_class text;
ALTER TABLE ews.messages ADD COLUMN IF NOT EXISTS attachments_json text;

CREATE TABLE IF NOT EXISTS ews.boilerplate_refs (
    id          bigserial PRIMARY KEY,
    label       text NOT NULL UNIQUE,
    text        text NOT NULL,
    embedding   public.vector(768) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ews.boilerplate_hits (
    id              bigserial PRIMARY KEY,
    message_ews_id  text NOT NULL,
    detector        text NOT NULL CHECK (detector IN ('embedding', 'llm')),
    paragraph       text NOT NULL,
    similarity      real,
    ref_label       text,
    dropped         smallint NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_hits_created ON ews.boilerplate_hits (created_at DESC);
CREATE INDEX IF NOT EXISTS ix_hits_message ON ews.boilerplate_hits (message_ews_id);

CREATE INDEX IF NOT EXISTS ix_msg_conv_date ON ews.messages (conversation_id, date_ts);

-- Short replies get thread context in chunk 0 from this version on; re-queue
-- them once so the existing index catches up. ~900 rows on the owner's
-- mailbox = 4-5 embed cycles.
UPDATE ews.messages SET embedded_at = NULL
    WHERE length(coalesce(body_clean, '')) < 600 AND conversation_id IS NOT NULL;
```

`ewsmcp/db.py`: `SCHEMA_VERSION = 4`. Add two test-support methods on `Database` if they do not exist (check `grep -n "def " ewsmcp/db.py` first): `schema_version() -> int` (`SELECT max(version)` from `ews.schema_migrations`) and `reapply_last_migration_for_tests()` which deletes the newest row from `schema_migrations` and calls `migrate()` again. All statements in 004 are idempotent (`IF NOT EXISTS`) precisely so this re-application is safe.

`ewsmcp/config.py`, after `archive_cycle_seconds`:

```python
    # --- Phase 3: boilerplate detectors (daemon only) ------------------------
    embed_boilerplate_threshold: float = 0.80
    archive_boilerplate_drop: str = "off"      # off | embedding | llm | both
    archive_boilerplate_llm: bool = True       # run the LLM detector at all
    gemini_clean_model: str = "gemini-2.5-flash-lite"
    # --- Phase 3: hygiene ----------------------------------------------------
    archive_gc_interval_hours: int = 168
    archive_max_item_mb: int = 50
    db_pool_max: int = 8

    @model_validator(mode="after")
    def _check_boilerplate_drop(self) -> "Settings":
        if self.archive_boilerplate_drop not in ("off", "embedding", "llm", "both"):
            raise ValueError("ARCHIVE_BOILERPLATE_DROP must be off|embedding|llm|both")
        return self
```

- [ ] **Step 4: Config test** — add to `tests/test_config.py`:

```python
def test_phase3_settings_defaults_and_drop_enum():
    s = make_settings()
    assert s.archive_boilerplate_drop == "off" and s.embed_boilerplate_threshold == 0.80
    assert s.archive_gc_interval_hours == 168 and s.archive_max_item_mb == 50
    assert s.db_pool_max == 8 and s.gemini_clean_model == "gemini-2.5-flash-lite"
    with pytest.raises(ValueError):
        make_settings(archive_boilerplate_drop="yes")
```

- [ ] **Step 5: Run** — `.venv/bin/python -m pytest tests/test_migrations.py tests/test_config.py tests/test_pg_store.py -q` → PASS. Also run `tests/test_pg_store.py` in full: `upsert_messages` must still work with the two new nullable columns absent from the row dict.

- [ ] **Step 6: Commit** — `feat(schema): migration 004 — item_class, attachments_json, boilerplate tables; Phase 3 settings`

---

### Task 2: Disclaimer tail cut in `bodyclean` (+ anything Task 0 ported)

**Files:**
- Modify: `ewsmcp/bodyclean.py` (after `strip_header_lines`, before `clean_body`)
- Test: `tests/test_bodyclean.py`

**Interfaces:**
- Produces: `tail_paragraphs(text: str) -> list[tuple[int, str]]` — `(char_offset, paragraph)` for paragraphs in the tail window (last 8 paragraphs whose offset ≥ 60 % of `len(text)`; empty when `len(text) < 400`); `strip_disclaimer_tail(text: str) -> tuple[str, bool]`; `clean_body(...)["disclaimer_cut"]: bool`. Later tasks (5, 6) call `tail_paragraphs` on the cleaned body.

- [ ] **Step 1: Failing tests** (append to `tests/test_bodyclean.py`)

```python
RU_FOOTER = (
    "Предоставляемая АО «BCC Invest» информация не является предложением о покупке "
    "и/или обязательством по продаже.\n\n"
    "Содержание этого электронного письма предназначено только для получателей, "
    "указанных в сообщении.\n\n"
    "Бұл электрондық хаттың мазмұны тек хабарламада көрсетілген алушыларға арналған.\n"
)


def _long_body(n_paragraphs=6):
    return "\n\n".join(f"Абзац номер {i}: обсуждаем условия сделки и сроки поставки, "
                       f"а также вопросы по документам." for i in range(n_paragraphs))


def test_disclaimer_tail_is_cut_in_ru_and_kz():
    body = _long_body() + "\n\n" + RU_FOOTER
    out = clean_body(body)
    assert out["disclaimer_cut"] is True
    assert "BCC Invest" not in out["text"] and "алушыларға" not in out["text"]
    assert out["text"].endswith("вопросы по документам.")


def test_disclaimer_anchor_in_the_middle_of_the_body_does_not_cut():
    body = ("Коллеги, документ является конфиденциальным, прошу не пересылать.\n\n"
            + _long_body(8))
    out = clean_body(body)
    assert out["disclaimer_cut"] is False and "Абзац номер 7" in out["text"]


def test_short_message_is_never_cut():
    body = "Ок.\n\nThis message is intended solely for the addressee."
    out = clean_body(body)
    assert out["disclaimer_cut"] is False and "intended solely" in out["text"]


def test_first_paragraph_is_protected_even_when_it_matches():
    body = ("If you are not the intended recipient please tell us — that is the whole "
            "message.\n\n" + _long_body(2))
    out = clean_body(body)
    assert out["text"].startswith("If you are not the intended recipient")


def test_tail_paragraphs_window():
    from ewsmcp.bodyclean import tail_paragraphs
    text = "\n\n".join(f"p{i} " + "x" * 90 for i in range(20))
    tail = tail_paragraphs(text)
    assert 1 <= len(tail) <= 8
    assert all(off >= int(len(text) * 0.6) for off, _p in tail)
    assert tail[-1][1].startswith("p19")
    assert tail_paragraphs("short") == []
```

- [ ] **Step 2: Run** — `.venv/bin/python -m pytest tests/test_bodyclean.py -q -k "disclaimer or tail_paragraphs"` → FAIL.

- [ ] **Step 3: Implement**

```python
_PARA_SPLIT_RE = re.compile(r"\n[ \t]*\n+")
TAIL_MIN_CHARS = 400
TAIL_MAX_PARAGRAPHS = 8
TAIL_START_FRACTION = 0.6

_DISCLAIMER_ANCHOR_RE = re.compile(
    r"(?:не является предложением|предназначено только для получател|"
    r"является конфиденциальн|если вы не являетесь адресатом|"
    r"получили это сообщение по ошибке|"
    r"тек хабарламада көрсетілген алушыларға|құпия ақпарат|"
    r"intended solely for|intended only for the|confidentiality notice|"
    r"if you are not the intended recipient|"
    r"received this (?:e-?mail|message) in error|privileged and confidential)",
    re.IGNORECASE)


def _paragraphs(text: str) -> list[tuple[int, str]]:
    """(char offset, paragraph) for every non-empty blank-line-separated block."""
    out, pos = [], 0
    for m in _PARA_SPLIT_RE.finditer(text):
        chunk = text[pos:m.start()]
        if chunk.strip():
            out.append((pos, chunk))
        pos = m.end()
    if text[pos:].strip():
        out.append((pos, text[pos:]))
    return out


def tail_paragraphs(text: str) -> list[tuple[int, str]]:
    """The tail window the disclaimer rules and detectors may act on: the
    last TAIL_MAX_PARAGRAPHS paragraphs that start in the last 40% of the
    text. Never includes the first paragraph. Empty for short messages."""
    if len(text) < TAIL_MIN_CHARS:
        return []
    paras = _paragraphs(text)
    if len(paras) < 2:
        return []
    floor = int(len(text) * TAIL_START_FRACTION)
    tail = [(off, p) for off, p in paras[1:] if off >= floor]
    return tail[-TAIL_MAX_PARAGRAPHS:]


def strip_disclaimer_tail(text: str) -> tuple[str, bool]:
    for off, para in tail_paragraphs(text):
        if _DISCLAIMER_ANCHOR_RE.search(para):
            return text[:off].rstrip(), True
    return text, False
```

In `clean_body`, after `t = strip_header_lines(t)`:

```python
    t, disclaimer_cut = strip_disclaimer_tail(t)
```

and add `"disclaimer_cut": disclaimer_cut` to the returned dict. Port any patterns Task 0 recommended into `_DISCLAIMER_ANCHOR_RE` (with one golden test each).

- [ ] **Step 4: Run** — `.venv/bin/python -m pytest tests/test_bodyclean.py -q` → PASS (all, including the pre-existing goldens; if a golden changed because its fixture body now matches an anchor in the tail, that fixture was boilerplate — update the expected output and say so in the commit message).

- [ ] **Step 5: Commit** — `feat(bodyclean): cut trailing disclaimers inside a bounded tail window`

---

### Task 3: Hydrate `item_class` and the attachment inventory; backfill

**Files:**
- Modify: `ewsmcp/cache/sync.py` (`HYDRATE_FIELDS`, `hydrate_bodies`, `row_from_message`, new `attachments_json`)
- Modify: `ewsmcp/cache/store.py` (`_UPSERT_MESSAGE` gains the two columns; `update_bodies` gains optional `extra: dict[str, dict]`)
- Modify: `scripts/backfill_bodies.py`
- Test: `tests/test_sync_engine.py`, `tests/test_pg_store.py`

**Interfaces:**
- Produces: `sync.attachments_json(item) -> str` (JSON list of `{name, size, content_type, inline}`); `row_from_message` returns `item_class` and `attachments_json` keys; `CacheStore.update_bodies(bodies, recipients=None, extra=None)` where `extra[ews_id] = {"item_class": str|None, "attachments_json": str|None}`.

- [ ] **Step 1: Failing sync test** (in `tests/test_sync_engine.py`)

```python
class _RaisingContent:
    """A FileAttachment double whose .content is a lazy GetAttachment call —
    the hydrator must NEVER touch it."""
    def __init__(self, name, size, content_type, inline=False):
        self.name, self.size, self.content_type, self.is_inline = name, size, content_type, inline
    @property
    def content(self):
        raise AssertionError("hydration must not download attachment bytes")


def test_hydration_fills_item_class_and_attachment_inventory(db):
    account = _account()
    account.inbox.queue([("create", _msg("M-1", body=None))], "tok-1")

    def fetch(items, only_fields=None, **kw):
        account.fetch_calls.append((len(list(items)), tuple(only_fields or ())))
        return iter([SimpleNamespace(
            id="M-1", text_body="hello", to_recipients=[],
            item_class="IPM.Note",
            attachments=[_RaisingContent("шаблон.xlsx", 789198,
                                         "application/vnd.ms-excel"),
                         SimpleNamespace(name="fwd.eml", size=None,
                                         is_inline=False)])])

    account.fetch = fetch
    engine, store = _engine(db, account)
    asyncio.run(engine._cycle())
    assert account.fetch_calls == [(1, ("text_body", "to_recipients",
                                        "item_class", "attachments"))]
    row = store.get_message("M-1")
    assert row["item_class"] == "IPM.Note"
    assert json.loads(row["attachments_json"]) == [
        {"name": "шаблон.xlsx", "size": 789198,
         "content_type": "application/vnd.ms-excel", "inline": False},
        {"name": "fwd.eml", "size": None, "content_type": "message/rfc822",
         "inline": False},
    ]
```

(`import json` at the top of the test module.) The second attachment is an `ItemAttachment` double: no `content_type` attribute → `message/rfc822` per spec §5. Distinguish by `isinstance(att, FileAttachment)` in production code; in the test the FileAttachment double must therefore be an instance — construct it as `FileAttachment.__new__(FileAttachment)` with attributes set, or check `hasattr(att, "content_type")`. Pick the `isinstance` route and build the double with `__new__`; the daemon side may import exchangelib.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

`ewsmcp/cache/sync.py`:

```python
from exchangelib import FileAttachment  # top of file, with the other imports

HYDRATE_FIELDS = ["text_body", "to_recipients", "item_class", "attachments"]


def attachments_json(item: Any) -> str:
    out = []
    for att in list(getattr(item, "attachments", None) or []):
        if isinstance(att, FileAttachment):
            # Metadata only — `.content` is a lazy GetAttachment round-trip.
            out.append({"name": getattr(att, "name", None) or "attachment",
                        "size": getattr(att, "size", None),
                        "content_type": getattr(att, "content_type", None),
                        "inline": bool(getattr(att, "is_inline", False))})
        else:
            out.append({"name": getattr(att, "name", None) or "attachment",
                        "size": getattr(att, "size", None),
                        "content_type": "message/rfc822",
                        "inline": bool(getattr(att, "is_inline", False))})
    return json.dumps(out, ensure_ascii=False)
```

In `hydrate_bodies`, after the `to_recipients` copy:

```python
            ic = getattr(res, "item_class", None)
            if isinstance(ic, str):
                item.item_class = ic
            if getattr(res, "attachments", None) is not None:
                item.attachments = list(res.attachments)
```

In `row_from_message` add `"item_class": getattr(item, "item_class", None) or None` and `"attachments_json": attachments_json(item)`.

`ewsmcp/cache/store.py`: extend `_UPSERT_MESSAGE` (find it with `grep -n "_UPSERT_MESSAGE" ewsmcp/cache/store.py`) with `item_class` and `attachments_json` in the column list, VALUES and `ON CONFLICT ... DO UPDATE SET`. Extend `update_bodies`:

```python
    def update_bodies(self, bodies, recipients=None, extra=None) -> int:
        ...
                    "  to_json = coalesce(%(to)s, to_json), "
                    "  item_class = coalesce(%(item_class)s, item_class), "
                    "  attachments_json = coalesce(%(atts)s, attachments_json) "
        ...
                    {"ews_id": ews_id, "body": body, "to": recipients.get(ews_id),
                     "item_class": (extra or {}).get(ews_id, {}).get("item_class"),
                     "atts": (extra or {}).get(ews_id, {}).get("attachments_json")}
```

`scripts/backfill_bodies.py`: `_Ref.__slots__` gains `item_class`, `attachments`; build `extra[ref.id] = {"item_class": ref.item_class, "attachments_json": attachments_json(ref) if ref.attachments is not None else None}` and pass `extra` to `update_bodies`. `messages_missing_body` also selects `item_class IS NULL` rows.

- [ ] **Step 4: Store test** (in `tests/test_pg_store.py`)

```python
def test_update_bodies_extra_sets_item_class_and_inventory(db):
    store = CacheStore(db)
    store.upsert_messages([make_row("A1")])
    store.update_bodies({"A1": "body"}, None,
                        {"A1": {"item_class": "IPM.Schedule.Meeting.Resp.Pos",
                                "attachments_json": "[]"}})
    row = store.get_message("A1")
    assert row["item_class"] == "IPM.Schedule.Meeting.Resp.Pos"
    assert row["attachments_json"] == "[]"
```

- [ ] **Step 5: Run** — `.venv/bin/python -m pytest tests/test_sync_engine.py tests/test_pg_store.py tests/test_no_lazy_imports.py -q` → PASS. Run `scripts/backfill_bodies.py --help` under `.venv` to be sure it still parses.

- [ ] **Step 6: Commit** — `feat(sync): hydrate item_class and attachment inventory into the mirror`

---

### Task 4: Tools — inventory from the mirror, calendar items out of search

**Files:**
- Modify: `ewsmcp/tools/cache_reads.py:96-109` (`_row_full`)
- Modify: `ewsmcp/cache/store.py` (`search_messages`, `similar_message_ids`, `_CALENDAR_CLAUSE`)
- Modify: `ewsmcp/tools/mail_read.py` (`_search_messages` signature + schema)
- Modify: `ewsmcp/semantic.py` (`hybrid_search` passes the flag through `**filters`)
- Modify: `docs/API.md` (generated)
- Test: `tests/test_mail_read.py`, `tests/test_pg_store.py`, `tests/test_archive_tools.py`

**Interfaces:**
- Produces: `CacheStore.search_messages(..., include_calendar_items: bool = False)`; `is_calendar_item_class(item_class: str | None) -> bool` in `ewsmcp/cache/store.py`; `similar_message_ids` always excludes calendar items; tool arg `include_calendar_items` (boolean, default false) on `search_messages`.

- [ ] **Step 1: Failing tests**

`tests/test_pg_store.py`:

```python
def test_search_excludes_calendar_items_unless_asked(db):
    store = CacheStore(db)
    store.upsert_messages([make_row("N1", subject="White Hill offer"),
                           make_row("C1", subject="Accepted: White Hill meeting")])
    store.update_bodies({"C1": ""}, None,
                        {"C1": {"item_class": "IPM.Schedule.Meeting.Resp.Pos"}})
    rows, total = store.search_messages(subject="white hill")
    assert [r["ews_id"] for r in rows] == ["N1"] and total == 1
    rows, total = store.search_messages(subject="white hill", include_calendar_items=True)
    assert total == 2
```

`tests/test_mail_read.py` — extend the cached `get_message` test (find it with `grep -n "attachments_hint" tests/test_mail_read.py`) so a live row with `attachments_json` returns `attachments` and no hint:

```python
def test_get_message_from_mirror_lists_attachments_without_a_live_call(db):
    ctx = make_context(db)
    row = make_row("A1", has_attachments=1)
    ctx.cache.upsert_messages([row])
    ctx.cache.update_bodies({"A1": "body"}, None, {"A1": {"attachments_json": json.dumps(
        [{"name": "шаблон.xlsx", "size": 10, "content_type": "x/y", "inline": False}])}})
    res = _run(ctx, "get_message", id="A1")
    assert res["attachments"] == [{"name": "шаблон.xlsx", "size": 10,
                                   "content_type": "x/y", "inline": False}]
    assert "attachments_hint" not in res
    assert ctx.gateway.calls == []          # FakeGateway records calls
```

`tests/test_archive_tools.py` — in the existing `find_similar` test add a calendar row with a near-identical subject/body and assert it is absent from the results.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

`ewsmcp/cache/store.py`:

```python
CALENDAR_CLASS_PREFIXES = ("IPM.Schedule.Meeting", "IPM.Appointment")
_CALENDAR_EXCLUDE = ("(m.item_class IS NULL OR NOT (m.item_class LIKE 'IPM.Schedule.Meeting%' "
                     "OR m.item_class LIKE 'IPM.Appointment%'))")


def is_calendar_item_class(item_class: str | None) -> bool:
    return bool(item_class) and item_class.startswith(CALENDAR_CLASS_PREFIXES)
```

`search_messages(..., include_calendar_items: bool = False)`: `if not include_calendar_items: where.append(_CALENDAR_EXCLUDE)`. `similar_message_ids`: append `AND {_CALENDAR_EXCLUDE}` to the outer `WHERE {clause}`.

`_row_full`: replace the `else:` hint branch with

```python
        else:
            try:
                inv = json.loads(row["attachments_json"] or "[]")
            except ValueError:
                inv = []
            if inv:
                full["attachments"] = inv
            else:
                full["attachments_hint"] = ("inventory not synced yet — call again "
                                            "with fresh=true, or get_attachment")
```

`ewsmcp/tools/mail_read.py`: add `include_calendar_items: bool = False` to `_search_messages` and pass it to both the keyword store call and `hybrid_search(**filters)`; schema entry:

```python
            "include_calendar_items": {
                "type": "boolean", "default": False,
                "description": "Meeting requests/responses (Accepted:, Declined:, "
                               "cancellations) are hidden by default; set true to "
                               "search them too.",
            },
```

`docs/API.md`: `.venv/bin/python scripts/dump_tool_table.py --write`.

- [ ] **Step 4: Run** — `.venv/bin/python -m pytest tests/test_pg_store.py tests/test_mail_read.py tests/test_archive_tools.py tests/test_semantic.py -q && .venv/bin/python scripts/dump_tool_table.py --check` → PASS.

- [ ] **Step 5: Commit** — `feat(tools): attachment inventory from the mirror; calendar items out of search by default`

---

### Task 5: Thread context in chunk 0

**Files:**
- Modify: `ewsmcp/embeddings.py:44-54` (`chunk_text`)
- Modify: `ewsmcp/cache/store.py` (`parent_in_thread`, `unembedded_messages` selects `conversation_id`, `date_ts`)
- Modify: `ewsmcp/semantic.py:48-66` (`index_messages`)
- Test: `tests/test_semantic.py`, `tests/test_pg_store.py`

**Interfaces:**
- Produces: `chunk_text(subject, body, chunk_chars=CHUNK_CHARS, context: str | None = None)`; `CacheStore.parent_in_thread(ews_id) -> dict | None`; constants `THREAD_CONTEXT_MAX_BODY = 600`, `THREAD_CONTEXT_PARENT_CHARS = 300` in `ewsmcp/semantic.py`.

- [ ] **Step 1: Failing tests**

`tests/test_pg_store.py`:

```python
def test_parent_in_thread_is_the_latest_earlier_message(db):
    store = CacheStore(db)
    store.upsert_messages([make_row("P0", conv="C", date_ts=100),
                           make_row("P1", conv="C", date_ts=200),
                           make_row("R", conv="C", date_ts=300),
                           make_row("X", conv="D", date_ts=250)])
    assert store.parent_in_thread("R")["ews_id"] == "P1"
    assert store.parent_in_thread("P0") is None
```

`tests/test_semantic.py` (uses `FakeEmbedder`; follow the file's existing `SemanticIndex(store, FakeEmbedder())` setup):

```python
def test_short_reply_chunk0_carries_parent_context(db):
    store = CacheStore(db)
    store.upsert_messages([
        make_row("P", conv="C", date_ts=100, subject="Прогноз - инвестиции в ДО",
                 body="Просим предоставить прогнозы по докапитализации ваших ДО. " * 3),
        make_row("R", conv="C", date_ts=200, subject="RE: Прогноз - инвестиции в ДО",
                 body="Добрый день. 1. 33,3 млрд тенге."),
        make_row("L", conv="C", date_ts=300, subject="RE: long", body="x " * 400),
    ])
    index = SemanticIndex(store, FakeEmbedder())
    index.index_messages(store.unembedded_messages(10))
    with store.db.conn() as c:
        texts = {r["message_ews_id"]: r["text"] for r in c.execute(
            "SELECT message_ews_id, text FROM ews.chunks WHERE seq = 0")}
    assert "In reply to: Прогноз - инвестиции в ДО" in texts["R"]
    assert "докапитализации" in texts["R"]
    assert "In reply to" not in texts["P"]          # no parent
    assert "In reply to" not in texts["L"]          # too long
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

`ewsmcp/embeddings.py`:

```python
def chunk_text(subject: str, body: str, chunk_chars: int = CHUNK_CHARS,
               context: str | None = None) -> list[str]:
    """``subject + "\\n" + body`` (+ an optional context block appended to
    the text before chunking) split into fixed-width character chunks."""
    subject = (subject or "").strip()
    body = body or ""
    text = f"{subject}\n{body}" if subject and body else (subject or body)
    text = text.strip("\n") if not subject or not body else text
    if context:
        text = f"{text}\n\n{context}"
    if not text.strip():
        return []
    size = max(1, int(chunk_chars))
    return [text[i:i + size] for i in range(0, len(text), size)]
```

`ewsmcp/cache/store.py`:

```python
    def parent_in_thread(self, ews_id: str) -> dict[str, Any] | None:
        """The latest message in the same conversation dated before this one."""
        with self.db.conn() as c:
            return c.execute(
                "SELECT p.ews_id, p.subject, p.body_clean FROM ews.messages m "
                "JOIN ews.messages p ON p.conversation_id = m.conversation_id "
                "  AND p.date_ts < m.date_ts AND p.ews_id <> m.ews_id "
                "WHERE m.ews_id = %s AND m.conversation_id IS NOT NULL "
                "ORDER BY p.date_ts DESC LIMIT 1", (ews_id,)).fetchone()
```

`unembedded_messages` selects `ews_id, subject, body_clean, conversation_id`.

`ewsmcp/semantic.py`:

```python
THREAD_CONTEXT_MAX_BODY = 600
THREAD_CONTEXT_PARENT_CHARS = 300


def _thread_context(store: CacheStore, row: dict[str, Any]) -> str | None:
    body = row.get("body_clean") or ""
    if len(body) >= THREAD_CONTEXT_MAX_BODY or not row.get("conversation_id"):
        return None
    parent = store.parent_in_thread(row["ews_id"])
    if parent is None:
        return None
    head = (parent.get("body_clean") or "")[:THREAD_CONTEXT_PARENT_CHARS].strip()
    return f"In reply to: {(parent.get('subject') or '').strip()}\n{head}".rstrip()
```

and in `index_messages`: `chunk_text(r.get("subject") or "", r.get("body_clean") or "", self.chunk_chars, context=_thread_context(self.store, r))`.

- [ ] **Step 4: Run** — `.venv/bin/python -m pytest tests/test_semantic.py tests/test_pg_store.py tests/test_archive_runner.py -q` → PASS.

- [ ] **Step 5: Commit** — `feat(semantic): short replies embed with their parent's subject and opening lines`

---

### Task 6: Boilerplate harness — tail paragraphs, refs/hits store, embedding detector, seed script

**Files:**
- Create: `ewsmcp/boilerplate.py`
- Create: `scripts/seed_boilerplate.py`, `scripts/boilerplate_refs.txt`
- Modify: `ewsmcp/cache/store.py` (refs/hits methods)
- Modify: `ewsmcp/semantic.py` (`index_messages` runs the harness)
- Modify: `ewsmcp/server.py` (construct `BoilerplateHarness` and pass to `SemanticIndex`)
- Test: `tests/test_boilerplate.py` (new), `tests/test_semantic.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class Hit:
    detector: str            # "embedding" | "llm"
    paragraph_index: int     # index into tail_paragraphs(text)
    paragraph: str
    similarity: float | None
    ref_label: str | None

class EmbeddingDetector:
    def __init__(self, refs: list[tuple[str, list[float]]], threshold: float): ...
    def detect(self, paragraphs: list[str], vectors: list[list[float]]) -> list[Hit]: ...

class BoilerplateHarness:
    def __init__(self, store, embedder, *, threshold: float, drop: str,
                 llm: "LlmDetector | None" = None): ...
    def refresh_refs(self) -> None: ...
    def analyse(self, ews_id: str, text: str) -> tuple[str, list[Hit]]:
        """Returns (text_to_chunk, hits). Logs every hit. Cuts only per `drop`."""
```

- `CacheStore.boilerplate_refs() -> list[dict]`, `upsert_boilerplate_ref(label, text, embedding)`, `log_boilerplate_hits(ews_id, hits: list[Hit], dropped: bool)`, `boilerplate_stats(days=7) -> dict` (`{"embedding": {"hits", "dropped"}, "llm": {"hits", "dropped", "errors"}}` — `errors` counted from hits rows with `ref_label LIKE 'error:%'`).

- [ ] **Step 1: Failing tests** (`tests/test_boilerplate.py`)

```python
import math
from ewsmcp.boilerplate import BoilerplateHarness, EmbeddingDetector, Hit
from ewsmcp.cache.store import CacheStore
from conftest import FakeEmbedder, make_row

BODY = ("Коллеги, добрый день.\n\nПо итогам встречи направляю обновлённую модель. "
        "Прошу посмотреть допущения на вкладке 2 и вернуться с комментариями до пятницы.\n\n"
        "Отдельно обращаю внимание на сроки согласования с юристами: они просят две недели.\n\n"
        "С уважением, Аскар.\n\n"
        "Предоставляемая АО «BCC Invest» информация не является предложением о покупке "
        "и/или обязательством по продаже ценных бумаг.")


def _unit(v):
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


def test_embedding_detector_flags_only_paragraphs_above_threshold():
    ref = _unit([1.0] + [0.0] * 767)
    det = EmbeddingDetector([("bcc_invest_ru", ref)], threshold=0.80)
    near = _unit([0.95, 0.31] + [0.0] * 766)
    far = _unit([0.1, 0.99] + [0.0] * 766)
    hits = det.detect(["p0", "p1"], [far, near])
    assert [h.paragraph_index for h in hits] == [1]
    assert hits[0].ref_label == "bcc_invest_ru" and hits[0].similarity > 0.8


def test_harness_logs_hits_and_cuts_only_when_drop_names_the_detector(db):
    store = CacheStore(db)
    store.upsert_messages([make_row("A1", body=BODY)])
    emb = FakeEmbedder()
    footer = BODY.split("\n\n")[-1]
    store.upsert_boilerplate_ref("bcc_invest_ru", footer, emb.embed([footer])[0])

    h_off = BoilerplateHarness(store, emb, threshold=0.80, drop="off")
    text, hits = h_off.analyse("A1", BODY)
    assert text == BODY and [h.detector for h in hits] == ["embedding"]
    assert store.boilerplate_stats()["embedding"] == {"hits": 1, "dropped": 0}

    h_on = BoilerplateHarness(store, emb, threshold=0.80, drop="embedding")
    text, hits = h_on.analyse("A1", BODY)
    assert "BCC Invest" not in text and text.endswith("С уважением, Аскар.")
    assert store.boilerplate_stats()["embedding"] == {"hits": 2, "dropped": 1}


def test_harness_never_drops_the_first_paragraph(db):
    store = CacheStore(db)
    emb = FakeEmbedder()
    short = "Предоставляемая АО «BCC Invest» информация не является предложением."
    store.upsert_boilerplate_ref("x", short, emb.embed([short])[0])
    h = BoilerplateHarness(store, emb, threshold=0.5, drop="embedding")
    text, hits = h.analyse("A1", short)      # under TAIL_MIN_CHARS: no tail window
    assert text == short and hits == []
```

- [ ] **Step 2: Run** → FAIL (module missing).

- [ ] **Step 3: Implement**

`ewsmcp/boilerplate.py`:

```python
"""Boilerplate detectors that run beside chunking (Phase 3 §2/§2b).

Two detectors look at the tail paragraphs of a cleaned body and say where
gateway/legal boilerplate begins. Every hit is logged; a paragraph is dropped
from the text that gets CHUNKED — never from the stored body — only when
ARCHIVE_BOILERPLATE_DROP names that detector. The first paragraph of a
message is never in the tail window, so it can never be dropped.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any

from .bodyclean import tail_paragraphs

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Hit:
    detector: str
    paragraph_index: int
    paragraph: str
    similarity: float | None
    ref_label: str | None


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(x * x for x in b)) or 1.0
    return dot / (na * nb)


class EmbeddingDetector:
    def __init__(self, refs: list[tuple[str, list[float]]], threshold: float) -> None:
        self.refs = list(refs)
        self.threshold = float(threshold)

    def detect(self, paragraphs: list[str], vectors: list[list[float]]) -> list[Hit]:
        hits: list[Hit] = []
        for i, (para, vec) in enumerate(zip(paragraphs, vectors)):
            best_label, best = None, -1.0
            for label, ref in self.refs:
                s = cosine(vec, ref)
                if s > best:
                    best_label, best = label, s
            if best_label is not None and best >= self.threshold:
                hits.append(Hit("embedding", i, para, round(best, 4), best_label))
        return hits


class BoilerplateHarness:
    def __init__(self, store: Any, embedder: Any, *, threshold: float, drop: str,
                 llm: Any | None = None) -> None:
        self.store, self.embedder, self.threshold, self.drop = store, embedder, threshold, drop
        self.llm = llm
        self._refs_stamp: Any = None
        self._detector = EmbeddingDetector([], threshold)
        self.refresh_refs()

    def refresh_refs(self) -> None:
        rows = self.store.boilerplate_refs()
        stamp = max((r["created_at"] for r in rows), default=None)
        if stamp != self._refs_stamp or not self._detector.refs:
            self._detector = EmbeddingDetector(
                [(r["label"], list(r["embedding"])) for r in rows], self.threshold)
            self._refs_stamp = stamp

    def analyse(self, ews_id: str, text: str) -> tuple[str, list[Hit]]:
        tail = tail_paragraphs(text)
        if not tail:
            return text, []
        self.refresh_refs()
        paragraphs = [p.strip() for _off, p in tail]
        hits: list[Hit] = []
        if self._detector.refs:
            vectors = self.embedder.embed(paragraphs)
            hits.extend(self._detector.detect(paragraphs, vectors))
        if self.llm is not None:
            hits.extend(self.llm.detect(ews_id, text.split("\n\n", 1)[0], paragraphs))
        cut_from: int | None = None
        for h in hits:
            allowed = self.drop == "both" or self.drop == h.detector
            if allowed and (cut_from is None or h.paragraph_index < cut_from):
                cut_from = h.paragraph_index
        dropped = cut_from is not None
        if hits:
            self.store.log_boilerplate_hits(ews_id, hits, dropped)
        if not dropped:
            return text, hits
        off = tail[cut_from][0]
        return text[:off].rstrip(), hits
```

`ewsmcp/cache/store.py` — the refs/hits methods (vectors go through the existing `_vector_literal`; reading `embedding` back returns a string like `[0.1,0.2,...]` from pgvector — parse it with `json.loads` in `boilerplate_refs()` so `list(r["embedding"])` in the harness is a list of floats):

```python
    def boilerplate_refs(self) -> list[dict[str, Any]]:
        with self.db.conn() as c:
            rows = c.execute("SELECT label, text, embedding::text AS embedding, created_at "
                             "FROM ews.boilerplate_refs ORDER BY id").fetchall()
        for r in rows:
            r["embedding"] = json.loads(r["embedding"])
        return rows

    def upsert_boilerplate_ref(self, label: str, text: str, embedding: list[float]) -> None:
        with self.db.conn() as c:
            c.execute("INSERT INTO ews.boilerplate_refs (label, text, embedding) "
                      "VALUES (%s, %s, %s::vector) ON CONFLICT (label) DO UPDATE SET "
                      "text = EXCLUDED.text, embedding = EXCLUDED.embedding, "
                      "created_at = now()", (label, text, _vector_literal(embedding)))

    def log_boilerplate_hits(self, ews_id: str, hits: list[Any], dropped: bool) -> None:
        if not hits:
            return
        with self.db.conn() as c:
            c.cursor().executemany(
                "INSERT INTO ews.boilerplate_hits (message_ews_id, detector, paragraph, "
                "similarity, ref_label, dropped) VALUES (%s, %s, %s, %s, %s, %s)",
                [(ews_id, h.detector, h.paragraph[:2000], h.similarity, h.ref_label,
                  1 if dropped else 0) for h in hits])

    def boilerplate_stats(self, days: int = 7) -> dict[str, dict[str, int]]:
        with self.db.conn() as c:
            rows = c.execute(
                "SELECT detector, COUNT(*) AS hits, "
                "  COUNT(*) FILTER (WHERE dropped = 1) AS dropped, "
                "  COUNT(*) FILTER (WHERE ref_label LIKE 'error:%%') AS errors "
                "FROM ews.boilerplate_hits WHERE created_at > now() - make_interval(days => %s) "
                "GROUP BY detector", (int(days),)).fetchall()
        out = {"embedding": {"hits": 0, "dropped": 0},
               "llm": {"hits": 0, "dropped": 0, "errors": 0}}
        for r in rows:
            d = out.setdefault(r["detector"], {})
            d["hits"], d["dropped"] = int(r["hits"]), int(r["dropped"])
            if r["detector"] == "llm":
                d["errors"] = int(r["errors"])
        return out
```

`ewsmcp/semantic.py`: `SemanticIndex.__init__(..., harness: BoilerplateHarness | None = None)`; in `index_messages`, for each row: `text = body; if self.harness: text, _hits = self.harness.analyse(r["ews_id"], body)`, then `chunk_text(subject, text, ..., context=...)`. Note the harness embeds tail paragraphs in its own `embed()` call; that is acceptable for this phase (the spec's "same batch" is an optimisation, not a requirement — record this as a ruling in the ledger).

`ewsmcp/server.py`: where `SemanticIndex` is built (grep `SemanticIndex(`), build `BoilerplateHarness(store, embedder, threshold=settings.embed_boilerplate_threshold, drop=settings.archive_boilerplate_drop, llm=None)` and pass it. Task 7 fills `llm`.

`scripts/boilerplate_refs.txt`: one ref per block, `## <label>` line followed by the text, blank line between blocks. Seed it with the BCC Invest RU footer, the KZ footer line, the RU "предназначено только для получателей" line, the KZ/RU/EN gateway banner lines, and the English "confidentiality notice" paragraph from the spec (take the real texts from the mirror: `docker exec postgres-ews psql -U ews -d ews -Atc "select body_clean from ews.messages where body_clean ilike '%BCC Invest%' limit 1"`).

`scripts/seed_boilerplate.py` (piped into `docker exec -i ewsd python -` like the backfill; it therefore embeds the refs text inline rather than reading a file — put the blocks in a module-level `REFS = [("label", "text"), ...]` list and keep `boilerplate_refs.txt` as the human-readable source with a comment saying the two must match): builds `GeminiEmbedder` from `Settings`, calls `store.upsert_boilerplate_ref` per block, prints the labels.

- [ ] **Step 4: Semantic test** — in `tests/test_semantic.py` add a test that `index_messages` with a harness whose `drop="embedding"` produces chunks without the footer and logs one hit; with `drop="off"` the chunk contains the footer and still logs.

- [ ] **Step 5: Run** — `.venv/bin/python -m pytest tests/test_boilerplate.py tests/test_semantic.py tests/test_pg_store.py tests/test_no_lazy_imports.py tests/test_mcp_import_boundary.py -q` → PASS.

- [ ] **Step 6: Commit** — `feat(boilerplate): embedding detector, hit log, seed script; harness wired into indexing (log-only)`

---

### Task 7: LLM boundary detector, drop enum end-to-end, report script, status block

**Files:**
- Modify: `ewsmcp/boilerplate.py` (`GeminiCleaner`, `LlmDetector`)
- Modify: `ewsmcp/server.py` (build `LlmDetector` when `archive_boilerplate_llm` and key present)
- Modify: `ewsmcp/archive/runner.py` (`status()["boilerplate"]`), `ewsmcp/mcp/local.py` (`_ARCHIVE_RUNNER_KEYS_TO_DROP` unchanged — `boilerplate` stays inside `runner`), `ewsmcp/tools/archive.py` (`_archive_status` copies `boilerplate` to the top level on the daemon side)
- Create: `scripts/boilerplate_report.py`
- Test: `tests/test_boilerplate.py`, `tests/test_archive_runner.py`, `tests/test_embeddings.py` (for the HTTP client, mirroring how `GeminiEmbedder` is tested with a fake httpx client)

**Interfaces:**
- Produces:

```python
class GeminiCleaner:
    def __init__(self, api_key: str, *, model: str, client=None, timeout: float = 10.0): ...
    def boundary(self, first_paragraph: str, paragraphs: list[str]) -> dict:
        """POST generateContent with responseMimeType=application/json and a
        responseSchema {drop_from: integer|null, reason: string}; returns the
        parsed dict. Raises on HTTP error/timeout/invalid JSON."""

class LlmDetector:
    def __init__(self, cleaner: GeminiCleaner): ...
    def detect(self, ews_id: str, first_paragraph: str, paragraphs: list[str]) -> list[Hit]:
        """One Hit (detector='llm', similarity=None, ref_label=reason) when the
        answer validates; on any error returns one Hit with
        paragraph_index=-1 and ref_label='error:<type>' so it is LOGGED but
        can never cause a drop (the harness ignores negative indexes)."""
```

- [ ] **Step 1: Failing tests** (`tests/test_boilerplate.py`)

```python
class _ScriptedCleaner:
    def __init__(self, answers):
        self.answers, self.calls = list(answers), []
    def boundary(self, first_paragraph, paragraphs):
        self.calls.append((first_paragraph, list(paragraphs)))
        a = self.answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a


def test_llm_detector_accepts_only_validated_answers():
    from ewsmcp.boilerplate import LlmDetector
    paras = ["thanks", "Best regards, X", "CONFIDENTIALITY NOTICE: ..."]
    det = LlmDetector(_ScriptedCleaner([
        {"drop_from": 2, "reason": "legal footer"},
        {"drop_from": 7, "reason": "out of range"},
        {"drop_from": None, "reason": "nothing"},
        {"garbage": True},
        TimeoutError("slow"),
    ]))
    ok = det.detect("m", "first", paras)
    assert ok[0].paragraph_index == 2 and ok[0].ref_label == "legal footer"
    assert det.detect("m", "first", paras)[0].paragraph_index == -1   # out of range
    assert det.detect("m", "first", paras) == []                        # null = no hit
    assert det.detect("m", "first", paras)[0].ref_label.startswith("error:")
    assert det.detect("m", "first", paras)[0].ref_label == "error:TimeoutError"


def test_harness_drop_enum_llm_and_both(db):
    store = CacheStore(db)
    emb = FakeEmbedder()
    footer = BODY.split("\n\n")[-1]
    store.upsert_boilerplate_ref("bcc", footer, emb.embed([footer])[0])
    from ewsmcp.boilerplate import LlmDetector
    tail_n = len(tail_paragraphs(BODY))
    llm = LlmDetector(_ScriptedCleaner([{"drop_from": tail_n - 2, "reason": "sig+footer"}] * 3))
    text, hits = BoilerplateHarness(store, emb, threshold=0.8, drop="llm", llm=llm).analyse("A", BODY)
    assert "С уважением" not in text                       # llm cut earlier than embedding
    text, _ = BoilerplateHarness(store, emb, threshold=0.8, drop="embedding", llm=llm).analyse("A", BODY)
    assert "С уважением" in text and "BCC Invest" not in text
    text, _ = BoilerplateHarness(store, emb, threshold=0.8, drop="both", llm=llm).analyse("A", BODY)
    assert "С уважением" not in text
    assert store.boilerplate_stats()["llm"]["hits"] == 3
```

(`from ewsmcp.bodyclean import tail_paragraphs` at the top.)

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

In `ewsmcp/boilerplate.py`:

```python
import json
import httpx

GEMINI_GENERATE_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_PROMPT = (
    "You see the FIRST paragraph of an email and its LAST paragraphs, numbered. "
    "Answer with JSON {\"drop_from\": <index or null>, \"reason\": <short string>}: "
    "drop_from is the index of the first numbered paragraph where signature, "
    "legal disclaimer, or mail-gateway boilerplate begins and continues to the end. "
    "If the numbered paragraphs are all real message content, answer null. "
    "Never pick a paragraph that contains the writer's actual message.\n\n"
    "FIRST PARAGRAPH:\n{first}\n\nLAST PARAGRAPHS:\n{numbered}")


class GeminiCleaner:
    def __init__(self, api_key: str, *, model: str, client: Any | None = None,
                 timeout: float = 10.0) -> None:
        if not api_key:
            raise ValueError("GeminiCleaner needs an API key")
        self.model, self.timeout = model, float(timeout)
        self._client = client or httpx.Client(timeout=timeout)
        self._headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}

    def boundary(self, first_paragraph: str, paragraphs: list[str]) -> dict[str, Any]:
        numbered = "\n\n".join(f"[{i}] {p}" for i, p in enumerate(paragraphs))
        body = {
            "contents": [{"parts": [{"text": _PROMPT.format(first=first_paragraph[:1000],
                                                             numbered=numbered[:6000])}]}],
            "generationConfig": {
                "temperature": 0,
                "responseMimeType": "application/json",
                "responseSchema": {"type": "OBJECT", "properties": {
                    "drop_from": {"type": "INTEGER", "nullable": True},
                    "reason": {"type": "STRING"}}, "required": ["reason"]},
            },
        }
        r = self._client.post(GEMINI_GENERATE_URL.format(model=self.model),
                              headers=self._headers, json=body, timeout=self.timeout)
        r.raise_for_status()
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(text)


class LlmDetector:
    def __init__(self, cleaner: Any) -> None:
        self.cleaner = cleaner

    def detect(self, ews_id: str, first_paragraph: str, paragraphs: list[str]) -> list[Hit]:
        try:
            ans = self.cleaner.boundary(first_paragraph, paragraphs)
            if not isinstance(ans, dict) or "reason" not in ans:
                raise ValueError("malformed answer")
            idx = ans.get("drop_from")
            if idx is None:
                return []
            idx = int(idx)
            reason = str(ans.get("reason") or "")[:200]
            if not 0 <= idx < len(paragraphs):
                return [Hit("llm", -1, "", None, f"error:index {idx} out of range")]
            return [Hit("llm", idx, paragraphs[idx], None, reason or "boilerplate")]
        except Exception as exc:  # noqa: BLE001 - a detector never breaks indexing
            logger.info("llm detector error for %s: %s", ews_id, type(exc).__name__)
            return [Hit("llm", -1, "", None, f"error:{type(exc).__name__}")]
```

In `BoilerplateHarness.analyse`, the cut loop must skip `h.paragraph_index < 0`. `httpx` is already a dependency (embeddings). The `error:` hits have an empty paragraph; `log_boilerplate_hits` stores them as-is (that is how `errors_7d` is counted).

`ewsmcp/server.py`: after the harness is built, `if settings.archive_boilerplate_llm and settings.gemini_api_key: harness.llm = LlmDetector(GeminiCleaner(settings.gemini_api_key, model=settings.gemini_clean_model))`.

`ewsmcp/archive/runner.py` `status()`: add `"boilerplate": {**self.store.boilerplate_stats(), "drop_detector": self.settings.archive_boilerplate_drop, "threshold": float(self.settings.embed_boilerplate_threshold)}`. Because `status()` is sync and called from the HTTP handler, do the DB read in `disk_stats`-style caching: compute in `_loop` after each cycle into `self._boilerplate_cache` and return the cache (empty dict before the first cycle). `tools/archive.py::_archive_status` copies `runner["boilerplate"]` to `out["boilerplate"]`; the MCP's `local.py` already forwards the whole runner block.

`scripts/boilerplate_report.py` (piped into ewsd like the others): prints per detector hits/dropped/errors for the last 14 days, the number of messages hit by one detector and not the other, and 20 random paragraphs per detector (`ORDER BY random() LIMIT 20`). Pure SQL through `CacheStore.db.conn()`.

- [ ] **Step 4: Client test** — in `tests/test_embeddings.py` add a `GeminiCleaner` test with a fake httpx client (copy the fake used for `GeminiEmbedder`) asserting the URL contains the model, the header carries the key, the body sets `temperature: 0` and `responseMimeType`, and the parsed dict is returned; plus one asserting a 500 raises. Runner test: `status()["boilerplate"]["drop_detector"] == "off"` after one cycle.

- [ ] **Step 5: Run** — `.venv/bin/python -m pytest tests/test_boilerplate.py tests/test_embeddings.py tests/test_archive_runner.py tests/test_archive_tools.py tests/test_mcp_thin.py -q` → PASS.

- [ ] **Step 6: Commit** — `feat(boilerplate): Gemini boundary detector beside the embedding one; drop enum; status + report`

---

### Task 8: Orphan blob GC lane

**Files:**
- Create: `ewsmcp/archive/gc.py`
- Modify: `ewsmcp/archive/runner.py` (`KINDS` += `"gc"`, gc lane in `_loop` on its own interval, `status()["gc"]`), `ewsmcp/cache/store.py` (`referenced_mime_shas()`, `referenced_blob_shas()`; `archive_runs.kind` CHECK must accept `gc` — add `ALTER TABLE ... DROP CONSTRAINT / ADD CONSTRAINT` to migration 004 in Task 1 if not already done; do it now and re-run the migration test)
- Modify: `ewsmcp/tools/archive.py` (`archive_run` accepts `kind="gc"`; its schema enum)
- Test: `tests/test_archive_gc.py` (new), `tests/test_archive_runner.py`, `tests/test_archive_tools.py`

**Interfaces:**
- Produces: `GcWorker(settings, store).run(dry_run: bool) -> {"scanned": int, "removed_files": int, "removed_bytes": int, "kept_recent": int, "error": None}`; runner `status()["gc"] = {"last_run_ts", "removed_files", "removed_bytes"}`.

- [ ] **Step 1: Failing test** (`tests/test_archive_gc.py`)

```python
import os, time
from ewsmcp.archive import files
from ewsmcp.archive.gc import GcWorker
from ewsmcp.cache.store import CacheStore
from conftest import make_row, make_settings


def _old(path, hours=48):
    t = time.time() - hours * 3600
    os.utime(path, (t, t))


def test_gc_removes_only_unreferenced_files_older_than_a_day(db, tmp_path):
    settings = make_settings(data_dir=str(tmp_path / "data"))
    store = CacheStore(db)
    ref_sha, ref_path = files.store_mime(settings.data_dir, b"referenced")
    orphan_sha, orphan_path = files.store_mime(settings.data_dir, b"orphan")
    fresh_sha, fresh_path = files.store_mime(settings.data_dir, b"fresh orphan")
    blob_sha, blob_path = files.store_blob(settings.data_dir, b"att")
    orphan_blob_sha, orphan_blob = files.store_blob(settings.data_dir, b"att-orphan")
    for p in (ref_path, orphan_path, blob_path, orphan_blob):
        _old(p)
    store.upsert_messages([make_row("A1")])
    store.mark_captured("A1", mime_sha256=ref_sha, mime_path=str(ref_path))
    store.replace_attachments("A1", [{"name": "a", "content_type": "x", "size": 3,
                                      "sha256": blob_sha, "is_inline": 0}])

    dry = asyncio.run(GcWorker(settings, store).run(dry_run=True))
    assert dry["removed_files"] == 2 and orphan_path.exists()
    out = asyncio.run(GcWorker(settings, store).run(dry_run=False))
    assert out["removed_files"] == 2 and out["removed_bytes"] == len(b"orphan") + len(b"att-orphan")
    assert ref_path.exists() and blob_path.exists() and fresh_path.exists()
    assert not orphan_path.exists() and not orphan_blob.exists()
    assert out["kept_recent"] == 1
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

`ewsmcp/archive/gc.py`:

```python
"""Orphan blob GC: files under mime/ and blobs/ that no row references.

A verification failure resets a row to live and forgets its capture; the
content-addressed files stay behind. Once a week this lane removes every
unreferenced file older than GC_MIN_AGE_S (a capture in flight is never that
old). Referenced = every messages.mime_sha256 and every attachments.sha256.
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from . import files

logger = logging.getLogger(__name__)
GC_MIN_AGE_S = 24 * 3600


class GcWorker:
    def __init__(self, settings: Any, store: Any) -> None:
        self.settings, self.store = settings, store

    async def run(self, *, dry_run: bool = True) -> dict[str, Any]:
        return await asyncio.to_thread(self._run, dry_run)

    def _run(self, dry_run: bool) -> dict[str, Any]:
        out = {"scanned": 0, "removed_files": 0, "removed_bytes": 0,
               "kept_recent": 0, "error": None}
        root = Path(self.settings.data_dir)
        keep_mime = set(self.store.referenced_mime_shas())
        keep_blob = set(self.store.referenced_blob_shas())
        now = time.time()
        for dirname, keep, key in ((files.MIME_DIRNAME, keep_mime, lambda p: p.name.split(".")[0]),
                                   (files.BLOB_DIRNAME, keep_blob, lambda p: p.name)):
            base = root / dirname
            if not base.is_dir():
                continue
            for p in base.rglob("*"):
                if not p.is_file():
                    continue
                out["scanned"] += 1
                if key(p) in keep:
                    continue
                if now - p.stat().st_mtime < GC_MIN_AGE_S:
                    out["kept_recent"] += 1
                    continue
                size = p.stat().st_size
                out["removed_files"] += 1
                out["removed_bytes"] += size
                if not dry_run:
                    try:
                        p.unlink()
                    except OSError as exc:
                        logger.warning("gc could not remove %s: %s", p, exc)
        return out
```

Store: `referenced_mime_shas()` = `SELECT DISTINCT mime_sha256 FROM ews.messages WHERE mime_sha256 IS NOT NULL`; `referenced_blob_shas()` = `SELECT DISTINCT sha256 FROM ews.attachments WHERE sha256 IS NOT NULL`. (Check `files.store_mime` names files `<sha>.eml` and blobs `<sha[:2]>/<sha>`; adjust `key` if not.)

Runner: `KINDS = ("capture", "verify", "delete", "embed", "gc", "all")`; `run_once(kind="gc")` runs only the GC (also inside `"all"`? — **no**: GC runs on its own interval, never in every cycle). In `_loop`, after the cycle: `if time.time() - self._last_gc_ts >= settings.archive_gc_interval_hours * 3600: run GC (dry_run=False) via _run_once_locked(kind="gc")`. `finish_run` records `captured=0...`; put `removed_files` into the run's `sample` as `[{"removed_files": n, "removed_bytes": b}]`. `status()["gc"]` from the last result. Migration 004: `ALTER TABLE ews.archive_runs DROP CONSTRAINT IF EXISTS archive_runs_kind_check; ALTER TABLE ews.archive_runs ADD CONSTRAINT archive_runs_kind_check CHECK (kind IN ('capture','verify','delete','embed','gc','all'));` (constraint name: verify with `\d ews.archive_runs`).

`tools/archive.py`: `archive_run` kind enum gains `gc`; `dry_run=false` for `gc` needs no confirmation token? **Ruling:** it deletes files under DATA_DIR, so it keeps the same two-phase confirmation as every non-dry run (no special case).

- [ ] **Step 4: Tests** — runner: `run_once(kind="gc", dry_run=True)` returns a run row with `kind='gc'`; the loop with `archive_gc_interval_hours=0` runs GC once per cycle (assert a `gc` run row exists after one cycle). Tools: `archive_run(kind="gc", dry_run=True)` is accepted.

- [ ] **Step 5: Run** — `.venv/bin/python -m pytest tests/test_archive_gc.py tests/test_archive_runner.py tests/test_archive_tools.py tests/test_migrations.py -q && .venv/bin/python scripts/dump_tool_table.py --write` → PASS.

- [ ] **Step 6: Commit** — `feat(archive): weekly orphan blob GC lane; archive_run kind=gc`

---

### Task 9: Per-item size cap in capture

**Files:**
- Modify: `ewsmcp/archive/capture.py` (`CAPTURE_FIELDS` += `"size"`; skip in `_capture_batch`)
- Modify: `ewsmcp/cache/store.py` (`archive_candidates` returns `size` if stored? — it is not; the cap is applied on the fetched item's `size`)
- Modify: `ewsmcp/archive/runner.py` (`status()["state_counts"]["skipped_too_large"]` from a per-process counter reset each cycle)
- Test: `tests/test_archive_capture.py`

**Interfaces:**
- Produces: `Capturer.run()` result gains `"too_large": int`; runner status `skipped_too_large`.

- [ ] **Step 1: Failing test**

```python
def test_capture_skips_items_over_the_size_cap(db, tmp_path):
    big = FakeItem("BIG-1"); big.size = 60 * 1024 * 1024
    small = FakeItem("OK-1"); small.size = 1024
    store = _store_with(db, ["BIG-1", "OK-1"])          # use the file's existing helper
    settings = make_settings(data_dir=str(tmp_path / "data"), archive_max_item_mb=50)
    cap = Capturer(settings, FakeGatewayFor(FakeAccount({"BIG-1": big, "OK-1": small})),
                   store, ArchivePolicy.from_settings(settings))
    out = asyncio.run(cap.run(dry_run=False))
    assert out["captured"] == 1 and out["too_large"] == 1 and out["failed"] == 0
    assert store.get_message("BIG-1")["archive_state"] == "live"
    assert any(s.get("reason") == "too_large" for s in out["sample"])
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** — in `_capture_batch`, before `_capture_one`: `size = getattr(item, "size", None); if size and size > self.settings.archive_max_item_mb * 1024 * 1024: too_large.append(raw_id); continue`. Return `(captured, failed, too_large)`; `run()` puts `too_large` count in the result and appends `{"ews_id": id, "reason": "too_large"}` entries to `sample`. `CAPTURE_FIELDS` gains `"size"`. Runner: `out["too_large"]` and `self._too_large_last = res["too_large"]`, reported in `status()["state_counts"]["skipped_too_large"]`.

- [ ] **Step 4: Run** — `.venv/bin/python -m pytest tests/test_archive_capture.py tests/test_archive_runner.py -q` → PASS.

- [ ] **Step 5: Commit** — `feat(capture): skip items over ARCHIVE_MAX_ITEM_MB, report them`

---

### Task 10: Audit line for skipped deletes

**Files:**
- Modify: `ewsmcp/archive/delete.py` (`_unusable_copies` and `_delete_batch` reasons → audit records in `_persist_batch` / a new `_persist_skips`)
- Test: `tests/test_archive_verify_delete.py`

**Interfaces:**
- Produces: audit records `tool="archive_delete_skipped", side_effect_class="destructive", outcome="skipped", detail={"ews_id", "reason", "run_id"}`.

- [ ] **Step 1: Failing test** (next to the existing deleter tests; `RecordingAudit` records `.entries`)

```python
def test_a_skipped_delete_writes_an_audit_line(db, tmp_path):
    # Arrange a verified row whose mime file is missing on disk (disk re-check fails)
    ...existing fixture pattern from test_last_mile_disk_check...
    out = asyncio.run(deleter.run(dry_run=False, run_id=42))
    skipped = [e for e in audit.entries if e["tool"] == "archive_delete_skipped"]
    assert len(skipped) == 1
    assert skipped[0]["detail"]["ews_id"] == "V1" and skipped[0]["detail"]["run_id"] == 42
    assert "mime file missing" in skipped[0]["detail"]["reason"]
    assert skipped[0]["outcome"] == "skipped"
```

Also assert the stale-changekey path (existing test for `_delete_batch` reasons) produces one record with `reason` starting `changekey`.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** — collect `(ews_id, reason)` pairs from `unusable` and `reasons` per chunk and write them in the same `asyncio.to_thread` hop as `_persist_batch` (a new `_persist_skips(pairs, run_id)` calling `self.audit.record(tool="archive_delete_skipped", side_effect_class="destructive", outcome="skipped", latency_ms=0, transport="archive", detail={...})`). When there is nothing deleted in the chunk, still persist the skips. Run `scripts/verify_audit_chain.py` against the test audit file if the test harness has one (see `tests/test_audit_persistence.py`); the chain must stay valid.

- [ ] **Step 4: Run** — `.venv/bin/python -m pytest tests/test_archive_verify_delete.py tests/test_audit_persistence.py -q` → PASS.

- [ ] **Step 5: Commit** — `feat(delete): audit record for every skipped candidate`

---

### Task 11: Pool sizing

**Files:**
- Modify: `ewsmcp/server.py:30` (`Database(settings.database_url, max_size=settings.db_pool_max)`), startup log line
- Modify: `ewsmcp/mcp/server.py:26` (explicit `max_size=4`, unchanged behaviour)
- Test: `tests/test_daemon_api.py` (or the existing server-construction test file)

- [ ] **Step 1: Failing test** — a test that builds the daemon app with `make_settings(db_pool_max=3, ews_max_concurrency=8)` and asserts (a) `app.state.db.pool.max_size == 3` and (b) a `WARNING` log record containing `"pool"` and `"consumers"` was emitted (use `caplog`). Find how the existing daemon tests build the app (`grep -n "build_app\|create_app" tests/test_daemon_api.py`).

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** — `consumers = settings.ews_max_concurrency + 4  # archive lanes: capture, verify, embed, delete/gc` `+ 2  # http handlers`; `logger.info("db pool max=%d, expected concurrent consumers=%d", ...)`; `logger.warning(...)` when `consumers > db_pool_max`.

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** — `feat(daemon): DB_POOL_MAX; warn when consumers exceed the pool`

---

### Task 12: Docs, version, changelog, boot smoke

**Files:**
- Modify: `pyproject.toml`, `ewsmcp/__init__.py` (`5.2.0a1`), `CHANGELOG.md` (`## [5.2.0a1] - <date>` with one line per task), `README.md` (settings table: the §8 knobs; rollout steps from spec §9), `DESIGN.md` (one paragraph on the boilerplate detectors and thread context), `docs/API.md` (`--write`), `docker-compose.yml` (commented `ARCHIVE_BOILERPLATE_DROP`, `GEMINI_CLEAN_MODEL`)
- Test: `scripts/boot_smoke.py` must pass; `tests/test_surface_completion.py` tool count assertions (35 tools unchanged; `archive_run` kind enum and `search_messages` schema changed).

- [ ] **Step 1** — bump version, write CHANGELOG, README, DESIGN.
- [ ] **Step 2** — `.venv/bin/python scripts/dump_tool_table.py --write && .venv/bin/python -m ruff check . && .venv/bin/python -m pytest tests -q` (foreground, `timeout 400`) → all green.
- [ ] **Step 3** — `.venv/bin/python scripts/boot_smoke.py` → passes (it builds the image and hits `/v1/status`; confirm the status has `boilerplate`, `gc`, `next_cycle_in_s`).
- [ ] **Step 4: Commit** — `chore(release): 5.2.0a1 — Phase 3`

---

## Rollout (after merge; the owner runs the docker commands)

1. `EWS_MCP_TAG=5.2-<sha>`; `docker compose up -d --build ewsd ews-mcp`; watch for `applying migration 004_phase3.sql`.
2. `docker exec -i ewsd python - < scripts/seed_boilerplate.py`.
3. `docker exec -i ewsd python - --all < scripts/backfill_bodies.py` (fills `item_class`, `attachments_json`, applies the disclaimer cut; changed bodies re-embed).
4. Verify: `archive_status` shows `boilerplate.drop_detector = "off"`, backlog draining; `search_messages(subject="White Hill")` no longer leads with "Accepted:"; `get_message` from the mirror lists attachments.
5. Two weeks later: `docker exec -i ewsd python - < scripts/boilerplate_report.py`; owner sets `ARCHIVE_BOILERPLATE_DROP`; re-queue: `UPDATE ews.messages SET embedded_at = NULL WHERE ews_id IN (SELECT DISTINCT message_ews_id FROM ews.boilerplate_hits)`; remove the losing detector in a follow-up.

## Self-review notes

- Spec §0 → Task 0; §1 → Task 2; §2 → Task 6; §2b/§2c → Task 7; §3 → Task 5; §4 → Tasks 3+4; §5 → Tasks 3+4; §6 → Task 1 (+ the `gc` CHECK amendment in Task 8); §7 → Tasks 8–11; §8 → Task 1; §9 → Rollout; §10 → each task's tests.
- Names used across tasks: `tail_paragraphs` (T2 → T6), `update_bodies(bodies, recipients, extra)` (T3 → T4 tests), `Hit`/`BoilerplateHarness`/`LlmDetector` (T6 → T7), `is_calendar_item_class`/`_CALENDAR_EXCLUDE` (T4), `parent_in_thread` (T5), `GcWorker` (T8), `KINDS` with `gc` (T8 → T12 docs).
- Ruling recorded for executors: the harness embeds tail paragraphs in its own `embed()` call rather than the chunk batch (spec said "same batch"; a separate call is simpler and the cost is the same).
