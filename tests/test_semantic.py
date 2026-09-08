"""Vector + hybrid search over real pgvector, with a deterministic fake embedder."""

import pytest
from conftest import FakeEmbedder, make_row

from ewsmcp.cache.store import CacheStore
from ewsmcp.embeddings import QUERY_PREFIX, EmbeddingError
from ewsmcp.semantic import RRF_K, SemanticIndex


@pytest.fixture
def indexed(db):
    store = CacheStore(db)
    store.replace_folders([
        {"ews_id": "FID-INBOX", "name": "Inbox", "path": "Inbox", "wk": "f:inbox",
         "total": 0, "unread": 0, "children": 0}])
    store.upsert_messages([
        make_row("M-BUDGET", subject="Quarterly budget",
                 body="the budget forecast spreadsheet for finance"),
        make_row("M-LUNCH", subject="Lunch plans",
                 body="shawarma at noon near the office"),
        make_row("M-FORECAST", subject="Forecast update",
                 body="finance forecast numbers revised upward"),
    ])
    index = SemanticIndex(store, FakeEmbedder())
    index.index_messages(store.unembedded_messages(100))
    return store, index


def test_replace_chunks_stamps_embedded_at_without_mark_embedded(db):
    """`replace_chunks` alone must set `embedded_at` — no separate call needed,
    and no window where chunks exist but the message still looks unembedded."""
    store = CacheStore(db)
    store.upsert_messages([make_row("M-DIRECT", subject="Direct", body="hello world")])
    assert store.get_message("M-DIRECT")["embedded_at"] is None
    store.replace_chunks("M-DIRECT", [{"seq": 0, "source": "body", "text": "hello world",
                                       "embedding": [0.1] * 768}])
    assert store.get_message("M-DIRECT")["embedded_at"] is not None


def test_replace_chunks_with_zero_chunks_still_stamps_embedded_at(db):
    """A message with nothing to embed (e.g. empty body) must still leave the
    backlog — otherwise it is re-selected by `unembedded_messages` forever."""
    store = CacheStore(db)
    store.upsert_messages([make_row("M-EMPTY", subject="", body="")])
    store.replace_chunks("M-EMPTY", [])
    assert store.get_message("M-EMPTY")["embedded_at"] is not None


def test_vector_search_uses_the_hnsw_index_when_unfiltered(db):
    """The unfiltered (`archived="any"`, no exclude) shape of the ANN query
    that `similar_message_ids` issues must be servable by `ix_chunks_embedding`
    — a `GROUP BY ... ORDER BY MIN(...)` shape cannot use it at all. 300 rows
    plus `enable_seqscan = off` inside the transaction removes any doubt that
    the planner would just pick a seq scan on a tiny table anyway."""
    from ewsmcp.cache.store import _vector_literal

    store = CacheStore(db)
    embedder = FakeEmbedder()
    rows = [make_row(f"M-{i}", subject=f"Msg {i}",
                     body=f"content varies {i} {i % 11} {i % 7}") for i in range(300)]
    store.upsert_messages(rows)
    with store.db.conn() as c:
        payload = [{"message_ews_id": r["ews_id"], "seq": 0, "source": "body",
                    "text": "x", "embedding": _vector_literal(
                        embedder.embed([r["subject"] + "\n" + r["body_clean"]])[0])}
                   for r in rows]
        c.cursor().executemany(
            "INSERT INTO ews.chunks (message_ews_id, seq, source, text, embedding) "
            "VALUES (%(message_ews_id)s, %(seq)s, %(source)s, %(text)s, %(embedding)s::vector)",
            payload)
    query_vec = _vector_literal(embedder.embed(["content varies"])[0])
    with store.db.conn() as c:
        c.execute("SET LOCAL enable_seqscan = off")
        plan_rows = c.execute(
            "EXPLAIN SELECT cand.ews_id AS ews_id, cand.dist AS dist FROM ("
            "  SELECT c.message_ews_id AS ews_id, "
            "         (c.embedding <=> %s::vector) AS dist "
            "  FROM ews.chunks c "
            "  WHERE c.embedding IS NOT NULL "
            "  ORDER BY c.embedding <=> %s::vector LIMIT %s"
            ") cand JOIN ews.messages m ON m.ews_id = cand.ews_id "
            "WHERE TRUE ORDER BY cand.dist ASC",
            [query_vec, query_vec, 40]).fetchall()
    plan_text = "\n".join(r["QUERY PLAN"] for r in plan_rows)
    assert "ix_chunks_embedding" in plan_text


def test_index_messages_writes_chunks_and_stamps_embedded_at(indexed):
    store, _index = indexed
    assert store.embedding_backlog() == 0
    assert store.embedded_count() == 3
    assert store.get_message("M-BUDGET")["embedded_at"] is not None
    with store.db.conn() as c:
        row = c.execute("SELECT source, seq, text, embedding IS NOT NULL AS has_vec "
                        "FROM ews.chunks WHERE message_ews_id = 'M-BUDGET'").fetchone()
    assert row["source"] == "body" and row["seq"] == 0 and row["has_vec"]
    assert "Quarterly budget" in row["text"]


def test_reindexing_replaces_chunks_instead_of_duplicating(indexed):
    store, index = indexed
    with store.db.conn() as c:
        c.execute("UPDATE ews.messages SET embedded_at = NULL")
    index.index_messages(store.unembedded_messages(100))
    with store.db.conn() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM ews.chunks "
                      "WHERE message_ews_id = 'M-BUDGET'").fetchone()["n"]
    assert n == 1


def test_a_long_body_becomes_several_chunks(db):
    store = CacheStore(db)
    # 899 repeats + "Long\n" = exactly 4500 chars = 3 * chunk_chars (1500).
    store.upsert_messages([make_row("M-LONG", subject="Long", body="word " * 899)])
    SemanticIndex(store, FakeEmbedder()).index_messages(store.unembedded_messages(10))
    with store.db.conn() as c:
        seqs = [r["seq"] for r in c.execute(
            "SELECT seq FROM ews.chunks WHERE message_ews_id = 'M-LONG' "
            "ORDER BY seq").fetchall()]
    assert seqs == [0, 1, 2]


def test_vector_search_ranks_the_related_message_first(indexed):
    _store, index = indexed
    ids = [i for i, _d in index.vector_ids("finance forecast", limit=3)]
    assert ids[0] in ("M-FORECAST", "M-BUDGET")
    assert ids[-1] == "M-LUNCH"


def test_query_text_is_embedded_with_the_task_instruction(indexed):
    _store, index = indexed
    index.embedder.calls.clear()
    index.vector_ids("budget", limit=2)
    assert index.embedder.calls[-1] == [QUERY_PREFIX + "budget"]


def test_similar_to_message_excludes_the_seed(indexed):
    _store, index = indexed
    ids = [r["ews_id"] for r in index.similar_to_message("M-BUDGET", limit=5)]
    assert "M-BUDGET" not in ids and ids


def test_archived_filter_applies_to_vector_search(indexed):
    store, index = indexed
    store.mark_captured("M-FORECAST", mime_sha256="a" * 64, mime_path="/x.eml")
    only = [i for i, _d in index.vector_ids("finance forecast", limit=5,
                                            archived="only")]
    assert only == ["M-FORECAST"]
    excl = [i for i, _d in index.vector_ids("finance forecast", limit=5,
                                            archived="exclude")]
    assert "M-FORECAST" not in excl


def test_hybrid_fuses_both_rankings_with_rrf(indexed):
    _store, index = indexed
    rows, degraded = index.hybrid_search("budget forecast", limit=3)
    assert degraded is False
    assert [r["ews_id"] for r in rows][:2] == ["M-BUDGET", "M-FORECAST"] or \
           [r["ews_id"] for r in rows][:2] == ["M-FORECAST", "M-BUDGET"]
    assert "M-LUNCH" not in [r["ews_id"] for r in rows][:1]


def test_hybrid_finds_a_message_only_one_engine_can_see(indexed):
    """RRF's whole point: keyword-only and vector-only hits both survive."""
    _store, index = indexed
    rows, _ = index.hybrid_search("shawarma", limit=3)
    assert "M-LUNCH" in [r["ews_id"] for r in rows]


def test_rrf_constant_is_sixty():
    assert RRF_K == 60


def test_hybrid_degrades_to_keyword_when_the_embedder_fails(indexed):
    store, _index = indexed

    class Broken:
        def embed(self, texts):
            raise EmbeddingError("gemini down")

    index = SemanticIndex(store, Broken())
    rows, degraded = index.hybrid_search("budget", limit=3)
    assert degraded is True
    assert [r["ews_id"] for r in rows] == ["M-BUDGET"]


def test_hybrid_passes_structured_filters_through(indexed):
    _store, index = indexed
    rows, _ = index.hybrid_search("budget", limit=5, sender="nobody@example.com")
    assert rows == []


def test_include_calendar_items_flag_alone_does_not_force_keyword_intersection(indexed):
    """`include_calendar_items` reaches `hybrid_search` as a `**filters` entry
    that is never None (cache_reads.search_messages always sets it), so it
    must NOT be treated like a genuine structured filter — otherwise every
    semantic-mode call through the tool would force the keyword/vector
    intersection and kill vector-only RRF survivors.

    "hummus" appears nowhere in the fixture, so the keyword AND across
    ("hummus", "lunch") matches nothing; the vector half still surfaces
    M-LUNCH through the shared "lunch" token (its subject is "Lunch
    plans"). include_calendar_items=False alone must not suppress that hit."""
    _store, index = indexed
    rows, _ = index.hybrid_search("hummus lunch", limit=3, include_calendar_items=False)
    assert "M-LUNCH" in [r["ews_id"] for r in rows]

    # A genuine structured filter still forces the intersection, same as
    # test_hybrid_passes_structured_filters_through above — the exclusion is
    # specific to include_calendar_items, not a blanket skip of the gate.
    rows2, _ = index.hybrid_search("hummus lunch", limit=3,
                                   include_calendar_items=False,
                                   sender="nobody@example.com")
    assert rows2 == []


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


BOILERPLATE_BODY = (
    "Коллеги, добрый день.\n\n"
    "По итогам встречи направляю обновлённую модель бюджета на следующий квартал. "
    "Прошу посмотреть допущения на вкладке 2 и вернуться с комментариями до пятницы.\n\n"
    "Отдельно обращаю внимание на сроки согласования с юристами: они просят две недели, "
    "поэтому финальную версию нужно собрать заранее и разослать участникам.\n\n"
    "С уважением, Аскар.\n\n"
    "Предоставляемая АО «BCC Invest» информация не является предложением о покупке "
    "и/или обязательством по продаже ценных бумаг.")


def _harnessed(db, drop):
    from ewsmcp.boilerplate import BoilerplateHarness
    store = CacheStore(db)
    store.upsert_messages([make_row("M-FOOT", body=BOILERPLATE_BODY)])
    emb = FakeEmbedder()
    footer = BOILERPLATE_BODY.split("\n\n")[-1]
    store.upsert_boilerplate_ref("bcc_invest_ru", footer, emb.embed([footer])[0])
    harness = BoilerplateHarness(store, emb, threshold=0.80, drop=drop)
    index = SemanticIndex(store, emb, harness=harness)
    rows = [r for r in store.messages_by_ids(["M-FOOT"]).values()]
    assert index.index_messages(rows) == 1
    with db.conn() as c:
        chunks = c.execute("SELECT text FROM ews.chunks WHERE message_ews_id = 'M-FOOT' "
                           "ORDER BY seq").fetchall()
    return store, " ".join(r["text"] for r in chunks)


def test_index_messages_drops_the_footer_when_drop_names_the_detector(db):
    store, text = _harnessed(db, "embedding")
    assert "BCC Invest" not in text
    assert store.boilerplate_stats()["embedding"] == {"hits": 1, "dropped": 1}


def test_index_messages_keeps_the_footer_but_still_logs_when_drop_is_off(db):
    store, text = _harnessed(db, "off")
    assert "BCC Invest" in text
    assert store.boilerplate_stats()["embedding"] == {"hits": 1, "dropped": 0}


def test_hybrid_vector_half_honours_include_calendar_items(indexed):
    """The vector half used to exclude calendar items unconditionally, so a
    calendar row that only the vector engine can find stayed invisible even
    with include_calendar_items=true. "hummus" is in no document, so the
    keyword AND matches nothing and the row can arrive by vector alone."""
    store, index = indexed
    store.upsert_messages([make_row("M-CAL", subject="Lunch sync accepted",
                                    body="lunch at noon with the team")])
    store.update_bodies({}, None,
                        {"M-CAL": {"item_class": "IPM.Schedule.Meeting.Resp.Pos"}})
    index.index_messages(store.unembedded_messages(10))

    rows, _ = index.hybrid_search("hummus lunch", limit=5,
                                  include_calendar_items=True)
    assert "M-CAL" in [r["ews_id"] for r in rows]

    rows, _ = index.hybrid_search("hummus lunch", limit=5,
                                  include_calendar_items=False)
    assert "M-CAL" not in [r["ews_id"] for r in rows]


def test_find_similar_always_excludes_calendar_items(indexed):
    """find_similar / similar_to_message have no such flag: meeting responses
    are never useful "more like this" answers."""
    store, index = indexed
    store.upsert_messages([make_row("M-CAL2", subject="Accepted: Quarterly budget",
                                    body="the budget forecast spreadsheet for finance")])
    store.update_bodies({}, None,
                        {"M-CAL2": {"item_class": "IPM.Schedule.Meeting.Resp.Pos"}})
    index.index_messages(store.unembedded_messages(10))
    hits = index.similar_to_message("M-BUDGET", limit=5)
    assert "M-CAL2" not in [r["ews_id"] for r in hits]
