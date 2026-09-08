"""Boilerplate detectors: embedding similarity, hit log, and the drop switch."""

import math

from conftest import FakeEmbedder, make_row

from ewsmcp.bodyclean import tail_paragraphs
from ewsmcp.boilerplate import BoilerplateHarness, EmbeddingDetector, LlmDetector
from ewsmcp.cache.store import CacheStore

BODY = ("Коллеги, добрый день.\n\nПо итогам встречи направляю обновлённую модель. "
        "Прошу посмотреть допущения на вкладке 2 и вернуться с комментариями до пятницы.\n\n"
        "Отдельно обращаю внимание на сроки согласования с юристами: они просят две недели, "
        "поэтому финальную версию нужно собрать заранее и разослать участникам.\n\n"
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
    assert det.detect("m", "first", paras) == []                     # null = no hit
    assert det.detect("m", "first", paras)[0].ref_label.startswith("error:")
    assert det.detect("m", "first", paras)[0].ref_label == "error:TimeoutError"


def test_harness_drop_enum_llm_and_both(db):
    store = CacheStore(db)
    emb = FakeEmbedder()
    footer = BODY.split("\n\n")[-1]
    store.upsert_boilerplate_ref("bcc", footer, emb.embed([footer])[0])
    tail_n = len(tail_paragraphs(BODY))
    llm = LlmDetector(_ScriptedCleaner(
        [{"drop_from": tail_n - 2, "reason": "sig+footer"}] * 3))
    text, hits = BoilerplateHarness(store, emb, threshold=0.8, drop="llm",
                                    llm=llm).analyse("A", BODY)
    assert "С уважением" not in text            # llm cut earlier than embedding
    text, _ = BoilerplateHarness(store, emb, threshold=0.8, drop="embedding",
                                 llm=llm).analyse("A", BODY)
    assert "С уважением" in text and "BCC Invest" not in text
    text, _ = BoilerplateHarness(store, emb, threshold=0.8, drop="both",
                                 llm=llm).analyse("A", BODY)
    assert "С уважением" not in text
    assert store.boilerplate_stats()["llm"]["hits"] == 3


def test_llm_budget_stops_calls_and_reset_restores_it():
    """Exhausted budget = no call at all: not a hit, not an `error:` row —
    the pass simply stops asking until the next reset."""
    cleaner = _ScriptedCleaner([{"drop_from": 0, "reason": "sig"}] * 5)
    det = LlmDetector(cleaner, budget=2)
    paras = ["Best regards, X"]
    assert det.detect("m1", "first", paras)[0].paragraph_index == 0
    assert det.detect("m2", "first", paras)[0].paragraph_index == 0
    assert det.detect("m3", "first", paras) == []
    assert len(cleaner.calls) == 2
    det.reset(1)
    assert det.detect("m4", "first", paras)[0].paragraph_index == 0
    assert len(cleaner.calls) == 3 and det.budget == 0


def test_embed_pass_caps_llm_calls_and_the_next_pass_rearms_them(db):
    """One EmbedWorker pass may call the boundary detector at most
    ARCHIVE_BOILERPLATE_LLM_PER_CYCLE times, however many messages it
    indexes; the following pass gets a fresh budget."""
    import asyncio

    from ewsmcp.archive.embed import EmbedWorker
    from ewsmcp.semantic import SemanticIndex

    store = CacheStore(db)
    store.upsert_messages([make_row(f"B{i}", body=BODY) for i in range(3)])
    cleaner = _ScriptedCleaner([{"drop_from": None, "reason": "clean"}] * 20)
    harness = BoilerplateHarness(store, FakeEmbedder(), threshold=0.99, drop="off",
                                 llm=LlmDetector(cleaner), llm_per_cycle=2)
    index = SemanticIndex(store, FakeEmbedder(), harness=harness)

    asyncio.run(EmbedWorker(store, index).run(limit=10))
    assert len(cleaner.calls) == 2          # 3 messages, 2 calls
    with store.db.conn() as c:              # re-queue everything
        c.execute("UPDATE ews.messages SET embedded_at = NULL")
    asyncio.run(EmbedWorker(store, index).run(limit=10))
    assert len(cleaner.calls) == 4          # budget re-armed, capped again


class _CountingStore:
    """Transparent proxy that counts the store methods the harness calls."""

    def __init__(self, store):
        self._store = store
        self.calls: dict[str, int] = {}

    def __getattr__(self, name):
        attr = getattr(self._store, name)
        if not callable(attr):
            return attr

        def counted(*a, **kw):
            self.calls[name] = self.calls.get(name, 0) + 1
            return attr(*a, **kw)
        return counted


def test_refs_are_reloaded_only_when_the_stamp_moves(db):
    """`refresh_refs` runs per message; re-selecting every 768-dim vector each
    time is the expensive part, so only the `max(created_at)` probe repeats."""
    real = CacheStore(db)
    emb = FakeEmbedder()
    footer = BODY.split("\n\n")[-1]
    real.upsert_boilerplate_ref("bcc", footer, emb.embed([footer])[0])
    spy = _CountingStore(real)

    h = BoilerplateHarness(spy, emb, threshold=0.80, drop="off")
    assert spy.calls["boilerplate_refs"] == 1        # once, at construction
    h.analyse("A1", BODY)
    h.analyse("A2", BODY)
    assert spy.calls["boilerplate_refs"] == 1        # no vector reload
    assert spy.calls["boilerplate_refs_stamp"] == 3  # one cheap probe each

    real.upsert_boilerplate_ref("other", "Some other disclaimer text",
                                emb.embed(["Some other disclaimer text"])[0])
    h.analyse("A3", BODY)
    assert spy.calls["boilerplate_refs"] == 2        # the stamp moved
    assert len(h._detector.refs) == 2
