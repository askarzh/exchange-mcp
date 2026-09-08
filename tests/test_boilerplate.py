"""Boilerplate detectors: embedding similarity, hit log, and the drop switch."""

import math

from conftest import FakeEmbedder, make_row

from ewsmcp.boilerplate import BoilerplateHarness, EmbeddingDetector
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
