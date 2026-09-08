"""Embeddings: chunking, the Gemini request/response contract, and backoff.
Not one test touches the network — httpx is driven by a MockTransport."""

import json

import httpx
import pytest
from conftest import FakeEmbedder

from ewsmcp.boilerplate import GeminiCleaner
from ewsmcp.embeddings import (
    GEMINI_MODEL,
    QUERY_PREFIX,
    EmbeddingError,
    GeminiEmbedder,
    chunk_text,
)


def test_chunk_text_joins_subject_and_body_and_splits_at_1500_chars():
    chunks = chunk_text("Budget", "x" * 3200)
    assert chunks[0].startswith("Budget\n")
    assert all(len(c) <= 1500 for c in chunks)
    assert len(chunks) == 3
    assert "".join(chunks) == "Budget\n" + "x" * 3200


def test_chunk_text_of_an_empty_body_is_the_subject_alone():
    assert chunk_text("Budget", "") == ["Budget"]
    assert chunk_text("", "") == []


def _transport(recorder, *responses):
    calls = iter(responses)

    def handler(request):
        recorder.append(json.loads(request.content))
        return next(calls)

    return httpx.MockTransport(handler)


def _ok(n, dims=768):
    return httpx.Response(200, json={"embeddings": [
        {"values": [0.1] * dims} for _ in range(n)]})


def test_request_shape_matches_the_gemini_batch_contract():
    seen = []
    client = httpx.Client(transport=_transport(seen, _ok(2)))
    emb = GeminiEmbedder("KEY", client=client)
    vectors = emb.embed(["alpha", "beta"])
    assert len(vectors) == 2 and len(vectors[0]) == 768
    body = seen[0]
    assert body["requests"][0] == {
        "model": f"models/{GEMINI_MODEL}",
        "content": {"parts": [{"text": "alpha"}]},
        "outputDimensionality": 768,
    }


def test_the_api_key_travels_in_a_header_not_the_url():
    """A query string is the one part of a request that leaks by default
    (proxy/access logs, Referer, crash reports). The key goes in
    x-goog-api-key instead, and the URL stays free of it."""
    seen_requests = []

    def handler(request):
        seen_requests.append(request)
        return _ok(1)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    emb = GeminiEmbedder("SECRET", client=client)
    emb.embed(["alpha"])
    request = seen_requests[0]
    assert request.headers["x-goog-api-key"] == "SECRET"
    assert "SECRET" not in str(request.url)
    assert str(request.url).endswith("batchEmbedContents")
    assert GEMINI_MODEL in str(request.url)
    assert "SECRET" not in emb.url


def test_batches_are_capped_at_100():
    seen = []
    client = httpx.Client(transport=_transport(seen, _ok(100), _ok(20)))
    vectors = GeminiEmbedder("K", client=client).embed([f"t{i}" for i in range(120)])
    assert len(vectors) == 120
    assert [len(b["requests"]) for b in seen] == [100, 20]


def test_429_is_retried_with_exponential_backoff():
    seen, slept = [], []
    client = httpx.Client(transport=_transport(
        seen, httpx.Response(429, json={}), httpx.Response(503, json={}), _ok(1)))
    emb = GeminiEmbedder("K", client=client, base_delay=1.0, sleep=slept.append)
    assert len(emb.embed(["x"])) == 1
    assert slept == [1.0, 2.0]
    assert len(seen) == 3


def test_a_permanent_400_raises_without_retrying():
    seen = []
    client = httpx.Client(transport=_transport(
        seen, httpx.Response(400, json={"error": {"message": "bad"}})))
    with pytest.raises(EmbeddingError, match="400"):
        GeminiEmbedder("K", client=client, sleep=lambda s: None).embed(["x"])
    assert len(seen) == 1


def test_exhausted_retries_raise():
    seen = []
    client = httpx.Client(transport=_transport(seen, *[httpx.Response(429, json={})] * 3))
    emb = GeminiEmbedder("K", client=client, max_attempts=3, sleep=lambda s: None)
    with pytest.raises(EmbeddingError):
        emb.embed(["x"])


def test_a_short_or_wrong_width_response_is_an_error():
    seen = []
    client = httpx.Client(transport=_transport(seen, _ok(1, dims=3)))
    with pytest.raises(EmbeddingError, match="768"):
        GeminiEmbedder("K", client=client, sleep=lambda s: None).embed(["x"])


def test_query_prefix_is_the_documented_task_instruction():
    assert QUERY_PREFIX == "Retrieve email messages relevant to the query: "


def test_fake_embedder_is_deterministic_and_768_wide():
    fake = FakeEmbedder()
    a, b = fake.embed(["budget review", "budget review"])
    assert a == b and len(a) == 768


def test_api_key_never_appears_in_logs_or_exception_text(caplog):
    seen = []
    client = httpx.Client(transport=_transport(
        seen, httpx.Response(429, json={}), httpx.Response(400, json={"error": "bad"})))
    emb = GeminiEmbedder("TOP-SECRET-KEY", client=client, sleep=lambda s: None)
    with caplog.at_level("DEBUG", logger="ewsmcp.embeddings"):
        with pytest.raises(EmbeddingError) as excinfo:
            emb.embed(["x"])
    assert "TOP-SECRET-KEY" not in str(excinfo.value)
    for record in caplog.records:
        if record.name == "ewsmcp.embeddings":
            assert "TOP-SECRET-KEY" not in record.getMessage()


# --------------------------------------------------------------------------
# GeminiCleaner (the boilerplate boundary call) — same fake-client pattern
# --------------------------------------------------------------------------


def _cleaner_client(seen, response):
    def handler(request):
        seen.append(request)
        return response

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_cleaner_asks_for_deterministic_json_and_returns_the_parsed_answer():
    answer = {"drop_from": 2, "reason": "legal footer"}
    seen = []
    client = _cleaner_client(seen, httpx.Response(200, json={"candidates": [
        {"content": {"parts": [{"text": json.dumps(answer)}]}}]}))
    cleaner = GeminiCleaner("SECRET", model="gemini-3.5-flash-lite", client=client)
    assert cleaner.boundary("hello", ["a", "b", "footer"]) == answer
    request = seen[0]
    assert "gemini-3.5-flash-lite" in str(request.url)
    assert str(request.url).endswith("generateContent")
    assert "SECRET" not in str(request.url)
    assert request.headers["x-goog-api-key"] == "SECRET"
    body = json.loads(request.content)
    cfg = body["generationConfig"]
    assert cfg["temperature"] == 0
    assert cfg["responseMimeType"] == "application/json"
    assert cfg["responseSchema"]["properties"]["drop_from"]["nullable"] is True
    assert cfg["responseSchema"]["required"] == ["reason"]
    assert "[2] footer" in body["contents"][0]["parts"][0]["text"]


def test_cleaner_raises_on_a_server_error():
    client = _cleaner_client([], httpx.Response(500, text="boom"))
    cleaner = GeminiCleaner("SECRET", model="gemini-3.5-flash-lite", client=client)
    with pytest.raises(httpx.HTTPStatusError):
        cleaner.boundary("hello", ["a"])
