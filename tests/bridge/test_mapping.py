import datetime as dt
import json

import pytest

from ewsmcp.bridge import mapping


def _row(**kw):
    base = {
        "ews_id": "m1",
        "conversation_id": "conv1",
        "folder_id": "inbox",
        "sender_name": "a colleague",
        "sender_email": "Colleague@Example.TEST",
        # Real shape: to_json is a JSON array of plain address strings, not objects.
        "to_json": json.dumps(["owner@example.test"]),
        "subject": "a subject",
        "date_ts": 1_700_000_000,
        "body_clean": "a body",
        "has_attachments": 0,
        "attachments_json": None,
        "internet_message_id": "<x@example.test>",
        "item_class": "IPM.Note",
        "changekey": "ck1",
        "seq": 7,
        "first_seen": dt.datetime(2023, 11, 14, 22, 13, 20, tzinfo=dt.timezone.utc),
    }
    return base | kw


def test_an_address_becomes_a_lowercased_email_key():
    assert mapping.identity("Colleague@Example.TEST") == "email:colleague@example.test"
    assert mapping.identity(None) is None
    assert mapping.identity("") is None


def test_identity_rejects_legacy_dn_strings():
    """Exchange stores unresolved internal recipients as legacy X.500 DNs.
    A key must come from an address shape with @."""
    assert mapping.identity("/o=ExchangeLabs/ou=x/cn=abc") is None


def test_a_sender_with_no_address_keeps_its_name_and_gets_no_key():
    """Spec §4: a key is minted only from something that identifies a person.
    A room notification or a malformed header has no address, and inventing one
    is how two different senders become one person."""
    m = mapping.message(_row(sender_email=None, sender_name="a system notice"))
    assert m["author"]["key"] is None
    # native_id falls back to sender_name when sender_email is missing.
    assert m["author"]["native_id"] == "a system notice"
    assert m["author"]["name"] == "a system notice"


def test_two_recipients_make_it_a_group_and_one_makes_it_direct():
    # Real shape: to_json is a list of plain address strings.
    one = json.dumps(["owner@example.test"])
    two = json.dumps(["owner@example.test", "another@example.test"])
    assert mapping.chat(_row(to_json=one))["kind"] == "direct"
    assert mapping.chat(_row(to_json=two))["kind"] == "group"


def test_recipients_accept_both_shapes():
    """to_json can be a list of plain strings (real shape in this store) or
    a list of objects (older or alternate writers)."""
    # Plain string shape (the real one).
    strings = json.dumps(["a@example.test", "b@example.test"])
    assert mapping.recipients(_row(to_json=strings)) == [
        {"name": None, "email": "a@example.test"},
        {"name": None, "email": "b@example.test"},
    ]
    # Object shape (for backward compatibility with fixtures or alternate writers).
    objects = json.dumps([
        {"name": "Alice", "email": "a@example.test"},
        {"name": "Bob", "email": "b@example.test"},
    ])
    assert mapping.recipients(_row(to_json=objects)) == [
        {"name": "Alice", "email": "a@example.test"},
        {"name": "Bob", "email": "b@example.test"},
    ]


def test_recipients_handle_null_json():
    """to_json="null" is valid JSON but not a list. Must degrade to []
    rather than raising TypeError on 'for p in None'."""
    assert mapping.recipients(_row(to_json="null")) == []


def test_the_chat_is_the_conversation_and_falls_back_to_the_message():
    assert mapping.chat(_row())["native_id"] == "conv1"
    # a mail with no conversation id is its own thread rather than joining a
    # nameless bucket with every other such mail
    assert mapping.chat(_row(conversation_id=None))["native_id"] == "m1"


def test_the_timestamp_is_rendered_from_the_epoch_not_the_stored_string():
    m = mapping.message(_row(date_ts=1_700_000_000))
    assert m["sent_at"] == "2023-11-14T22:13:20+00:00"


def test_timestamp_falls_back_to_first_seen_when_date_ts_is_none():
    """A mail whose send time couldn't be parsed falls back to when the store
    first saw it, rather than silently using epoch."""
    first_seen = dt.datetime(2023, 9, 15, 10, 30, 45, tzinfo=dt.timezone.utc)
    m = mapping.message(_row(date_ts=None, first_seen=first_seen))
    assert m["sent_at"] == "2023-09-15T10:30:45+00:00"


def test_message_raises_when_no_timestamp_at_all():
    """A message with neither date_ts nor first_seen has no place in a ledger
    of deadlines."""
    with pytest.raises(ValueError, match="has no date_ts or first_seen"):
        mapping.message(_row(date_ts=None, first_seen=None))


def test_a_subject_with_no_body_still_carries_the_subject():
    """Mail routinely says everything in the subject line. An empty body would
    otherwise make the message invisible to triage and to search."""
    m = mapping.message(_row(body_clean="", subject="approve the invoice"))
    assert m["text"] == "approve the invoice"
    m = mapping.message(_row(body_clean="a body", subject="approve the invoice"))
    assert m["text"] == "a body"


def test_author_raw_field_holds_smtp_address():
    """Spec §4: every author carries every raw identifier the source has."""
    m = mapping.message(_row(sender_email="Colleague@Example.TEST"))
    assert m["author"]["raw"] == [{"type": "smtp", "value": "Colleague@Example.TEST"}]
    # No sender_email means no raw.
    m = mapping.message(_row(sender_email=None))
    assert m["author"]["raw"] == []


def test_is_owner_set_when_sender_key_matches_owner_key():
    """A bridge marks is_owner on the owner's own handles (logged-in account)."""
    owner_key = "email:owner@example.test"
    # Message from the owner.
    m = mapping.message(
        _row(sender_email="owner@example.test"),
        owner_key=owner_key,
    )
    assert m["author"]["is_owner"] is True
    # Message from someone else.
    m = mapping.message(
        _row(sender_email="other@example.test"),
        owner_key=owner_key,
    )
    assert m["author"]["is_owner"] is False
    # Message with no owner_key provided (not the owner's mailbox).
    m = mapping.message(_row(sender_email="owner@example.test"), owner_key=None)
    assert m["author"]["is_owner"] is False
