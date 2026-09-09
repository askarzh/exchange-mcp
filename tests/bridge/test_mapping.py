import json

from ewsmcp.bridge import mapping


def _row(**kw):
    base = {"ews_id": "m1", "conversation_id": "conv1", "folder_id": "inbox",
            "sender_name": "a colleague", "sender_email": "Colleague@Example.TEST",
            "to_json": json.dumps([{"name": "the owner", "email": "owner@example.test"}]),
            "subject": "a subject", "date_ts": 1_700_000_000, "body_clean": "a body",
            "has_attachments": 0, "attachments_json": None,
            "internet_message_id": "<x@example.test>", "item_class": "IPM.Note",
            "changekey": "ck1", "seq": 7}
    return base | kw


def test_an_address_becomes_a_lowercased_email_key():
    assert mapping.identity("Colleague@Example.TEST") == "email:colleague@example.test"
    assert mapping.identity(None) is None
    assert mapping.identity("") is None


def test_a_sender_with_no_address_keeps_its_name_and_gets_no_key():
    """Spec §4: a key is minted only from something that identifies a person.
    A room notification or a malformed header has no address, and inventing one
    is how two different senders become one person."""
    m = mapping.message(_row(sender_email=None, sender_name="a system notice"))
    assert m["author"]["key"] is None
    assert m["author"]["native_id"] == "m1:sender"
    assert m["author"]["name"] == "a system notice"


def test_two_recipients_make_it_a_group_and_one_makes_it_direct():
    one = json.dumps([{"name": "the owner", "email": "owner@example.test"}])
    two = json.dumps([{"name": "the owner", "email": "owner@example.test"},
                      {"name": "another", "email": "another@example.test"}])
    assert mapping.chat(_row(to_json=one))["kind"] == "direct"
    assert mapping.chat(_row(to_json=two))["kind"] == "group"


def test_the_chat_is_the_conversation_and_falls_back_to_the_message():
    assert mapping.chat(_row())["native_id"] == "conv1"
    # a mail with no conversation id is its own thread rather than joining a
    # nameless bucket with every other such mail
    assert mapping.chat(_row(conversation_id=None))["native_id"] == "m1"


def test_the_timestamp_is_rendered_from_the_epoch_not_the_stored_string():
    m = mapping.message(_row(date_ts=1_700_000_000))
    assert m["sent_at"] == "2023-11-14T22:13:20+00:00"


def test_a_subject_with_no_body_still_carries_the_subject():
    """Mail routinely says everything in the subject line. An empty body would
    otherwise make the message invisible to triage and to search."""
    m = mapping.message(_row(body_clean="", subject="approve the invoice"))
    assert m["body"] == "approve the invoice"
    m = mapping.message(_row(body_clean="a body", subject="approve the invoice"))
    assert m["body"] == "a body"
