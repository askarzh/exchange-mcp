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
        "chat_id": "conv1",
        # `first_seen` moves on every amendment; `first_arrival` is written
        # once. A message's fallback send time reads the one that stands still.
        "first_seen": dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc),
        "first_arrival": dt.datetime(2023, 11, 14, 22, 13, 20, tzinfo=dt.timezone.utc),
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
    """A conversation's kind is decided once, by the union across its whole
    thread — `mapping.chat()` used to decide it a second time from a single
    row's recipients, and the two answers could differ."""
    # Real shape: to_json is a list of plain address strings.
    one = json.dumps(["owner@example.test"])
    two = json.dumps(["owner@example.test", "another@example.test"])
    row = {"native_id": "conv1", "subject": "s", "sender_email": "sender@example.test"}
    assert mapping.chats_from_rows([{**row, "to_json": one}])[0]["kind"] == "direct"
    assert mapping.chats_from_rows([{**row, "to_json": two}])[0]["kind"] == "group"


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
    assert mapping.chat_native_id(_row()) == "conv1"
    # a mail with no conversation id is its own thread rather than joining a
    # nameless bucket with every other such mail
    assert mapping.chat_native_id(_row(conversation_id=None)) == "m1"


def test_the_timestamp_is_rendered_from_the_epoch_not_the_stored_string():
    m = mapping.message(_row(date_ts=1_700_000_000))
    assert m["sent_at"] == "2023-11-14T22:13:20+00:00"


def test_timestamp_falls_back_to_first_arrival_when_date_ts_is_none():
    """A mail whose send time couldn't be parsed falls back to when the store
    first met it, rather than silently using epoch."""
    first_arrival = dt.datetime(2023, 9, 15, 10, 30, 45, tzinfo=dt.timezone.utc)
    m = mapping.message(_row(date_ts=None, first_arrival=first_arrival))
    assert m["sent_at"] == "2023-09-15T10:30:45+00:00"


def test_the_fallback_send_time_is_the_one_that_does_not_move():
    """`first_seen` moves to now() on every amendment, because it orders the
    arrival stream. `first_arrival` is written once. An undated draft that
    reported the moving one appeared to have been sent a little later every
    time the owner flagged it in Outlook."""
    m = mapping.message(_row(date_ts=None,
                             first_arrival=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
                             first_seen=dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)))
    assert m["sent_at"] == "2026-01-01T00:00:00+00:00"


def test_message_raises_when_no_timestamp_at_all():
    """A message with neither date_ts nor first_arrival has no place in a
    ledger of deadlines."""
    with pytest.raises(ValueError, match="has no date_ts or first_arrival"):
        mapping.message(_row(date_ts=None, first_arrival=None))


def test_the_chat_is_the_pinned_one_not_a_fresh_coalesce():
    """Exchange fills a conversation id in late for a draft or an unindexed
    item. If the chat were recomputed per read, that mail would be handed over
    under one chat and later under another — and the consumer, keying on
    (venue, native_id), would record it twice: one mail, two directives."""
    m = mapping.message(_row(chat_id="pinned-at-first-sight",
                             conversation_id="the-one-exchange-learned-later"))
    assert m["chat"] == "pinned-at-first-sight"


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


# ------------------------------------------------------- meetings are not mail
#
# The owner's calendar is in his mailbox: Exchange delivers an invitation, each
# reply and every cancellation as items whose class says what they are. The
# bridge handed them over as ordinary mail, so a consumer could not tell a
# meeting from a message, when it starts, or whether it was called off — and an
# invitation's body is usually empty, so what survived was nothing at all.


def test_an_invitation_says_it_is_a_meeting_and_keeps_its_subject():
    m = mapping.message(_row(item_class="IPM.Schedule.Meeting.Request",
                             subject="Quarterly review", body_clean=""))
    assert m["meta"]["meeting"] == {"kind": "invitation"}
    assert m["text"] == "Quarterly review"
    assert m["kind"] == "mail"


def test_an_invitation_with_a_body_keeps_both():
    m = mapping.message(_row(item_class="IPM.Schedule.Meeting.Request",
                             subject="Quarterly review", body_clean="agenda inside"))
    assert m["text"] == "Quarterly review\n\nagenda inside"


@pytest.mark.parametrize(
    ("item_class", "meeting"),
    [
        ("IPM.Schedule.Meeting.Request", {"kind": "invitation"}),
        ("IPM.Schedule.Meeting.Resp.Pos", {"kind": "reply", "response": "accepted"}),
        ("IPM.Schedule.Meeting.Resp.Neg", {"kind": "reply", "response": "declined"}),
        ("IPM.Schedule.Meeting.Resp.Tent", {"kind": "reply", "response": "tentative"}),
        ("IPM.Schedule.Meeting.Canceled", {"kind": "cancellation"}),
        ("IPM.Schedule.Meeting.Notification.Forward", {"kind": "forwarded"}),
    ],
)
def test_every_meeting_class_is_named_in_plain_words(item_class, meeting):
    assert mapping.message(_row(item_class=item_class))["meta"]["meeting"] == meeting


def test_ordinary_mail_carries_no_meeting_marker():
    assert "meeting" not in mapping.message(_row())["meta"]
    assert mapping.message(_row(item_class=None))["meta"] == {}


def test_a_meeting_class_this_bridge_does_not_know_is_still_a_meeting():
    """Exchange has more schedule classes than these; an unknown one must not
    read as ordinary mail, because that is how a cancellation goes unnoticed."""
    m = mapping.message(_row(item_class="IPM.Schedule.Meeting.Something.New"))
    assert m["meta"]["meeting"] == {"kind": "other", "class": "IPM.Schedule.Meeting.Something.New"}


def test_a_meeting_subject_is_not_repeated_when_the_body_already_starts_with_it():
    m = mapping.message(_row(item_class="IPM.Schedule.Meeting.Canceled",
                             subject="Quarterly review",
                             body_clean="Quarterly review has been cancelled"))
    assert m["text"] == "Quarterly review has been cancelled"
