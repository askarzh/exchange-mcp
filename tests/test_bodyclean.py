"""Golden-style tests for src.body_clean — deterministic email body cleaning.

All fixtures are realistic inline constants; assertions pin exact cleaned
outputs (not just lengths) so any behavior drift fails loudly.
"""

from ewsmcp.bodyclean import (
    clean_body,
    html_to_text,
    strip_header_lines,
    strip_quoted_history,
    strip_signature,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

EN_OUTLOOK = (
    "Hi team,\n"
    "\n"
    "The Q2 numbers are attached. Please review the variance on slide 4\n"
    "before Thursday's meeting.\n"
    "\n"
    "From: John Smith <john.smith@contoso.com>\n"
    "Sent: Monday, June 1, 2026 9:14 AM\n"
    "To: Omar <omar@bank.example>\n"
    "Subject: RE: Q2 numbers\n"
    "\n"
    "Earlier text that should be stripped.\n"
)
EN_OUTLOOK_KEPT = (
    "Hi team,\n"
    "\n"
    "The Q2 numbers are attached. Please review the variance on slide 4\n"
    "before Thursday's meeting."
)

GMAIL_EN = (
    "Sounds good — see you at 10.\n"
    "I will bring the printed deck.\n"
    "\n"
    "On Mon, Jun 1, 2026 at 9:14 AM Sarah Lee <sarah@fabrikam.com> wrote:\n"
    "> Can we move the sync to 10?\n"
    "> The room is taken at 9.\n"
)
GMAIL_EN_KEPT = "Sounds good — see you at 10.\nI will bring the printed deck."

QUOTED_RUN = (
    "Thanks for the update.\n"
    "Looks good to me.\n"
    "\n"
    "> Status: deployment finished at 22:00.\n"
    "> All checks passed.\n"
    "> Rollback plan was not needed.\n"
)
QUOTED_RUN_KEPT = "Thanks for the update.\nLooks good to me."

# Body that STARTS with a From:/Sent:-looking pair — must NOT be cut.
FROM_START = (
    "From: my point of view, the proposal is solid.\n"
    "Sent: I mean, submitted, last night for review.\n"
    "Let me know your thoughts."
)

# Mid-paragraph "From:" with no Sent:/Date: partner — must NOT be cut.
FROM_MIDPARA = (
    "The committee reviewed the file.\n"
    "All members agreed.\n"
    "From: a governance standpoint this is fine.\n"
    "We can proceed."
)

PLAIN = (
    "Quick note — the dashboard refresh moved to 6 AM.\n"
    "No action needed from your side."
)

SIG_EN = (
    "The contract is signed and archived.\n"
    "Finance has been notified.\n"
    "\n"
    "Best regards,\n"
    "John Smith\n"
    "Director, Legal\n"
)
SIG_EN_KEPT = "The contract is signed and archived.\nFinance has been notified."

SIG_DELIM = (
    "Numbers confirmed.\n"
    "Invoice goes out today.\n"
    "\n"
    "-- \n"
    "Sara\n"
    "Fabrikam Ltd\n"
)
SIG_DELIM_KEPT = "Numbers confirmed.\nInvoice goes out today."

FULL_EMAIL = (
    "Hi Sarah,\n"
    "\n"
    "Approved — please proceed with phase two.\n"
    "Loop in finance on the PO.\n"
    "\n"
    "\n"
    "\n"
    "Best regards,\n"
    "Omar\n"
    "\n"
    "From: Sarah Lee <sarah@fabrikam.com>\n"
    "Sent: Monday, June 1, 2026 8:02 AM\n"
    "To: Omar\n"
    "Subject: Phase two\n"
    "\n"
    "May we proceed?\n"
)
FULL_EMAIL_CLEAN = (
    "Hi Sarah,\n"
    "\n"
    "Approved — please proceed with phase two.\n"
    "Loop in finance on the PO."
)

TRUNC_SUFFIX = "… [truncated]"

HTML_DOC = (
    "<html><head><title>x</title><style>p {color: red}</style></head><body>"
    "<p>مرحباً <b>عمر</b>،</p>"
    "<table><tr><td>البند</td><td>القيمة</td></tr>"
    "<tr><td>Total</td><td>500 &amp; up</td></tr></table>"
    '<p>See <a href="https://example.com/report">report</a> first.</p>'
    '<p>Track: <a href="https://example.com/r?token=' + "x" * 90 + '">'
    "this link</a></p>"
    '<p><a href="https://example.com/">example.com</a></p>'
    "</body></html>"
)


# ---------------------------------------------------------------------------
# strip_quoted_history
# ---------------------------------------------------------------------------

def test_outlook_en_chain_cut_at_from_sent_pair():
    assert strip_quoted_history(EN_OUTLOOK) == (EN_OUTLOOK_KEPT, 1)


def test_gmail_en_attribution_cut():
    assert strip_quoted_history(GMAIL_EN) == (GMAIL_EN_KEPT, 1)


def test_quoted_run_of_three_lines_cut_as_one_block():
    assert strip_quoted_history(QUOTED_RUN) == (QUOTED_RUN_KEPT, 1)


def test_body_starting_with_from_like_line_survives():
    assert strip_quoted_history(FROM_START) == (FROM_START, 0)


def test_midparagraph_from_without_pair_not_cut():
    assert strip_quoted_history(FROM_MIDPARA) == (FROM_MIDPARA, 0)


def test_plain_body_unchanged_count_zero():
    assert strip_quoted_history(PLAIN) == (PLAIN, 0)


def test_original_message_may_cut_even_on_first_line():
    text = (
        "-----Original Message-----\n"
        "From: X <x@y.example>\n"
        "Sent: Monday\n"
        "\n"
        "Old."
    )
    assert strip_quoted_history(text) == ("", 2)


def test_strip_quoted_history_empty_inputs():
    assert strip_quoted_history("") == ("", 0)
    assert strip_quoted_history(None) == ("", 0)


def test_arabic_markers_are_no_longer_special_cased():
    """Phase 1.5: bodyclean is English-only. An Arabic Outlook header is
    just text now — the body survives whole."""
    text = (
        "شكراً جزيلاً.\n"
        "\n"
        "من: سارة <sara@example.com>\n"
        "التاريخ: 31 مايو 2026\n"
    )
    assert strip_quoted_history(text) == (text.rstrip(), 0)


def test_english_markers_still_cut_with_bidi_characters_present():
    """A stray RLM no longer gets scrubbed, so a marker carrying one is
    simply not a marker. The plain English marker still cuts."""
    text = "Noted.\n\nFrom: A <a@b.example>\nSent: Monday\n\nOld."
    assert strip_quoted_history(text) == ("Noted.", 1)


# ---------------------------------------------------------------------------
# strip_signature
# ---------------------------------------------------------------------------

def test_signature_en_closer_block_stripped():
    assert strip_signature(SIG_EN) == SIG_EN_KEPT


def test_arabic_closer_is_not_a_signature():
    text = "تم اعتماد المسودة النهائية.\nسيتم الرفع للإدارة غداً صباحاً.\n\nمع التحية\nسارة"
    assert strip_signature(text) == text


def test_signature_dash_dash_delimiter_stripped():
    assert strip_signature(SIG_DELIM) == SIG_DELIM_KEPT


def test_signature_sent_from_my_stripped():
    text = "On it.\nWill confirm by noon.\n\nSent from my iPhone"
    assert strip_signature(text) == "On it.\nWill confirm by noon."


def test_signature_not_stripped_when_body_too_short():
    text = "Approved.\n\nBest regards,\nJohn"
    assert strip_signature(text) == text


def test_closer_like_sentence_in_body_not_stripped():
    # "Thanks," followed by real prose must not be treated as a closer.
    text = (
        "Hi team\n"
        "The report is ready for review.\n"
        "Thanks, that works for me as well."
    )
    assert strip_signature(text) == text


def test_strip_signature_empty_inputs():
    assert strip_signature("") == ""
    assert strip_signature(None) == ""


# ---------------------------------------------------------------------------
# clean_body pipeline
# ---------------------------------------------------------------------------

def test_clean_body_full_email_quote_and_signature():
    assert clean_body(FULL_EMAIL) == {
        "text": FULL_EMAIL_CLEAN,
        "quoted_blocks_stripped": 1,
        "truncated": False,
        "original_chars": len(FULL_EMAIL),
        "disclaimer_cut": False,
    }


def test_clean_body_collapses_three_plus_blank_lines():
    text = "Line one.\nLine two.\n\n\n\nLine three."
    result = clean_body(text)
    assert result["text"] == "Line one.\nLine two.\n\nLine three."
    assert result["quoted_blocks_stripped"] == 0
    assert result["truncated"] is False


def test_clean_body_normalizes_crlf():
    result = clean_body("A line.\r\nSecond line.\r\n")
    assert result["text"] == "A line.\nSecond line."
    assert result["original_chars"] == len("A line.\r\nSecond line.\r\n")


def test_clean_body_truncates_at_word_boundary():
    text = "alpha bravo charlie " * 40  # 800 chars, no markers
    result = clean_body(text, max_chars=100)
    assert result["truncated"] is True
    assert result["original_chars"] == 800
    assert len(result["text"]) <= 100
    assert result["text"].endswith(TRUNC_SUFFIX)
    body = result["text"][: -len(TRUNC_SUFFIX)]
    # No word may be cut in half.
    assert set(body.split()) <= {"alpha", "bravo", "charlie"}


def test_clean_body_no_truncation_under_limit():
    result = clean_body(PLAIN, max_chars=4000)
    assert result["text"] == PLAIN
    assert result["truncated"] is False
    assert TRUNC_SUFFIX not in result["text"]


def test_clean_body_empty_and_none():
    expected = {
        "text": "",
        "quoted_blocks_stripped": 0,
        "truncated": False,
        "original_chars": 0,
        "disclaimer_cut": False,
    }
    assert clean_body("") == expected
    assert clean_body(None) == expected


# ---------------------------------------------------------------------------
# html_to_text
# ---------------------------------------------------------------------------

def test_html_to_text_table_links_and_non_latin():
    out = html_to_text(HTML_DOC)
    nonempty = [ln for ln in out.split("\n") if ln]
    assert nonempty == [
        "مرحباً عمر،",
        "البند القيمة",
        "Total 500 & up",
        "See report (https://example.com/report) first.",
        "Track: this link",
        "example.com",
    ]
    # <style>/<head> content and the long tracking href are dropped.
    assert "color" not in out
    assert "token=" not in out


def test_html_to_text_br_and_entities():
    assert html_to_text("<div>One<br>Two &amp; Three</div>") == "One\nTwo & Three"


def test_html_to_text_link_label_equals_href():
    html = '<p>Visit <a href="https://example.com/">https://example.com/</a> today</p>'
    assert html_to_text(html) == "Visit https://example.com/ today"


def test_html_to_text_empty_inputs():
    assert html_to_text("") == ""
    assert html_to_text(None) == ""


def test_russian_forward_header_lines_are_dropped_but_content_stays():
    """A Russian forward (Apple Mail / Outlook RU) is not quoted history:
    the forwarded text is often the only copy in the mailbox and must
    survive — only the boilerplate header lines go, so embeddings stop
    matching every forward to every other forward."""
    text = (
        "Для работы\n"
        "Покажите мне перед отправкой\n"
        "\n"
        "Начало переадресованного письма:\n"
        "От: Ильясова Асем <assem@bank.example>\n"
        "Дата: 3 сентября 2026 г. в 19:39:00 GMT+5\n"
        "Кому: Енсебаев Руслан <ruslan@bank.example>\n"
        "Копия: Овсянникова Анастасия <a@bank.example>\n"
        "Тема: Прогноз - инвестиции в ДО\n"
        "\n"
        "Добрый день, коллеги!\n"
        "Просим предоставить прогнозы по докапитализации ваших ДО.\n"
    )
    out = clean_body(text)["text"]
    assert out == (
        "Для работы\n"
        "Покажите мне перед отправкой\n"
        "\n"
        "Добрый день, коллеги!\n"
        "Просим предоставить прогнозы по докапитализации ваших ДО."
    )
    # A body line that merely starts with one of the words is not a header.
    assert clean_body("Тема встречи обсуждалась вчера.")["text"] == (
        "Тема встречи обсуждалась вчера.")


def test_external_sender_banner_and_invisible_marks_are_dropped():
    """The gateway's KZ/RU 'external sender' banner rides on every inbound
    mail and must not reach the embedder; Apple Mail's U+FEFF and NBSPs are
    normalised away too."""
    text = (
        "Назар аударыңыз! Бұл хатты сыртқы адресат жіберген. Сақ болыңыз!\n"
        "Егер хатты күдікті деп ойласаңыз, АҚҚО-на дереу хабарласыңыз: "
        "АҚҚО<mailto:ib-incident@bank.example>\n"
        "Внимание! Данное письмо отправлено внешним адресатом. "
        "Будьте осторожны при работе с вложениями и ссылками!\n"
        "Если считаете письмо подозрительным, незамедлительно обратитесь в ЦОИБ\n"
        "\n"
        "\ufeff\n"
        "Джан, потрясающие идеи.\n"
        "Давайте сконцентрируемся на Кейсе 1 и 2.\n"
    )
    out = clean_body(text)["text"]
    assert out == "Джан, потрясающие идеи.\nДавайте сконцентрируемся на Кейсе 1 и 2."
    assert "\ufeff" not in out and "\u00a0" not in out


def test_gmail_attribution_with_outlook_rendered_address_still_cuts_history():
    """Outlook renders the quoted address as "<a@b<mailto:a@b>>", pushing the
    "On … wrote:" line past 80 chars; the quoted chain must still be cut."""
    text = (
        "Please find attached the signed NDA from our side.\n"
        "\n"
        "On Thu, Aug 20, 2026 at 1:09 PM Saheli Maitra "
        "<saheli.maitra@tuum.example<mailto:saheli.maitra@tuum.example>> wrote:\n"
        "Dear Askar,\n"
        "Thank you for the update.\n"
    )
    out = clean_body(text)
    assert out["text"] == "Please find attached the signed NDA from our side."
    assert out["quoted_blocks_stripped"] == 1


def test_outlook_rule_line_is_dropped_between_paragraphs():
    """Outlook's auto-inserted horizontal rule separating a forwarded
    message from the rest of the body is decorative and must go, while both
    surrounding paragraphs survive."""
    text = (
        "Коллеги, посмотрите, пожалуйста.\n"
        "________________________________\n"
        "От кого: Иванов Иван\n"
    )
    out = strip_header_lines(text)
    assert "________________________________" not in out
    assert "Коллеги, посмотрите, пожалуйста." in out
    assert "От кого: Иванов Иван" in out


# ---------------------------------------------------------------------------
# RU signature closers
# ---------------------------------------------------------------------------

SIG_RU = (
    "Договор подписан и передан в архив.\n"
    "Финансовый отдел уведомлен.\n"
    "\n"
    "С уважением,\n"
    "Аскар Жакенов\n"
    "BCC-HUB\n"
    "Mobile: +7 701 0000000\n"
)
SIG_RU_KEPT = "Договор подписан и передан в архив.\nФинансовый отдел уведомлен."


def test_signature_ru_closer_block_stripped():
    assert strip_signature(SIG_RU) == SIG_RU_KEPT


# ---------------------------------------------------------------------------
# Disclaimer tail cut
# ---------------------------------------------------------------------------

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


def test_kazakh_disclaimer_anchor_is_cut():
    """Kazakh 'if you received this by mistake, please delete it' anchor."""
    kz_footer = (
        "Егер сіз бұл хабарламаны қателесіп алсаңыз, оны кез келген түрде "
        "пайдаланбауды және жоюды өтінеміз.\n"
    )
    body = _long_body(6) + "\n\n" + kz_footer
    out = clean_body(body)
    assert out["disclaimer_cut"] is True
    assert "қателесіп" not in out["text"]
    assert out["text"].endswith("вопросы по документам.")
