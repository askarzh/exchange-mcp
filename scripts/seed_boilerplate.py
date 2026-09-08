"""One-off: seed ews.boilerplate_refs with the reference disclaimer texts.

The embedding detector (ewsmcp/boilerplate.py) compares the tail paragraphs of
every message against these vectors. Run it once after migration 004, and
again whenever scripts/boilerplate_refs.txt changes — upserts are keyed by
label, so re-running is idempotent.

The image ships only the `ewsmcp` package, so pipe the script in:

    docker exec -i ewsd python - < scripts/seed_boilerplate.py

REFS below duplicates scripts/boilerplate_refs.txt (the human-readable source)
because a piped script cannot read a file from the repo. The two MUST be kept
in sync.
"""

from __future__ import annotations

import sys

from ewsmcp.cache.store import CacheStore
from ewsmcp.config import Settings
from ewsmcp.db import Database
from ewsmcp.embeddings import GeminiEmbedder

REFS: list[tuple[str, str]] = [
    (
        "bcc_invest_ru",
        "Предоставляемая АО «BCC Invest» информация не является предложением о покупке и/или "
        "обязательством по продаже того или иного финансового инструмента, побуждением к "
        "заключению сделки.\n"
        "Перед совершением сделок с финансовыми инструментами просим ознакомиться с их "
        "характеристиками и информацией, размещенной на сайте АО «BCC Invest» "
        "https://www.bcc-invest.kz/documents?page=1\n"
        "Государственная лицензия № 3.2.235/12 от 10.07.2018 года на осуществление деятельности "
        "на рынке ценных бумаг."
    ),
    (
        "confidentiality_ru",
        "Содержание этого электронного письма предназначено только для получателей, указанных в "
        "сообщении.\n"
        "Если вы получили это сообщение по ошибке, пожалуйста сообщите об этом отправителю и "
        "затем удалите письмо."
    ),
    (
        "confidentiality_kz",
        "Бұл электрондық хаттың мазмұны тек хабарламада көрсетілген алушыларға арналған.\n"
        "Егер сіз бұл хабарламаны қателесіп алсаңыз, бұл жөнінде жөнелтушіге хабарлауды және "
        "содан кейін хатты жоюды өтінеміз."
    ),
    (
        "gateway_banner_kz",
        "Назар аударыңыз! Бұл хатты сыртқы адресат жіберген. Салынған файлдармен және "
        "сілтемелермен жұмыс істегенде, сақ болыңыз! Егер хатты күдікті деп ойласаңыз, АҚҚО-на "
        "дереу хабарласыңыз: АҚҚО"
    ),
    (
        "gateway_banner_ru",
        "Внимание! Данное письмо отправлено внешним адресатом. Будьте осторожны при работе с "
        "вложениями и ссылками! Если считаете письмо подозрительным, незамедлительно обратитесь "
        "в ЦОИБ"
    ),
    (
        "gateway_banner_en",
        "Attention! This message was sent by an external sender. Be careful when working with "
        "attachments and links! If you consider this message suspicious, contact the "
        "information security centre immediately."
    ),
    (
        "confidentiality_en",
        "CONFIDENTIALITY NOTICE This e-mail message and any attachments are only for the use of "
        "the intended recipient and may contain information that is privileged, confidential or "
        "exempt from disclosure under applicable law. If you are not the intended recipient, "
        "any disclosure, distribution or other use of this e-mail message or attachments is "
        "prohibited. If you have received this e-mail message in error, please delete and "
        "notify the sender immediately. Thank you."
    ),
    (
        "intended_recipient_en",
        "This e-mail and any files transmitted with it are intended solely for the use of the "
        "individual or entity to whom they are addressed. If you have received this email in "
        "error please notify the sender and delete it from your system. Any unauthorised "
        "copying, disclosure or distribution of the material in this e-mail is strictly "
        "forbidden."
    ),
]


def main() -> int:
    settings = Settings()
    if not settings.semantic_enabled():
        print("GEMINI_API_KEY unset - cannot embed reference texts", file=sys.stderr)
        return 2
    db = Database(settings.database_url)
    db.migrate()
    store = CacheStore(db)
    embedder = GeminiEmbedder(settings.gemini_api_key, dims=settings.embed_dims)
    vectors = embedder.embed([text for _label, text in REFS])
    for (label, text), vector in zip(REFS, vectors):
        store.upsert_boilerplate_ref(label, text, vector)
        print(f"seeded {label} ({len(text)} chars)")
    print(f"{len(REFS)} reference(s) in ews.boilerplate_refs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
