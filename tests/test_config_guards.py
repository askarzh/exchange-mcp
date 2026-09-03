"""Settings guards: absolute data_dir + the synced-folder refusal.

The data dir holds mail-at-rest (aliases, audit chain, cache mirror) —
booting with it inside OneDrive/Dropbox/… replicates a mailbox to every
synced device, so the default posture is refusal with an explicit escape
hatch.
"""

import pytest
from conftest import make_settings


def test_data_dir_is_always_absolute(tmp_path):
    s = make_settings(data_dir=str(tmp_path / "d"))
    import os
    assert os.path.isabs(s.data_dir)


def test_default_data_dir_is_home_scoped_absolute(monkeypatch, tmp_path):
    monkeypatch.delenv("DATA_DIR", raising=False)
    s = make_settings()
    assert s.data_dir.endswith(".ewsmcp")


@pytest.mark.parametrize("marker", ["OneDrive", "Dropbox", "Google Drive"])
def test_synced_paths_are_refused(tmp_path, marker):
    with pytest.raises(Exception, match="synced"):
        make_settings(data_dir=str(tmp_path / marker / "data"))


def test_synced_path_escape_hatch(tmp_path):
    s = make_settings(data_dir=str(tmp_path / "OneDrive" / "data"),
                      data_dir_allow_synced=True)
    assert "OneDrive" in s.data_dir


def test_confirm_ttl_default_matches_confirm_module():
    from ewsmcp import confirm
    assert make_settings().confirm_ttl_seconds == confirm.DEFAULT_TTL_SECONDS == 600


def test_require_exchange_lists_missing():
    from ewsmcp.config import Settings
    s = Settings(ews_email="a@b.c", database_url="postgresql://x")
    with pytest.raises(ValueError) as e:
        s.require_exchange()
    assert "EWS_SERVER_URL" in str(e.value) and "EWS_PASSWORD" in str(e.value)


def test_ewsmcp_does_not_require_ews_email():
    """ewsmcp's documented launch (DATABASE_URL, EWSD_URL, EWSD_API_KEY) must
    not fail with a ValidationError for a missing EWS_EMAIL."""
    from ewsmcp.config import Settings
    s = Settings(database_url="postgresql://x")
    assert s.ews_email == ""


def test_require_exchange_names_ews_email_when_blank():
    from ewsmcp.config import Settings
    s = Settings(database_url="postgresql://x")
    with pytest.raises(ValueError) as e:
        s.require_exchange()
    assert "EWS_EMAIL" in str(e.value)
