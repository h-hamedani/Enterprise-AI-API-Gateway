from __future__ import annotations

from pathlib import Path

import pytest

from app import cli


def test_secret_output_requires_interactive_tty(monkeypatch):
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: False)
    with pytest.raises(cli.UnsafeSecretOutputError):
        cli._require_interactive_output()


def test_recovery_secret_file_permissions_are_restricted(tmp_path: Path):
    secret_file = tmp_path / "recovery.secret"
    secret_file.write_text("recovery-value", encoding="utf-8")
    secret_file.chmod(0o644)
    with pytest.raises(PermissionError):
        cli._read_recovery_secret(secret_file)
