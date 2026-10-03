# Final M3 GO evidence

- Candidate: `7c491d74613aee8f57310d32ebd33f6085ae6d9d`; local HEAD and `origin/feature/m3-runtime` matched.
- Final check: 2026-09-30 15:49 +03:30. Branch: `feature/m3-runtime`.
- Decision: **GO — M3 PASS**. All mandatory M3 release gates passed on the exact candidate commit.
- Earlier NO-GO context: three Gitleaks findings were confined to the unrelated, untracked `.doris-recovery-20260929/` workspace directory. It was moved outside the repository to `D:\Projects\_local-recovery\doris-recovery-20260929`. No Gitleaks rule, path exemption, fingerprint, or suppression was added for this remediation.

| Gate | Result |
| --- | --- |
| Docker baseline/final | Redis and PostgreSQL running and healthy; Redis `PONG`; PostgreSQL accepting connections. |
| Dependencies | `uv sync --frozen --no-cache`: exit 0, 47 packages checked. Existing `.venv` lock access warning. |
| Full pytest | Exit 0: 666 passed, 7 skipped, 4 warnings in 67.16s. Fresh unique repository-local `--basetemp`, no deselection. Recovery-secret permission test also passed in an explicit fresh-basetemp check. |
| M3.11-A | Exit 0: 1 passed. Real Redis stop/restart; Redis returned healthy. |
| M3.11-B/C | Exit 0: 2 passed. Real two-process flow and same-orphan concurrent cleanup. |
| Opt-in safety | Exit 0: three M3.11 acceptance tests skipped without opt-in flags. |
| Gitleaks current tree | `gitleaks dir .`: exit 0, zero findings; no leaks found. Redacted temporary report removed. |
| Gitleaks history | `gitleaks git`: exit 0, 65 commits scanned, zero findings. `.gitleaksignore` is unchanged with exactly three previously audited synthetic historical fingerprints. Redacted temporary report removed. |
| Ruff | Exit 0: repository check passed with no-cache mode. |
| Format | Exit 0: 181 files already formatted, checked unsandboxed because the sandboxed whole-tree formatter hit a host ACL-related crash. |
| Compileall | Exit 0: `app` and `tests`, with temporary bytecode prefix for Windows ACL compatibility. |
| Git diff checks | `git diff --check` and `git diff --cached --check`: exit 0. Only this evidence file is staged for release-record review. |
| Alembic | Single head `d4a7c9e2f1b6`; current equals head; check found no new upgrade operations. All three commands exited 0. |
| OpenAPI/route reconciliation | Exit 0: 10 passed. No contract regenerated. |
| Health HTTP smoke | Real localhost HTTP: `/health/live` 200 `ok`, `/health/ready` 200 `ready`, `/health/traffic` 200 `accepting`/`NORMAL` with a Windows SelectorEventLoop runner. |
| Circuit/error/security/telemetry checks | Exit 0: 119 targeted tests passed, including lifecycle cleanup, M3-R7 error boundary, M3.5 protection, and M3.10 telemetry/security. |
| Processes | No matching M3.11 worker or health-smoke Python processes remained. |

Host-specific caveat: the stock Windows uvicorn Proactor launch-path observation remains outside the validated deployment path and did not block the tested M3 release candidate. The SelectorEventLoop HTTP health smoke passed; no production startup code was changed.

The two temporary `.pytest-m311d-*` directories were removed. Repository worktree was clean immediately before the final evidence file was staged for release-record review. This evidence file is normally ignored by repository policy and was force-staged alone; no commit or push was made.

**Final M3 release verdict: GO — M3 PASS.** All mandatory M3 release gates passed on `7c491d74613aee8f57310d32ebd33f6085ae6d9d`.
