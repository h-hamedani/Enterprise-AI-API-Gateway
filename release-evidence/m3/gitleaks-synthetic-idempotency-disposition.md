# M3 historical Gitleaks disposition

Three `generic-api-key` findings in `tests/integration/test_llm_credential_rotation.py` were investigated. All three are occurrences of the same synthetic, test-only idempotency key introduced in commit `c7a4e341f9bbafe097b814664130400e714a9389`. The test reuses it to verify the original credential-rotation request, idempotent replay, and conflicting-payload rejection. It is not the provider credential fixture, and repository evidence shows no provider or service authentication use.

The current test source now generates one synthetic idempotency key at runtime and reuses it for those three calls. Historical findings are dispositioned only by the three exact fingerprints in `.gitleaksignore`. No path-wide or rule-wide exclusion, test-directory exemption, or broad baseline was added. Credential rotation and Git history rewrite are not warranted for this synthetic fixture.

Verification with Gitleaks 8.30.1 and `.gitleaks.toml` (JSON reports redacted and temporary reports removed):

- `gitleaks dir .`: exit 0, zero findings.
- `gitleaks git`: exit 0, zero findings.
- Affected credential-rotation integration test: 1 passed.
- Related idempotency and registry tests: 15 passed.
- Full pytest: 665 passed, 4 skipped, 1 unrelated setup error (`WinError 5` accessing the pytest temporary directory for `test_recovery_secret_file_permissions_are_restricted`).

The default `generic-api-key` rule and scanning of future test content remain enabled. This disposition applies only to the three audited historical fingerprints.
