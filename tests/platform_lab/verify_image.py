"""Run with the image's Python; verifies the frozen export and source bytes."""

import hashlib
import importlib.metadata
import json
import platform
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

assert platform.python_version_tuple()[:2] == ("3", "12")
assert platform.machine() == "x86_64"
expected = {}
for line in Path("/tmp/locked.txt").read_text().replace("\\\n", " ").splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    requirement = Requirement(line.split(" --hash=", 1)[0].strip())
    if requirement.marker and not requirement.marker.evaluate():
        continue
    pins = list(requirement.specifier)
    assert len(pins) == 1 and pins[0].operator == "==", line
    expected[canonicalize_name(requirement.name)] = pins[0].version
actual = {
    canonicalize_name(d.metadata["Name"]): d.version
    for d in importlib.metadata.distributions()
}
assert actual == expected, {"expected": expected, "actual": actual}
assert actual["redis"] == "8.1.0"
for line in Path("/tmp/source.sha256").read_text().splitlines():
    digest, path = line.split("  ", 1)
    assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest, path
import app.server  # noqa: F401
import tests.integration.test_m4_platform_shutdown  # noqa: F401

print(json.dumps({"python": platform.python_version(), "packages": actual}, indent=2))
