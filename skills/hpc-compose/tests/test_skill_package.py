from __future__ import annotations

import importlib.util
import io
import sys
import tarfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[3] / "scripts/package_skill.py"
SPEC = importlib.util.spec_from_file_location("package_skill", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
package_skill = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = package_skill
SPEC.loader.exec_module(package_skill)


class SkillPackageTests(unittest.TestCase):
    def test_skill_contract_and_budgets_validate(self) -> None:
        files = package_skill.validate_skill(package_skill.crate_version())
        relative = {path.relative_to(package_skill.SKILL).as_posix() for path in files}
        self.assertIn("SKILL.md", relative)
        self.assertIn("VERSION", relative)
        self.assertIn("agents/openai.yaml", relative)
        self.assertIn("references/command-safety.md", relative)
        self.assertNotIn("agents/README.md", relative)
        self.assertFalse(any(path.startswith("tests/") for path in relative))
        self.assertFalse(any(path.is_symlink() for path in files))

    def test_archive_is_deterministic_and_contains_versioned_router(self) -> None:
        files = package_skill.validate_skill(package_skill.crate_version())
        first = package_skill.archive_bytes(files)
        second = package_skill.archive_bytes(files)
        self.assertEqual(first, second)
        with tarfile.open(fileobj=io.BytesIO(first), mode="r:gz") as archive:
            names = archive.getnames()
            self.assertIn("hpc-compose/SKILL.md", names)
            self.assertIn("hpc-compose/VERSION", names)
            version = archive.extractfile("hpc-compose/VERSION")
            assert version is not None
            self.assertEqual(version.read().decode().strip(), package_skill.crate_version())
            self.assertFalse(any("/tests/" in name for name in names))


if __name__ == "__main__":
    unittest.main()
