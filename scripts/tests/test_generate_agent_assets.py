from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "generate_agent_assets.py"
SPEC = importlib.util.spec_from_file_location("generate_agent_assets", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
assets = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = assets
SPEC.loader.exec_module(assets)


class AgentAssetGeneratorTests(unittest.TestCase):
    def test_include_expansion_is_recursive_and_rejects_missing_files(self) -> None:
        with tempfile.TemporaryDirectory(dir=assets.ROOT) as directory:
            root = Path(directory)
            nested = root / "nested.md"
            nested.write_text("nested\n", encoding="utf-8")
            child = root / "child.md"
            child.write_text("child\n{{#include nested.md}}\n", encoding="utf-8")
            source = root / "source.md"
            source.write_text("start\n{{#include child.md}}\nend\n", encoding="utf-8")
            self.assertEqual(assets.expand_includes(source), "start\nchild\nnested\nend\n")
            source.write_text("{{#include missing.md}}\n", encoding="utf-8")
            with self.assertRaisesRegex(assets.GenerationError, "unresolved mdBook include"):
                assets.expand_includes(source)

    def test_llms_index_rejects_duplicate_urls_and_oversize(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            index = Path(directory) / "llms.txt"
            index.write_text(
                "# Demo\n\n> Summary\n\nInvariant.\n\n## Docs\n\n"
                "- [A](https://example.test/a)\n"
                "- [B](https://example.test/a)\n\n"
                "## Optional\n\n- [C](https://example.test/c)\n",
                encoding="utf-8",
            )
            with mock.patch.object(assets, "LLMS_INDEX", index):
                with self.assertRaisesRegex(assets.GenerationError, "duplicate URLs"):
                    assets.validate_llms_index()
                index.write_text("# Demo\n\n> Summary\n" + "x" * (assets.MAX_LLMS_BYTES + 1))
                with self.assertRaisesRegex(assets.GenerationError, "limit"):
                    assets.validate_llms_index()

    def test_site_surface_contains_all_raw_pages_schemas_and_policy_aliases(self) -> None:
        policy = assets.load_toml(assets.POLICY_SOURCE)
        assets.validate_policy(policy)
        outputs = assets.site_outputs(policy)
        self.assertIn(Path("llms.txt"), outputs)
        self.assertIn(Path("llms-ctx.txt"), outputs)
        self.assertIn(Path("llms-ctx-full.txt"), outputs)
        self.assertLessEqual(
            len(outputs[Path("llms-ctx.txt")]), assets.MAX_ESSENTIAL_CONTEXT_BYTES
        )
        self.assertLessEqual(
            len(outputs[Path("llms-ctx-full.txt")]), assets.MAX_FULL_CONTEXT_BYTES
        )
        self.assertIn(Path("agent-command-policy.json"), outputs)
        self.assertIn(
            Path(f"agent-command-policy-v{assets.crate_version()}.json"), outputs
        )
        self.assertIn(Path("schema/hpc-compose.schema.json"), outputs)
        self.assertIn(Path("schema/hpc-compose-settings.schema.json"), outputs)
        expected_output_schemas = {
            Path("schema/outputs") / path.name
            for path in (assets.ROOT / "schema/outputs").glob("*.schema.json")
        }
        self.assertTrue(expected_output_schemas <= set(outputs))
        self.assertTrue(all(b"{{#include" not in content for path, content in outputs.items() if path.parts[0] == "raw"))

    def test_compact_skill_reference_contains_every_policy_override(self) -> None:
        policy = assets.load_toml(assets.POLICY_SOURCE)
        assets.validate_policy(policy)
        rendered = assets.render_skill_safety(policy).decode()
        for override in policy.get("global_overrides", []):
            self.assertIn(override["description"], rendered)
        for command in policy["commands"]:
            for override in command.get("overrides", []):
                self.assertIn(override["description"], rendered)


if __name__ == "__main__":
    unittest.main()
