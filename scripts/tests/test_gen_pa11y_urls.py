from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "gen_pa11y_urls.py"
SPEC = importlib.util.spec_from_file_location("gen_pa11y_urls", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
pa11y = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = pa11y
SPEC.loader.exec_module(pa11y)


class Pa11yUrlTests(unittest.TestCase):
    def test_nested_pages_are_discovered_with_relative_urls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            build = root / "book"
            (build / "nested").mkdir(parents=True)
            (build / "index.html").write_text("index", encoding="utf-8")
            (build / "nested/page name.html").write_text("nested", encoding="utf-8")
            (build / "print.html").write_text("helper", encoding="utf-8")
            template = root / "pa11y.json"
            template.write_text(
                json.dumps({"defaults": {"standard": "WCAG2AA"}, "urls": []}),
                encoding="utf-8",
            )

            config = pa11y.generated_config(template, build, "http://localhost:3000/")

            self.assertEqual(
                config["urls"],
                [
                    "http://localhost:3000/index.html",
                    "http://localhost:3000/nested/page%20name.html",
                ],
            )


if __name__ == "__main__":
    unittest.main()
