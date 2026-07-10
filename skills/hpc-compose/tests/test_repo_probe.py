from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts/hpc_compose_repo_probe.py"
SPEC = importlib.util.spec_from_file_location("hpc_compose_repo_probe", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
probe = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = probe
SPEC.loader.exec_module(probe)


class RepositoryProbeTests(unittest.TestCase):
    def signal_names(self, report: dict[str, object]) -> set[str]:
        return {signal["name"] for signal in report["signals"]}

    def test_prose_mpi_mention_is_not_workload_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text(
                "This project is not an MPI workload and never calls mpirun.\n",
                encoding="utf-8",
            )
            (root / "notes.py").write_text(
                "# The legacy project did not use mpirun or mpi4py.\nprint('serial')\n",
                encoding="utf-8",
            )
            report = probe.scan_repository(root)
            self.assertNotIn("mpi", self.signal_names(report))

    def test_hidden_tool_captures_never_become_workload_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture = root / ".playwright-mcp"
            capture.mkdir()
            (capture / "page.yml").write_text(
                "copied_page: torchrun mpirun deepspeed cuda\n",
                encoding="utf-8",
            )
            report = probe.scan_repository(root)
            self.assertFalse(
                {"pytorch", "mpi", "deepspeed", "gpu"} & self.signal_names(report)
            )
            self.assertEqual(report["scan"]["skipped_hidden_dirs"], 1)

    def test_real_mpi_and_pytorch_code_produce_evidence_not_snippets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "train.py").write_text("import torch\nprint(torch.cuda.is_available())\n")
            (root / "launch.sh").write_text("#!/bin/sh\nmpirun -n 4 python train.py\n")
            report = probe.scan_repository(root)
            self.assertTrue({"mpi", "pytorch", "gpu"} <= self.signal_names(report))
            encoded = json.dumps(report)
            self.assertNotIn("torch.cuda.is_available", encoded)
            self.assertNotIn("mpirun -n 4", encoded)
            mpi = next(signal for signal in report["signals"] if signal["name"] == "mpi")
            self.assertEqual(mpi["evidence_paths"], ["launch.sh"])

    def test_secret_files_and_symlinks_are_never_read(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = Path(directory)
            secret = "never-emit-this-token"
            (root / ".env").write_text(f"TOKEN={secret}\n")
            (root / "credentials.json").write_text(json.dumps({"token": secret}))
            outside_secret = Path(outside) / "train.py"
            outside_secret.write_text(f"import torch\nTOKEN={secret!r}\n")
            (root / "linked.py").symlink_to(outside_secret)
            report = probe.scan_repository(root)
            encoded = json.dumps(report)
            self.assertNotIn(secret, encoded)
            self.assertNotIn("linked.py", encoded)
            self.assertEqual(report["scan"]["skipped_sensitive_files"], 2)
            self.assertEqual(report["scan"]["skipped_symlinks"], 1)

    def test_output_is_deterministic_and_evidence_is_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "z.py").write_text("import torch\n")
            (root / "a.py").write_text("from torch import nn\n")
            first = probe.scan_repository(root)
            second = probe.scan_repository(root)
            self.assertEqual(first, second)
            pytorch = next(signal for signal in first["signals"] if signal["name"] == "pytorch")
            self.assertEqual(pytorch["evidence_paths"], ["a.py", "z.py"])

    def test_scan_limits_and_evidence_cap_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index in range(5):
                (root / f"train-{index}.py").write_text("import torch\n")
            report = probe.scan_repository(
                root,
                probe.ScanLimits(
                    max_files=3,
                    max_total_text_bytes=1024,
                    max_file_text_bytes=64,
                    max_evidence_paths=2,
                ),
            )
            self.assertTrue(report["scan"]["truncated"])
            self.assertIn("file_limit", report["scan"]["truncation_reasons"])
            pytorch = next(signal for signal in report["signals"] if signal["name"] == "pytorch")
            self.assertEqual(len(pytorch["evidence_paths"]), 2)
            self.assertTrue(pytorch["evidence_truncated"])

    def test_per_file_and_total_byte_limits_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "large.py").write_text("import torch\n" + "x = 1\n" * 100)
            report = probe.scan_repository(
                root,
                probe.ScanLimits(
                    max_files=10,
                    max_total_text_bytes=32,
                    max_file_text_bytes=24,
                    max_evidence_paths=20,
                ),
            )
            self.assertTrue(report["scan"]["truncated"])
            self.assertIn("per_file_text_byte_limit", report["scan"]["truncation_reasons"])
            self.assertLessEqual(report["scan"]["text_bytes_read"], 32)


if __name__ == "__main__":
    unittest.main()
