"""Run: python -m unittest test_train_state_policy -v"""

import ast
import json
import os
from pathlib import Path
import tempfile
import importlib.util
from types import SimpleNamespace
import unittest

import numpy as np

from train_state_policy import choose_indices, train_model, vector_stats, parse_args


class StatePolicyTests(unittest.TestCase):
    def test_shared_output_default_and_override(self):
        from common import DEFAULT_STATE_POLICY_OUTPUT

        original = Path.cwd()
        try:
            with tempfile.TemporaryDirectory() as directory:
                os.chdir(directory)
                self.assertEqual(
                    parse_args(["--samples-table", "samples"]).output,
                    DEFAULT_STATE_POLICY_OUTPUT,
                )
                self.assertEqual(
                    parse_args(
                        ["--samples-table", "samples", "--output", "custom"]
                    ).output,
                    Path("custom"),
                )
        finally:
            os.chdir(original)
        notebook = json.loads(
            Path(__file__).with_name("aloha_prediction_viewer.ipynb").read_text()
        )
        namespace = {}
        for cell in notebook["cells"]:
            if cell["cell_type"] != "code":
                continue
            for node in ast.parse("".join(cell["source"])).body:
                if (isinstance(node, ast.ImportFrom) and node.module == "common") or (
                    isinstance(node, ast.Assign)
                    and any(
                        isinstance(t, ast.Name) and t.id == "OUTPUT_DIR"
                        for t in node.targets
                    )
                ):
                    exec(
                        compile(
                            ast.Module(body=[node], type_ignores=[]), "notebook", "exec"
                        ),
                        namespace,
                    )
        self.assertEqual(namespace["OUTPUT_DIR"], DEFAULT_STATE_POLICY_OUTPUT)

    def test_sampling_preserves_split_and_is_reproducible(self):
        rows = [
            {"sample_id": str(i), "split": "train" if i < 10 else "val"}
            for i in range(20)
        ]
        picked = choose_indices(rows, "train", 4, 42)
        self.assertEqual(picked, choose_indices(rows, "train", 4, 42))
        self.assertTrue(all(r["split"] == "train" for r in picked))
        with self.assertRaises(ValueError):
            choose_indices(rows, "test", 4, 42)

    def test_constant_dimension_scaling(self):
        values = np.array([[[1, 2]], [[1, 4]]], dtype=np.float32)
        mean, scale = vector_stats(values)
        np.testing.assert_allclose(mean, [1, 3])
        np.testing.assert_allclose(scale, [1, 1])

    @unittest.skipUnless(importlib.util.find_spec("torch"), "PyTorch not installed")
    def test_training_learns_synthetic_mapping(self):
        rng = np.random.default_rng(42)
        x = rng.normal(size=(96, 2, 3)).astype(np.float32)
        y = x * 0.5 + 0.2
        args = SimpleNamespace(
            seed=42, threads=1, batch_size=16, learning_rate=0.01, epochs=30
        )
        checkpoint, predicted, metrics = train_model(
            x[:64], y[:64], x[64:], y[64:], args
        )
        self.assertEqual(predicted.shape, y[64:].shape)
        self.assertTrue(np.isfinite(predicted).all())
        self.assertTrue(metrics["beats_mean_action_baseline"])
        np.testing.assert_allclose(
            checkpoint["state_mean"].numpy(), x[:64].mean(axis=(0, 1))
        )


if __name__ == "__main__":
    unittest.main()
