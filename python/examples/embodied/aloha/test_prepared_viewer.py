"""Validate camera configuration from the actual Notebook without PG/S3."""

import ast
import json
from pathlib import Path
import unittest


class CameraSelectionTests(unittest.TestCase):
    def test_camera_selection(self):
        notebook = json.loads(
            Path(__file__).with_name("aloha_prepared_viewer.ipynb").read_text()
        )
        namespace = {}
        for cell in notebook["cells"]:
            if cell["cell_type"] != "code":
                continue
            tree = ast.parse("".join(cell["source"]))
            for node in tree.body:
                if (
                    isinstance(node, ast.FunctionDef)
                    and node.name == "selected_cameras"
                ) or (
                    isinstance(node, ast.Assign)
                    and any(
                        isinstance(t, ast.Name) and t.id in ("CAMERAS", "CAMERA_NAMES")
                        for t in node.targets
                    )
                ):
                    exec(
                        compile(
                            ast.Module(body=[node], type_ignores=[]), "notebook", "exec"
                        ),
                        namespace,
                    )
        names = list(namespace["CAMERA_NAMES"])
        self.assertEqual(namespace["selected_cameras"](names), names)
        for selected in [["cam_high"], ["cam_high", "cam_left_wrist"], []]:
            namespace["CAMERAS"] = selected
            self.assertEqual(namespace["selected_cameras"](names), selected)
        for invalid in ["cam_high", ["cam_high", "cam_high"], ["cam_typo"], [None]]:
            namespace["CAMERAS"] = invalid
            with self.assertRaises(ValueError):
                namespace["selected_cameras"](names)
        namespace["CAMERAS"] = ["cam_low"]
        with self.assertRaises(ValueError):
            namespace["selected_cameras"](["cam_high"])


if __name__ == "__main__":
    unittest.main()
