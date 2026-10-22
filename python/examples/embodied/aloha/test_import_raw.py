"""Validate camera CLI selection without connecting to LakeSoul or Ray."""

import contextlib
import io
import unittest
from unittest.mock import patch

import import_raw


class CameraSelectionTest(unittest.TestCase):
    def parse(self, *extra):
        argv = [
            "import_raw.py",
            "--source",
            "/unused",
            "--raw-table",
            "test",
            "--raw-path",
            "s3://unused/test",
            "--output",
            "/unused/report",
            *extra,
        ]
        with patch("sys.argv", argv), patch.object(import_raw, "run") as run:
            import_raw.main()
            return run.call_args.args[0]

    def test_default_four_cameras(self):
        self.assertEqual(
            self.parse().cameras,
            ["cam_high", "cam_low", "cam_left_wrist", "cam_right_wrist"],
        )

    def test_explicit_two_camera_compatibility(self):
        self.assertEqual(
            self.parse("--cameras", "cam_high,cam_left_wrist").cameras,
            ["cam_high", "cam_left_wrist"],
        )

    def test_reject_empty_and_colliding_names(self):
        for cameras in (
            "",
            "cam_high,cam_high",
            "cam_high,observation.images.cam_high",
        ):
            with (
                self.subTest(cameras=cameras),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                with self.assertRaises(SystemExit) as error:
                    self.parse("--cameras", cameras)
                self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
