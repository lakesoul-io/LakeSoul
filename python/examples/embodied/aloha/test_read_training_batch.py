"""Test image batching without a database; substitute only the Tensor conversion."""

import io
import argparse
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
from PIL import Image
from read_training_batch import build_batch, parse_cameras, select_camera_fields


class BatchTests(unittest.TestCase):
    def sample(self, cameras):
        observation = {"state": [[1.0, 2.0], [3.0, 4.0]]}
        for i, camera in enumerate(cameras):
            stream = io.BytesIO()
            Image.new("RGB", (4, 3), (i * 50, 0, 0)).save(stream, format="JPEG")
            observation[camera] = [stream.getvalue()] * 2
        return {
            "observation": observation,
            "action": [[5.0, 6.0]],
            "metadata": {"episode_id": "ep0"},
        }

    def test_camera_fields_and_pixels_preserved(self):
        for cameras in [
            ("cam_high", "cam_low", "cam_left_wrist", "cam_right_wrist"),
            ("cam_high", "cam_left_wrist"),
            (),
        ]:
            with (
                self.subTest(cameras=cameras),
                patch.dict(
                    "sys.modules", {"torch": SimpleNamespace(from_numpy=lambda a: a)}
                ),
            ):
                sample = self.sample(cameras)
                batch = build_batch([sample, sample], image_size=(4, 3))
                self.assertEqual(set(batch["observation"]), {"state", *cameras})
                for i, camera in enumerate(cameras):
                    image = batch["observation"][camera]
                    self.assertEqual(image.shape, (2, 2, 3, 3, 4))
                    self.assertEqual(image.dtype, np.float32)
                    self.assertAlmostEqual(
                        float(image[0, 0, 0, 0, 0]), i * 50 / 255, delta=2 / 255
                    )
                np.testing.assert_array_equal(
                    batch["action"], [[[5.0, 6.0]], [[5.0, 6.0]]]
                )

    def test_camera_projection_before_reading(self):
        index = {
            "sample_id": "s1",
            "observation_fields": [
                "observation_state",
                "cam_high",
                "cam_low",
                "cam_left_wrist",
                "cam_right_wrist",
            ],
        }
        for value, fields in [
            ("cam_high", ["observation_state", "cam_high"]),
            (
                "cam_high,cam_left_wrist",
                ["observation_state", "cam_high", "cam_left_wrist"],
            ),
            ("none", ["observation_state"]),
        ]:
            self.assertEqual(
                select_camera_fields(index, parse_cameras(value))["observation_fields"],
                fields,
            )
        self.assertEqual(len(index["observation_fields"]), 5)
        self.assertIs(select_camera_fields(index, None), index)
        with self.assertRaisesRegex(ValueError, "lacks requested fields"):
            select_camera_fields(index, ["cam_missing"])
        for value in ["", "cam_high,", "cam_high,cam_high", "action"]:
            with self.assertRaises(argparse.ArgumentTypeError):
                parse_cameras(value)

    def test_rejects_empty_or_mixed_camera_batch(self):
        with patch.dict(
            "sys.modules", {"torch": SimpleNamespace(from_numpy=lambda a: a)}
        ):
            with self.assertRaisesRegex(ValueError, "empty"):
                build_batch([])
            with self.assertRaisesRegex(ValueError, "same observation fields"):
                build_batch([self.sample(("cam_high",)), self.sample(("cam_low",))])


if __name__ == "__main__":
    unittest.main()
