"""Run with python -m unittest discover -s python/examples/embodied/aloha -p test_metadata_publication.py."""

import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import sys
from dataclasses import make_dataclass
import import_raw
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import common
from prepare_dataset import load_configuration


class PublicationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.raw = str(Path(self.tmp.name) / "raw")
        self.root = common.metadata_root(self.raw)
        self.manifest = dict(
            status="imported",
            import_id="import-1",
            namespace="default",
            raw_table="raw",
            raw_path=self.raw,
            expected_episodes={"ep0": 3},
            cameras=list(common.DEFAULT_CAMERAS),
            metadata_files={},
        )
        info = {
            "features": {
                "observation.state": {"shape": [14]},
                "action": {"shape": [14]},
            }
        }
        info["features"].update(
            {
                "observation.images." + c: {"shape": [480, 640, 3]}
                for c in common.DEFAULT_CAMERAS
            }
        )
        tasks = io.BytesIO()
        pq.write_table(pa.table({"task_index": [0], "task": ["thread"]}), tasks)
        for name, data in (
            ("info.json", common.json_bytes(info)),
            ("tasks.parquet", tasks.getvalue()),
        ):
            uri = self.root + "/" + name
            common.write_uri(uri, data)
            self.manifest["metadata_files"][name] = dict(
                uri=uri, sha256=common.sha256(data)
            )
        common.write_uri(self.root + "/manifest.json", common.json_bytes(self.manifest))

    def segment(self, episode="ep0", selected=False):
        return dict(
            episode_id=episode,
            start_frame=0,
            end_frame_exclusive=3,
            behavior="approach",
            reason="reviewed",
            selected=selected,
            confirmed=True,
        )

    def test_import_publishes_metadata_after_verification(self):
        raw_path = str(Path(self.tmp.name) / "fresh_raw")
        source = Path(self.tmp.name) / "source"
        (source / "meta").mkdir(parents=True)
        for name in ("info.json", "tasks.parquet"):
            (source / "meta" / name).write_bytes(
                common.read_uri(self.root + "/" + name)
            )
        args = SimpleNamespace(
            source=source,
            output=Path(self.tmp.name) / "report",
            runner="native",
            namespace="default",
            raw_table="fresh_raw",
            raw_path=raw_path,
            episodes=[0],
            cameras=list(common.DEFAULT_CAMERAS),
            fps=50,
            width=640,
            height=480,
        )

        class NotFound(Exception):
            pass

        table = SimpleNamespace(path=raw_path)

        class Catalog:
            created = False

            def table(self, name):
                if not self.created:
                    raise NotFound()
                return table

        catalog = Catalog()

        def importer(*args, **kwargs):
            catalog.created = True
            return make_dataclass("Summary", [("rows", int)])(3)

        def verify(*args, **kwargs):
            self.assertIsNone(
                common.read_uri(
                    common.metadata_root(raw_path) + "/manifest.json", optional=True
                )
            )
            return {"rows": 3}

        modules = {
            "daft": SimpleNamespace(),
            "lakesoul.catalog": SimpleNamespace(TableNotFoundError=NotFound),
            "lakesoul.embodied.daft": SimpleNamespace(import_lerobot=importer),
        }
        with (
            patch.dict(sys.modules, modules),
            patch.object(import_raw, "catalog_from_environment", return_value=catalog),
            patch.object(import_raw, "expected_episodes", return_value={"ep000000": 3}),
            patch.object(import_raw, "verify_import", side_effect=verify),
            patch("builtins.print"),
        ):
            import_raw.run(args)
        manifest = common.read_import_manifest(raw_path, "default", "fresh_raw")
        self.assertEqual(len(manifest["cameras"]), 4)
        self.assertEqual(
            set(manifest["metadata_files"]), {"info.json", "tasks.parquet"}
        )
        self.assertEqual(
            json.loads((args.output / "manifest.json").read_text()), manifest
        )

    def test_configuration_discovers_four_cameras(self):
        args = SimpleNamespace(namespace="default", raw_table="raw")
        metadata, expected, dimensions, cameras, tasks = load_configuration(
            args, SimpleNamespace(path=self.raw)
        )
        self.assertEqual(len(cameras), 4)
        self.assertEqual(tasks, {0: "thread"})
        self.assertEqual(dimensions["action"], 14)
        with self.assertRaises(ValueError):
            common.read_import_manifest(self.raw, "other", "raw")
        common.write_uri(self.root + "/info.json", b"{}")
        with self.assertRaises(ValueError):
            load_configuration(args, SimpleNamespace(path=self.raw))

    def test_publish_preserves_episodes_and_versions(self):
        self.assertFalse(
            common.read_annotation_version(self.raw, self.manifest)[2]["published"]
        )
        first = common.publish_annotations(
            self.raw, self.manifest, "ep0", [self.segment()]
        )
        old_data = common.read_uri(first["annotations_uri"])
        second = common.publish_annotations(
            self.raw, self.manifest, "ep1", [self.segment("ep1")]
        )
        rows, _, version = common.read_annotation_version(self.raw, self.manifest)
        self.assertEqual({r["episode_id"] for r in rows}, {"ep0", "ep1"})
        self.assertEqual(version["excluded_behaviors"], ["approach"])
        self.assertNotEqual(first["revision"], second["revision"])
        self.assertEqual(common.read_uri(first["annotations_uri"]), old_data)
        common.publish_annotations(
            self.raw, self.manifest, "ep0", [dict(self.segment(), reason="updated")]
        )
        rows, _, _ = common.read_annotation_version(self.raw, self.manifest)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["reason"], "updated")

    def test_failed_upload_does_not_switch_pointer(self):
        common.publish_annotations(self.raw, self.manifest, "ep0", [self.segment()])
        latest = self.root + "/annotations/latest.json"
        old = common.read_uri(latest)
        write = common.write_uri

        def fail_review(uri, data):
            if "aloha_review_" in uri:
                raise OSError("upload failed")
            write(uri, data)

        with patch.object(common, "write_uri", side_effect=fail_review):
            with self.assertRaises(OSError):
                common.publish_annotations(
                    self.raw, self.manifest, "ep0", [self.segment()]
                )
        self.assertEqual(common.read_uri(latest), old)

    def test_tampered_or_wrong_import_rejected(self):
        pointer = common.publish_annotations(
            self.raw, self.manifest, "ep0", [self.segment()]
        )
        with self.assertRaises(ValueError):
            common.read_annotation_version(
                self.raw, dict(self.manifest, import_id="new-import")
            )
        common.write_uri(pointer["annotations_uri"], b"[]")
        with self.assertRaises(ValueError):
            common.read_annotation_version(self.raw, self.manifest)

    def test_policy_conflict_and_unconfirmed_rejected(self):
        common.publish_annotations(self.raw, self.manifest, "ep0", [self.segment()])
        with self.assertRaises(ValueError):
            common.publish_annotations(
                self.raw, self.manifest, "ep1", [self.segment("ep1", True)]
            )
        with self.assertRaises(ValueError):
            common.publish_annotations(
                self.raw, self.manifest, "ep0", [dict(self.segment(), confirmed=False)]
            )


if __name__ == "__main__":
    unittest.main()
