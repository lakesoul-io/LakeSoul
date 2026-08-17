# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "metadata_migrate", Path(__file__).resolve().parents[1] / "metadata_migrate.py"
)
assert SPEC is not None and SPEC.loader is not None
metadata_migrate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = metadata_migrate
SPEC.loader.exec_module(metadata_migrate)


class MetadataMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary_directory.name)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def write_migration(self, name: str, sql: str = "SELECT 1;\n") -> Path:
        path = self.directory / name
        path.write_text(sql, encoding="utf-8")
        return path

    def test_loads_ordered_migrations_and_checksums_content(self) -> None:
        self.write_migration("V2__second_change.sql", "SELECT 2;\n")
        self.write_migration("V1__first_change.sql")

        migrations = metadata_migrate.load_migrations(self.directory)

        self.assertEqual([1, 2], [migration.version for migration in migrations])
        self.assertEqual("first change", migrations[0].description)
        self.assertEqual(64, len(migrations[0].checksum))

    def test_rejects_invalid_filename_and_embedded_transaction(self) -> None:
        self.write_migration("migration.sql")
        with self.assertRaisesRegex(
            metadata_migrate.MigrationError, "invalid migration filename"
        ):
            metadata_migrate.load_migrations(self.directory)

        (self.directory / "migration.sql").unlink()
        self.write_migration("V1__invalid.sql", "BEGIN;\nSELECT 1;\nCOMMIT;\n")
        with self.assertRaisesRegex(
            metadata_migrate.MigrationError, "transaction control"
        ):
            metadata_migrate.load_migrations(self.directory)

    def test_normalizes_jdbc_url_and_removes_jdbc_only_parameter(self) -> None:
        normalized = metadata_migrate.normalize_database_url(
            "jdbc:postgresql://db.example:5432/lakesoul?sslmode=require&stringtype=unspecified"
        )
        self.assertEqual(
            "postgresql://db.example:5432/lakesoul?sslmode=require", normalized
        )

    def test_requires_explicit_database_url(self) -> None:
        with mock.patch.dict(metadata_migrate.os.environ, {}, clear=True):
            with self.assertRaisesRegex(
                metadata_migrate.MigrationError, "URL is required"
            ):
                metadata_migrate.database_url(None)

    def test_validation_detects_pending_unknown_and_changed_migrations(self) -> None:
        path = self.write_migration("V1__first_change.sql")
        migration = metadata_migrate.load_migrations(self.directory)[0]

        self.assertEqual(
            [f"pending migration {path.name}"],
            metadata_migrate.validate_history([migration], [], require_current=True),
        )
        changed = metadata_migrate.AppliedMigration(1, "first change", "0" * 64)
        self.assertEqual(
            [f"checksum mismatch for {path.name}"],
            metadata_migrate.validate_history(
                [migration], [changed], require_current=True
            ),
        )
        unknown = metadata_migrate.AppliedMigration(2, "unknown", "0" * 64)
        self.assertIn(
            "applied migration V2 is absent from the repository",
            metadata_migrate.validate_history(
                [migration], [unknown], require_current=False
            ),
        )

    def test_generated_script_locks_and_records_each_migration_atomically(self) -> None:
        self.write_migration(
            "V1__first_change.sql", "ALTER TABLE example ADD COLUMN value text;\n"
        )
        migration = metadata_migrate.load_migrations(self.directory)[0]

        script = metadata_migrate.migration_script([migration])

        self.assertIn("pg_advisory_lock", script)
        self.assertIn("CREATE TABLE IF NOT EXISTS lakesoul_schema_migrations", script)
        self.assertIn("BEGIN;\nALTER TABLE example", script)
        self.assertIn("INSERT INTO lakesoul_schema_migrations", script)
        self.assertLess(script.index("INSERT INTO"), script.index("COMMIT;"))


if __name__ == "__main__":
    unittest.main()
