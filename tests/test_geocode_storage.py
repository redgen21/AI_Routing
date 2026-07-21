from __future__ import annotations

import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from smart_routing.census_geocoder import (
    build_unique_addresses,
    empty_geocode_cache_frame,
    load_geocode_cache,
    read_table,
)
from smart_routing.google_geocoder import GoogleGeocoder
from smart_routing.geocode_storage import (
    DatabaseGeocodeStore,
    FileGeocodeStore,
    GeocodeStorageConfigurationError,
    resolve_geocode_store,
)
from smart_routing.here_geocoder import HereGeocoder


class ResolveGeocodeStoreTests(unittest.TestCase):
    def test_missing_environment_defaults_to_file_store(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("COMMON_VRP_CONFIG_PATH", None)
            store = resolve_geocode_store()

        self.assertIsInstance(store, FileGeocodeStore)

    def test_explicit_database_backend_requires_config(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("COMMON_VRP_CONFIG_PATH", None)
            with self.assertRaises(GeocodeStorageConfigurationError):
                resolve_geocode_store("database")

    def test_explicit_environment_preserves_database_service_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "common_vrp.json"
            config_path.write_text("{}", encoding="utf-8")
            with patch.dict(os.environ, {"COMMON_VRP_CONFIG_PATH": str(config_path)}):
                store = resolve_geocode_store()

        self.assertIsInstance(store, DatabaseGeocodeStore)
        self.assertEqual(store.config_path, config_path.resolve())

    def test_file_backend_rejects_database_config(self) -> None:
        with self.assertRaises(GeocodeStorageConfigurationError):
            resolve_geocode_store("file", "common_vrp.json")

    def test_explicit_file_backend_overrides_inherited_database_environment(self) -> None:
        with patch.dict(os.environ, {"COMMON_VRP_CONFIG_PATH": "inherited-prod-config.json"}):
            store = resolve_geocode_store("file")

        self.assertIsInstance(store, FileGeocodeStore)


class FileGeocodeStoreTests(unittest.TestCase):
    def test_cache_and_daily_log_round_trip_without_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "cache.csv"
            log_path = Path(temp_dir) / "daily.json"
            frame = empty_geocode_cache_frame()
            frame.loc[0, ["address_key", "latitude", "longitude"]] = ["A", 33.7, -84.4]

            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("COMMON_VRP_CONFIG_PATH", None)
                store = resolve_geocode_store()
                store.save_cache(cache_path, frame)
                loaded = load_geocode_cache(cache_path)
                store.increment_daily_log("2026-07-18", 2, "census", log_path)

            self.assertEqual(loaded.loc[0, "address_key"], "A")
            self.assertEqual(store.load_daily_log("census", log_path), {"2026-07-18": 2})


class DatabaseGeocodeStoreTests(unittest.TestCase):
    def test_database_error_is_not_silently_replaced_by_file_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "common_vrp.json"
            config_path.write_text("{}", encoding="utf-8")
            cache_path = Path(temp_dir) / "cache.csv"
            pd.DataFrame([{"address_key": "local"}]).to_csv(cache_path, index=False)
            fake_db = types.ModuleType("smart_routing.common_vrp_db")

            def fail_load(*args, **kwargs):
                raise RuntimeError("database unavailable")

            fake_db.load_geocode_cache_df = fail_load
            store = DatabaseGeocodeStore(config_path)
            with patch.dict(sys.modules, {"smart_routing.common_vrp_db": fake_db}):
                with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                    store.load_cache(cache_path)

    def test_attempt_log_store_forwards_provider_path_and_config(self) -> None:
        from smart_routing import common_vrp_db

        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "common_vrp.json"
            config_path.write_text("{}", encoding="utf-8")
            here_path = Path("data/here_attempt_log.csv")
            google_path = Path("data/google_attempt_log.csv")
            frame = pd.DataFrame(
                [{"address_key": "A", "attempted_date": "2026-07-18", "status": "failed"}]
            )
            store = DatabaseGeocodeStore(config_path)

            with (
                patch.object(common_vrp_db, "load_geocode_attempt_log_df", return_value=pd.DataFrame()) as load_mock,
                patch.object(common_vrp_db, "upsert_geocode_attempt_log_df", return_value=1) as save_mock,
                patch.object(common_vrp_db, "cleanup_geocode_cache") as cleanup_mock,
            ):
                store.load_attempt_log(here_path)
                store.save_attempt_log(google_path, frame)

        load_mock.assert_called_once_with(here_path, config_path=config_path.resolve())
        save_mock.assert_called_once_with(google_path, frame, config_path=config_path.resolve())
        cleanup_mock.assert_called_once_with(
            retention_days=common_vrp_db.GEOCODE_CACHE_RETENTION_DAYS,
            config_path=config_path.resolve(),
            attempt_retention_days=common_vrp_db.GEOCODE_ATTEMPT_RETENTION_DAYS,
        )


class DatabaseAttemptLogContractTests(unittest.TestCase):
    def test_load_filters_here_and_google_with_separate_buckets(self) -> None:
        from smart_routing import common_vrp_db

        config_path = Path("dev-config.json")
        with patch.object(common_vrp_db, "_fetch_df", return_value=pd.DataFrame()) as fetch_mock:
            common_vrp_db.load_geocode_attempt_log_df(Path("cache/here_attempts.csv"), config_path)
            common_vrp_db.load_geocode_attempt_log_df(Path("cache/google_attempts.csv"), config_path)

        expected_retention = common_vrp_db.GEOCODE_ATTEMPT_RETENTION_DAYS
        self.assertEqual(fetch_mock.call_args_list[0].args[1], ("here", expected_retention))
        self.assertEqual(fetch_mock.call_args_list[1].args[1], ("google", expected_retention))
        for call_item in fetch_mock.call_args_list:
            self.assertIn("where source_bucket = %s", call_item.args[0].lower())
            self.assertIn("attempted_date >= current_date", call_item.args[0].lower())
            self.assertEqual(call_item.kwargs["config_path"], config_path)

    def test_upsert_uses_provider_bucket_in_rows_and_conflict_key(self) -> None:
        from smart_routing import common_vrp_db

        frame = pd.DataFrame(
            [
                {
                    "address_key": "same-address",
                    "attempted_date": "2026-07-18",
                    "status": "failed",
                    "source": "provider_api",
                }
            ]
        )
        with patch.object(common_vrp_db, "_execute_values_upsert", return_value=1) as upsert_mock:
            common_vrp_db.upsert_geocode_attempt_log_df(Path("here_attempts.csv"), frame)
            common_vrp_db.upsert_geocode_attempt_log_df(Path("google_attempts.csv"), frame)

        here_call, google_call = upsert_mock.call_args_list
        expected_columns = ["address_key", "source_bucket", "attempted_date", "status", "source"]
        expected_conflict = ["address_key", "source_bucket", "attempted_date"]
        self.assertEqual(here_call.args[1], expected_columns)
        self.assertEqual(here_call.args[2][0][1], "here")
        self.assertEqual(here_call.args[3], expected_conflict)
        self.assertEqual(google_call.args[2][0][1], "google")
        self.assertEqual(google_call.args[3], expected_conflict)

    def test_schema_and_idempotent_migration_include_provider_key(self) -> None:
        from smart_routing import common_vrp_db

        compact_schema = " ".join(common_vrp_db.SCHEMA_SQL.lower().split())
        compact_migration = " ".join(common_vrp_db.GEOCODE_ATTEMPT_LOG_MIGRATION_SQL.lower().split())
        expected_key = "primary key (address_key, source_bucket, attempted_date)"

        self.assertIn("source_bucket text not null", compact_schema)
        self.assertIn(expected_key, compact_schema)
        self.assertIn("add column if not exists source_bucket text", compact_migration)
        self.assertIn("alter column source_bucket set not null", compact_migration)
        self.assertIn(expected_key, compact_migration)
        self.assertIn("like '%here%' then 'here'", compact_migration)
        self.assertIn("like '%google%' then 'google'", compact_migration)

    def test_init_schema_schedules_migration_without_database_connection(self) -> None:
        from smart_routing import common_vrp_db

        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        with patch.object(common_vrp_db, "get_db_connection", return_value=connection):
            common_vrp_db.init_schema(Path("not-used.json"))

        cursor.execute.assert_any_call(common_vrp_db.GEOCODE_ATTEMPT_LOG_MIGRATION_SQL)
        attempt_cleanup_calls = [
            item
            for item in cursor.execute.call_args_list
            if item.args and "delete from common_geocode_attempt_log" in item.args[0].lower()
        ]
        self.assertEqual(
            attempt_cleanup_calls[0].args[1],
            (common_vrp_db.GEOCODE_ATTEMPT_RETENTION_DAYS,),
        )
        connection.commit.assert_called_once_with()

    def test_cleanup_uses_long_attempt_retention_not_cache_retention(self) -> None:
        from smart_routing import common_vrp_db

        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        with patch.object(common_vrp_db, "get_db_connection", return_value=connection):
            common_vrp_db.cleanup_geocode_cache(config_path=Path("not-used.json"))

        attempt_calls = [
            item
            for item in cursor.execute.call_args_list
            if item.args and "common_geocode_attempt_log" in item.args[0].lower()
        ]
        self.assertEqual(len(attempt_calls), 1)
        self.assertIn("attempted_date < current_date", attempt_calls[0].args[0].lower())
        self.assertEqual(
            attempt_calls[0].args[1],
            (common_vrp_db.GEOCODE_ATTEMPT_RETENTION_DAYS,),
        )
        self.assertNotEqual(
            common_vrp_db.GEOCODE_ATTEMPT_RETENTION_DAYS,
            common_vrp_db.GEOCODE_CACHE_RETENTION_DAYS,
        )


class ProviderMonthlyAttemptTests(unittest.TestCase):
    @staticmethod
    def _write_service(path: Path) -> None:
        pd.DataFrame(
            [
                {
                    "ADDRESS_LINE1_INFO": "123 Main St",
                    "CITY_NAME": "Atlanta",
                    "STATE_NAME": "GA",
                    "POSTAL_CODE": "30301",
                    "COUNTRY_NAME": "USA",
                }
            ]
        ).to_csv(path, index=False, encoding="utf-8-sig")

    def _make_geocoder(self, geocoder_class, root: Path):
        prefix = "here" if geocoder_class is HereGeocoder else "google"
        return geocoder_class(
            api_key="test-key",
            cache_path=root / f"{prefix}_cache.csv",
            attempt_log_path=root / f"{prefix}_attempts.csv",
            monthly_limit=10,
            sleep_sec=0,
            cache_backend="file",
        )

    def test_same_month_attempt_older_than_seven_days_blocks_retry_and_counts_quota(self) -> None:
        for geocoder_class in (HereGeocoder, GoogleGeocoder):
            with self.subTest(provider=geocoder_class.__name__), tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                service_path = root / "service.csv"
                self._write_service(service_path)
                geocoder = self._make_geocoder(geocoder_class, root)
                address_key = build_unique_addresses(read_table(service_path)).iloc[0]["address_key"]
                pd.DataFrame(
                    [
                        {
                            "address_key": address_key,
                            "attempted_date": "2026-07-01",
                            "status": "failed",
                            "source": "provider_api",
                        }
                    ]
                ).to_csv(geocoder.attempt_log_path, index=False, encoding="utf-8-sig")

                with patch.object(geocoder, "_geocode_one") as geocode_mock:
                    result = geocoder.run_for_unmatched(
                        service_path,
                        root / "census_cache.csv",
                        run_date="2026-07-20",
                    )

                self.assertEqual(result.monthly_used_before_run, 1)
                self.assertEqual(result.attempted, 0)
                geocode_mock.assert_not_called()

    def test_next_month_retries_and_preserves_prior_month_history(self) -> None:
        for geocoder_class in (HereGeocoder, GoogleGeocoder):
            with self.subTest(provider=geocoder_class.__name__), tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                service_path = root / "service.csv"
                self._write_service(service_path)
                geocoder = self._make_geocoder(geocoder_class, root)
                address_key = build_unique_addresses(read_table(service_path)).iloc[0]["address_key"]
                pd.DataFrame(
                    [
                        {
                            "address_key": address_key,
                            "attempted_date": "2026-07-01",
                            "status": "failed",
                            "source": "provider_api",
                        }
                    ]
                ).to_csv(geocoder.attempt_log_path, index=False, encoding="utf-8-sig")
                new_attempt = {
                    "address_key": address_key,
                    "attempted_date": "2026-08-01",
                    "status": "failed",
                    "source": "provider_api",
                }

                with patch.object(geocoder, "_geocode_one", return_value=(None, new_attempt)) as geocode_mock:
                    result = geocoder.run_for_unmatched(
                        service_path,
                        root / "census_cache.csv",
                        run_date="2026-08-01",
                    )

                self.assertEqual(result.monthly_used_before_run, 0)
                self.assertEqual(result.attempted, 1)
                geocode_mock.assert_called_once()
                saved = pd.read_csv(geocoder.attempt_log_path, encoding="utf-8-sig")
                self.assertEqual(set(saved["attempted_date"].astype(str)), {"2026-07-01", "2026-08-01"})


if __name__ == "__main__":
    unittest.main()
