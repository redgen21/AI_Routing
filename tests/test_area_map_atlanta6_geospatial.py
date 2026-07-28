from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import geopandas as gpd
import pandas as pd

from smart_routing import area_map


def _reviewed_rows() -> pd.DataFrame:
    rows = []
    postals = ("30028", "30040", "30041", "30107", "30005", "30006")
    for sequence, postal_code in enumerate(postals, start=1):
        rows.append(
            {
                "POSTAL_CODE": postal_code,
                "STRATEGIC_CITY_NAME": "Atlanta_6area",
                "region_id": f"atlanta_6area_r{sequence:02d}",
                "region_seq": sequence,
                "AREA_NAME": f"Zone {sequence}",
            }
        )
    return pd.DataFrame(rows)


class Atlanta6AreaMapGeospatialTests(unittest.TestCase):
    def test_alias_uses_atlanta_profile_geometry_and_osrm_graph(self) -> None:
        self.assertEqual(area_map._base_city_name("Atlanta_6area"), "Atlanta, GA")
        self.assertEqual(area_map._route_client_key("Atlanta_6area"), "Atlanta, GA")
        self.assertEqual(
            area_map.DEFAULT_CITY_OSRM_URLS["Atlanta_6area"],
            area_map.DEFAULT_CITY_OSRM_URLS["Atlanta, GA"],
        )

    def test_alias_filters_profile_and_service_with_base_atlanta_city(self) -> None:
        zip_profile = pd.DataFrame(
            {
                "POSTAL_CODE": ["30028", "90001"],
                "SVC_ENGINEER_CODE": ["ATL-1", "LA-1"],
                "STRATEGIC_CITY_NAME": ["Atlanta, GA", "Los Angeles, CA"],
                "AREA_NAME": ["Atlanta current", "Los Angeles current"],
                "SVC_CENTER_TYPE": ["DMS", "DMS"],
            }
        )
        slot_profile = pd.DataFrame(
            {
                "SVC_ENGINEER_CODE": ["ATL-1", "LA-1"],
                "STRATEGIC_CITY_NAME": ["Atlanta, GA", "Los Angeles, CA"],
            }
        )
        product_profile = pd.DataFrame({"SVC_ENGINEER_CODE": ["ATL-1", "LA-1"]})
        service = pd.DataFrame(
            {
                "POSTAL_CODE": ["30028", "90001"],
                "STRATEGIC_CITY_NAME": ["Atlanta, GA", "Los Angeles, CA"],
            }
        )
        empty_layer = area_map.gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        with mock.patch.object(area_map, "_configured_area_map_path", side_effect=lambda _key, path, **_kwargs: path), mock.patch.object(
            area_map, "_is_cache_valid", return_value=False
        ), mock.patch.object(area_map, "_save_cached_city_map"), mock.patch.object(
            area_map, "load_profile_data", return_value=(zip_profile, slot_profile, product_profile)
        ), mock.patch.object(area_map, "load_tech_list_data", return_value=pd.DataFrame()), mock.patch.object(
            area_map, "load_service_points", return_value=service
        ), mock.patch.object(area_map, "_load_zcta_subset", return_value=empty_layer), mock.patch.object(
            area_map, "_build_zip_layer", return_value=empty_layer
        ), mock.patch.object(area_map, "_build_area_layer", return_value=empty_layer), mock.patch.object(
            area_map, "_enrich_area_layer_with_tech", return_value=empty_layer
        ), mock.patch.object(area_map, "_build_context_zip_layer", return_value=empty_layer), mock.patch.object(
            area_map, "_build_area_stats", return_value=pd.DataFrame()
        ), mock.patch.object(area_map, "_simplify_geometry_layer", side_effect=lambda layer, _tolerance: layer):
            loaded = area_map.load_city_map_data(
                "Atlanta_6area", service_file=Path("Atlanta_6area_service.csv")
            )

        self.assertEqual(loaded.city_name, "Atlanta_6area")
        self.assertEqual(
            loaded.zip_coverage_df["STRATEGIC_CITY_NAME"].tolist(), ["Atlanta, GA"]
        )
        self.assertEqual(loaded.service_df["STRATEGIC_CITY_NAME"].tolist(), ["Atlanta, GA"])

    def test_reviewed_six_area_file_is_the_only_fixed_layer_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            reviewed_directory = Path(temporary_directory)
            reviewed_path = (
                reviewed_directory
                / area_map.ATLANTA_6AREA_REVIEWED_FIXED_REGION_FILENAME
            )
            _reviewed_rows().to_csv(reviewed_path, index=False, encoding="utf-8-sig")
            with mock.patch.object(area_map, "FIXED_REGION_MAP_DIR", reviewed_directory):
                self.assertEqual(
                    area_map._fixed_region_map_path("Atlanta_6area", 6), reviewed_path
                )
                self.assertIsNone(area_map._fixed_region_map_path("Atlanta_6area", 5))
                self.assertEqual(area_map.load_region_count_options("Atlanta_6area"), [6])
                loaded = area_map._load_fixed_region_postal_map("Atlanta_6area", 6)

        self.assertEqual(loaded["region_seq"].tolist(), [1, 2, 3, 4, 5, 6])
        self.assertEqual(set(loaded["STRATEGIC_CITY_NAME"]), {"Atlanta_6area"})

    def test_bundled_reviewed_six_area_map_resolves_and_is_offered(self) -> None:
        expected_path = (
            area_map.FIXED_REGION_MAP_DIR
            / area_map.ATLANTA_6AREA_REVIEWED_FIXED_REGION_FILENAME
        )

        self.assertTrue(expected_path.is_file())
        self.assertEqual(
            area_map._fixed_region_map_path("Atlanta_6area", 6), expected_path
        )
        self.assertEqual(area_map.load_region_count_options("Atlanta_6area"), [6])

    def test_ambiguous_zip_memberships_fail_instead_of_being_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            reviewed_directory = Path(temporary_directory)
            reviewed_path = (
                reviewed_directory
                / area_map.ATLANTA_6AREA_REVIEWED_FIXED_REGION_FILENAME
            )
            duplicate_rows = pd.concat(
                [_reviewed_rows(), _reviewed_rows().iloc[:4]], ignore_index=True
            )
            duplicate_rows.to_csv(reviewed_path, index=False, encoding="utf-8-sig")
            with mock.patch.object(area_map, "FIXED_REGION_MAP_DIR", reviewed_directory):
                with self.assertRaisesRegex(
                    ValueError,
                    "FIXED_REGION_POSTAL_MEMBERSHIP_CONFLICT: 30028,30040,30041,30107",
                ):
                    area_map._load_fixed_region_postal_map("Atlanta_6area", 6)

    def test_unreviewed_six_area_identity_never_falls_back_to_inferred_clusters(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch.object(area_map, "FIXED_REGION_MAP_DIR", Path(temporary_directory)):
                self.assertEqual(area_map.load_region_count_options("Atlanta_6area"), [])
                self.assertTrue(
                    area_map._load_fixed_region_postal_map("Atlanta_6area", 6).empty
                )

    def test_zcta_coverage_reports_missing_postals_without_centroid_fallback(self) -> None:
        geometry = gpd.GeoDataFrame(
            {"POSTAL_CODE": ["30028"]},
            geometry=gpd.GeoSeries.from_wkt(["POINT (-84.0 34.0)"], crs="EPSG:4326"),
            crs="EPSG:4326",
        )
        geometry.attrs["source_crs"] = "EPSG:4269"
        with mock.patch.object(
            area_map, "_configured_area_map_path", return_value=Path("zcta.zip")
        ), mock.patch.object(area_map, "_load_zcta_subset", return_value=geometry):
            loaded, coverage = area_map.load_zcta_geometry_with_coverage(
                ["30028", "30040"]
            )

        self.assertEqual(loaded.crs.to_string(), "EPSG:4326")
        self.assertEqual(coverage["coordinate_order"], "longitude,latitude")
        self.assertEqual(coverage["missing_postal_codes"], ("30040",))
        self.assertEqual(coverage["missing_geometry_fallback"], "none")
        self.assertEqual(
            coverage["usps_zip_status"], "not_determined_by_census_zcta_source"
        )


if __name__ == "__main__":
    unittest.main()
