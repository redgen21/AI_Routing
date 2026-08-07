import unittest
from unittest.mock import patch

import pandas as pd
from shapely.geometry import Polygon

from smart_routing.live_atlanta_runtime import _apply_postal_coordinate_fallback


class PostalCoordinateFallbackTests(unittest.TestCase):
    def setUp(self):
        self.config = {"geocoding": {"postal_fallback_enabled": True}}
        self.reference = {
            "30047": {
                "latitude": 33.87,
                "longitude": -84.11,
                "geometry": Polygon(
                    [(-84.20, 33.80), (-84.00, 33.80), (-84.00, 33.95), (-84.20, 33.95)]
                ),
            }
        }

    @patch("smart_routing.live_atlanta_runtime._load_postal_reference")
    def test_rejects_provider_result_outside_postal_and_uses_centroid(self, load_reference):
        load_reference.return_value = self.reference
        source = pd.DataFrame(
            [
                {
                    "POSTAL_CODE": "30047",
                    "CITY_NAME": "LILBURN",
                    "latitude": 33.748902,
                    "longitude": -85.3644752,
                    "matched_address": "US-78, United States",
                    "match_type": "GEOMETRIC_CENTER",
                    "source": "google_geocoding_api",
                }
            ]
        )

        result = _apply_postal_coordinate_fallback(source, self.config)

        self.assertEqual(float(result.loc[0, "latitude"]), 33.87)
        self.assertEqual(float(result.loc[0, "longitude"]), -84.11)
        self.assertEqual(result.loc[0, "coordinate_quality"], "POSTAL_CENTROID")
        self.assertEqual(result.loc[0, "coordinate_source"], "zcta_intpt")
        self.assertTrue(bool(result.loc[0, "coordinate_warning"]))
        self.assertEqual(result.loc[0, "source"], "postal_centroid_fallback")

    @patch("smart_routing.live_atlanta_runtime._load_postal_reference")
    def test_keeps_valid_exact_provider_result(self, load_reference):
        load_reference.return_value = self.reference
        source = pd.DataFrame(
            [
                {
                    "POSTAL_CODE": "30047",
                    "CITY_NAME": "LILBURN",
                    "latitude": 33.87,
                    "longitude": -84.11,
                    "matched_address": "123 Main St, Lilburn, GA 30047",
                    "match_type": "Exact",
                    "source": "us_census_geocoder",
                }
            ]
        )

        result = _apply_postal_coordinate_fallback(source, self.config)

        self.assertEqual(float(result.loc[0, "latitude"]), 33.87)
        self.assertEqual(float(result.loc[0, "longitude"]), -84.11)
        self.assertEqual(result.loc[0, "coordinate_warning"], False)
        self.assertNotEqual(result.loc[0, "source"], "postal_centroid_fallback")


if __name__ == "__main__":
    unittest.main()
