from __future__ import annotations

from pathlib import Path
import json
import os
import re
import tempfile
import unittest
from unittest import mock

import sr_area_map
from tools.operations.build_current_coverage_report_ko import (
    EXPECTED_SERVICES,
    EXPECTED_OSRM_FINGERPRINT,
    KM_TO_MILE,
    _add_city_route_metrics,
    DEFAULT_ROUTE_DETAIL,
    DEFAULT_ROUTE_METADATA,
    DEFAULT_OUTPUT,
    _bucket_table,
    _bucket_detail,
    _load_current_coverage,
    _load_route_detail,
    _render,
    _source_groups,
    _summarize,
    _table,
)


SOURCE = Path("260310/input/Service_202607071543_normalized_geocoded.csv")


class CurrentCoverageSourceTests(unittest.TestCase):
    def test_default_report_output_is_root_report_directory(self) -> None:
        self.assertEqual(DEFAULT_OUTPUT.parent.name, "보고서")
        self.assertEqual(DEFAULT_OUTPUT.name, "current_coverage_report_ko.html")

    def test_report_uses_july_source_exact_city_totals(self) -> None:
        summary = _summarize(_load_current_coverage(SOURCE))
        self.assertEqual(summary.set_index("city")["services"].to_dict(), EXPECTED_SERVICES)
        self.assertEqual(int(summary["services"].sum()), 9280)

    def test_bucket_detail_preserves_accounting_and_population_stddev(self) -> None:
        frame = _load_current_coverage(SOURCE)
        summary = _summarize(frame).set_index("city")
        detail = _bucket_detail(frame)
        for city, overview in summary.iterrows():
            city_detail = detail[detail["city"].eq(city)]
            base = city_detail[city_detail["bucket"].isin(["DMS", "DMS2", "ASC"])]
            self.assertEqual(int(base["services"].sum()), int(overview["services"]))
            self.assertEqual(int(city_detail["daily_sm_groups"].sum()), int(overview["assigned_sm_groups"]))
        atlanta_dms = detail[(detail["city"].eq("Atlanta, GA")) & (detail["bucket"].eq("DMS"))].iloc[0]
        atlanta = frame[frame["STRATEGIC_CITY_NAME"].eq("Atlanta, GA")]
        grouped = atlanta.groupby(["service_date_key", "assigned_sm_code"]).agg(
            bucket=("CENTER_BUCKET", sr_area_map._classify_assignment_group_bucket),
            jobs=("GSFS_RECEIPT_NO", "nunique"),
        )
        grouped_jobs = grouped.loc[grouped["bucket"].eq("DMS"), "jobs"]
        self.assertAlmostEqual(float(atlanta_dms["jobs_stddev"]), float(grouped_jobs.std(ddof=0)))

    def test_route_detail_strictly_joins_and_aggregates_success_only(self) -> None:
        frame = _load_current_coverage(SOURCE)
        joined, metadata = _load_route_detail(
            DEFAULT_ROUTE_DETAIL, DEFAULT_ROUTE_METADATA, SOURCE, _source_groups(frame)
        )
        self.assertEqual(metadata["source"]["sha256"], "05caa003c56fce18bc283cba856cbfa0214f544916501399abd5048e0ac22249")
        self.assertEqual(metadata["schema"], "current-coverage-osrm-result/v2")
        self.assertEqual(metadata["computation"]["fingerprint"]["sha256"], EXPECTED_OSRM_FINGERPRINT)
        self.assertTrue(metadata["computation"]["single_uninterrupted_result_set"])
        self.assertEqual(metadata["checkpoint"]["resumed_group_count"], 0)
        self.assertEqual(metadata["counts"]["cache_hit_group_count"], 0)
        self.assertEqual(int(joined["status"].eq("success").sum()), 2830)
        self.assertEqual(int(joined["status"].eq("failed").sum()), 23)
        self.assertEqual(int(joined["_merge"].eq("left_only").sum()), 0)
        self.assertTrue(joined.loc[joined["status"].eq("failed"), ["distance_km", "duration_min"]].isna().all().all())
        detail = _bucket_detail(frame, joined)
        self.assertEqual(int(detail["routed_groups"].sum()), 2830)
        self.assertEqual(int(detail["failed_groups"].sum()), 23)
        table = _bucket_table(detail)
        self.assertIn("<th>평균 km</th>", table)
        self.assertIn("<th>합계 분</th>", table)
        self.assertNotIn("<th>경로 실패</th>", table)
        self.assertNotIn("<th>미매칭</th>", table)
        self.assertNotIn("<th>경로 커버리지</th>", table)
        self.assertEqual(table.count("<th>"), 14)
        body_rows = re.findall(r"<tr class='bucket-row bucket-[^']+'>(.*?)</tr>", table)
        self.assertTrue(body_rows)
        self.assertTrue(all(row.count("<td>") == 14 for row in body_rows))
        for bucket in ("dms", "dms2", "asc", "mixed"):
            self.assertIn(f"<tr class='bucket-row bucket-{bucket}'>", table)
        self.assertNotIn("route_group_id", table)
        summary = _add_city_route_metrics(_summarize(frame), joined)
        washington = summary[summary["city"].eq("Washington, DC")].iloc[0]
        washington_success = joined[(joined["city"].eq("Washington, DC")) & joined["status"].eq("success")]
        self.assertAlmostEqual(
            float(washington["avg_distance_mile"]),
            float(washington_success["distance_km"].mean()) * KM_TO_MILE,
        )
        self.assertAlmostEqual(
            float(washington["avg_duration_min"]),
            float(washington_success["duration_min"].mean()),
        )
        city_table = _table(summary)
        self.assertLess(city_table.index("평균 서비스/그룹"), city_table.index("평균 거리 (mile)"))
        self.assertLess(city_table.index("평균 거리 (mile)"), city_table.index("평균 시간 (분)"))
        self.assertNotIn("서비스 날짜 범위", city_table)
        document = _render(summary, detail, SOURCE, joined, metadata)
        self.assertIn('class="date-range">서비스 날짜: 2026-06-01 ~ 2026-06-30', document)
        self.assertIn(".bucket-dms", document)
        self.assertIn(".bucket-dms2", document)
        self.assertIn(".bucket-asc", document)
        self.assertIn(".bucket-mixed", document)
        self.assertIn(".bucket-dms>td", document)
        self.assertNotIn(".route-failure", document)
        self.assertNotIn(".route-unmatched", document)
        self.assertNotIn(".route-coverage", document)
        self.assertNotIn("데이터 출처와 계산 규칙", document)
        self.assertNotIn("검증 및 한계", document)

    def test_route_detail_rejects_source_sha_mismatch(self) -> None:
        frame = _load_current_coverage(SOURCE)
        metadata = json.loads(DEFAULT_ROUTE_METADATA.read_text(encoding="utf-8"))
        metadata["source"]["sha256"] = "0" * 64
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metadata.json"
            path.write_text(json.dumps(metadata), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "source SHA-256"):
                _load_route_detail(DEFAULT_ROUTE_DETAIL, path, SOURCE, _source_groups(frame))

    def test_explorer_uses_authoritative_loader_without_overlay(self) -> None:
        sentinel = {"authoritative": True}
        sr_area_map.get_route_explorer_data.clear()
        with mock.patch.object(sr_area_map, "load_route_explorer_data", return_value=sentinel) as loader:
            result = sr_area_map.get_route_explorer_data(
                "Los Angeles, CA", None, "test-cache", "missing-source"
            )
        self.assertEqual(result, sentinel)
        loader.assert_called_once_with(city_name="Los Angeles, CA", region_count=None)

    def test_source_fingerprint_changes_when_provenance_changes(self) -> None:
        fingerprint = sr_area_map._current_coverage_source_fingerprint(SOURCE)
        self.assertIn("Service_202607071543_normalized_geocoded.csv", fingerprint)
        self.assertIn("::", fingerprint)
        self.assertEqual(
            sr_area_map._current_coverage_source_fingerprint(None), "unavailable"
        )
        with tempfile.TemporaryDirectory() as directory:
            probe = Path(directory) / "service.csv"
            probe.write_bytes(b"a")
            os.utime(probe, ns=(1_000_000_000, 1_000_000_000))
            before = sr_area_map._current_coverage_source_fingerprint(probe)
            probe.write_bytes(b"ab")
            os.utime(probe, ns=(2_000_000_000, 2_000_000_000))
            after = sr_area_map._current_coverage_source_fingerprint(probe)
        self.assertNotEqual(before, after)


if __name__ == "__main__":
    unittest.main()
