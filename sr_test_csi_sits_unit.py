from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

import smart_routing.production_assign_atlanta as base
import smart_routing.production_assign_atlanta_csi as csi
from smart_routing.production_assign_atlanta_hybrid import build_atlanta_production_assignment_hybrid_from_frames
from smart_routing.production_assign_atlanta_sits import build_atlanta_production_assignment_sits_from_frames


REAL_TEST_DATE = "2026-01-12"
_REAL_DAY_CACHE: dict[str, object] | None = None


@dataclass
class DummyRouteClient:
    speed_kmh: float = 60.0

    def get_distance_duration_matrix(self, coords: list[tuple[float, float]]) -> tuple[list[list[float]], list[list[float]]]:
        normalized = [(float(lon), float(lat)) for lon, lat in coords]
        size = len(normalized)
        dist_matrix = [[0.0] * size for _ in range(size)]
        dur_matrix = [[0.0] * size for _ in range(size)]
        for i in range(size):
            lon1, lat1 = normalized[i]
            for j in range(size):
                lon2, lat2 = normalized[j]
                distance = abs(lon1 - lon2) + abs(lat1 - lat2)
                dist_matrix[i][j] = float(distance)
                dur_matrix[i][j] = float(distance) / float(self.speed_kmh) * 60.0
        return dist_matrix, dur_matrix


def _load_real_day_results() -> dict[str, object]:
    global _REAL_DAY_CACHE
    if _REAL_DAY_CACHE is not None:
        return _REAL_DAY_CACHE

    _, engineer_region_df, home_df, service_df = base._load_inputs()
    prepared_df = csi._prepare_service_df(service_df)
    day_df = prepared_df[prepared_df["service_date_key"].astype(str) == REAL_TEST_DATE].copy()
    source_jobs = csi._dedupe_day_jobs(day_df)

    csi_assignment_df, csi_summary_df, _ = csi.build_atlanta_production_assignment_csi_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=day_df,
        attendance_limited=True,
    )
    sits_assignment_df, sits_summary_df, _ = build_atlanta_production_assignment_sits_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=day_df,
        attendance_limited=True,
    )
    hybrid_assignment_df, hybrid_summary_df, _ = build_atlanta_production_assignment_hybrid_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=day_df,
        attendance_limited=True,
    )
    _REAL_DAY_CACHE = {
        "expected_jobs": int(source_jobs["GSFS_RECEIPT_NO"].astype(str).nunique()),
        "csi_assignment_df": csi_assignment_df,
        "csi_summary_df": csi_summary_df,
        "hybrid_assignment_df": hybrid_assignment_df,
        "hybrid_summary_df": hybrid_summary_df,
        "sits_assignment_df": sits_assignment_df,
        "sits_summary_df": sits_summary_df,
    }
    return _REAL_DAY_CACHE


def _route_travel_sum(states: dict[str, dict[str, object]]) -> float:
    return float(sum(float(state["travel_time_min"]) for state in states.values()))


def test_phase1_cluster_count() -> None:
    jobs_df = pd.DataFrame(
        [
            {"latitude": 0.0, "longitude": 0.0},
            {"latitude": 0.1, "longitude": 0.1},
            {"latitude": 10.0, "longitude": 10.0},
            {"latitude": 10.1, "longitude": 10.1},
            {"latitude": 20.0, "longitude": 20.0},
            {"latitude": 20.1, "longitude": 20.1},
        ]
    )
    labels, centroids = csi._kmeans_cluster_jobs(jobs_df, 3)
    assert len(centroids) == 3
    assert int(labels.nunique()) == 3


def test_hungarian_1to1() -> None:
    route_client = DummyRouteClient()
    engineer_home_coords = [(0.0, 0.0), (10.0, 0.0), (20.0, 0.0)]
    cluster_centroids = [(19.9, 0.0), (0.1, 0.0), (10.2, 0.0)]
    match = csi._hungarian_match_engineers_to_clusters(engineer_home_coords, cluster_centroids, route_client)
    assert set(match.keys()) == {0, 1, 2}
    assert len(set(match.values())) == 3


def test_insertion_delta_correctness() -> None:
    route_client = DummyRouteClient()
    route_coords = [(0.0, 0.0), (10.0, 0.0), (20.0, 0.0)]
    job_coord = (30.0, 0.0)
    dist_matrix, dur_matrix = route_client.get_distance_duration_matrix(route_coords + [job_coord])
    delta_km, delta_min = csi._compute_insertion_delta(route_coords, job_coord, 3, dist_matrix, dur_matrix)
    after_dist, after_dur = route_client.get_distance_duration_matrix(route_coords + [job_coord])
    before_dist, before_dur = route_client.get_distance_duration_matrix(route_coords)
    before_km = sum(float(before_dist[i][i + 1]) for i in range(len(route_coords) - 1))
    before_min = sum(float(before_dur[i][i + 1]) for i in range(len(route_coords) - 1))
    after_km = sum(float(after_dist[i][i + 1]) for i in range(len(route_coords + [job_coord]) - 1))
    after_min = sum(float(after_dur[i][i + 1]) for i in range(len(route_coords + [job_coord]) - 1))
    assert abs((after_km - before_km) - delta_km) < 1e-6
    assert abs((after_min - before_min) - delta_min) < 1e-6


def test_prepare_service_df_disables_tv_jobs() -> None:
    source_df = pd.DataFrame(
        [
            {"service_date_key": REAL_TEST_DATE, "latitude": 1.0, "longitude": 2.0, "service_time_min": 45, "is_tv_job": True},
            {"service_date_key": REAL_TEST_DATE, "latitude": 3.0, "longitude": 4.0, "service_time_min": 100, "is_tv_job": True},
        ]
    )
    prepared = csi._prepare_service_df(source_df)
    assert bool(prepared["is_tv_job"].any()) is False


def test_real_day_outputs_valid() -> None:
    payload = _load_real_day_results()
    for assignment_df, summary_df, expected_jobs in [
        (payload["csi_assignment_df"], payload["csi_summary_df"], payload["expected_jobs"]),
        (payload["hybrid_assignment_df"], payload["hybrid_summary_df"], payload["expected_jobs"]),
        (payload["sits_assignment_df"], payload["sits_summary_df"], payload["expected_jobs"]),
    ]:
        assert int(assignment_df["GSFS_RECEIPT_NO"].astype(str).nunique()) == int(expected_jobs)
        assert int(assignment_df["GSFS_RECEIPT_NO"].astype(str).duplicated().sum()) == 0
        assert int(pd.to_numeric(summary_df["overflow_480"], errors="coerce").fillna(0).astype(int).sum()) == 0
        required_assignment = {
            "GSFS_RECEIPT_NO",
            "assigned_sm_code",
            "assigned_sm_name",
            "assigned_center_type",
            "home_start_longitude",
            "home_start_latitude",
            "route_visit_seq",
        }
        required_summary = {
            "service_date_key",
            "SVC_ENGINEER_CODE",
            "job_count",
            "service_time_min",
            "travel_time_min",
            "travel_distance_km",
            "total_work_min",
            "overflow_480",
        }
        assert required_assignment.issubset(set(assignment_df.columns))
        assert required_summary.issubset(set(summary_df.columns))
        assert int(pd.to_numeric(summary_df["job_count"], errors="coerce").fillna(0).sum()) == len(assignment_df)


def test_sits_relocation_reduces_travel() -> None:
    route_client = DummyRouteClient()
    jobs_df = pd.DataFrame(
        [
            {"GSFS_RECEIPT_NO": "J1", "longitude": 1.0, "latitude": 0.0, "service_time_min": 45.0, "region_seq": 1, "service_date_key": "2026-01-01", "is_tv_job": False, "is_heavy_repair": False},
            {"GSFS_RECEIPT_NO": "J2", "longitude": 19.0, "latitude": 0.0, "service_time_min": 45.0, "region_seq": 1, "service_date_key": "2026-01-01", "is_tv_job": False, "is_heavy_repair": False},
            {"GSFS_RECEIPT_NO": "J3", "longitude": 21.0, "latitude": 0.0, "service_time_min": 45.0, "region_seq": 1, "service_date_key": "2026-01-01", "is_tv_job": False, "is_heavy_repair": False},
        ]
    )
    engineer_df = pd.DataFrame(
        [
            {"SVC_ENGINEER_CODE": "A", "Name": "A", "SVC_CENTER_TYPE": "DMS", "assigned_region_seq": 1, "start_coord": (0.0, 0.0), "REF_HEAVY_REPAIR_FLAG": "Y"},
            {"SVC_ENGINEER_CODE": "B", "Name": "B", "SVC_CENTER_TYPE": "DMS", "assigned_region_seq": 1, "start_coord": (20.0, 0.0), "REF_HEAVY_REPAIR_FLAG": "Y"},
        ]
    )
    states = csi._build_states(engineer_df)
    csi._insert_job(states["A"], 0, jobs_df.loc[0], 1, 1.0, 1.0)
    csi._insert_job(states["A"], 1, jobs_df.loc[1], 2, 18.0, 18.0)
    csi._insert_job(states["B"], 2, jobs_df.loc[2], 1, 1.0, 1.0)
    csi._refresh_state(states["A"], jobs_df, route_client)
    csi._refresh_state(states["B"], jobs_df, route_client)

    before_travel = _route_travel_sum(states)
    changed = csi._relocation_pass(states, jobs_df, engineer_df, route_client, span_weight=csi.SITS_RELOCATION_SPAN_WEIGHT)
    after_travel = _route_travel_sum(states)

    assert changed is True
    assert after_travel <= before_travel
    assert 1 in states["B"]["job_indices"]


def test_hybrid_travel_budget_blocks_large_increase() -> None:
    route_client = DummyRouteClient()
    jobs_df = pd.DataFrame(
        [
            {"GSFS_RECEIPT_NO": "J1", "longitude": 1.0, "latitude": 0.0, "service_time_min": 45.0, "region_seq": 1, "service_date_key": "2026-01-01", "is_tv_job": False, "is_heavy_repair": False},
            {"GSFS_RECEIPT_NO": "J2", "longitude": 2.0, "latitude": 0.0, "service_time_min": 45.0, "region_seq": 1, "service_date_key": "2026-01-01", "is_tv_job": False, "is_heavy_repair": False},
            {"GSFS_RECEIPT_NO": "J3", "longitude": 3.0, "latitude": 0.0, "service_time_min": 45.0, "region_seq": 1, "service_date_key": "2026-01-01", "is_tv_job": False, "is_heavy_repair": False},
        ]
    )
    engineer_df = pd.DataFrame(
        [
            {"SVC_ENGINEER_CODE": "A", "Name": "A", "SVC_CENTER_TYPE": "DMS", "assigned_region_seq": 1, "start_coord": (0.0, 0.0), "REF_HEAVY_REPAIR_FLAG": "Y"},
            {"SVC_ENGINEER_CODE": "B", "Name": "B", "SVC_CENTER_TYPE": "DMS", "assigned_region_seq": 1, "start_coord": (10.0, 0.0), "REF_HEAVY_REPAIR_FLAG": "Y"},
        ]
    )
    states = csi._build_states(engineer_df)
    csi._insert_job(states["A"], 0, jobs_df.loc[0], 1, 1.0, 1.0)
    csi._insert_job(states["A"], 1, jobs_df.loc[1], 2, 1.0, 1.0)
    csi._insert_job(states["A"], 2, jobs_df.loc[2], 3, 1.0, 1.0)
    csi._refresh_state(states["A"], jobs_df, route_client)
    csi._refresh_state(states["B"], jobs_df, route_client)

    before_travel = float(states["A"]["travel_distance_km"] + states["B"]["travel_distance_km"])
    changed_without_budget = csi._relocation_pass(states, jobs_df, engineer_df, route_client, span_weight=10.0)
    assert changed_without_budget is True

    states = csi._build_states(engineer_df)
    csi._insert_job(states["A"], 0, jobs_df.loc[0], 1, 1.0, 1.0)
    csi._insert_job(states["A"], 1, jobs_df.loc[1], 2, 1.0, 1.0)
    csi._insert_job(states["A"], 2, jobs_df.loc[2], 3, 1.0, 1.0)
    csi._refresh_state(states["A"], jobs_df, route_client)
    csi._refresh_state(states["B"], jobs_df, route_client)

    changed_with_budget = csi._relocation_pass(
        states,
        jobs_df,
        engineer_df,
        route_client,
        span_weight=10.0,
        baseline_total_travel_km=before_travel,
        max_travel_budget_ratio=0.0,
    )
    assert changed_with_budget is False


TESTS = [
    ("phase1_cluster_count", test_phase1_cluster_count),
    ("hungarian_1to1", test_hungarian_1to1),
    ("insertion_delta_correctness", test_insertion_delta_correctness),
    ("prepare_service_df_disables_tv_jobs", test_prepare_service_df_disables_tv_jobs),
    ("real_day_outputs_valid", test_real_day_outputs_valid),
    ("sits_relocation_reduces_travel", test_sits_relocation_reduces_travel),
    ("hybrid_travel_budget_blocks_large_increase", test_hybrid_travel_budget_blocks_large_increase),
]


def main() -> bool:
    passed = 0
    failed = 0
    for name, fn in TESTS:
        try:
            fn()
            passed += 1
            print(f"[PASS] {name}")
        except Exception as exc:
            failed += 1
            print(f"[FAIL] {name}: {exc}")
    print(f"Summary: {passed}/{len(TESTS)} passed")
    return failed == 0


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
