"""
VRP-Level mode integration test
Cluster-aware seed + travel pass + balance pass validation
"""
from __future__ import annotations

import pandas as pd

import smart_routing.production_assign_atlanta as base
from smart_routing.production_assign_atlanta_osrm import build_atlanta_production_assignment_osrm_from_frames


TARGET_DATES = ["2026-01-12", "2026-01-19", "2026-01-20"]


def _dedupe_service(service_df: pd.DataFrame) -> pd.DataFrame:
    deduped = service_df.copy()
    deduped = deduped.sort_values(
        [col for col in ["service_date_key", "GSFS_RECEIPT_NO", "service_time_min"] if col in deduped.columns],
        ascending=[True, True, False] if "service_date_key" in deduped.columns else [True, False],
    ).reset_index(drop=True)
    if "GSFS_RECEIPT_NO" in deduped.columns:
        deduped = deduped.drop_duplicates(subset=["service_date_key", "GSFS_RECEIPT_NO"], keep="first").reset_index(drop=True)
    return deduped


def main() -> None:
    print("=" * 80)
    print("VRP-Level Mode Test: Cluster Seed + Travel Pass + Balance Pass")
    print("=" * 80)
    
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    wanted = {str(v) for v in TARGET_DATES}
    service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()
    service_df = _dedupe_service(service_df)
    
    # VRP-Level 모드 실행
    print("\n[1/2] Running VRP-Level Assignment (Cluster Seed + Travel Pass + Balance Pass)...")
    vrp_level_assignment_df, vrp_level_summary_df, vrp_level_schedule_df = build_atlanta_production_assignment_osrm_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=True,
        assignment_strategy="vrp_level",
    )
    
    vrp_level_assignment_df.to_csv("260310/production_output/atlanta_assignment_result_vrp_level_test_3days.csv", index=False, encoding="utf-8-sig")
    vrp_level_summary_df.to_csv("260310/production_output/atlanta_engineer_day_summary_vrp_level_test_3days.csv", index=False, encoding="utf-8-sig")
    vrp_level_schedule_df.to_csv("260310/production_output/atlanta_schedule_vrp_level_test_3days.csv", index=False, encoding="utf-8-sig")
    
    # Cluster Iteration 모드 실행 (비교용)
    print("\n[2/2] Running Cluster Iteration Assignment (for comparison)...")
    cluster_assignment_df, cluster_summary_df, cluster_schedule_df = build_atlanta_production_assignment_osrm_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=True,
        assignment_strategy="cluster_iteration",
    )
    
    cluster_assignment_df.to_csv("260310/production_output/atlanta_assignment_result_cluster_iter_test_3days.csv", index=False, encoding="utf-8-sig")
    cluster_summary_df.to_csv("260310/production_output/atlanta_engineer_day_summary_cluster_iter_test_3days.csv", index=False, encoding="utf-8-sig")
    cluster_schedule_df.to_csv("260310/production_output/atlanta_schedule_cluster_iter_test_3days.csv", index=False, encoding="utf-8-sig")
    
    # 성능 비교
    print("\n" + "=" * 80)
    print("PERFORMANCE COMPARISON")
    print("=" * 80)
    
    for metric_date in TARGET_DATES:
        vrp_day = vrp_level_summary_df[vrp_level_summary_df["service_date_key"] == metric_date]
        cluster_day = cluster_summary_df[cluster_summary_df["service_date_key"] == metric_date]
        
        if vrp_day.empty or cluster_day.empty:
            continue
        
        vrp_total_km = float(vrp_day["travel_distance_km"].sum())
        vrp_max_work = float(vrp_day["total_work_min"].max())
        vrp_work_std = float(vrp_day["total_work_min"].std(ddof=0))
        vrp_overflow = int(vrp_day["overflow_480"].sum())
        
        cluster_total_km = float(cluster_day["travel_distance_km"].sum())
        cluster_max_work = float(cluster_day["total_work_min"].max())
        cluster_work_std = float(cluster_day["total_work_min"].std(ddof=0))
        cluster_overflow = int(cluster_day["overflow_480"].sum())
        
        print(f"\nDate: {metric_date}")
        print("-" * 80)
        print(f"{'Metric':<25} {'VRP-Level':<20} {'Cluster Iter':<20} {'Improvement':<15}")
        print("-" * 80)
        
        # 총 이동거리
        improvement_km = ((cluster_total_km - vrp_total_km) / cluster_total_km * 100) if cluster_total_km > 0 else 0
        print(f"{'Total Distance (km)':<25} {vrp_total_km:<20.2f} {cluster_total_km:<20.2f} {improvement_km:>13.1f}%")
        
        # 최대 작업시간
        improvement_max = ((cluster_max_work - vrp_max_work) / cluster_max_work * 100) if cluster_max_work > 0 else 0
        print(f"{'Max Work Time (min)':<25} {vrp_max_work:<20.2f} {cluster_max_work:<20.2f} {improvement_max:>13.1f}%")
        
        # 작업시간 표준편차 (낮을수록 좋음)
        improvement_std = ((cluster_work_std - vrp_work_std) / cluster_work_std * 100) if cluster_work_std > 0 else 0
        print(f"{'Work Time Std Dev':<25} {vrp_work_std:<20.2f} {cluster_work_std:<20.2f} {improvement_std:>13.1f}%")
        
        # 480분 초과 엔지니어 수
        print(f"{'Overflow 480min Count':<25} {vrp_overflow:<20} {cluster_overflow:<20} {cluster_overflow - vrp_overflow:>13}")
    
    print("\n" + "=" * 80)
    print("Test completed! Check output files:")
    print("  - atlanta_assignment_result_vrp_level_test_3days.csv")
    print("  - atlanta_engineer_day_summary_vrp_level_test_3days.csv")
    print("  - atlanta_schedule_vrp_level_test_3days.csv")
    print("  - atlanta_assignment_result_cluster_iter_test_3days.csv")
    print("  - atlanta_engineer_day_summary_cluster_iter_test_3days.csv")
    print("  - atlanta_schedule_cluster_iter_test_3days.csv")
    print("=" * 80)


if __name__ == "__main__":
    main()
