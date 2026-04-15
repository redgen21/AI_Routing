#!/usr/bin/env python3
"""
Codex 리뷰 피드백을 적용하여 vrp_level 전략을 수정합니다.
- Savings 로직 수정 (Job1 only → 양쪽 job feasibility 체크)
- 2-opt/SA 제거 → Iteration + Local Rebalance로 교체
"""

import re

# 파일 읽기
file_path = r"c:\Python\북미 라우팅\smart_routing\production_assign_atlanta_osrm.py"
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Savings 함수 수정 (Job1만 체크 → 양쪽 job feasibility 체크)
old_savings = r'''    # 모든 job pair에 대해 savings 계산
    savings_list: list\[tuple\[float, int, int, str\]\] = \[\]
    
    for i in range\(len\(jobs\)\):
        for j in range\(i \+ 1, len\(jobs\)\):
            job1 = jobs\.iloc\[i\]
            job2 = jobs\.iloc\[j\]
            job1_coord = \(float\(job1\["longitude"\]\), float\(job1\["latitude"\]\)\)
            job2_coord = \(float\(job2\["longitude"\]\), float\(job2\["latitude"\]\)\)
            
            # 모든 엔지니어에 대해 savings 계산
            for _, engineer in engineers\.iterrows\(\):
                eng_code = str\(engineer\["SVC_ENGINEER_CODE"\]\)
                start_coord = base\._get_engineer_start_coord\(engineer, region_centers\)
                if start_coord is None:
                    continue
                
                # Job 후보 필터링 \(TV, Heavy repair 제약\)
                candidates = base\._candidate_engineers\(job1, engineers\)
                if eng_code not in candidates\["SVC_ENGINEER_CODE"\]\.astype\(str\)\.values:
                    continue'''

new_savings = '''    # 각 job pair에 대해 모든 엔지니어 후보 평가
    savings_list: list[tuple[float, int, int, str]] = []
    
    for i in range(len(jobs)):
        for j in range(i + 1, len(jobs)):
            job1 = jobs.iloc[i]
            job2 = jobs.iloc[j]
            job1_coord = (float(job1["longitude"]), float(job1["latitude"]))
            job2_coord = (float(job2["longitude"]), float(job2["latitude"]))
            
            # 양쪽 job 모두 할당 가능한 엔지니어만 고려
            candidates1 = base._candidate_engineers(job1, engineers)
            candidates2 = base._candidate_engineers(job2, engineers)
            common_candidates = set(candidates1["SVC_ENGINEER_CODE"].astype(str).values) & \
                                set(candidates2["SVC_ENGINEER_CODE"].astype(str).values)
            
            if not common_candidates:
                continue  # 공통 후보 없음
            
            for eng_code in common_candidates:
                engineer = engineer_lookup[eng_code]
                start_coord = base._get_engineer_start_coord(engineer, region_centers)
                if start_coord is None:
                    continue'''

content = re.sub(old_savings, new_savings, content, flags=re.DOTALL)

# 2. vrp_level 전략 섹션 수정 (두 군데 모두)
# 패턴 1: 첫 번째 함수 내
old_vrp_level_1 = r'''            elif assignment_strategy == "vrp_level":
                # VRP 수준 할당: Savings \+ 2-opt \+ Simulated Annealing
                print\(f"  \[VRP-Level\] Savings Algorithm 초기 할당..."\)
                assignment_df = _savings_algorithm_assign\(
                    day_service_df\.copy\(\),
                    day_engineer_master_df\.copy\(\),
                    route_client,
                    region_centers,
                \)
                
                if not assignment_df\.empty:
                    print\(f"  \[VRP-Level\] 2-opt 반복 개선..."\)
                    assignment_df = _two_opt_improve_routes\(
                        assignment_df,
                        day_engineer_master_df\.copy\(\),
                        route_client,
                        region_centers,
                        max_iterations=50,
                    \)
                    
                    print\(f"  \[VRP-Level\] Simulated Annealing 최종 최적화..."\)
                    assignment_df = _simulated_annealing_improve\(
                        assignment_df,
                        day_engineer_master_df\.copy\(\),
                        route_client,
                        region_centers,
                        max_iterations=2000,
                    \)
                
                summary_df = base\._build_summary_from_assignment\(
                    assignment_df,
                    day_engineer_master_df\.copy\(\),
                    region_centers,
                    str\(day_service_df\["service_date_key"\]\.iloc\[0\]\),
                    route_client=route_client,
                \)'''

new_vrp_level = '''            elif assignment_strategy == "vrp_level":
                # VRP 수준 할당: Savings Algorithm + Iteration 개선 + Local Rebalance
                print(f"  [VRP-Level] Savings Algorithm 초기 할당...")
                assignment_df = _savings_algorithm_assign(
                    day_service_df.copy(),
                    day_engineer_master_df.copy(),
                    route_client,
                    region_centers,
                )
                
                if not assignment_df.empty:
                    print(f"  [VRP-Level] Iteration 반복 개선...")
                    assignment_df = base._iterative_improve_assignment_df(
                        assignment_df,
                        day_engineer_master_df.copy(),
                        region_centers,
                        route_client=route_client,
                        iterations=4,
                        priority_mode="balance_first",
                    )
                    
                    print(f"  [VRP-Level] Local Rebalance 최종 조정...")
                    assignment_df = base._local_rebalance_assignment_df(
                        assignment_df,
                        day_engineer_master_df.copy(),
                        region_centers,
                        route_client=route_client,
                    )
                
                summary_df = base._build_summary_from_assignment(
                    assignment_df,
                    day_engineer_master_df.copy(),
                    region_centers,
                    str(day_service_df["service_date_key"].iloc[0]),
                    route_client=route_client,
                )'''

content = re.sub(old_vrp_level_1, new_vrp_level, content, flags=re.DOTALL)

# 파일 쓰기
with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ vrp_level 전략 수정 완료!")
print("변경 사항:")
print("1. Savings 로직: Job1 only → 양쪽 job feasibility 체크")
print("2. 2-opt/SA 제거 → Iteration + Local Rebalance 적용")
