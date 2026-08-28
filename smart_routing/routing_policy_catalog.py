"""Human-readable routing-policy choices shared by the VRP clients."""
from __future__ import annotations


ROUTING_POLICY_OPTIONS: tuple[dict[str, str], ...] = (
    {
        "value": "home_distance_only",
        "label": "Region 미사용 · Home 기반",
        "description": "Region 경계를 사용하지 않고 기사 Home과 작업지의 이동거리를 중심으로 배정합니다.",
    },
    {
        "value": "preferred_region_soft",
        "label": "Region Soft · Region 우선",
        "description": "기사의 담당 Region을 우선하지만, 전체 기사를 후보로 두고 거리와 시간도 함께 고려합니다.",
    },
    {
        "value": "own_region_with_approved_boundary_overflow/v2",
        "label": "Region Hard · 승인된 경계 Overflow",
        "description": "담당 Region을 hard 후보로 제한하고, Plan에 승인된 인접 Region만 제한적으로 허용합니다.",
    },
    {
        "value": "explicit_workbook_membership/v1",
        "label": "Region Hard · Workbook 소속 고정",
        "description": "Workbook에 등록된 Region 소속을 hard 후보 제한으로 사용하고, 소속 밖 기사는 기본 후보에서 제외합니다.",
    },
    {
        "value": "active_roster_type_hard_region_soft/v1",
        "label": "Region Soft + DMS/DMS2 타입 Hard",
        "description": "Region은 선호 조건으로 적용하고, 작업의 DMS/DMS2 타입과 기사의 센터 타입은 정확히 일치시킵니다.",
    },
    {
        "value": "active_roster_area_type_fallback_region_soft/v1",
        "label": "Region Soft + DMS2 Fallback",
        "description": "Region은 선호 조건으로 적용하고, DMS 작업에 적합한 DMS 기사가 없으면 DMS2를 fallback으로 사용합니다. DMS2 작업은 DMS2만 사용합니다.",
    },
)

ROUTING_POLICY_BY_VALUE = {option["value"]: option for option in ROUTING_POLICY_OPTIONS}
ROUTING_POLICY_VALUES = tuple(option["value"] for option in ROUTING_POLICY_OPTIONS)

_ROUTING_POLICY_BILINGUAL_DESCRIPTIONS = {
    "home_distance_only": "Home Distance Only (Region 미사용 · Home 기반): Assigns work using technician home-to-job distance/time without Region constraints. (Region 경계를 적용하지 않고 기사 Home과 작업지의 거리·시간을 중심으로 배정합니다.)",
    "preferred_region_soft": "Preferred Region Soft (Region Soft · Region 우선): Prefers each technician's assigned Region but permits all eligible technicians. (기사 담당 Region을 우선하지만, 조건을 충족하는 모든 기사를 후보로 허용합니다.)",
    "own_region_with_approved_boundary_overflow/v2": "Hard Region with Approved Boundary Overflow (Region Hard · 승인된 경계 Overflow): Limits work to its Region and explicitly approved adjacent-boundary overflow technicians. (작업을 해당 Region과 승인된 인접 경계 Overflow 기사로만 제한합니다.)",
    "explicit_workbook_membership/v1": "Hard Region by Workbook Membership (Region Hard · Workbook 소속 고정): Uses the workbook's postal and technician membership as hard eligibility. (Workbook에 저장된 우편번호와 기사 소속을 하드 제약으로 적용합니다.)",
    "active_roster_type_hard_region_soft/v1": "Soft Region with Hard DMS/DMS2 Type (Region Soft + DMS/DMS2 타입 Hard): Uses Region as an affinity while strictly matching DMS/DMS2 job and technician types. (Region은 선호 조건으로 적용하고 DMS/DMS2 작업·기사 타입은 엄격히 일치시킵니다.)",
    "active_roster_area_type_fallback_region_soft/v1": "Soft Region with DMS2 Fallback (Region Soft + DMS2 Fallback): Prefers the Region; DMS work may fall back to DMS2, while DMS2 work remains DMS2-only. (Region을 우선하며 DMS 작업은 DMS2로 대체 가능하지만 DMS2 작업은 DMS2 기사만 배정합니다.)",
}


def routing_policy_bilingual_description(value: object) -> str:
    policy = str(value or "").strip()
    return _ROUTING_POLICY_BILINGUAL_DESCRIPTIONS.get(
        policy,
        "Unknown Routing Policy (알 수 없는 라우팅 정책입니다.)",
    )


def routing_policy_label(value: object) -> str:
    policy = str(value or "").strip()
    option = ROUTING_POLICY_BY_VALUE.get(policy)
    return option["label"] if option else ("기존/미등록 정책" if policy else "설정되지 않음")


def routing_policy_description(value: object) -> str:
    policy = str(value or "").strip()
    option = ROUTING_POLICY_BY_VALUE.get(policy)
    return option["description"] if option else "등록되지 않은 정책입니다. 정책 목록에서 다시 선택해 주세요."
