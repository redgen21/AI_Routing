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


def routing_policy_label(value: object) -> str:
    policy = str(value or "").strip()
    option = ROUTING_POLICY_BY_VALUE.get(policy)
    return option["label"] if option else ("기존/미등록 정책" if policy else "설정되지 않음")


def routing_policy_description(value: object) -> str:
    policy = str(value or "").strip()
    option = ROUTING_POLICY_BY_VALUE.get(policy)
    return option["description"] if option else "등록되지 않은 정책입니다. 정책 목록에서 다시 선택해 주세요."
