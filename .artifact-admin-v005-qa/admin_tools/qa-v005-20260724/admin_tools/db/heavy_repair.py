"""Heavy-repair lookup handling for offline administrative input builds."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from admin_tools.db.data_catalog import na_data_path


RULE_COLUMNS = ["product_group_code", "product_code", "detailed_symptom_code"]


def normalize_heavy_repair_rules(rule_df: pd.DataFrame) -> pd.DataFrame:
    if rule_df.empty:
        return pd.DataFrame(columns=RULE_COLUMNS)
    renamed = rule_df.rename(
        columns={
            "SERVICE_PRODUCT_GROUP_CODE": "product_group_code",
            "SERVICE_PRODUCT_CODE": "product_code",
            "SYMP_CODE_THREE": "detailed_symptom_code",
        }
    ).copy()
    for column in RULE_COLUMNS:
        if column not in renamed.columns:
            renamed[column] = ""
        renamed[column] = renamed[column].fillna("").astype(str).str.strip().str.upper()
    result = renamed[RULE_COLUMNS]
    result = result[
        result["product_group_code"].ne("")
        & result["product_code"].ne("")
        & result["detailed_symptom_code"].ne("")
    ]
    return result.drop_duplicates().reset_index(drop=True)


def load_heavy_repair_rules(data_catalog_path: Path | str | None = None) -> pd.DataFrame:
    """Load the cataloged lookup, or its cataloged source workbook as fallback.

    The fallback is intentionally local and deterministic; it does not invoke
    production routing or geocoding modules.
    """
    lookup_path = na_data_path("heavy_repair_lookup", data_catalog_path)
    if lookup_path.exists():
        source = pd.read_csv(lookup_path, encoding="utf-8-sig")
    else:
        symptom_path = na_data_path("symptom_mapping", data_catalog_path)
        if not symptom_path.exists():
            raise FileNotFoundError(
                f"Missing heavy-repair lookup {lookup_path} and fallback symptom mapping {symptom_path}"
            )
        source = pd.read_excel(symptom_path)
    return normalize_heavy_repair_rules(source)
