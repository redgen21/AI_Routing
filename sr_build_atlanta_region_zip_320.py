from __future__ import annotations

from pathlib import Path

import pandas as pd

from sr_production_map import build_region_layers


MANUAL_PATH = Path("260310/ATL Three Markets.xlsx")
OUT_PATH = Path("260310/production_input/atlanta_fixed_region_zip_3_manual320.csv")
SUMMARY_PATH = Path("260310/production_output/atlanta_fixed_region_zip_3_manual320_summary.csv")

BUCKET_TO_REGION = {
    "ATL West": (1, "Atlanta New Region 1"),
    "ATL East": (2, "Atlanta New Region 2"),
    "ATL South": (3, "Atlanta New Region 3"),
}


def main() -> None:
    visible_layer, _ = build_region_layers.__wrapped__()
    visible_df = (
        visible_layer[["POSTAL_CODE", "region_seq", "new_region_name"]]
        .drop_duplicates()
        .copy()
    )
    visible_df["POSTAL_CODE"] = visible_df["POSTAL_CODE"].astype(str).str.zfill(5)
    visible_df["region_seq"] = pd.to_numeric(visible_df["region_seq"], errors="coerce").astype("Int64")
    visible_df["source"] = "current_visible_region"

    manual_df = pd.read_excel(MANUAL_PATH, sheet_name=0, dtype={"Zip Code": str})
    manual_df.columns = [str(c).strip() for c in manual_df.columns]
    manual_df["POSTAL_CODE"] = manual_df["Zip Code"].astype(str).str.zfill(5)
    manual_df = manual_df.rename(columns={"Bucket": "manual_bucket"})
    manual_df = manual_df[["POSTAL_CODE", "manual_bucket"]].drop_duplicates().copy()

    missing_df = manual_df[~manual_df["POSTAL_CODE"].isin(set(visible_df["POSTAL_CODE"]))].copy()
    missing_df["region_seq"] = missing_df["manual_bucket"].map(lambda v: BUCKET_TO_REGION.get(str(v), (pd.NA, ""))[0])
    missing_df["new_region_name"] = missing_df["manual_bucket"].map(lambda v: BUCKET_TO_REGION.get(str(v), (pd.NA, ""))[1])
    missing_df["source"] = "manual_bucket_fill"

    merged_df = pd.concat(
        [
            visible_df[["POSTAL_CODE", "region_seq", "new_region_name", "source"]],
            missing_df[["POSTAL_CODE", "region_seq", "new_region_name", "source"]],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["POSTAL_CODE"]).sort_values(["region_seq", "POSTAL_CODE"]).reset_index(drop=True)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    summary_df = (
        merged_df.groupby(["region_seq", "new_region_name", "source"], dropna=False)
        .size()
        .rename("zip_count")
        .reset_index()
        .sort_values(["region_seq", "source"])
        .reset_index(drop=True)
    )
    summary_df.to_csv(SUMMARY_PATH, index=False, encoding="utf-8-sig")

    print(f"out={OUT_PATH}")
    print(f"summary={SUMMARY_PATH}")
    print(f"total_zip={merged_df['POSTAL_CODE'].nunique()}")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
