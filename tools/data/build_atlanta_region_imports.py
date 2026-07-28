"""Thin Atlanta scenario launcher for the generic region workbook importer."""
from pathlib import Path
from build_region_plan_import import build

SCENARIOS={
 "atlanta_3area_canonical_workbook.xlsx":("Atlanta_3area","atlanta_3area_20260723",["ATL East","ATL South","ATL West"],False),
 "atlanta_6area_canonical_workbook_new.xlsx":("Atlanta_6area_new","atlanta_6area_new_20260723",["Zone 1","Zone 2","Zone 3","Zone 4","Zone 5","PO Box"],True),
 "atlanta_6area_canonical_workbook_overlap.xlsx":("Atlanta_6area_overlab","atlanta_6area_overlab_20260723",["Zone 1","Zone 2","Zone 3","Zone 4","Zone 5","PO Box"],True)}
def main():
 import argparse
 p=argparse.ArgumentParser();p.add_argument('--source-dir',type=Path,required=True);p.add_argument('--output-dir',type=Path,required=True);a=p.parse_args()
 for file,(city,plan,territories,overflow) in SCENARIOS.items():
  build(a.source_dir/file,a.output_dir,city=city,plan=plan,territory_order=territories,source_technician_city='Atlanta, GA',policy_version='explicit_workbook_membership/v1',overflow_enabled=overflow)
if __name__=='__main__': main()
