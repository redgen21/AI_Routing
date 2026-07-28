import csv, hashlib, json, subprocess, sys, tempfile, unittest, openpyxl
from pathlib import Path
ROOT=Path(__file__).parents[1]; SRC=ROOT/'260310'/'atlanta 2606_test'/'new_region'
class RegionImportTests(unittest.TestCase):
 def runit(self,book,city,territories,overflow='explicit'):
  d=Path(tempfile.mkdtemp()); cmd=[sys.executable,str(ROOT/'tools/data/build_region_plan_import.py'),'--workbook',str(SRC/book),'--output-dir',str(d),'--target-city',city,'--plan-id','test-plan','--source-technician-city','Source City','--policy-version','explicit_workbook_membership/v1']
  for x in territories: cmd += ['--territory',x]
  cmd += ['--overflow-mode', overflow]
  subprocess.run(cmd,check=True); return d/city
 def test_three_and_arbitrary_city(self):
  d=self.runit('atlanta_3area_canonical_workbook.xlsx','Any City',['ATL East','ATL South','ATL West'],'disabled')
  m=json.loads((d/'manifest.json').read_text())
  self.assertEqual(m['city'],'Any City'); self.assertFalse(m['overflow_policy']['enabled'])
  self.assertEqual(m['row_accounting']['area_rejected'],0)
 def test_six_regions(self):
  d=self.runit('atlanta_6area_canonical_workbook_new.xlsx','Other City',['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5','PO Box']); m=json.loads((d/'manifest.json').read_text())
  self.assertEqual(m['row_accounting']['area_input_nonblank'],323); self.assertEqual(m['row_accounting']['area_rejected'],0)
  with (d/'boundary_overflow.csv').open() as f: self.assertEqual(len(list(csv.DictReader(f))),4)
 def test_explicit_overlap(self):
  d=self.runit('atlanta_6area_canonical_workbook_overlap.xlsx','Overlap City',['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5','PO Box'])
  with (d/'boundary_overflow.csv').open() as f: rows=list(csv.DictReader(f))
  self.assertEqual(len(rows),101); self.assertEqual({x['relation_syntax'] for x in rows},{'>','<>'})
 def test_header_contract_rejects_semantic_alias(self):
  from tools.data.build_region_plan_import import build
  d=Path(tempfile.mkdtemp()); bad=d/'bad.xlsx'; wb=openpyxl.load_workbook(SRC/'atlanta_3area_canonical_workbook.xlsx'); wb['1. Area']['A1']='Zip'; wb.save(bad)
  with self.assertRaisesRegex(ValueError,'AREA_HEADER_CONTRACT_INVALID'):
   build(bad,d,city='Any',plan='p',territory_order=['ATL East','ATL South','ATL West'],source_technician_city='Source',policy_version='explicit_workbook_membership/v1')
 def test_generated_sql_stages_candidate_without_runtime_or_activation_mutation(self):
  d=self.runit('atlanta_3area_canonical_workbook.xlsx','Guard City',['ATL East','ATL South','ATL West'],'disabled')
  sql=(d/'import_vrp_db_dev.sql').read_text()
  for token in ('current_database()', 'vrp_db_dev', 'ISOLATION LEVEL SERIALIZABLE',
                'pg_advisory_xact_lock', "plan_status='candidate'",
                'source technician roster missing/inactive'):
   self.assertIn(token,sql)
  for token in ('target and source technician city must differ',
                'immutable plan region payload drift',
                'immutable plan postal payload drift',
                'immutable plan technician payload drift',
                'immutable plan overflow payload drift',
                ' EXCEPT (VALUES '):
   self.assertIn(token,sql)
  self.assertNotIn('schema_migrations',sql)
  for forbidden in ("plan_status='reviewed'", "plan_status='active'", "plan_status='superseded'",
                    'common_region_plan_activation', 'DELETE FROM common_region_master',
                    'INSERT INTO common_region_master', 'DELETE FROM common_technician_master',
                    'INSERT INTO common_technician_master',
                    'DELETE FROM common_technician_capability_master',
                    'INSERT INTO common_technician_capability_master'):
   self.assertNotIn(forbidden,sql)
 def test_target_and_source_city_must_differ(self):
  from tools.data.build_region_plan_import import build
  out=Path(tempfile.mkdtemp())
  with self.assertRaisesRegex(ValueError,'TARGET_CITY_MUST_DIFFER'):
   build(SRC/'atlanta_3area_canonical_workbook.xlsx',out,city='Same City',plan='p',territory_order=['ATL East','ATL South','ATL West'],source_technician_city='Same City',policy_version='explicit_workbook_membership/v1',overflow_enabled=False)
 def test_missing_source_city_is_rejected(self):
  out=Path(tempfile.mkdtemp())
  cmd=[sys.executable,str(ROOT/'tools/data/build_region_plan_import.py'),'--workbook',str(SRC/'atlanta_3area_canonical_workbook.xlsx'),'--output-dir',str(out),'--target-city','Missing Source','--plan-id','test-plan','--territory','ATL East','--territory','ATL South','--territory','ATL West']
  result=subprocess.run(cmd,capture_output=True,text=True)
  self.assertNotEqual(result.returncode,0)
 def test_generic_employee_codes_and_blank_assignment(self):
  from tools.data.build_region_plan_import import build
  d=Path(tempfile.mkdtemp()); book=d/'generic-tech.xlsx'
  wb=openpyxl.Workbook(); area=wb.active; area.title='1. Area'
  area.append(['ZIPCode','Territory','Area Type']); area.append(['90001','Region 1','DMS'])
  tech=wb.create_sheet('2. Technician'); tech.append(['Tech ID','Tech Name','Assignment'])
  tech.append(['AI123456','Assigned AI','Region 1'])
  tech.append(['AATOYAN','Assigned Alpha','Region 1'])
  tech.append(['43032200','Assigned Numeric','Region 1'])
  tech.append(['AI654321','Excluded Blank',''])
  wb.save(book)
  manifest=build(book,d,city='Generic City',plan='generic-plan',territory_order=['Region 1'],source_technician_city='Source City',policy_version='explicit_workbook_membership/v1',overflow_enabled=False)
  self.assertEqual(manifest['row_accounting']['technician_input'],4)
  self.assertEqual(manifest['row_accounting']['technician_accepted'],3)
  self.assertEqual(manifest['row_accounting']['technician_rejected'],1)
  self.assertIn('V003__region_plan_technician_source_id',manifest['required_migrations'])
  with (d/'Generic City'/'technician_assignments.csv').open() as f:
   self.assertEqual({r['employee_code'] for r in csv.DictReader(f)},{'AI123456','AATOYAN','43032200'})
  with (d/'Generic City'/'rejects.csv').open() as f:
   self.assertIn('BLANK_TECHNICIAN_ASSIGNMENT',{r['reason'] for r in csv.DictReader(f)})
  self.assertIn('V003__region_plan_technician_source_id is required',(d/'Generic City'/'import_vrp_db_dev.sql').read_text())
 def test_v003_source_id_migration_is_registered_and_safe(self):
  from admin_tools.db.release_backend import classify_sql_statement, split_sql_statements
  root=ROOT/'admin_tools/db/migrations'; registry=json.loads((root/'manifest.json').read_text())
  entry=next(x for x in registry['migrations'] if x['migration_id']=='V003__region_plan_technician_source_id')
  sql_path=root/entry['sql_file']; self.assertEqual(hashlib.sha256(sql_path.read_bytes()).hexdigest(),entry['checksum_sha256'])
  sql=sql_path.read_text(); statements=split_sql_statements(sql)
  self.assertEqual([classify_sql_statement(x) for x in statements],['select','alter_table','alter_table','alter_table','alter_table'])
  self.assertIn("^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$",sql)
  self.assertIn('not valid',sql.lower()); self.assertIn('validate constraint',sql.lower())
  from admin_tools.db.region_plan_backend import _build_cli_parser
  parsed=_build_cli_parser().parse_args(['migration-preview','--config','config.json','--migration-id','V003__region_plan_technician_source_id'])
  self.assertEqual(parsed.migration_id,'V003__region_plan_technician_source_id')
  parsed=_build_cli_parser().parse_args(['migration-preview','--config','config.json','--migration-id','V004__region_plan_area_type_region_soft'])
  self.assertEqual(parsed.migration_id,'V004__region_plan_area_type_region_soft')
 def test_type_hard_region_soft_policy_emits_uniform_region_contract(self):
  from tools.data.build_region_plan_import import build
  source=ROOT/'260310'/'la bucket test'/'la_new_region_6_canonical_workbook.xlsx'
  out=Path(tempfile.mkdtemp()); plan='la_6area_type_hard_soft_test'
  manifest=build(source,out,city='LA_6area',plan=plan,
   territory_order=[f'Region {n}' for n in range(1,7)],
   source_technician_city='Los Angeles, CA - Bucket Sim Draft',
   policy_version='active_roster_type_hard_region_soft/v1',overflow_enabled=False)
  self.assertEqual(manifest['technician_policy_mode'],'active_roster_type_hard_region_soft')
  self.assertEqual([x['required_center_type'] for x in manifest['territories']],['DMS','DMS2','DMS','DMS2','DMS2','DMS'])
  self.assertIn('V004__region_plan_area_type_region_soft',manifest['required_migrations'])
  bundle=out/'LA_6area'
  with (bundle/'boundary_overflow.csv').open() as f: self.assertEqual(list(csv.DictReader(f)),[])
  with (bundle/'technician_assignments.csv').open() as f:
   self.assertEqual({r['policy_mode'] for r in csv.DictReader(f)},{'active_roster_type_hard_region_soft'})
  sql=(bundle/'import_vrp_db_dev.sql').read_text()
  for token in ('V004__region_plan_area_type_region_soft is required','required_center_type',
                "'active_roster_type_hard_region_soft/v1'","'active_roster_type_hard_region_soft'"):
   self.assertIn(token,sql)
 def test_type_hard_region_soft_rejects_overflow_and_mixed_territory_type(self):
  from tools.data.build_region_plan_import import build
  source=ROOT/'260310'/'la bucket test'/'la_new_region_6_canonical_workbook.xlsx'; out=Path(tempfile.mkdtemp())
  with self.assertRaisesRegex(ValueError,'REQUIRES_DISABLED_OVERFLOW'):
   build(source,out,city='LA',plan='p',territory_order=[f'Region {n}' for n in range(1,7)],source_technician_city='Source',policy_version='active_roster_type_hard_region_soft/v1',overflow_enabled=True)
  book=out/'mixed.xlsx'; wb=openpyxl.Workbook(); area=wb.active; area.title='1. Area'
  area.append(['ZIPCode','Territory','Area Type']); area.append(['90001','R1','DMS']); area.append(['90002','R1','DMS2'])
  tech=wb.create_sheet('2. Technician'); tech.append(['Tech ID','Tech Name','Assignment']); tech.append(['AI123456','T','R1']); wb.save(book)
  with self.assertRaisesRegex(ValueError,'TERRITORY_AREA_TYPE_NOT_UNIFORM:R1'):
   build(book,out,city='LA',plan='p',territory_order=['R1'],source_technician_city='Source',policy_version='active_roster_type_hard_region_soft/v1',overflow_enabled=False)
 def test_area_type_fallback_region_soft_is_candidate_only(self):
  from tools.data.build_region_plan_import import build
  source=ROOT/'260310'/'la bucket test'/'la_new_region_6_canonical_workbook.xlsx'; out=Path(tempfile.mkdtemp())
  manifest=build(source,out,city='LA_6area',plan='fallback-plan',territory_order=[f'Region {n}' for n in range(1,7)],source_technician_city='Los Angeles, CA - Bucket Sim Draft',policy_version='active_roster_area_type_fallback_region_soft/v1',overflow_enabled=False)
  self.assertEqual(manifest['technician_policy_mode'],'active_roster_area_type_fallback_region_soft')
  self.assertEqual(manifest['lifecycle_stage'],'candidate')
  self.assertEqual([x['required_center_type'] for x in manifest['territories']],['DMS','DMS2','DMS','DMS2','DMS2','DMS'])
  self.assertIn('V004__region_plan_area_type_region_soft',manifest['required_migrations'])
  sql=(out/'LA_6area'/'import_vrp_db_dev.sql').read_text()
  self.assertIn("'active_roster_area_type_fallback_region_soft'",sql)
  for forbidden in ("plan_status='reviewed'","plan_status='active'",'common_region_plan_activation','DELETE FROM common_region_master','DELETE FROM common_technician_master'):
   self.assertNotIn(forbidden,sql)
 def test_v004_policy_migration_is_registered_and_backward_compatible(self):
  from admin_tools.db.release_backend import classify_sql_statement, split_sql_statements
  root=ROOT/'admin_tools/db/migrations'; registry=json.loads((root/'manifest.json').read_text())
  entry=next(x for x in registry['migrations'] if x['migration_id']=='V004__region_plan_area_type_region_soft')
  sql_path=root/entry['sql_file']; self.assertEqual(hashlib.sha256(sql_path.read_bytes()).hexdigest(),entry['checksum_sha256'])
  sql=sql_path.read_text(); statements=split_sql_statements(sql)
  self.assertTrue(all(classify_sql_statement(x)=='alter_table' for x in statements))
  self.assertIn("required_center_type is null",sql)
  self.assertIn("'assigned_region_boundary_spillover'",sql)
  self.assertIn("'active_roster_type_hard_region_soft'",sql)
  self.assertIn("'active_roster_area_type_fallback_region_soft'",sql)
