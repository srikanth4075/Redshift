
-- Generate External schema Definitions
SELECT 'CREATE EXTERNAL SCHEMA IF NOT EXISTS ' + schemaname + ' FROM  DATA CATALOG' + ' DATABASE ' + QUOTE_LITERAL(Databasename) + ' REGION ' + QUOTE_LITERAL(JSON_EXTRACT_PATH_TEXT(esoptions,'REGION')) + ' IAM_ROLE ' + QUOTE_LITERAL(JSON_EXTRACT_PATH_TEXT(esoptions,'IAM_ROLE')) + ';'
FROM svv_external_schemas;

CREATE EXTERNAL SCHEMA IF NOT EXISTS scratch_spectrum FROM  DATA CATALOG DATABASE 'scratch_spectrum' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift';
CREATE EXTERNAL SCHEMA IF NOT EXISTS dwh_mfg_oa_prod FROM  DATA CATALOG DATABASE 'mfg_ops_analytics-prod' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift,arn:aws:iam::461307682526:role/sfly-mfg-aws-prod-svc-mfg-dwh-redshiftspectrum';
CREATE EXTERNAL SCHEMA IF NOT EXISTS dw_mktg_data FROM  DATA CATALOG DATABASE 'mktg_glue_outbound' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift,arn:aws:iam::813833348497:role/sfly-aws-marketing-prod-svc-mktg-dwh-redshift-spectrum';
CREATE EXTERNAL SCHEMA IF NOT EXISTS pipeline FROM  DATA CATALOG DATABASE 'pipeline' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift';
CREATE EXTERNAL SCHEMA IF NOT EXISTS dw_bo FROM  DATA CATALOG DATABASE 'dw_bo' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift';


 grant all on schema dataquality to dataquality;
 grant usage on schema dataquality to group l1_developer_group;
 grant all on schema devscratchdb to group l1_developer_group ,airflow,dataquality;
 grant usage on schema devscratchdb to group l1_user_group,group l1_pii_user_group;
 grant usage on schema dw to group l1_user_group ,group l1_developer_group,group l1_pii_user_group,group l1_cia_app_a_group;
 grant all on schema dw_bi to group l1_bi_user_a_group,jthompson;
 grant usage on schema dw_bi to group l1_bi_user_wr_group,group l1_bi_user_r_group,group l1_user_group,group l1_pii_user_group;
 grant usage on schema dw_bo to group l1_user_spectrum_group,group l1_developer_group,dataquality,airflow,bi_admin;
 grant all on schema dw_cia_app to group l1_cia_app_a_group;
 grant usage on schema dw_cia_app to group l1_cia_app_wr_group,group l1_cia_app_r_group,group l1_user_group,group l1_pii_user_group;
 grant all on schema dw_cia_udfs to group l1_user_udf_group; 
 grant usage on schema dw_core to group l1_developer_group,group svc_group,group l1_user_group,group l1_pii_user_group;
 grant all on schema dw_core to airflow;
 grant usage on schema dw_core_tmp to group l1_developer_group;
 grant usage on schema dw_mktg_data to group l1_user_spectrum_group;
 grant usage on schema dw_pii to group l1_developer_group,group l1_pii_user_group,group l1_user_group,airflow;
 grant all on schema dw_procs to airflow;
 grant usage on schema dw_procs to group l1_developer_group;
 grant usage on schema dw_rev to airflow,group l1_developer_group,group l1_user_rev_group;
 grant usage on schema dw_stage to group l1_developer_group,group l1_user_group;
 grant all on schema dw_stage to airflow;
 grant usage on schema dwh_mfg_oa_prod to group l1_developer_group,group l1_user_spectrum_group;
 grant usage on schema pipeline to group l1_user_spectrum_group,group l1_developer_group,dataquality,airflow,bi_admin;
 grant usage on schema scratch to svc_infa_edc,group l1_bi_user_a_group,group l1_bi_user_wr_group,group l1_bi_user_r_group,group l1_pii_user_group;
 grant all on schema scratch to group l1_user_group,group svc_group;
 grant usage on schema scratch_pii to airflow,group l1_bi_user_a_group,group l1_bi_user_wr_group,group l1_bi_user_r_group,group l1_user_group,group l1_developer_group;
 grant all on schema scratch_pii to group l1_pii_user_group;
 grant usage on schema scratch_spectrum to group l1_user_group,group l1_svc_group,group svc_group,group etl_group,group l1_bi_user_a_group;
 grant usage on schema scratch_spectrum to group l1_bi_user_group,group l1_cia_app_a_group,group l1_cia_app_wr_group,group l1_cia_app_r_group;
 grant usage on schema scratch_spectrum to group l1_bi_user_wr_group,group l1_bi_user_r_group,group l1_user_spectrum_group,group l1_pii_user_group,group l1_developer_group;
 grant usage on schema sflymonitor to group l1_user_group,group l1_pii_user_group,group l1_developer_group,svc_ops360;
 
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_core GRANT SELECT ON TABLES  to group l1_developer_group;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_core GRANT all ON TABLES  to airflow;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_stage GRANT SELECT ON TABLES  to group l1_developer_group;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_stage GRANT all ON TABLES  to airflow;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT SELECT ON TABLES  to group l1_developer_group,group l1_user_group,group l1_pii_user_group,group svc_group;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_PII GRANT SELECT ON TABLES  to group l1_developer_group,group l1_pii_user_group;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dataquality GRANT SELECT ON TABLES  to group l1_developer_group;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dataquality GRANT SELECT ON TABLES  to group l1_developer_group;
 ALTER DEFAULT PRIVILEGES IN SCHEMA devscratchdb GRANT all ON TABLES  to group l1_user_group;
 
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_cia_app GRANT SELECT ON TABLES to group l1_cia_app_r_group;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_cia_app GRANT ALL ON TABLES to group l1_cia_app_a_group , group l1_cia_app_wr_group;
  
 set session authorization cia_app_admin;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_cia_app GRANT SELECT ON TABLES to group l1_cia_app_r_group;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_cia_app GRANT ALL ON TABLES to group l1_cia_app_wr_group;
 
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_core_tmp GRANT SELECT ON TABLES to group l1_developer_group;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw_rev GRANT SELECT ON TABLES to group l1_developer_group,group l1_user_rev_group,airflow;
 ALTER DEFAULT PRIVILEGES IN SCHEMA sflymonitor GRANT SELECT ON TABLES to group l1_developer_group,group l1_user_group,group l1_pii_user_group;
 grant usage on schema dw_rev,dw_stage,pipeline,dw_bo,dw_bi,dw_core,dw,dw_pii to dataquality;
 ALTER DEFAULT PRIVILEGES IN SCHEMA dw,dw_stage,dw_rev,dw_pii,dw_bo,dw_bi,dw_core  grant select on tables to dataquality

 
 select ddl from sflymonitor.v_generate_tbl_ddl where (schemaname not like 'pg_%') AND schemaname not in ('information_schema','public') order by schemaname,tablename,seq;
        
