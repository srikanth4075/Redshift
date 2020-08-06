
-- Generate External schema Definitions
SELECT 'CREATE EXTERNAL SCHEMA IF NOT EXISTS ' + schemaname + ' FROM  DATA CATALOG' + ' DATABASE ' + QUOTE_LITERAL(Databasename) + ' REGION ' + QUOTE_LITERAL(JSON_EXTRACT_PATH_TEXT(esoptions,'REGION')) + ' IAM_ROLE ' + QUOTE_LITERAL(JSON_EXTRACT_PATH_TEXT(esoptions,'IAM_ROLE')) + ';'
FROM svv_external_schemas;

CREATE EXTERNAL SCHEMA IF NOT EXISTS scratch_spectrum FROM  DATA CATALOG DATABASE 'scratch_spectrum' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift';
CREATE EXTERNAL SCHEMA IF NOT EXISTS dwh_mfg_oa_prod FROM  DATA CATALOG DATABASE 'mfg_ops_analytics-prod' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift,arn:aws:iam::461307682526:role/sfly-mfg-aws-prod-svc-mfg-dwh-redshiftspectrum';
CREATE EXTERNAL SCHEMA IF NOT EXISTS dw_mktg_data FROM  DATA CATALOG DATABASE 'mktg_glue_outbound' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift,arn:aws:iam::813833348497:role/sfly-aws-marketing-prod-svc-mktg-dwh-redshift-spectrum';
CREATE EXTERNAL SCHEMA IF NOT EXISTS pipeline FROM  DATA CATALOG DATABASE 'pipeline' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift';
CREATE EXTERNAL SCHEMA IF NOT EXISTS dw_bo FROM  DATA CATALOG DATABASE 'dw_bo' REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift';
