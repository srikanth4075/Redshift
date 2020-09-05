unload ('SELECT *, date_pacific AS ptn_orderdate_pacific
FROM dw_core.f_orderitem_details
WHERE date_pacific >= ''2020-01-01''
') 
 to 's3://sfly-aws-dwh-prod-datapipeline-staging-objects-levelone-enc/f_orderitem_details_20200905/' 
 IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift'
 KMS_KEY_ID 'f6dff027-6286-4de4-8503-e92c31d04b88' 
 FORMAT AS PARQUET
 PARTITION BY (ptn_orderdate_pacific)
 allowoverwrite
 encrypted
 ;
 
 unload ('SELECT *, orderdate_pacific AS ptn_orderdate_pacific
FROM dw_core.d_order') 
 to 's3://sfly-aws-dwh-prod-datapipeline-staging-objects-levelone-enc/d_order_backup_20200905/' 
 IAM_ROLE 'arn:aws:iam::157816743405:role/sfly-aws-dwh-prod-svc-redshift'
 KMS_KEY_ID 'f6dff027-6286-4de4-8503-e92c31d04b88' 
 FORMAT AS PARQUET
 PARTITION BY (ptn_orderdate_pacific)
 allowoverwrite
 encrypted
 MANIFEST  VERBOSE
;
