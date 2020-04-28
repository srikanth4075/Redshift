-- Create database in Glue
--  Add the table to read bucket 
CREATE EXTERNAL TABLE redshiftuseractivitylog (
    logrecord STRING
)
  ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ''
      ESCAPED BY '\\'
      LINES TERMINATED BY '\n'
    LOCATION 's3://sfly-aws-dwh-prod-redshift-audit-logging/prod-audit-logs/AWSLogs/157816743405/redshift/us-east-1';

-- Create views as required.
CREATE or REPLACE VIEW redshiftuseractivitylog_view AS
SELECT date_parse(regexp_extract(logrecord, '\d+-\d+-\d+T\d+:\d+:\d+Z UTC'), '%Y-%m-%dT%TZ UTC') AS recordtimestamp,
          regexp_extract(logrecord, '\[ db=(.*?) user=(.*?) pid=(\d+) userid=(\d+) xid=(\d+) \]', 1) AS db,
          regexp_extract(logrecord, '\[ db=(.*?) user=(.*?) pid=(\d+) userid=(\d+) xid=(\d+) \]', 2) AS user,
          regexp_extract(logrecord, '\[ db=(.*?) user=(.*?) pid=(\d+) userid=(\d+) xid=(\d+) \]', 3) AS pid,
          regexp_extract(logrecord, '\[ db=(.*?) user=(.*?) pid=(\d+) userid=(\d+) xid=(\d+) \]', 4) AS userid,
          regexp_extract(logrecord, '\[ db=(.*?) user=(.*?) pid=(\d+) userid=(\d+) xid=(\d+) \]', 5) AS xid,
          regexp_extract(logrecord, 'LOG: (.*)', 1) AS query
FROM redshiftuseractivitylog
WHERE regexp_like("$path", '[0-9]+_redshift_us-east-1_redshift-stack-enc-redshiftcluster-10r8oprxwf5w8_useractivitylog_.*');

CREATE or REPLACE VIEW redshiftuserlog_view AS
select regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*)', 1) AS userid,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*)', 2) AS username,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*?)\|(.*)', 3) AS oldusername,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*?)\|(.*?)\|(.*)', 4) AS action,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*)', 5) AS usecreatedb,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(\d+)\|(.*)', 6) AS usesuper,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(\d+)\|(\d+)\|(.*)', 7) AS usecatupd,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(\d+)\|(\d+)\|(.*?)\|(.*)', 8) AS valuntil,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(\d+)\|(\d+)\|(.*?)\|(\d+)\|(.*)', 9) AS pid,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(\d+)\|(\d+)\|(.*?)\|(\d+)\|(\d+)\|(.*)', 10) AS xid,
regexp_extract(logrecord, '(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(\d+)\|(\d+)\|(.*?)\|(\d+)\|(\d+)\|(.*)', 11) AS recordtime
FROM redshiftuserlog_viewactivitylog
WHERE regexp_like("$path", '[0-9]+_redshift_us-east-1_redshift-stack-enc-redshiftcluster-10r8oprxwf5w8_userlog_.*'); 

CREATE or REPLACE VIEW redshiftconnectionlog_view AS
select regexp_extract(logrecord, '(.*?)\|(.*)', 1) AS event,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*)', 2) AS recordtime,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*)', 3) AS remotehost,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(.*)', 4) AS remoteport,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*)', 5) AS pid,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*)', 6) AS dbname,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*)', 7) AS username,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(.*)', 8) AS authmethod,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*)', 9) AS duration,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*)', 10) AS sslversion,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*)', 11) AS sslcipher,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(\d+)\|(.*)', 12) AS mtu,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*)', 13) AS sslcompression,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*)', 14) AS sslexpansion,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(.*)', 15) AS iamauthguid,
regexp_extract(logrecord, '(.*?)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(\d+)\|(.*?)\|(.*?)\|(.*?)\|(.*)', 16) AS application_name
FROM redshiftuseractivitylog
WHERE regexp_like("$path", '[0-9]+_redshift_us-east-1_redshift-stack-enc-redshiftcluster-10r8oprxwf5w8_connectionlog_.*');
