DROP VIEW if exists q42020_replay.stl_query_testresults_0901_v;
CREATE OR REPLACE VIEW q42020_replay.stl_query_testresults_0901_v 
AS
SELECT db_snapshot_ts,
       "CLUSTER",
       label,
       SPLIT_PART(label,'/',2) AS Sql_date,
       SPLIT_PART(label,'/',3) AS sql_file_name,
       SPLIT_PART(SPLIT_PART(sql_file_name,'-',3),'.',1) AS test_pid,
       SPLIT_PART(sql_file_name,'-',2) AS test_username,
       CASE
         WHEN lower(sql_file_name) ilike '%spectrum%' THEN 'Spectrum'
         WHEN lower(sql_file_name) SIMILAR TO '%(airflow|dataquality)%' THEN 'ETL'
         WHEN lower(sql_file_name) SIMILAR TO '%(bi_admin|appcia_tableau_rs)%' THEN 'BI'
         WHEN lower(test_username) IN ('admin','admin_automate') THEN 'Adminstration'
         ELSE 'Adhoc'
       END AS Work_group,
       datediff(seconds,starttime,endtime)::NUMERIC(12,2) AS test_run_seconds,
       test_run_seconds / 60 AS test_run_minutes,
       a.querytxt
FROM q42020_replay.stl_query a
WHERE DATABASE = 'dwhprod'
AND   test_run ilike '%RA34xlNormalDay20200901%'
AND   label iLIKE 'auditlog%'
AND   Sql_date = '2020-09-01';

select *
from q42020_replay.stl_query_actualresults_v act
join q42020_replay.stl_query_testresults_0901_v tst
on act.pid = tst.test_pid
where act.pid =98903
;

select * from q42020_replay.stl_query_actualresults_v;
drop view if exists q42020_replay.stl_query_actualresults_0901_v;
CREATE OR REPLACE VIEW q42020_replay.stl_query_actualresults_0901_v 
AS
SELECT a.pid,
       a.starttime,
       a.endtime,
       datediff(seconds,starttime,endtime)::NUMERIC(12,2) AS actual_run_seconds,
       actual_run_seconds / 60 AS actual_run_minutes,
       a.querytxt,
       b.usename AS username,
       a.label
FROM history.hist_stl_query a
  JOIN pg_user b 
ON a.userid = b.usesysid
WHERE CAST(a.starttime AS DATE) = '2020-09-01'
AND   a."DATABASE" = 'dwhprod'
AND   a.userid <> 1;

-- Difference in counts expected as we removed lot of fetch sqls
select count(distinct test_pid),count(*)  from q42020_replay.stl_query_testresults_0901_v ;
5733	12734
select count(distinct pid),count(*) from q42020_replay.stl_query_actualresults_0901_v;
5778	16818


-- Filter all queries less than one minute queries not really comprable time
select count(distinct test_pid),count(*)  from q42020_replay.stl_query_testresults_0901_v where test_run_minutes > 1;
--172	291
select count(distinct pid),count(*) from q42020_replay.stl_query_actualresults_0901_v where actual_run_minutes > 1;
--194	340

select * from q42020_replay.Group_by_pid_actualresults_0901_v order by Sum_Act_Mins Desc;
select * from q42020_replay.Group_by_pid_testresults_0901_v order by Sum_Tst_Mins Desc;

select dt.work_group, total_execution_minutes ,dt.systemtype,row_number() over (partition by dt.work_group Order by total_execution_minutes) as Rank
from (
select 'RA3.4xl.6Nodes.NormalDay.2020-09-01' as SystemType,
       CASE
         WHEN lower(test_Username) SIMILAR TO '%(airflow|dataquality)%' THEN 'ETL'
         WHEN lower(test_Username) SIMILAR TO '%(bi_admin|appcia_tableau_rs)%' THEN 'BI'
         WHEN lower(test_username) IN ('admin','admin_automate') THEN 'Adminstration'
         ELSE 'Adhoc'
       END AS Work_group, 
       Sum(Sum_Tst_Mins) as Total_Execution_Minutes
  from q42020_replay.Group_by_pid_testresults_0901_v
  group by 1,2
 union all
 select 'DS2.8xl.3Nodes.NormalDay.2020-09-01' as SystemType,
       CASE
         WHEN lower(Username) SIMILAR TO '%(airflow|dataquality)%' THEN 'ETL'
         WHEN lower(Username) SIMILAR TO '%(bi_admin|appcia_tableau_rs)%' THEN 'BI'
         WHEN lower(username) IN ('admin','admin_automate') THEN 'Adminstration'
         ELSE 'Adhoc'
       END AS Work_group, 
       Sum(Sum_Act_Mins) as Total_Execution_Minutes
  from q42020_replay.Group_by_pid_actualresults_0901_v
  group by 1,2 ) dt
 ;



drop view if exists q42020_replay.Group_by_pid_actualresults_0901_v;
create or replace view q42020_replay.Group_by_pid_actualresults_0901_v as 
select pid ,Username,Sum(actual_run_seconds) as Sum_Act_Secs,Sum(actual_run_minutes) Sum_Act_Mins
from q42020_replay.stl_query_actualresults_0901_v 
group by pid,username;

drop view if exists q42020_replay.Group_by_pid_testresults_0901_v;
create or replace view q42020_replay.Group_by_pid_testresults_0901_v as 
select test_pid ,test_Username,Sum(test_run_seconds) as Sum_Tst_Secs,Sum(test_run_minutes) Sum_Tst_Mins
from q42020_replay.stl_query_testresults_0901_v
group by test_pid,test_Username;

select  * from q42020_replay.Group_by_pid_actualresults_v;
select * from q42020_replay.Group_by_pid_testresults_0901_v;


select * from 
history.hist_stl_query 
where querytxt like '%adset_name%'
and  CAST(starttime AS DATE) = '2020-09-01'
AND   "DATABASE" = 'dwhprod'
AND   userid <> 1;


select distinct label from   q42020_replay.stl_query  where test_run ='RA34xlNormalDay20200901' and label not like '%stmt%';
                                                                                                                                                                                                                                                                                                                         ;
delete from   q42020_replay.stl_query  where test_run ='RA34xlNormalDay20200901' and label like ('%auditlog/2020-09-02/%'
'cmstats' ,                                                                                                                                                                                                                                                                                                                         
'default',                                                                                                                                                                                                                                                                                                                         
'health',                                                                                                                                                                                                                                                                                                                          
'maintenance',                                                                                                                                                                                                                                                                                                                     
'metrics');
