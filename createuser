STORED PROCEDURE DEFINITION
CREATE OR REPLACE PROCEDURE selfserve."createuser"(userid character varying(256), piiflag integer, OUT c1 character varying(256), OUT c2 character varying(256), OUT c3 character varying(256))
 LANGUAGE plpgsql
AS $_$
DECLARE varsmall Varchar :='abcdefghijklmnopqrstuvwxyz';
DECLARE varbig Varchar :='ABCDEFGHILKLMNOPQRSTUVXWYZ';
DECLARE varnumber Varchar :='0123456789';
DECLARE varspl Varchar :='!#$%^&*()_';
DECLARE varstart Varchar :='\'';
DECLARE varend Varchar :='\'';
DECLARE VarPass text;
DECLARE qtemp text;
DECLARE qtemp1 text;
DECLARE qtemp2 text;
DECLARE qtemp3 text;
DECLARE qtemp4 text;
DECLARE VarUser Varchar :=LOWER(TRIM(USERID));
DECLARE VarEmail Varchar :=LOWER(TRIM(USERID))||'@shutterfly.com';

BEGIN 
   SELECT  varstart || Substring(varsmall,cast (random()*26  as int),1)
         || Substring(varbig,cast (random()*26  as int),1)
         || Substring(varspl,cast (random()*6  as int),1)
         || Substring(varsmall,cast (random()*26  as int),1)
         || Substring(varbig,cast (random()*26  as int),1) 
         || Substring(varbig,cast (random()*26  as int),1) 
         || Substring(varnumber,cast (random()*10  as int),1)
         || Substring(varsmall,cast (random()*26  as int),1) 
         || Substring(varbig,cast (random()*26  as int),1) 
         || Substring(varnumber,cast (random()*10  as int),1)
         || varend
         into VarPass; 
         
    SELECT VarUser  INTO C1;     
    SELECT VarEmail  INTO C2;
    SELECT VarPass INTO C3;
         
  IF PIIFlag = 1 THEN
  SELECT 'CREATE USER "'||LOWER(TRIM(USERID))||'" with PASSWORD '|| TRIM(VarPass)||' IN GROUP l1_pii_user_group CONNECTION LIMIT 2;' INTO qtemp; 
  EXECUTE qtemp;
  SELECT 'ALTER USER "'||LOWER(TRIM(USERID))||'" SET SEARCH_PATH TO dw_pii,scratch_pii,dw;' INTO qtemp1;
  EXECUTE qtemp1;
  SELECT 'ALTER USER "'||LOWER(TRIM(USERID))||'" SET STATEMENT_TIMEOUT TO 900000;' INTO qtemp2;
  EXECUTE qtemp2;
  SELECT 'ALTER USER "'||LOWER(TRIM(USERID))||'" SET describe_field_name_in_uppercase TO ON;' INTO qtemp3;
  EXECUTE qtemp3;
  SELECT 'ALTER USER "'||LOWER(TRIM(USERID))||'" SET timezone to PST8PDT;' INTO qtemp4;
  EXECUTE qtemp4;
  --SELECT 'CREATE USER "'||LOWER(TRIM(USERID))||'" SUCCESS with PASSWORD '||replace (TRIM(VarPass),'\'','') INTO MSG; 
  SELECT replace (TRIM(VarPass),'\'','')  INTO C3;
  ELSE           
  SELECT 'CREATE USER "'||LOWER(TRIM(USERID))||'" with PASSWORD '|| TRIM(VarPass)||' IN GROUP l1_user_group CONNECTION LIMIT 2;' INTO qtemp; 
  EXECUTE qtemp;
  SELECT 'ALTER USER "'||LOWER(TRIM(USERID))||'" SET SEARCH_PATH TO dw,scratch;' INTO qtemp1;
  EXECUTE qtemp1;
  SELECT 'ALTER USER "'||LOWER(TRIM(USERID))||'" SET STATEMENT_TIMEOUT TO 900000;' INTO qtemp2;
  EXECUTE qtemp2;
  SELECT 'ALTER USER "'||LOWER(TRIM(USERID))||'" SET describe_field_name_in_uppercase TO ON;' INTO qtemp3;
  EXECUTE qtemp3;
  SELECT 'ALTER USER "'||LOWER(TRIM(USERID))||'" SET timezone to PST8PDT;' INTO qtemp4;
  EXECUTE qtemp4;
  --SELECT 'CREATE USER "'||LOWER(TRIM(USERID))||'" SUCCESS with PASSWORD '||replace (TRIM(VarPass),'\'','') INTO MSG;
  SELECT replace (TRIM(VarPass),'\'','')  INTO C3;   
  END IF; 
  
  SELECT VarUser  INTO C1;     
  SELECT VarEmail INTO C2;
  SELECT replace (TRIM(VarPass),'\'','')  INTO C3;
END
$_$

