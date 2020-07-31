CREATE OR REPLACE PROCEDURE selfserve.resetpassword(userid character varying(256), OUT msg character varying(256))
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
                                                                                                                             
 BEGIN                                                                                                                       
        SELECT  varstart || Substring(varsmall,cast (random()*25  as int)+1,1)                                                    
          || Substring(varbig,cast (random()*25  as int)+1,1)                                                                  
          || Substring(varspl,cast (random()*9  as int)+1,1)                                                                   
          || Substring(varsmall,cast (random()*25  as int)+1,1)                                                                
          || Substring(varbig,cast (random()*25  as int)+1,1)                                                                  
          || Substring(varbig,cast (random()*25  as int)+1,1)                                                                  
          || Substring(varnumber,cast (random()*9  as int)+1,1)                                                               
          || Substring(varsmall,cast (random()*25  as int)+1,1)                                                                
          || Substring(varbig,cast (random()*25  as int)+1,1)                                                                  
          || Substring(varnumber,cast (random()*9  as int)+1,1)                                                               
          || varend                                                                                                          
          into VarPass;                                                                                                  
                                                                                                                             
   SELECT 'ALTER USER "'||LOWER(TRIM(USERID))||'"PASSWORD '|| TRIM(VarPass)||';' INTO qtemp;                                 
   EXECUTE qtemp;                                                                                                            
   SELECT 'PASSWORD RESET FOR "'||LOWER(TRIM(USERID))||'" SUCCESS with PASSWORD '||replace (TRIM(VarPass),'\'','') INTO MSG; 
 END                                                                                                                       
$_$
