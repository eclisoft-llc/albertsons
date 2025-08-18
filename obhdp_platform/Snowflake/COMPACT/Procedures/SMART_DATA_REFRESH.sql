CREATE OR REPLACE PROCEDURE COMPACT.SMART_DATA_REFRESH()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
      declare result varchar;
      load_strt_ts timestamp;
      load_end_ts timestamp;
      lastexec_ts timestamp;
      upr_ts timestamp;
      ins_cnt number;
      updt_cnt number;
      unique_id varchar;
      begin
       SELECT UUID_STRING() into unique_id;
       SELECT CURRENT_TIMESTAMP() INTO load_strt_ts ;
       SELECT dateadd(hour, -1, current_timestamp) INTO upr_ts ;
       SELECT sf_last_executed INTO lastexec_ts FROM ETL.DBA_CDC_CONTROL
       WHERE TARGET_TABLE_NAME = ''SMART_DATA_REFRESH'' ;
--START SP CALLS
CALL COMPACT.SMART_TABLES_CLONES();
CALL COMPACT.CLM_TRANS_SRVC_LN_ALL_SMART_LOAD(:lastexec_ts,:upr_ts);
--END SP CALLS

 UPDATE ETL.DBA_CDC_CONTROL
 SET SF_LAST_EXECUTED = :upr_ts
 WHERE TARGET_TABLE_NAME = ''SMART_DATA_REFRESH'' ;
 
 result:=''SMART_DATA_REFRESH refresh completed successfully'';
 return result;
 end;
 ';