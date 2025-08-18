CREATE OR REPLACE PROCEDURE ECT_PRD_OBH_DP_DB.COMPACT.LOAD_EDW_OBHDP_CLAIMS_COS()
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
       WHERE TARGET_TABLE_NAME = ''COS_CLAIMS_PROCESS'' ;

--START SP CALLS
CALL COMPACT.CLM_TRANS_SRVC_LN_ALL_COS_LOAD(:lastexec_ts,:upr_ts);
--END SP CALLS


 UPDATE ETL.DBA_CDC_CONTROL
 SET SF_LAST_EXECUTED = :upr_ts
 WHERE TARGET_TABLE_NAME = ''COS_CLAIMS_PROCESS'' ;

 SELECT CURRENT_TIMESTAMP() INTO load_end_ts ;
   

 INSERT INTO ETL.DBA_PROCESS_LOG VALUES
 (:unique_id,''COS_CLAIMS_PROCESS'',''COS_CLAIMS_PROCESS'',''LOAD'',0,0,:load_strt_ts,:load_end_ts,
 CURRENT_TIMESTAMP,0,0,0) ;

 result:=''COS_CLAIMS_PROCESS refresh completed successfully'';
 return result;
 end;

 ';