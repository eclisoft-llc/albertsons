CREATE OR REPLACE PROCEDURE COMPACT.LOAD_DW_CIRRUS_CLAIMS_CRR()
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
       WHERE TARGET_TABLE_NAME = ''CIRRUS_CLAIMS_PROCESS'' ;
--START SP CALLS
CALL COMPACT.TB_CLM_CRR_CLM_HEAD_Load(:lastexec_ts);
CALL COMPACT.TB_CLM_CRR_CLM_SRVC_Load(:lastexec_ts);
CALL COMPACT.TB_CLM_CRR_CLM_ADJ_Load(:lastexec_ts);
CALL COMPACT.TB_DIM_FCT_CLM_TRANS_CRR_Load(:lastexec_ts);
--END SP CALLS
 UPDATE ETL.DBA_CDC_CONTROL
 SET SF_LAST_EXECUTED = :upr_ts
 WHERE TARGET_TABLE_NAME = ''CIRRUS_CLAIMS_PROCESS'' ;
 SELECT CURRENT_TIMESTAMP() INTO load_end_ts ;
 INSERT INTO ETL.DBA_PROCESS_LOG VALUES
 (:unique_id,''CIRRUS_CLAIMS_PROCESS'',''CIRRUS_CLAIMS_PROCESS'',''LOAD'',0,0,:load_strt_ts,:load_end_ts,
 CURRENT_TIMESTAMP,0,0,0) ;
 result:=''CIRRUS_CLAIMS_PROCESS refresh completed successfully'';
 return result;
 end;
 ';
