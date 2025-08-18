CREATE OR REPLACE PROCEDURE ETL.DBA_MERGE_IMDM_INDV_CHNG()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

   var result = "";
   var db = "";
   var tableName = "";
   var lastExec = "";
   var trueFlag = "true";
   var SRCTABLE = "OCM_TABLES" ;
   var TRGTABLE = "ETL.IMDM_INDV_CHNG" ;
   var PROCESSD = "HIST" ;
   var src_count = "0000" ;
   
   try {
     var SQLcmd = "SELECT CURRENT_TIMESTAMP()";
     var stmtDate = snowflake.createStatement({ sqlText: SQLcmd } );
     var resultDate = stmtDate.execute();
     resultDate.next();
     var START_DATE = resultDate.getColumnValue(1);

     SQLcmd = "SELECT UUID_STRING()";
     var stmtUUID = snowflake.createStatement({ sqlText: SQLcmd } );
     var resultUUID = stmtUUID.execute();
     resultUUID.next();
     var unique_id = resultUUID.getColumnValue(1);

     SQLcmd = "SELECT dateadd(hour, -3, current_timestamp)";
     var stmtDate = snowflake.createStatement({ sqlText: SQLcmd } );
     var resultDate = stmtDate.execute();
     resultDate.next();
     var UPR_DT = resultDate.getColumnValue(1);

     SQLcmd = "SELECT * FROM ETL.DBA_CDC_CONTROL WHERE TARGET_TABLE_NAME = 'IMDM_INDV_CHNG' ;" ;
     var sql1 = snowflake.createStatement( {sqlText: SQLcmd } );
     var return_row = sql1.execute();
     return_row.next();
	 db = return_row.getColumnValue(1);
     tableName = return_row.getColumnValue(2);
     lastExec = return_row.getColumnValue(3);

     SQLcmd = "DELETE FROM ETL." + tableName ;
     sql1 = snowflake.createStatement( {sqlText: SQLcmd} );
     sql1.execute();

     SQLcmd = "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); ";
     sql1 = snowflake.createStatement({sqlText: SQLcmd});
     var resultCountd = sql1.execute();
     resultCountd.next();
     del_count = resultCountd.getColumnValue(1);

     SQLcmd = "INSERT INTO ETL." + tableName + "(INDV_SRC_ID, CDC_FLG)";
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID,'U' FROM " + db + ".FOUNDATION.DW_INDV "
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' AND DW_SRC_REC_STS_CD= :2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION ";
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'U' FROM " + db + ".FOUNDATION.DW_INDV_ELCTR_ADR " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'U' FROM " + db + ".FOUNDATION.DW_INDV_TEL_NBR " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'U' FROM " + db + ".FOUNDATION.DW_INDV_PST_ADR " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'U' FROM " + db + ".FOUNDATION.DW_INDV_KEY " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID,'D' FROM " + db + ".FOUNDATION.DW_INDV "
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' AND DW_SRC_REC_STS_CD <> :2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION ";
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'D' FROM " + db + ".FOUNDATION.DW_INDV_ELCTR_ADR " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD <>:2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'D' FROM " + db + ".FOUNDATION.DW_INDV_TEL_NBR " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD <>:2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'D' FROM " + db + ".FOUNDATION.DW_INDV_PST_ADR " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD <>:2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'D' FROM " + db + ".FOUNDATION.DW_INDV_KEY " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD <>:2 AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'D' FROM " + db + ".FOUNDATION.DW_INDV_IMDM_PTY_LNK " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND LNK_RSN_CD IN (1,4,2,5,3,6) AND INDV_ID=SRC_PTY_ID AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 UNION ";
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'D' FROM " + db + ".FOUNDATION.DW_INDV_IMDM_PTY_LNK " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND LNK_RSN_CD IN (1,4,2,5,3,6) AND INDV_ID=TGT_PTY_ID AND SYSTEMTIMESTAMP > :1 AND SYSTEMTIMESTAMP <= :3 ; " ;

     var sql2 = snowflake.createStatement( {sqlText: SQLcmd, binds: [lastExec,trueFlag,UPR_DT]} );
     var ins = sql2.execute();
		 
	 SQLcmd = "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); ";
     var sql3 = snowflake.createStatement({sqlText: SQLcmd});
     var resultCount2 = sql3.execute();
     resultCount2.next();
     result = resultCount2.getColumnValue(1);
     var trg_count = result;
     var ins_count = result;
     var upd_count = "0";

     SQLcmd = "UPDATE ETL.DBA_CDC_CONTROL SET SF_LAST_EXECUTED = :1 WHERE SOURCE_DATABASE_NAME = :2 AND TARGET_TABLE_NAME = :3 ;" ;
     sql1 = snowflake.createStatement( {sqlText: SQLcmd, binds: [UPR_DT,db,tableName]} );
     sql1.execute();

 	 SQLcmd = "SELECT CURRENT_TIMESTAMP()";
     stmtDate = snowflake.createStatement({ sqlText: SQLcmd } );
     resultDate = stmtDate.execute();
     resultDate.next();
     var END_DATE = resultDate.getColumnValue(1);

     SQLcmd = "INSERT INTO ETL.DBA_PROCESS_LOG VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12 ); "
     var sql4 = snowflake.createStatement({sqlText: SQLcmd,binds: [unique_id, SRCTABLE, TRGTABLE, PROCESSD, src_count,
                          trg_count, START_DATE, END_DATE, END_DATE, ins_count, upd_count, del_count]});
     var result2 = sql4.execute();

   }
   catch (err) {
     result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
     result += "\n  Message: " + err.message;
     result += "\nStack Trace:\n" + err.stackTraceTxt; 
	 throw result;
   }

   return result;


$$
;