CREATE OR REPLACE PROCEDURE ETL.DBA_MERGE_IMDM_INDV_KEY(SRCTABLE VARCHAR, TRGTABLE VARCHAR, PROCESSD VARCHAR)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

   if (SRCTABLE == "COMPACT.ETL_BKT_MBR_SF" && TRGTABLE == "COMPACT.IMDM_EDW_INDV_KEY"){
   var ins_count = 0;
   var upd_count = 0;
   var del_count = 0;
   var result = "";
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

   SQLcmd = "SELECT COUNT(*) FROM " + SRCTABLE;
   var sql1 = snowflake.createStatement({sqlText: SQLcmd});
   var resultCount = sql1.execute();
   resultCount.next();
   var src_count = resultCount.getColumnValue(1);

   SQLcmd = "SELECT SUBSTRING(DATABASE_NAME,5,3) ENVIRONMENT FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME LIKE '%_OBH_DP_DB';"
   var getEnv = snowflake.createStatement({sqlText: SQLcmd});
   var resultEnv = getEnv.execute();
   resultEnv.next();
   var Env = resultEnv.getColumnValue(1);

   try {
     if (src_count > 0) {

        // execute the delete
        SQLcmd = "INSERT INTO COMPACT.IMDM_EDW_INDV_KEY(INDV_KEY_VAL,INDV_KEY_TYP_REF_DESC,CREAT_DTTM,CHG_DTTM,CREAT_SYS_REF_CD,CHG_SYS_REF_CD,DATA_SECUR_RULE_LIST,IMDM_SURRG_KEY,INDV_SRC_ID,DW_SYS_REF_CD,DW_CREAT_DTTM,DW_CHG_DTTM)"
        SQLcmd += "SELECT INDV_KEY_VAL,INDV_KEY_TYP_REF_DESC,CREAT_DTTM ,CHG_DTTM,CREAT_SYS_REF_CD,CHG_SYS_REF_CD,DATA_SECUR_RULE_LIST,IMDM_SURRG_KEY,INDV_SRC_ID,DW_SYS_REF_CD,DW_CREAT_DTTM,DW_CHG_DTTM FROM ECT_"
        SQLcmd += Env
        SQLcmd += "_INDIVIDUAL_DB.FOUNDATION.DW_INDV_KEY WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD ='true';";
                  
        var sqld = snowflake.createStatement({sqlText: SQLcmd});
        var resultStmt = sqld.execute();

        SQLcmd = "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); ";
        sqld = snowflake.createStatement({sqlText: SQLcmd});
        var resultCountd = sqld.execute();
        resultCountd.next();
        del_count = resultCountd.getColumnValue(1);

        SQLcmd = "SELECT STM_HIST_MERGE FROM ETL.DBA_MERGE_META WHERE SOURCE_SCHEMA_TABLE_NAME = :1 AND TARGET_SCHEMA_TABLE_NAME = :2;";
        var sql1 = snowflake.createStatement({sqlText: SQLcmd, binds: [SRCTABLE, TRGTABLE]});
        var resultStmt = sql1.execute();
        resultStmt.next();
        SQLcmd = resultStmt.getColumnValue(1);
        SQLcmd.replace("{env}", Env);

        var sql2 = snowflake.createStatement({sqlText: SQLcmd, binds: [START_DATE, START_DATE, unique_id]});
        var result1 = sql2.execute();

        SQLcmd = "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); ";
        var sql3 = snowflake.createStatement({sqlText: SQLcmd});
        var resultCount2 = sql3.execute();
        resultCount2.next();
        ins_count = resultCount2.getColumnValue(1);
        upd_count = 0;
     }

     SQLcmd = "SELECT COUNT(*) FROM " + TRGTABLE;
     var sql3 = snowflake.createStatement({sqlText: SQLcmd});
     var resultCount2 = sql3.execute();
     resultCount2.next();
     var trg_count = resultCount2.getColumnValue(1);

     SQLcmd = "SELECT CURRENT_TIMESTAMP()";
     stmtDate = snowflake.createStatement({ sqlText: SQLcmd } );
     resultDate = stmtDate.execute();
     resultDate.next();
     var END_DATE = resultDate.getColumnValue(1);

     SQLcmd = "INSERT INTO ETL.DBA_PROCESS_LOG VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12 ); "
     var sql4 = snowflake.createStatement({
             sqlText: SQLcmd,
             binds: [unique_id, SRCTABLE, TRGTABLE, PROCESSD, src_count,
                     trg_count, START_DATE, END_DATE, END_DATE, ins_count, upd_count, del_count]
           });
     var result2 = sql4.execute();

     result=trg_count;
   }
catch (err) {
   result =  "Failed: Code: " + err.code + " | State: " + err.state;
   result += "\\\\n  Message: " + err.message;
   result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
   }
}
else {
   result = "Please use for COMPACT.IMDM_EDW_INDV_KEY table only, other tables are not supported"
}
return result;

//Author: Berenice Balboa
//Created: 09-13-2021
//Description: Merge the data stored in the ETL schema into the COMPACT schema, COMPACT.IMDM_EDW_INDV_KEY
//Execution: CALL ETL.DBA_MERGE_IMDM_INDV_KEY(SRCTABLE VARCHAR, TRGTABLE VARCHAR, 'Historical Load');
//
//Changes: Padmaja Korrapati
//Updated: 05-25-2022
//Description: Updated the insert statement
$$
;
