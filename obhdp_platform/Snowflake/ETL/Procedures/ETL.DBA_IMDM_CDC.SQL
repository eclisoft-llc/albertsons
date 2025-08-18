CREATE OR REPLACE PROCEDURE ETL.DBA_IMDM_CDC()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

   var ins_count = 0;
   var upd_count = 0;
   var result = "";
   var db = "";
   var tableName = "";
   var lastExec = "";
   var trueFlag = "true";
   var persDescr = "Preferred";
   var telDescr = "HOME TELEPHONE";
   var futureDate = "9999-12-31";
   
   try {

     var SQLcmd = "SELECT CURRENT_TIMESTAMP()";
     var stmtDate = snowflake.createStatement({ sqlText: SQLcmd } );
     var resultDate = stmtDate.execute();
     resultDate.next();
     var CURRENT_DT = resultDate.getColumnValue(1);

     SQLcmd = "SELECT SUBSTRING(DATABASE_NAME,5,3) ENVIRONMENT FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME LIKE '%_OBH_DP_DB';"
     var getEnv = snowflake.createStatement({sqlText: SQLcmd});
     var resultEnv = getEnv.execute();
     resultEnv.next();

     var Env = resultEnv.getColumnValue(1);

     var dbName = "ECT_" + Env + "_INDIVIDUAL_DB";

     SQLcmd = "SELECT * FROM ETL.DBA_CDC_CONTROL WHERE SOURCE_DATABASE_NAME = :1 ;" ;
     var sql1 = snowflake.createStatement( {sqlText: SQLcmd, binds: [dbName]} );
     var return_row = sql1.execute();
     return_row.next();

     db = return_row.getColumnValue(1);
     tableName = return_row.getColumnValue(2);
     lastExec = return_row.getColumnValue(3);

     SQLcmd = "DELETE FROM ETL." + tableName ;
     sql1 = snowflake.createStatement( {sqlText: SQLcmd} );
     sql1.execute();

     SQLcmd = "INSERT INTO ETL." + tableName + "(INDV_ID, CDC_FLG)";
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID,'U' FROM " + db + ".FOUNDATION.DW_INDV "
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' AND DW_SRC_REC_STS_CD= :2 AND (SYSTEMTIMESTAMP)>= :1 UNION ";
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'U' FROM " + db + ".FOUNDATION.DW_INDV_ELCTR_ADR " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND SYSTEMTIMESTAMP >= :1 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'U' FROM " + db + ".FOUNDATION.DW_INDV_TEL_NBR " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND SYSTEMTIMESTAMP >= :1 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'U' FROM " + db + ".FOUNDATION.DW_INDV_PST_ADR " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND SYSTEMTIMESTAMP >= :1 UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'U' FROM " + db + ".FOUNDATION.DW_INDV_KEY " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND SYSTEMTIMESTAMP >= :1) UNION " ;
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'D' FROM " + db + ".FOUNDATION.DW_INDV_IMDM_PTY_LNK " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND LNK_RSN_CD IN (1,4,2,5,3,6) AND INDV_ID=SRC_PTY_ID AND SYSTEMTIMESTAMP >= :1 UNION ";
     SQLcmd = SQLcmd + "SELECT DISTINCT INDV_ID, 'D' FROM " + db + ".FOUNDATION.DW_INDV_IMDM_PTY_LNK " ;
     SQLcmd = SQLcmd + "WHERE DW_SYS_REF_CD='IMDM' and DW_SRC_REC_STS_CD =:2 AND LNK_RSN_CD IN (1,4,2,5,3,6) AND INDV_ID=TGT_PTY_ID AND SYSTEMTIMESTAMP >= :1 " ;

     var sql2 = snowflake.createStatement( {sqlText: SQLcmd, binds: [lastExec,trueFlag,persDescr,telDescr,futureDate]} );
     var ins = sql2.execute();

     SQLcmd = "UPDATE ETL.DBA_CDC_CONTROL SET SF_LAST_EXECUTED = :1 WHERE SOURCE_DATABASE_NAME = :2 AND TARGET_TABLE_NAME = :3 ;" ;
     sql1 = snowflake.createStatement( {sqlText: SQLcmd, binds: [CURRENT_DT,db,tableName]} );
     sql1.execute();
   }
   catch (err) {
     result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
     result += "\n  Message: " + err.message;
     result += "\nStack Trace:\n" + err.stackTraceTxt; 
   }

   return result;
   
//Author: Berenice Balboa
//Created: 01-21-2022
//Updated: 05-26-2022; Padmaja Korrapati
//Description: Captures the data changes from IMDM
//Execution: CALL ETL.DBA_IMDM_CDC();
//Changes:
//
$$
;
