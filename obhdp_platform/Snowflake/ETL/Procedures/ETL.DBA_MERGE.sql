CREATE OR REPLACE PROCEDURE ETL.DBA_MERGE(SRCTABLE VARCHAR, TRGTABLE VARCHAR, PROCESSD VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

    var ins_count = 0;
    var upd_count = 0;
    var result = "";
    var SRC_SCHEMA = "";
    var TRG_SCHEMA = "";
    var SRC_TB = "";
    var TRG_TB = "";

   var SQLcmd = "select convert_timezone('America/Chicago', current_timestamp())";
   var stmtDate = snowflake.createStatement({ sqlText: SQLcmd } );
   var resultDate = stmtDate.execute();
   resultDate.next();
   var START_DATE = resultDate.getColumnValue(1);

   SQLcmd = "SELECT UUID_STRING()";
   var stmtUUID = snowflake.createStatement({ sqlText: SQLcmd } );
   var resultUUID = stmtUUID.execute();
   resultUUID.next();
   var unique_id = resultUUID.getColumnValue(1);

   var ind = PROCESSD;
   var tbl = TRGTABLE;
   schm = tbl.split('.',1);
   if (ind=='Historical Load' && (schm=='FOUNDATION' || schm=='OBH_DP')){
   SQLc = "DELETE FROM " +TRGTABLE;
   var s1 = snowflake.createStatement({sqlText: SQLc});
   var rslt = s1.execute();
   }

   if (TRGTABLE=="FOUNDATION.CLM_TRANS_SRVC_LN")  {
      SQLcmd = "SELECT COUNT(*) FROM COMPACT.FACT_CLAIM WHERE CLAIM_TYPE_DIM_ID NOT IN (3, 8)";
   } else if (TRGTABLE=="FOUNDATION.PHRM_DRG_CLM") {
      SQLcmd = "SELECT COUNT(*) FROM COMPACT.FACT_CLAIM ";
      SQLcmd = SQLcmd + "WHERE CLAIM_TYPE_DIM_ID IN (3,8) AND PRIMARY_SVC_DATE >= '2018-01-01' AND DETAIL_SVC_DATE >= '2019-01-01' ";
      SQLcmd = SQLcmd + "AND NOT (CONVERTED_FROM_DIM_ID = 102 AND CHECK_DATE = '9999-09-09')";
   } else {
      SQLcmd = "SELECT COUNT(*) FROM " + SRCTABLE;
   }

   var sql1 = snowflake.createStatement({sqlText: SQLcmd});
   var resultCount = sql1.execute();
   resultCount.next();
   var src_count = resultCount.getColumnValue(1);

   try {
     if (src_count > 0) {
      SQLcmd = "SELECT STM_HIST_MERGE, SUBSTRING(:1,1,POSITION('.',:1)-1), SUBSTRING(:2,1,POSITION('.',:2)-1), ";
      SQLcmd = SQLcmd + "SUBSTRING(:1,POSITION('.',:1)+1), SUBSTRING(:2,POSITION('.',:2)+1) ";
      SQLcmd = SQLcmd + "FROM ETL.DBA_MERGE_META WHERE SOURCE_SCHEMA_TABLE_NAME = :1 AND TARGET_SCHEMA_TABLE_NAME = :2;";

      sql1 = snowflake.createStatement({sqlText: SQLcmd, binds: [SRCTABLE, TRGTABLE]});
      var resultStmt = sql1.execute();
      resultStmt.next();

      SQLcmd = resultStmt.getColumnValue(1);
      SRC_SCHEMA = resultStmt.getColumnValue(2);
      TRG_SCHEMA = resultStmt.getColumnValue(3);
      SRC_TB = resultStmt.getColumnValue(4);
      TRG_TB = resultStmt.getColumnValue(5);

      var sql2 = snowflake.createStatement({sqlText: SQLcmd, binds: [START_DATE, START_DATE, unique_id]});
      var result1 = sql2.execute();

      SQLcmd = "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); ";
      var sql3 = snowflake.createStatement({sqlText: SQLcmd});
      var resultCount2 = sql3.execute();
      resultCount2.next();
      ins_count = resultCount2.getColumnValue(1);

      if (TRGTABLE == "FOUNDATION.CLM_TRANS_SRVC_LN")
	  {
        upd_count = 0;
      } else {
        upd_count = resultCount2.getColumnValue(2);
     }
   }

    SQLcmd = "SELECT COUNT(*) FROM " + TRGTABLE;
    sql3 = snowflake.createStatement({sqlText: SQLcmd});
    resultCount2 = sql3.execute();
    resultCount2.next();
    var trg_count = resultCount2.getColumnValue(1);

    SQLcmd = "select convert_timezone('America/Chicago', current_timestamp())";
    stmtDate = snowflake.createStatement({ sqlText: SQLcmd } );
    resultDate = stmtDate.execute();
    resultDate.next();
    var END_DATE = resultDate.getColumnValue(1);

    SQLcmd = "INSERT INTO ETL.DBA_PROCESS_LOG VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12 ); "
    var sql4 = snowflake.createStatement({
             sqlText: SQLcmd,
             binds: [unique_id, SRCTABLE, TRGTABLE, PROCESSD, src_count,
                     trg_count, START_DATE, END_DATE, END_DATE, ins_count, upd_count, 0]
           });
    var result2 = sql4.execute();

    SQLcmd = "INSERT INTO ETL.DBA_VALIDATION_COUNTS VALUES('THO', :1, :2, :3, :7, :8 ), ('THO', :4, :5, :6, :7, :8 ); "
    sql4 = snowflake.createStatement({
             sqlText: SQLcmd,
             binds: [SRC_SCHEMA,SRC_TB, src_count, TRG_SCHEMA, TRG_TB, trg_count, unique_id, END_DATE]
           });
    result2 = sql4.execute();

    result=trg_count;
    return result;
   }
   catch (err) {
    result =  "Failed: Code: " + err.code + ", State: " + err.state;
    result += "\n Message: " + err.message;
    result += "\n Stack Trace:\n" + err.stackTraceTxt;
    throw result;
   }

//Author: Berenice Balboa
//Created: 05-05-2021
//Description: Merge the data stored in the ETL schema into the FOUND schema, incremental merge
//Execution: CALL ETL.DBA_MERGE(SRCTABLE VARCHAR, TRGTABLE VARCHAR, 'Historical Load');
//Execution: CALL ETL.DBA_MERGE(SRCTABLE VARCHAR, TRGTABLE VARCHAR, 'Incremental Load');
//Changes:
// 05-06-2021 BBalboa - added logic to handle incremental load, additional to the historical load
//Changes
//US3670038 - Added try catch logic
// 2021-07-19 BBalboa - added logic for when we have inserts only (FACT_CLAIM_DELETED_LOG)
// 2021-08-23 BBalboa - added to the exception list the 2 DM common format tables
// 2021-10-29 BBalboa - added to the exception list the 1 DM unified common format table
// 2022-02-11 BBalboa - added logic to persist validation counts for source and target
$$
;