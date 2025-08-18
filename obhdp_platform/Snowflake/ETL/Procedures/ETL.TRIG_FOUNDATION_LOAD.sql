CREATE OR REPLACE PROCEDURE ETL.TRIG_FOUNDATION_LOAD(SRCTABLE VARCHAR , TRGTABLE VARCHAR , LOADTYPE VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try
    {
        SQLcmd = "SELECT CASE WHEN (SELECT IFNULL((SELECT LOAD_END FROM (SELECT TABLE_TARGET,LOAD_END,row_number() OVER (PARTITION BY TABLE_TARGET order by LOAD_END DESC) R_NUM FROM ETL.DBA_PROCESS_LOG where TABLE_TARGET = 'COMPACT_LOAD') WHERE R_NUM = 1), '2000-01-01 00:00:00.000') LOAD_END) > (SELECT IFNULL((SELECT LOAD_END FROM (SELECT TABLE_TARGET,LOAD_END,row_number() OVER (PARTITION BY TABLE_TARGET order by LOAD_END DESC) R_NUM FROM ETL.DBA_PROCESS_LOG where TABLE_TARGET = '" +TRGTABLE+ "') WHERE R_NUM = 1), '2000-01-01 00:00:00.000') LOAD_END) THEN 'SUCCESS' ELSE 'FAIL' END AS RESULT FROM DUAL";
        var stmt = snowflake.createStatement({ sqlText: SQLcmd } );
        var result = stmt.execute();
        result.next();
        var msg = result.getColumnValue(1);
   
        if (msg=="SUCCESS")  {
        SQLcmd = "CALL ETL.DBA_MERGE('" +SRCTABLE+ "','" +TRGTABLE+ "','" +LOADTYPE+ "')";
        }
        else {
         SQLcmd = "SELECT 'FAIL' AS RESULT FROM DUAL";
        }
        var sql1 = snowflake.createStatement({sqlText: SQLcmd});
        var resultCount = sql1.execute();
        resultCount.next();
        var src_count = resultCount.getColumnValue(1);
        return src_count;
    }
    catch (err) {
        result =  "Failed: Code: " + err.code + ", State: " + err.state;
        result += "\n Message: " + err.message;
        result += "\n Stack Trace:\n" + err.stackTraceTxt;
        throw result;
       }

//Description: This procedure will check if the COMPACT load is finished and then trigger corresponding FOUNDATION table merge
//Execution: CALL ETL.TRIG_FOUNDATION_LOAD(SRCTABLE VARCHAR , TRGTABLE VARCHAR , LOADTYPE VARCHAR); should pass the schema name also
$$
;

