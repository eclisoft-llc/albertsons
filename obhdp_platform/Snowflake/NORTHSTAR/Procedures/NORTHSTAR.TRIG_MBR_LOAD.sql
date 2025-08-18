CREATE OR REPLACE PROCEDURE NORTHSTAR.TRIG_MBR_LOAD(START_DATE VARCHAR,END_DATE VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try
    {
     SQLcmd = "select case when (select count(*) from ETL.DATA_LOAD_STATUS where ENTITY_KEY='FACT_CLAIM' and ETL_STS = 'ACTIVE')=0  THEN 'SUCCESS' ELSE 'FAIL' END AS RESULT FROM DUAL";
        var stmt = snowflake.createStatement({ sqlText: SQLcmd } );
        var result = stmt.execute();
        result.next();
        var msg = result.getColumnValue(1);

        if (msg=="SUCCESS")  {
        SQLcmd = "call NORTHSTAR.NS_MBR_LOAD(:1,:2);"
                   }
        else {
         SQLcmd = "SELECT 'FAIL' AS RESULT FROM DUAL";
        }
        var sql1 = snowflake.createStatement({sqlText: SQLcmd,binds: [START_DATE, END_DATE]});
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

//Description: This procedure will check if the COMPACT load is finished and then trigger NORTH STAR MEMBER SEG LOAD
$$
;