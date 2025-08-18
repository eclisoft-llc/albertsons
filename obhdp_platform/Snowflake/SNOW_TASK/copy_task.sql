-----------------------------------------------------------------------
----- Create TASKS to COPY data from stage files to TABLES ------------
-----------------------------------------------------------------------

--- Set your context to use your role and assigned schema -------------
USE ROLE AZU_DWS_ECT_DEV_OBH_DP_ETL_SC_F_RL;
USE WAREHOUSE LOAD_WH;
USE DATABASE ECT_DEV_OBH_DP_DB;
USE SCHEMA ECT_DEV_OBH_DP_DB.ETL;
-----------------------------------------------------------------------
-------- Create TASK for BILL_TYPE Table ------------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_BILL_TYPE
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_BILL_TYPE table'
  AS
  COPY INTO ETL.DIM_BILL_TYPE
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_bill_type.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
--------- Create TASK for DIM_CLAIM_STATUS Table ----------------------
CREATE OR REPLACE TASK COPY_TO_DIM_CLAIM_STATUS
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_CLAIM_STATUS table'
  AS
  COPY INTO ETL.DIM_CLAIM_STATUS
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_claim_status.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_CLAIM_TYPE Table -----------------------
CREATE OR REPLACE TASK COPY_TO_DIM_CLAIM_TYPE
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_CLAIM_TYPE table'
  AS
  COPY INTO ETL.DIM_CLAIM_TYPE
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_claim_type.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_CLASS Table ----------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_CLASS
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_CLASS table'
  AS
  COPY INTO ETL.DIM_CLASS
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_class.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
--------- Create TASK for DIM_COS_STL_4 Table -------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_COS_STL_4
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_COS_STL_4 table'
  AS
  COPY INTO ETL.DIM_COS_STL_4
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_cos_stl_4.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_DIAGNOSIS Table ------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_DIAGNOSIS
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_DIAGNOSIS table'
  AS
  COPY INTO ETL.DIM_DIAGNOSIS
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_diagnosis.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_DRG_CODE Table -------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_DRG_CODE
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_DRG_CODE table'
  AS
  COPY INTO ETL.DIM_DRG_CODE
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_drg_code.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_GEOGRAPHY Table ------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_GEOGRAPHY
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_GEOGRAPHY table'
  AS
  COPY INTO ETL.DIM_GEOGRAPHY
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_geography.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_PLACE_OF_SVC Table ---------------------
CREATE OR REPLACE TASK COPY_TO_DIM_PLACE_OF_SVC
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_PLACE_OF_SVC table'
  AS
  COPY INTO ETL.DIM_PLACE_OF_SVC
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_place_of_svc.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_PLAN Table -----------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_PLAN
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_PLAN table'
  AS
  COPY INTO ETL.DIM_PLAN
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_plan.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_PROCEDURE_CODE Table -------------------
CREATE OR REPLACE TASK COPY_TO_DIM_PROCEDURE_CODE
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_PROCEDURE_CODE table'
  AS
  COPY INTO ETL.DIM_PROCEDURE_CODE
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_procedure_code.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_PROCEDURE_MODIFIER Table ---------------
CREATE OR REPLACE TASK COPY_TO_DIM_PROCEDURE_MODIFIER
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_PROCEDURE_MODIFIER table'
  AS
  COPY INTO ETL.DIM_PROCEDURE_MODIFIER
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_procedure_modifier.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_PROVIDER_PAR_STAT Table ----------------
CREATE OR REPLACE TASK COPY_TO_DIM_PROVIDER_PAR_STAT
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_PROVIDER_PAR_STAT table'
  AS
  COPY INTO ETL.DIM_PROVIDER_PAR_STAT
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_provider_par_stat.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_PROVIDER_TYPE Table --------------------
CREATE OR REPLACE TASK COPY_TO_DIM_PROVIDER_TYPE
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_PROVIDER_TYPE table'
  AS
  COPY INTO ETL.DIM_PROVIDER_TYPE
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_provider_type.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
-------- Create TASK for DIM_COMPANY Table ----------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_COMPANY
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_COMPANY table'
  AS
  COPY INTO ETL.DIM_COMPANY
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_company.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_VENDOR Table ---------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_VENDOR
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_VENDOR table'
  AS
  COPY INTO ETL.DIM_VENDOR
    FROM @DIM_VENDOR_STAGE
  PATTERN = '.*dim_vendor.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for FACT_CLAIM Table ---------------------------
CREATE OR REPLACE TASK COPY_TO_FACT_CLAIM
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to FACT_CLAIM table'
  AS
  COPY INTO FACT_CLAIM
    FROM @FACT_CLAIM_STAGE
  PATTERN = '.*FACT_CLAIM.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for COPY_TO_DIM_MEMBER Table -------------------
CREATE OR REPLACE TASK COPY_TO_DIM_MEMBER
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_MEMBER table'
  AS
  COPY INTO ETL.DIM_MEMBER
    FROM @MEMBER_HASH_STAGE
  PATTERN = '.*DIM_MEMBER.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
---------- Create TASK for DIM_PRODUCT Table --------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_PRODUCT
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_PRODUCT table'
  AS
  COPY INTO ETL.DIM_PRODUCT
    FROM @MY_CSV_STAGE
  PATTERN = '.*DIM_PRODUCT.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
 ---------- Create TASK for DIM_PROVIDER Table ------------------------
CREATE OR REPLACE TASK COPY_TO_DIM_PROVIDER
  WAREHOUSE = LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  USER_TASK_TIMEOUT_MS = 600000
  COMMENT = 'Copy to DIM_PROVIDER table'
  AS
  COPY INTO ETL.DIM_PROVIDER
    FROM @MY_CSV_STAGE
  PATTERN = '.*dim_provider.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
------- Activate/Resume all TASKS -------------------------------------
ALTER TASK COPY_TO_DIM_BILL_TYPE RESUME;
ALTER TASK COPY_TO_DIM_CLAIM_STATUS RESUME;
ALTER TASK COPY_TO_DIM_CLAIM_TYPE RESUME;
ALTER TASK COPY_TO_DIM_CLASS RESUME;
ALTER TASK COPY_TO_DIM_COS_STL_4 RESUME;
ALTER TASK COPY_TO_DIM_DIAGNOSIS RESUME;
ALTER TASK COPY_TO_DIM_DRG_CODE RESUME;
ALTER TASK COPY_TO_DIM_GEOGRAPHY RESUME;
ALTER TASK COPY_TO_DIM_PLACE_OF_SVC RESUME;
ALTER TASK COPY_TO_DIM_PLAN RESUME;
ALTER TASK COPY_TO_DIM_PROCEDURE_CODE RESUME;
ALTER TASK COPY_TO_DIM_PROCEDURE_MODIFIER RESUME;
ALTER TASK COPY_TO_DIM_PROVIDER_PAR_STAT RESUME;
ALTER TASK COPY_TO_DIM_PROVIDER_TYPE RESUME;
ALTER TASK COPY_TO_DIM_COMPANY RESUME;
ALTER TASK COPY_TO_DIM_VENDOR RESUME;
ALTER TASK COPY_TO_FACT_CLAIM RESUME;
ALTER TASK COPY_TO_DIM_MEMBER RESUME;
ALTER TASK COPY_TO_DIM_PRODUCT RESUME;
ALTER TASK COPY_TO_DIM_PROVIDER RESUME;
------------------------------------------------------------------------
------------------------------------------------------------------------
