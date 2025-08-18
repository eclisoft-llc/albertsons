------------ Create FACT_CLAIM_RS table only missing column -----------
  CREATE TABLE
    ETL.FACT_CLAIM_RS
    (
        "ADJUD_DATE" VARCHAR(255),
        "CLAIM_NUMBER" VARCHAR(255),
        "PRODUCT_DIM_ID" VARCHAR(255),
        "DIAGNOSIS_1_DIM_ID" VARCHAR(255),
        "DIAGNOSIS_2_DIM_ID" VARCHAR(255),
        "DIAGNOSIS_3_DIM_ID" VARCHAR(255),
        "SEQ_CLAIM_ID" VARCHAR(255),
        "COMPANY_DIM_ID" VARCHAR(255),
        "LINE_NUMBER" VARCHAR(255),
        "SUB_LINE_CODE" VARCHAR(255)
      );
-----------------------------------------------------------------------
--------- Alter FACT_CLAIM table add missing column -------------------
ALTER TABLE ETL.FACT_CLAIM ADD COLUMN PRODUCT_DIM_ID VARCHAR(200)
ALTER TABLE ETL.FACT_CLAIM ADD COLUMN DIAGNOSIS_2_DIM_ID NUMBER(38,0)
ALTER TABLE ETL.FACT_CLAIM ADD COLUMN DIAGNOSIS_3_DIM_ID NUMBER(38,0);
-----------------------------------------------------------------------
------ Copy all records to FACT_CLAIM_RS ------------------------------
  COPY INTO TCOC.FACT_CLAIM_RS
   FROM @ETL.MY_CSV_STAGE
  PATTERN = '.*fact_claim_10.*'
  ON_ERROR = SKIP_FILE
  FILE_FORMAT = MYCSVFORMAT;
-----------------------------------------------------------------------
------ Clean all records ----------------------------------------------
  DELETE FROM TCOC.FACT_CLAIM_RS WHERE CLAIM_NUMBER = 'CLAIM_NUMBER'
-----------------------------------------------------------------------
------ Create a table for records with E39 ----------------------------
  CREATE or REPLACE TABLE TCOC.FACT_CLAIM_PART_38
  AS
  SELECT * from TCOC.FACT_CLAIM_RS where SEQ_CLAIM_ID like '%E3%';
  -- 2457 records
-----------------------------------------------------------------------
------ Create new table FACT_CLAIM_PART with transformation -----------
  CREATE or REPLACE TABLE TCOC.FACT_CLAIM_PART
  AS
  SELECT
      cast(ADJUD_DATE as date) as ADJUD_DATE,
      CLAIM_NUMBER,
      PRODUCT_DIM_ID as PRODUCT_DIM_ID_2,
      cast(DIAGNOSIS_1_DIM_ID as integer) as DIAGNOSIS_1_DIM_ID,
      cast(DIAGNOSIS_2_DIM_ID as integer) as DIAGNOSIS_2_DIM_ID_2,
      cast(DIAGNOSIS_3_DIM_ID as integer) as DIAGNOSIS_3_DIM_ID_2,
      cast(SEQ_CLAIM_ID as integer) as SEQ_CLAIM_ID,
      cast(COMPANY_DIM_ID as integer) as COMPANY_DIM_ID,
      cast(LINE_NUMBER as integer) as LINE_NUMBER,
      SUB_LINE_CODE
  FROM TCOC.FACT_CLAIM_RS;
  -- Moved 147,446,691
-----------------------------------------------------------------------
-------- Create FACT_TABLE_NEW with all columns -----------------------
  CREATE TABLE ETL.FACT_CLAIM_NEW
  AS
  SELECT FC.*, PRODUCT_DIM_ID, DIAGNOSIS_2_DIM_ID, DIAGNOSIS_3_DIM_ID
  FROM FACT_CLAIM FC
  LEFT OUTER JOIN FACT_CLAIM_PART FCP
  ON
    FC.SEQ_CLAIM_ID = FCP.SEQ_CLAIM_ID
    AND FC.COMPANY_DIM_ID = FCP.COMPANY_DIM_ID
    AND FC.LINE_NUMBER = FCP.LINE_NUMBER
    AND FC.SUB_LINE_CODE = FCP.SUB_LINE_CODE;
-- Created 128,241,071 records
-----------------------------------------------------------------------