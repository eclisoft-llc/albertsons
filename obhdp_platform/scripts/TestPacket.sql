
---------------------------------------------------------------------------
CREATE OR REPLACE TABLE ETL.DATA_EXTRACT_TEST (
  UNIQUE_ID	VARCHAR(50) UNIQUE,
  SOURCE_NM VARCHAR(50) NOT NULL,
  ENTITY_KEY VARCHAR(100) NOT NULL,
  ENTITY_VAL VARCHAR(100) NOT NULL,
  ENTITY_TYP VARCHAR(100) NOT NULL,
  INSERT_TS DATETIME DEFAULT CURRENT_TIMESTAMP,
  UPDATE_TS DATETIME NOT NULL,
  ACTIVE VARCHAR(1) NOT NULL DEFAULT 'N',
  ENTITY_DESC VARCHAR(50) NOT NULL,
  CONSTRAINT UK_ETL_DATA_EXT_CONFIGURATION UNIQUE (SOURCE_NM, ENTITY_KEY)
  );
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
INSERT into ETL.DATA_EXTRACT_TEST (UNIQUE_ID, SOURCE_NM, ENTITY_KEY, ENTITY_VAL, ENTITY_TYP, INSERT_TS, UPDATE_TS, ACTIVE, ENTITY_DESC) values ('1001', 'SMART', 'STAGE_DEST_PREFIX', '', 'constant', '2000-01-01T00:00:00.000Z', '2000-01-01T00:00:00.000Z', 'Y', '');
INSERT into ETL.DATA_EXTRACT_TEST (UNIQUE_ID, SOURCE_NM, ENTITY_KEY, ENTITY_VAL, ENTITY_TYP, INSERT_TS, UPDATE_TS, ACTIVE, ENTITY_DESC) values ('1002', 'SMART', 'NAS_STAGE_PATH', '/obhdp_nas/dataset/', 'constant', '2000-01-01T00:00:00.000Z', '2000-01-01T00:00:00.000Z', 'Y', '');
INSERT into ETL.DATA_EXTRACT_TEST (UNIQUE_ID, SOURCE_NM, ENTITY_KEY, ENTITY_VAL, ENTITY_TYP, INSERT_TS, UPDATE_TS, ACTIVE, ENTITY_DESC) values ('1003', 'SMART', 'PROCESSOR_COUNT', '16', 'constant', '2000-01-01T00:00:00.000Z', '2000-01-01T00:00:00.000Z', 'Y', '');
INSERT into ETL.DATA_EXTRACT_TEST (UNIQUE_ID, SOURCE_NM, ENTITY_KEY, ENTITY_VAL, ENTITY_TYP, INSERT_TS, UPDATE_TS, ACTIVE, ENTITY_DESC) values ('1004', 'SMART', 'FULL_UPLOAD', 'FALSE', 'constant', '2000-01-01T00:00:00.000Z', '2000-01-01T00:00:00.000Z', 'Y', '');
INSERT into ETL.DATA_EXTRACT_TEST (UNIQUE_ID, SOURCE_NM, ENTITY_KEY, ENTITY_VAL, ENTITY_TYP, INSERT_TS, UPDATE_TS, ACTIVE, ENTITY_DESC) values ('1005', 'SMART', 'EXT_TYP', 'INC', 'constant', '2000-01-01T00:00:00.000Z', '2000-01-01T00:00:00.000Z', 'Y', '');
INSERT into ETL.DATA_EXTRACT_TEST (UNIQUE_ID, SOURCE_NM, ENTITY_KEY, ENTITY_VAL, ENTITY_TYP, INSERT_TS, UPDATE_TS, ACTIVE, ENTITY_DESC) values ('1006', 'SMART', 'INTERMEDIATE_TARGET', 'NO_TARGET', 'constant', '2000-01-01T00:00:00.000Z', '2000-01-01T00:00:00.000Z', 'Y', '');
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
