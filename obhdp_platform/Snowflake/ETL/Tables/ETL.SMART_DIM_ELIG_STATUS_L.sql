CREATE TABLE IF NOT EXISTS
    ETL.SMART_DIM_ELIG_STATUS_L
    (
        ELIG_STATUS_DIM_ID VARCHAR(50) NOT NULL CONSTRAINT PK_DIM_ELIG_STATUS_DIM_ID PRIMARY KEY,
        ELIG_STATUS VARCHAR(1) NOT NULL,
        ELIG_STATUS_DESC VARCHAR(20),
        DW_INSERT_DATETIME VARCHAR(35),
        DW_UPDATE_DATETIME VARCHAR(35)
    );
COMMENT ON TABLE ETL.SMART_DIM_ELIG_STATUS_L
IS
    'This table stores attributes that describe members eligibility status. ';
COMMENT ON COLUMN ETL.SMART_DIM_ELIG_STATUS_L.ELIG_STATUS_DIM_ID
IS
    'Represents the link to SMART_DIM_ELIG_STATUS_L table describing the members eligibility status. Values are 1 = ''Y'' Eligible, 2 = ''N'' Suspended, 3 = ''V'' Void, 4 = ''A'' Applied, 0 = ''U'' unknown and -1 = ''-'' N/A.'
    ;
COMMENT ON COLUMN ETL.SMART_DIM_ELIG_STATUS_L.ELIG_STATUS
IS
    'Code represents the members'' eligibility status. Values are ''Y'' Eligible, ''N'' Suspended, ''V'' Void, ''A'' Applied, ''U'' unknown and ''-'' N/A.'
    ;
COMMENT ON COLUMN ETL.SMART_DIM_ELIG_STATUS_L.ELIG_STATUS_DESC
IS
    'Describes the members'' eligibility status. Values are Eligible, Suspended, Void, Applied, Unknown and N/A.'
    ;
COMMENT ON COLUMN ETL.SMART_DIM_ELIG_STATUS_L.DW_INSERT_DATETIME
IS
    'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN ETL.SMART_DIM_ELIG_STATUS_L.DW_UPDATE_DATETIME
IS
    'Date the record was updated in the Data Warehouse.';
