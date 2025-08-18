CREATE TABLE IF NOT EXISTS
    COMPACT.DIM_ELIG_STATUS
    (
        ELIG_STATUS_DIM_ID INTEGER NOT NULL CONSTRAINT PK_F_DIM_ELIG_STATUS_DIM_ID PRIMARY KEY,
        ELIG_STATUS VARCHAR(1) NOT NULL,
        ELIG_STATUS_DESC VARCHAR(20),
        DW_INSERT_DATETIME DATETIME,
        DW_UPDATE_DATETIME DATETIME,
        SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_PROCESS_ID VARCHAR(36)
    );
COMMENT ON TABLE COMPACT.DIM_ELIG_STATUS
IS
    'This table stores attributes that describe members eligibility status. ';
COMMENT ON COLUMN COMPACT.DIM_ELIG_STATUS.ELIG_STATUS_DIM_ID
IS
    'Represents the link to DIM_ELIG_STATUS table describing the members eligibility status. Values are 1 = ''Y'' Eligible, 2 = ''N'' Suspended, 3 = ''V'' Void, 4 = ''A'' Applied, 0 = ''U'' unknown and -1 = ''-'' N/A.'
    ;
COMMENT ON COLUMN COMPACT.DIM_ELIG_STATUS.ELIG_STATUS
IS
    'Code represents the members'' eligibility status. Values are ''Y'' Eligible, ''N'' Suspended, ''V'' Void, ''A'' Applied, ''U'' unknown and ''-'' N/A.'
    ;
COMMENT ON COLUMN COMPACT.DIM_ELIG_STATUS.ELIG_STATUS_DESC
IS
    'Describes the members'' eligibility status. Values are Eligible, Suspended, Void, Applied, Unknown and N/A.'
    ;
COMMENT ON COLUMN COMPACT.DIM_ELIG_STATUS.DW_INSERT_DATETIME
IS
    'Date the record was added to the SMART Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_ELIG_STATUS.DW_UPDATE_DATETIME
IS
    'Date the record was updated in the SMART Data Warehouse.';

COMMENT ON COLUMN COMPACT.DIM_ELIG_STATUS.SF_INSERT_DATETIME
IS
    'Date the record was added to the OBHDP.';
COMMENT ON COLUMN COMPACT.DIM_ELIG_STATUS.SF_UPDATE_DATETIME
IS
    'Date the record was updated in the OBHDP.';

COMMENT ON COLUMN COMPACT.DIM_ELIG_STATUS.SF_PROCESS_ID
IS
    'Unique identifier assigned to the process that inserts/updates the data in OBHDP.';

