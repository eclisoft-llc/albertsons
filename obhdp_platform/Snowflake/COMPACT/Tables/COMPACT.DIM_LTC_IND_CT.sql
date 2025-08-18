
CREATE TABLE IF NOT EXISTS
    COMPACT.DIM_LTC_IND
    (
        LTC_IND_DIM_ID NUMBER NOT NULL CONSTRAINT PK_F_DIM_LTC_IND_DIM_ID PRIMARY KEY,
        LTC_IND VARCHAR(1) NOT NULL,
        LTC_IND_DESC VARCHAR(50),
        DW_INSERT_DATETIME DATETIME,
        DW_UPDATE_DATETIME DATETIME,
        SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_PROCESS_ID VARCHAR(36)
    );
COMMENT ON TABLE COMPACT.DIM_LTC_IND
IS
    'This table stores attributes that describe if the member is in Long Term Care services.';
COMMENT ON COLUMN COMPACT.DIM_LTC_IND.LTC_IND_DIM_ID
IS
    'Represents the link to DIM_LTC_IND indicating if the member is in Long Term Care services. Values are 1 = ''L'' - Long Term Care Yes, 2 = ''N'' Long Term Care NO, 0 = ''U'' Unknown, -1 = Not Available, 141 = F, 21 = ''B'', 22 = ''O'', 41 = ''M'', 61 = ''G'', 81 = ''6'', 101 = ''A'' and 121 = ''J''.'
    ;
COMMENT ON COLUMN COMPACT.DIM_LTC_IND.LTC_IND
IS
    'Code that represents if the member is in Long Term Care services. Values are ''L'' - Long Term Care Yes, ''N'' Long Term Care NO, ''U'' Unknown, Not Available, ''F'', ''B'', ''O'', ''M'', ''G'', ''6'', ''A'' and ''J''.'
    ;
COMMENT ON COLUMN COMPACT.DIM_LTC_IND.LTC_IND_DESC
IS
    'Describes if the member is in Long Term Care services. Values are Long Term Care Yes, Long Term Care NO, Unknown, Not Available, ''F'', ''B'', ''O'', ''M'', ''G'', ''6'', ''A'' and ''J''.'
    ;
COMMENT ON COLUMN COMPACT.DIM_LTC_IND.DW_INSERT_DATETIME
IS
    'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_LTC_IND.DW_UPDATE_DATETIME
IS
    'Date the record was updated in the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_LTC_IND.SF_INSERT_DATETIME
IS
    'Date the record was added to the OBHDP.';
COMMENT ON COLUMN COMPACT.DIM_LTC_IND.SF_UPDATE_DATETIME
IS
    'Date the record was updated in the OBHDP.';

COMMENT ON COLUMN COMPACT.DIM_LTC_IND.SF_PROCESS_ID
IS
    'Unique identifier assigned to the process that inserts/updates the data in OBHDP.';
