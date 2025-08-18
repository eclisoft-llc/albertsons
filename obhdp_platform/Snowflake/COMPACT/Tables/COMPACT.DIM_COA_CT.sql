CREATE TABLE IF NOT EXISTS
    COMPACT.DIM_COA
    (
        COA_DIM_ID INTEGER NOT NULL CONSTRAINT PK_F_DIM_COA_DIM_ID PRIMARY KEY,
        COA VARCHAR(4),
        COA_DESC VARCHAR(200),
        DW_INSERT_DATE DATETIME,
        DW_UPDATE_DATE DATETIME,
        SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_PROCESS_ID VARCHAR(36)
    );
COMMENT ON TABLE COMPACT.DIM_COA
IS
    'This table stores attributes that describe Category of Aid like SSI, Working Disabled or Breast/Cervical Group.'
    ;
COMMENT ON COLUMN COMPACT.DIM_COA.COA_DIM_ID
IS
    'Represents the link to the DIM_COA table describing the Category of Aid. i.e. SSI, Working Disabled or Breast/Cervical Group.'
    ;
COMMENT ON COLUMN COMPACT.DIM_COA.COA
IS
    'Code that represents the Category of Aid. i.e. SSI, Working Disabled or Breast/Cervical Group.'
    ;
COMMENT ON COLUMN COMPACT.DIM_COA.COA_DESC
IS
    'Describes the Category of Aid. i.e. SSI, Working Disabled or Breast/Cervical Group.';
COMMENT ON COLUMN COMPACT.DIM_COA.DW_INSERT_DATE
IS
    'Date the record was added to the SMART Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_COA.DW_UPDATE_DATE
IS
    'Date the record was updated in the SMART Data Warehouse.';

COMMENT ON COLUMN COMPACT.DIM_COA.SF_INSERT_DATETIME
IS
    'Date the record was added to the OBHDP.';
COMMENT ON COLUMN COMPACT.DIM_COA.SF_UPDATE_DATETIME
IS
    'Date the record was updated in the OBHDP.';

COMMENT ON COLUMN COMPACT.DIM_COA.SF_PROCESS_ID
IS
    'Unique identifier assigned to the process that inserts/updates the data in OBHDP.';
