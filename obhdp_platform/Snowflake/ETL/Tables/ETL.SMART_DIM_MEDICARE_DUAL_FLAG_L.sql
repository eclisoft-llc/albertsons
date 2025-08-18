CREATE TABLE IF NOT EXISTS
    ETL.SMART_DIM_MEDICARE_DUAL_FLAG_L
    (
        MEDICARE_DUAL_FLAG_DIM_ID VARCHAR(50) NOT NULL CONSTRAINT PK_DIM_MEDICARE_DUAL_FLAG_DIM_ID PRIMARY KEY,
        MEDICARE_DUAL_FLAG VARCHAR(20),
        MEDICARE_DUAL_FLAG_DESC VARCHAR(25),
        DW_INSERT_DATETIME VARCHAR(35),
        DW_UPDATE_DATETIME VARCHAR(35)
    );
COMMENT ON TABLE ETL.SMART_DIM_MEDICARE_DUAL_FLAG_L
IS
    'This table stores attributes that describe Medicare Dual eligible program types. ';
COMMENT ON COLUMN ETL.SMART_DIM_MEDICARE_DUAL_FLAG_L.MEDICARE_DUAL_FLAG_DIM_ID
IS
    'Represents the link to SMART_DIM_MEDICARE_DUAL_FLAG_L table describing if the program types are Medicare Dual eligible. Values are 1 = Non-Dual, 2 = Dual, 0 = Unknown and -1 = N/A.'
    ;
COMMENT ON COLUMN ETL.SMART_DIM_MEDICARE_DUAL_FLAG_L.MEDICARE_DUAL_FLAG
IS
    'Code that represents the Medicare Dual eligible status values are Non-Dual, Dual, UNK and N/A.'
    ;
COMMENT ON COLUMN ETL.SMART_DIM_MEDICARE_DUAL_FLAG_L.MEDICARE_DUAL_FLAG_DESC
IS
    'Describes the Medicare Dual eligible status values are Non Dual, Dual, UNKNOWN and N/A.';
COMMENT ON COLUMN ETL.SMART_DIM_MEDICARE_DUAL_FLAG_L.DW_INSERT_DATETIME
IS
    'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN ETL.SMART_DIM_MEDICARE_DUAL_FLAG_L.DW_UPDATE_DATETIME
IS
    'Date the record was updated in the Data Warehouse.';
