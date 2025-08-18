CREATE TABLE IF NOT EXISTS
    ETL.SMART_DIM_COA_L
    (
        COA_DIM_ID VARCHAR(50) NOT NULL CONSTRAINT PK_DIM_COA_DIM_ID PRIMARY KEY,
        COA VARCHAR(4),
        COA_DESC VARCHAR(200),
        DW_INSERT_DATE VARCHAR(35),
        DW_UPDATE_DATE VARCHAR(35)
    );
COMMENT ON TABLE ETL.SMART_DIM_COA_L
IS
    'This table stores attributes that describe Category of Aid like SSI, Working Disabled or Breast/Cervical Group.'
    ;
COMMENT ON COLUMN ETL.SMART_DIM_COA_L.COA_DIM_ID
IS
    'Represents the link to the SMART_DIM_COA_L table describing the Category of Aid. i.e. SSI, Working Disabled or Breast/Cervical Group.'
    ;
COMMENT ON COLUMN ETL.SMART_DIM_COA_L.COA
IS
    'Code that represents the Category of Aid. i.e. SSI, Working Disabled or Breast/Cervical Group.'
    ;
COMMENT ON COLUMN ETL.SMART_DIM_COA_L.COA_DESC
IS
    'Describes the Category of Aid. i.e. SSI, Working Disabled or Breast/Cervical Group.';
COMMENT ON COLUMN ETL.SMART_DIM_COA_L.DW_INSERT_DATE
IS
    'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN ETL.SMART_DIM_COA_L.DW_UPDATE_DATE
IS
    'Date the record was updated in the Data Warehouse.';
