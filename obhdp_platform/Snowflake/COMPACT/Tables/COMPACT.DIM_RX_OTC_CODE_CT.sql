CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_OTC_CODE (
    RX_OTC_CODE_DIM_ID NUMBER NOT NULL,
    RX_OTC_CODE VARCHAR(3) NOT NULL,
    RX_OTC_CODE_DESC VARCHAR(30),
    DW_INSERT_DATETIME DATETIME NOT NULL,
    DW_UPDATE_DATETIME DATETIME NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PK_RX_OTC_CODE_DIM_ID PRIMARY KEY (RX_OTC_CODE_DIM_ID),
    CONSTRAINT UK_RX_OTC_CODE UNIQUE (RX_OTC_CODE)
);
COMMENT ON TABLE COMPACT.DIM_RX_OTC_CODE IS 'This table stores attributes that describe the pharmacy over the counter medication code.';
COMMENT ON COLUMN COMPACT.DIM_RX_OTC_CODE.RX_OTC_CODE_DIM_ID IS 'Represents the link to the DIM_RX_OTC_CODE describing the pharmacy over the counter medication code. i.e. 1 = O OTC,single source or 4 = S RX, multi source.';
COMMENT ON COLUMN COMPACT.DIM_RX_OTC_CODE.RX_OTC_CODE IS 'Code that represents pharmacy over the counter medication code. i.e. O = OTC,single source or S = RX, multi source.';
COMMENT ON COLUMN COMPACT.DIM_RX_OTC_CODE.RX_OTC_CODE_DESC IS 'Describes pharmacy over the counter medication code. i.e. OTC,single source or RX, multi source.';
COMMENT ON COLUMN COMPACT.DIM_RX_OTC_CODE.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_OTC_CODE.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
