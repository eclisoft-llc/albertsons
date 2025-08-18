CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_PLAN_QUALIFIER_CODE (
    PLAN_QUALIFIER_CODE_DIM_ID NUMBER NOT NULL,
    PLAN_QUALIFIER_CODE VARCHAR(10) NOT NULL,
    PLAN_QUALIFIER_CODE_DESC VARCHAR(50),
    DW_INSERT_DATETIME DATETIME NOT NULL,
    DW_UPDATE_DATETIME DATETIME NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PK_PLAN_QUALIFIER_CODE_DIM_ID PRIMARY KEY (PLAN_QUALIFIER_CODE_DIM_ID),
    CONSTRAINT UK_PLAN_QUALIFIER_CODE UNIQUE (PLAN_QUALIFIER_CODE)
);
COMMENT ON TABLE COMPACT.DIM_RX_PLAN_QUALIFIER_CODE IS 'This table stores attributes that describe the pharmacy prescription pricing plan or program that they qualify for.';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_QUALIFIER_CODE.PLAN_QUALIFIER_CODE_DIM_ID IS 'Represents the link to the DIM_RX_PLAN_QUALIFIER_CODE table describing the pharmacy prescription pricing plan or program that they qualify for. i.e. 1 = 1 open formulary or 8 = 8 6 tier pricing. ';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_QUALIFIER_CODE.PLAN_QUALIFIER_CODE IS 'Code that represents the pharmacy prescription pricing plan or program that they qualify for. i.e. 1 = open formulary or 8 = 6 tier pricing. ';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_QUALIFIER_CODE.PLAN_QUALIFIER_CODE_DESC IS 'Describes the pharmacy prescription pricing plan or program that they qualify for. i.e. open formulary or 6 tier pricing. ';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_QUALIFIER_CODE.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_QUALIFIER_CODE.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
