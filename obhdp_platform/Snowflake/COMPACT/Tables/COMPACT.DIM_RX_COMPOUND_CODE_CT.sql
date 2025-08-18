CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_COMPOUND_CODE (
    COMPOUND_CODE_DIM_ID NUMBER NOT NULL,
    COMPOUND_CODE VARCHAR(3) NOT NULL,
    COMPOUND_CODE_DESC VARCHAR(50),
    DW_INSERT_DATETIME DATETIME NOT NULL,
    DW_UPDATE_DATETIME DATETIME NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PK_COMPOUND_CODE_DIM_ID PRIMARY KEY (COMPOUND_CODE_DIM_ID),
    CONSTRAINT UK_COMPOUND_CODE UNIQUE (COMPOUND_CODE)
);
COMMENT ON TABLE COMPACT.DIM_RX_COMPOUND_CODE IS 'This table stores attributes that describe if the pharmacy makes and sells prescription medications. A compounding pharmacy can often concoct drug formulas that are specially tailored to patients. ';
COMMENT ON COLUMN COMPACT.DIM_RX_COMPOUND_CODE.COMPOUND_CODE_DIM_ID IS 'Represents the link to the DIM_RX_COMPOUND_CODE describing if the pharmacy makes and sells prescription medications. A compounding pharmacy can often concoct drug formulas that are specially tailored to patients. Values are 1 = 0 not specified, 2 = 1 not a compound, 3 = 2 compound, 0 = UNK UNKNOWN and -1 = N/A N/A. ';
COMMENT ON COLUMN COMPACT.DIM_RX_COMPOUND_CODE.COMPOUND_CODE IS 'Code that represents if the pharmacy makes and sells prescription medications. A compounding pharmacy can often concoct drug formulas that are specially tailored to patients. Values are 0 = not specified, 1 = not a compound, 2 = compound, UNK = UNKNOWN and N/A = N/A. ';
COMMENT ON COLUMN COMPACT.DIM_RX_COMPOUND_CODE.COMPOUND_CODE_DESC IS 'Describes if the pharmacy makes and sells prescription medications. A compounding pharmacy can often concoct drug formulas that are specially tailored to patients. Values are not specified, not a compound, compound, UNKNOWN and N/A. ';
COMMENT ON COLUMN COMPACT.DIM_RX_COMPOUND_CODE.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_COMPOUND_CODE.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
