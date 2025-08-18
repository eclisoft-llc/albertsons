CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_COST_DETERMIN_BASIC (
    COST_DETERMIN_BASIC_DIM_ID NUMBER NOT NULL,
    COST_DETERMIN_BASIC VARCHAR(3) NOT NULL,
    COST_DETERMIN_BASIC_DESC VARCHAR(60),
    DW_INSERT_DATETIME DATETIME NOT NULL,
    DW_UPDATE_DATETIME DATETIME NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PK_COST_DETERMIN_BASIC_DIM_ID PRIMARY KEY (COST_DETERMIN_BASIC_DIM_ID),
    CONSTRAINT UK_COST_DETERMIN_BASIC UNIQUE (COST_DETERMIN_BASIC)
);
COMMENT ON TABLE COMPACT.DIM_RX_COST_DETERMIN_BASIC IS 'This table stores attributes that describe the basic cost determinate for the pharmacy prescription. i.e. 4 = 04 usual and customary paid as submitted or 7 = 07 mac pricing ingredient cost reduced to mac pricing.';
COMMENT ON COLUMN COMPACT.DIM_RX_COST_DETERMIN_BASIC.COST_DETERMIN_BASIC_DIM_ID IS 'Represents the link to the DIM_RX_COST_DETERMIN_BASIC table that describes the basic cost determinate for the pharmacy prescription. i.e. 4 = 04 usual and customary paid as submitted or 7 = 07 mac pricing ingredient cost reduced to mac pricing.';
COMMENT ON COLUMN COMPACT.DIM_RX_COST_DETERMIN_BASIC.COST_DETERMIN_BASIC IS 'Code that represents the basic cost determinate for the pharmacy prescription. i.e. 04 = usual and customary paid as submitted or 07 = mac pricing ingredient cost reduced to mac pricing.';
COMMENT ON COLUMN COMPACT.DIM_RX_COST_DETERMIN_BASIC.COST_DETERMIN_BASIC_DESC IS 'Describes the basic cost determinate for the pharmacy prescription. i.e. usual and customary paid as submitted or mac pricing ingredient cost reduced to mac pricing.';
COMMENT ON COLUMN COMPACT.DIM_RX_COST_DETERMIN_BASIC.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_COST_DETERMIN_BASIC.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
