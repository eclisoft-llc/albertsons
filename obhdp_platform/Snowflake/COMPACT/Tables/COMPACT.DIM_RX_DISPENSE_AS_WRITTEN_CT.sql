CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_DISPENSE_AS_WRITTEN (
    DISPENSE_AS_WRITTEN_DIM_ID NUMBER NOT NULL,
    DISPENSE_AS_WRITTEN VARCHAR2(3) NOT NULL,
    DISPENSE_AS_WRITTEN_DESC VARCHAR(65),
    DW_INSERT_DATETIME DATETIME NOT NULL,
    DW_UPDATE_DATETIME DATETIME NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PK_DISPENSE_AS_WRITTEN_DIM_ID PRIMARY KEY (DISPENSE_AS_WRITTEN_DIM_ID),
    CONSTRAINT UK_DISPENSE_AS_WRITTEN UNIQUE (DISPENSE_AS_WRITTEN)
);
COMMENT ON COLUMN COMPACT.DIM_RX_DISPENSE_AS_WRITTEN.DISPENSE_AS_WRITTEN_DIM_ID IS 'Represents the link to the DIM_RX_DISPENSE_AS_WRITTEN table describing if the pharmacy prescription was substituted or dispensed as written. i.e. 1 = 1 substitution not allowed by prescriber or 2 = 2 substitution allowed patient requested product dispensed.';
COMMENT ON COLUMN COMPACT.DIM_RX_DISPENSE_AS_WRITTEN.DISPENSE_AS_WRITTEN IS 'Code that represents if the pharmacy prescription was substituted or dispensed as written. i.e. 1 = substitution not allowed by prescriber or 2 = substitution allowed patient requested product dispensed.';
COMMENT ON COLUMN COMPACT.DIM_RX_DISPENSE_AS_WRITTEN.DISPENSE_AS_WRITTEN_DESC IS 'Describes if the pharmacy prescription was substituted or dispensed as written. i.e. substitution not allowed by prescriber or substitution allowed patient requested product dispensed.';
COMMENT ON COLUMN COMPACT.DIM_RX_DISPENSE_AS_WRITTEN.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_DISPENSE_AS_WRITTEN.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
