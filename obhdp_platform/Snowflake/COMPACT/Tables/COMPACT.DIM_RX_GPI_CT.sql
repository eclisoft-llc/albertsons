CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_GPI (
    RX_GPI_DIM_ID NUMBER NOT NULL,
    GPI VARCHAR(30) NOT NULL,
    GPI_DESC VARCHAR(100),
    DW_INSERT_DATETIME DATETIME NOT NULL,
    DW_UPDATE_DATETIME DATETIME NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PK_RX_GPI_DIM_ID PRIMARY KEY (RX_GPI_DIM_ID),
    CONSTRAINT UK_GPI UNIQUE (GPI)
);
COMMENT ON TABLE COMPACT.DIM_RX_GPI IS 'This table stores attributes that describe the Pharmacy claims Generic Product Indicator code which can be up to 14-digits that  identify a group of similar drugs. Each successive two digits of the code represents a more specific relationship of the drugs. Example, the first 2 digits indicate a general drug group. As 2-digit blocks are added to the selection, the group of drugs becomes more specific. The full 14-digits indicate a group of NDC numbers which are a specific drug, specific dose form and specific strength.';
COMMENT ON COLUMN COMPACT.DIM_RX_GPI.RX_GPI_DIM_ID IS 'Represents the link to DIM_RX_GPI table describing Pharmacy claims Generic Product Indicator code which can be up to 14-digits that identify a group of similar drugs. Each successive two digits of the code represents a more specific relationship of the drugs. Example, the first 2 digits indicate a general drug group. As 2-digit blocks are added to the selection, the group of drugs becomes more specific. The full 14-digits indicate a group of NDC numbers which are a specific drug, specific dose form and specific strength.';
COMMENT ON COLUMN COMPACT.DIM_RX_GPI.GPI IS 'Generic Product Indicator code which can be up to 14-digits that identify a group of similar drugs. The full 14-digits indicate a group of NDC numbers which are a specific drug, specific dose form and specific strength';
COMMENT ON COLUMN COMPACT.DIM_RX_GPI.GPI_DESC IS 'Generic Product Indicator description which identifies a group of similar drugs. ';
COMMENT ON COLUMN COMPACT.DIM_RX_GPI.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_GPI.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
