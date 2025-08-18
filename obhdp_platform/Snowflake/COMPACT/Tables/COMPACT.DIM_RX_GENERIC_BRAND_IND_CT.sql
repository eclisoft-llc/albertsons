CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_GENERIC_BRAND_IND
    (
        RX_GENERIC_BRAND_IND_DIM_ID INTEGER NOT NULL,
        RX_GENERIC_BRAND_IND VARCHAR(1) NOT NULL,
        RX_GENERIC_BRAND_IND_DESC VARCHAR(100), 
        DW_INSERT_DATETIME DATETIME ,
        DW_UPDATE_DATETIME DATETIME ,
        SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_PROCESS_ID VARCHAR(36),
        CONSTRAINT PK_F_DIM_RX_GRC_BRD_ND PRIMARY KEY (RX_GENERIC_BRAND_IND_DIM_ID)
    );
COMMENT ON COLUMN COMPACT.DIM_RX_GENERIC_BRAND_IND.RX_GENERIC_BRAND_IND_DIM_ID IS 'Represents the link to DIM_RX_GENERIC_BRAND_IND table describing if Prescription drugs are generic for Pharmacy claims. Values include 1 = 0 - Multi-source brand (more than one ''branded'' version of a drug available), 2 = 1 - Single-source brand(a brand-name drug only, no generic(s) available) , 3 = 2 - Generic, 0 = U - Unknown or -1 = - N/A.';
COMMENT ON COLUMN COMPACT.DIM_RX_GENERIC_BRAND_IND.RX_GENERIC_BRAND_IND IS 'Code that represents if Prescription drugs are generic for Pharmacy claims. Values include 0 - Multi-source brand (more than one ''branded'' version of a drug available), 1 - Single-source brand(a brand-name drug only, no generic(s) available) , 2 - Generic, U - Unknown or - N/A.';
COMMENT ON COLUMN COMPACT.DIM_RX_GENERIC_BRAND_IND.RX_GENERIC_BRAND_IND_DESC IS 'Describes if Prescription drugs are generic for Pharmacy claims. Values include Multi-source brand (more than one ''branded'' version of a drug available), Single-source brand(a brand-name drug only, no generic(s) available) , Generic, Unknown or N/A.';
COMMENT ON COLUMN COMPACT.DIM_RX_GENERIC_BRAND_IND.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_GENERIC_BRAND_IND.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
