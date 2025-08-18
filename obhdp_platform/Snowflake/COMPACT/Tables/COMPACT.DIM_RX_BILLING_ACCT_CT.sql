CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_BILLING_ACCT (
    RX_BILLING_ACCT_DIM_ID NUMBER NOT NULL,
    LINE_OF_BUSINESS_DIM_ID NUMBER,
    ACCOUNT_NO VARCHAR(10) NOT NULL,
    COMPANY_DIM_ID NUMBER NOT NULL,
    DW_INSERT_DATETIME DATETIME NOT NULL,
    DW_UPDATE_DATETIME DATETIME NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PK_RX_BILLING_ACCT_DIM_ID PRIMARY KEY (RX_BILLING_ACCT_DIM_ID),
    CONSTRAINT UK_ACCOUNT_NO_COMPANY_DIM_ID UNIQUE (ACCOUNT_NO, COMPANY_DIM_ID)
);
COMMENT ON TABLE COMPACT.DIM_RX_BILLING_ACCT IS 'This table stores attributes that describe company specific pharmacy billing account information.';
COMMENT ON COLUMN COMPACT.DIM_RX_BILLING_ACCT.RX_BILLING_ACCT_DIM_ID IS 'Represents the link to company specific pharmacy billing account information.';
COMMENT ON COLUMN COMPACT.DIM_RX_BILLING_ACCT.LINE_OF_BUSINESS_DIM_ID IS 'Represents the link to DIM_LINE_OF_BUSINESS table describing UHC Community and State created codes for every line of business used by the plans. i.e. Medicare, Medicaid, FHP, Evercare, Long Term Care and CHIP . Line of Business can be further categorized by the DIM_LINE_OF_BUSINESS_TYPE table as Medicaid, Medicare, N/A, Non Risk and Unknown. ';
COMMENT ON COLUMN COMPACT.DIM_RX_BILLING_ACCT.ACCOUNT_NO IS 'Describes company specific pharmacy billing account information';
COMMENT ON COLUMN COMPACT.DIM_RX_BILLING_ACCT.COMPANY_DIM_ID IS 'Represents the link to DIM_COMPANY table that describes UHC Community and State Company created codes for every plan used in the Data Warehouse. Queries should be restricting or grouping by COMPANY_DIM_ID. This table is linked to several tables describing the company and state information. i.e. AC Pennsylvania with corporation UnitedHealthGroup under the AmeriChoice division from the NorthEast region is from the state of Pennsylvania. ';
COMMENT ON COLUMN COMPACT.DIM_RX_BILLING_ACCT.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_BILLING_ACCT.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
