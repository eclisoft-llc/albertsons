CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_PRODUCT_QUALIFIER (
    RX_PRODUCT_QUALIFIER_DIM_ID NUMBER NOT NULL,
    RX_PRODUCT_ID_QUALIFIER VARCHAR(3) NOT NULL,
    RX_PRODUCT_ID_QUALIFIER_DESC VARCHAR(30),
    DW_INSERT_DATETIME DATETIME NOT NULL,
    DW_UPDATE_DATETIME DATETIME NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PK_RX_PRODUCT_QUALIFIER_DIM_IDRIMARY PRIMARY KEY (RX_PRODUCT_QUALIFIER_DIM_ID),
    CONSTRAINT UK_RX_PRODUCT_ID_QUALIFIER UNIQUE (RX_PRODUCT_ID_QUALIFIER)
);
COMMENT ON TABLE COMPACT.DIM_RX_PRODUCT_QUALIFIER IS 'This table stores attributes that describe the qualifier used to define code set that identifies product';
COMMENT ON COLUMN COMPACT.DIM_RX_PRODUCT_QUALIFIER.RX_PRODUCT_QUALIFIER_DIM_ID IS 'This column is auto generated and gets incremented for every value being added and mainly used in fact table.';
COMMENT ON COLUMN COMPACT.DIM_RX_PRODUCT_QUALIFIER.RX_PRODUCT_ID_QUALIFIER IS 'This column is used to identify the code sets and it is one time load and below represents the values 01=universal product code (upc) ,02=health related item (hri) ,03= ndc 04=universal product number (upn), 09= hcpcs home infusion';
COMMENT ON COLUMN COMPACT.DIM_RX_PRODUCT_QUALIFIER.RX_PRODUCT_ID_QUALIFIER_DESC IS 'This column represents the description for the product qualifier as 01 for universal product code (upc)';
COMMENT ON COLUMN COMPACT.DIM_RX_PRODUCT_QUALIFIER.DW_INSERT_DATETIME IS 'This table represents the date value as when the record got updated';
