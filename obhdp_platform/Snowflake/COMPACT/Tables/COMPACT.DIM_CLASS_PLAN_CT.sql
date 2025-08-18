CREATE TABLE IF NOT EXISTS COMPACT.DIM_CLASS_PLAN (
CLASS_PLAN_DIM_ID NUMBER NOT NULL CONSTRAINT PK_F_DIM_CLASS_PLAN_DIM_ID PRIMARY KEY,
CLASS_PLAN_ID VARCHAR(10) NOT NULL CONSTRAINT UK_F_DIM_CLASS_PLAN_CLASS_PLAN_ID UNIQUE,
CLASS_PLAN_DESC VARCHAR(70) NOT NULL,
DW_INSERT_DATETIME DATE NOT NULL,
DW_UPDATE_DATETIME DATE NOT NULL,
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36)
);
COMMENT ON TABLE COMPACT.DIM_CLASS_PLAN IS 'This table stores attributes that describe FACETS benefit plan information.  It will be primarily maintained using the Facet Class Plan table CMC_PLDS_PLAN_DESC.  This plan table differs quite differently than our DIM_PLAN table.  The Class Plan will be used to store high level benefit plan information whereas the DIM_PLAN table has risk group or rate cell information. ';
COMMENT ON COLUMN COMPACT.DIM_CLASS_PLAN.CLASS_PLAN_DIM_ID IS 'Represents the link for the DIM_CLASS_PLAN table. The DIM_CLASS_PLAN table maintains benefit plan information from the Facet Class Plan table CMC_PLDS_PLAN_DESC. This table stores high level benefit plan information.';
COMMENT ON COLUMN COMPACT.DIM_CLASS_PLAN.CLASS_PLAN_ID IS 'Class Plan Identifier for each plan or program from the Facet Class Plan table CMC_PLDS_PLAN_DESC e.g. KSKCSDD or KSLTAWN.';
COMMENT ON COLUMN COMPACT.DIM_CLASS_PLAN.CLASS_PLAN_DESC IS 'Class Plan Description for each plan or program from the Facet Class Plan table CMC_PLDS_PLAN_DESC e.g. KS KanCare Spenddown Members Dual or KS LTC Autism Waiver.';
COMMENT ON COLUMN COMPACT.DIM_CLASS_PLAN.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_CLASS_PLAN.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
