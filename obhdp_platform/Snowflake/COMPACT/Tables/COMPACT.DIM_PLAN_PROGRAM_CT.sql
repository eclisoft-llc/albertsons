CREATE TABLE IF NOT EXISTS COMPACT.DIM_PLAN_PROGRAM (
PLAN_PROGRAM_DIM_ID NUMBER NOT NULL CONSTRAINT PK_F_DIM_PLAN_PROGRAM_DIM_ID PRIMARY KEY,
PROGRAM_CODE VARCHAR(16) NOT NULL,
PROGRAM_DESC VARCHAR(30) NOT NULL,
COMPANY_DIM_ID NUMBER NOT NULL,
DW_INSERT_DATETIME DATETIME,
DW_UPDATE_DATETIME DATETIME,
PLAN_PROGRAM_TYPE_DIM_ID NUMBER NOT NULL,
PROGRAM_TYPE_CODE VARCHAR(6) NOT NULL,
PROGRAM_TYPE_DESC VARCHAR(30),
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36),
CONSTRAINT UK_F_DIM_PLAN_PROGRAM_CODE_COMPANY UNIQUE (PROGRAM_CODE,COMPANY_DIM_ID),
CONSTRAINT FK_DIM_PLAN_PROGRAM_DIM_COMPANY FOREIGN KEY (COMPANY_DIM_ID) REFERENCES COMPACT.DIM_COMPANY(COMPANY_DIM_ID),
CONSTRAINT FK_DIM_PLAN_PROGRAM_DIM_PLAN_PROGRAM_TYPE FOREIGN KEY (PLAN_PROGRAM_TYPE_DIM_ID) REFERENCES COMPACT.DIM_PLAN_PROGRAM_TYPE(PLAN_PROGRAM_TYPE_DIM_ID)
);

COMMENT ON TABLE COMPACT.DIM_PLAN_PROGRAM IS 'This table stores attributes that describe programs that a plan may have specifically by company. ';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.PLAN_PROGRAM_DIM_ID IS 'Represents the link to DIM_PLAN_PROGRAM table describing the programs that a plan may have specifically by company. i.e. UHC C+S Tennessee and Iowa company may have plan programs such as Nursing Facility, Medicaid or Home/Comm-based Dual.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.PROGRAM_CODE IS 'Codes that represent the programs that a plan may have, specified by company. i.e. TANF, CMS, CHP and SSN.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.PROGRAM_DESC IS 'Describes the programs that a plan may have, specified by company. i.e. TANF-UHCLA, San Diego CMS, CHP and SSN-SSI No MC.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.COMPANY_DIM_ID IS 'Represents the link to DIM_COMPANY table that describes UHC Community and State Company created codes for every plan used in the Data Warehouse. Queries should be restricting or grouping by COMPANY_DIM_ID. This table is linked to several tables describing the company and state information. i.e. AC Pennsylvania with corporation UnitedHealthGroup under the AmeriChoice division from the NorthEast region is from the state of Pennsylvania. ';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.PLAN_PROGRAM_TYPE_DIM_ID IS 'Represents the link to DIM_PLAN_PROGRAM_TYPE table describing a list of program types and if the types are Medicare Dual eligible 1 = Non-Dual and 2 = Dual. i.e. MCRD = MCR dual (Medicare Dual eligible) or HCB = Home/Comm-based program.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.PROGRAM_TYPE_CODE IS 'Codes that represent the program type that a plan may have, specified by company. i.e. TANF, CMS, CHP and SSI.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.PROGRAM_TYPE_DESC IS 'Describes the program type that a plan may have, specified by company. i.e. TANF, San Diego CMS, CHP and SSI without MCR.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.SF_INSERT_DATETIME IS 'Date the record was updated in the OBHDP Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.SF_UPDATE_DATETIME IS 'Date the record was updated in the OBHDP Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_PLAN_PROGRAM.SF_PROCESS_ID IS 'GUID of the process when the record was inserted or updated in the OBHDP Data Warehouse.';

