CREATE TABLE IF NOT EXISTS COMPACT.FACT_MEMBER_REVENUE_DETAIL
    (
        MEMB_DIM_ID	INTEGER	,
        COMPANY_DIM_ID	INTEGER	,
        REVENUE_TYPE_DIM_ID	INTEGER	,
        DATA_SOURCE	varchar(50)	,
        LINE_NUMBER	varchar(2)	,
        MEMBER_ID	varchar(30)	,
        MEMBER_SSN	varchar(9)	,
        MEMBER_LAST_NAME	varchar(30)	,
        MEMBER_FIRST_NAME	varchar(30)	,
        MEMBER_INIT	varchar(30)	,
        MEMBER_DOB	DATE	,
        MEMBER_COUNT	INTEGER	,
        TRANSACTION_TYPE	varchar(30)	,
        PAYMENT_AMT	NUMBER(22,2) DEFAULT 0	,
        PAYMENT_DATE	DATE	,
        REMIT_MONTH	DATE	,
        SERVICE_START_DATE	DATE	,
        SERVICE_END_DATE	DATE	,
        DW_INSERT_DATETIME	DATETIME	,
        DW_UPDATE_DATETIME	DATETIME	,
        OTHER_MEMBER_ID_1	varchar(30)	,
        OTHER_MEMBER_ID_2	varchar(30)	,
        OTHER_MEMBER_ID_3	varchar(30)	,
        OTHER_AMT_1	NUMBER(22,2) DEFAULT 0	,
        OTHER_AMT_2	NUMBER(22,2) DEFAULT 0	,
        OTHER_DATE_1	DATE	,
        OTHER_DATE_2	DATE	,
        OTHER_DATE_3	DATE	,
        USER_DEFINED_ALPHA_1	varchar(30)	,
        USER_DEFINED_NUM_1	INTEGER	,
        USER_DEFINED_KEY	varchar(100)	,
        SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_PROCESS_ID VARCHAR(36),
        CONSTRAINT PK_F_FACT_MEMBER_REVENUE_DETAIL PRIMARY KEY
        (MEMBER_ID, COMPANY_DIM_ID, DATA_SOURCE, REVENUE_TYPE_DIM_ID, LINE_NUMBER, PAYMENT_DATE, SERVICE_START_DATE, SERVICE_END_DATE)
    );
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.MEMB_DIM_ID IS 'Represents the link to DIM_MEMBER table containing the member''s identification, company, eligibility, dob, dod, ssn, name, address, phone numbers, medicare no, medicaid no, Risk score and responsible party.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.COMPANY_DIM_ID IS 'Represents the link to DIM_COMPANY table that describes UHC Community and State Company created codes for every plan used in the Data Warehouse. Queries should be restricting or grouping by COMPANY_DIM_ID. This table is linked to several tables describing the company and state information. i.e. AC Pennsylvania with corporation UnitedHealthGroup under the AmeriChoice division from the NorthEast region is from the state of Pennsylvania. ';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.REVENUE_TYPE_DIM_ID IS 'Represents the link to the DIM_REVENUE_TYPE table used to categorize charges based on the revenue type. i.e. 2 = ''M'' maternity, 4 = ''K'' Risk pool and 12 = ''Z'' CHIP. ';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.DATA_SOURCE IS 'The source system where the member revenue data is loaded from.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.LINE_NUMBER IS 'Claims can contain several detail lines and are given a line number.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.MEMBER_ID IS 'Unique member identification. The Electronic Visit Verification software is specifically for the Tennessee Vendor.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.MEMBER_SSN IS 'Member''sSocial Security Number.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.MEMBER_LAST_NAME IS 'Member''s last name.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.MEMBER_FIRST_NAME IS 'Member''s First Name.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.MEMBER_INIT IS 'Member''s middle initial.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.MEMBER_DOB IS 'Member Date of Birth';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.PAYMENT_AMT IS 'The Member''s  Payment amount.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.PAYMENT_DATE IS 'The Member''s Payment date.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.REMIT_MONTH IS 'Month the payment was transmitted.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.SERVICE_START_DATE IS 'Member claim service start date.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.SERVICE_END_DATE IS 'Member claim service end date.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.OTHER_MEMBER_ID_1 IS 'Member''s additional identification number.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.OTHER_MEMBER_ID_2 IS 'Member''s additional identification number.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.OTHER_MEMBER_ID_3 IS 'Member''s additional identification number.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_REVENUE_DETAIL.USER_DEFINED_KEY IS 'User defined key.';
