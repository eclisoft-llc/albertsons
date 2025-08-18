CREATE TABLE IF NOT EXISTS COMPACT.FACT_CLAIM_EXTN_PROV
    (
        COMPANY_DIM_ID	INTEGER	NOT NULL,
        SEQ_CLAIM_ID	VARCHAR(50)	NOT NULL,
        CLAIM_NUMBER	VARCHAR(36)	NOT NULL,
        PRIMARY_SVC_DATE	DATE	NOT NULL,
        CONVERTED_FROM_DIM_ID	INTEGER	NOT NULL,
        MEMB_DIM_ID	INTEGER	NOT NULL,
        BI_PROV_DIM_ID	INTEGER	NOT NULL,
        BI_PROV_NPI	varchar(10)	,
        BI_PROVIDER_ID	varchar(20)	,
        BI_PROV_TAX_ID	varchar(20)	,
        BI_PROV_SSN	varchar(20)	,
        BI_PROV_TAXONOMY	varchar(12)	,
        BI_PROV_FIRST_NAME	varchar(50)	,
        BI_PROV_LAST_NAME	varchar(50)	,
        BI_PROV_ADDR1	varchar(40)	,
        BI_PROV_ADDR2	varchar(40)	,
        BI_PROV_CITY	varchar(19)	,
        BI_PROV_STATE	varchar(2)	,
        BI_PROV_ZIP	varchar(11)	,
        AT_PROV_NPI	varchar(10)	,
        AT_PROV_TAXONOMY	varchar(12)	,
        AT_PROV_FIRST_NAME	varchar(50)	,
        AT_PROV_LAST_NAME	varchar(50)	,
        RE_PROV_NPI	varchar(10)	,
        RE_PROV_TAXONOMY	varchar(12)	,
        RE_PROV_FIRST_NAME	varchar(50)	,
        RE_PROV_LAST_NAME	varchar(50)	,
        OP_PROV_NPI	varchar(10)	,
        OP_PROV_FIRST_NAME	varchar(50)	,
        OP_PROV_LAST_NAME	varchar(50)	,
        OT_OP_PROV_NPI	varchar(10)	,
        OT_OP_PROV_FIRST_NAME	varchar(50)	,
        OT_OP_PROV_LAST_NAME	varchar(50)	,
        RF_PROV_NPI	varchar(10)	,
        RF_PROV_FIRST_NAME	varchar(50)	,
        RF_PROV_LAST_NAME	varchar(50)	,
        SP_PROV_NPI	varchar(10)	,
        SP_PROV_FIRST_NAME	varchar(50)	,
        SP_PROV_LAST_NAME	varchar(50)	,
        SE_PROV_NPI	varchar(10)	,
        SE_PROV_FAC_ID	varchar(20)	,
        SE_PROV_FIRST_NAME	varchar(50)	,
        SE_PROV_LAST_NAME	varchar(50)	,
        SE_PROV_ADDR1	varchar(40)	,
        SE_PROV_ADDR2	varchar(40)	,
        SE_PROV_CITY	varchar(19)	,
        SE_PROV_STATE	varchar(2)	,
        SE_PROV_ZIP	varchar(11)	,
        SYS1	varchar(60)	,
        SYS2	varchar(60)	,
        SYS3	varchar(60)	,
        DW_INSERT_DATETIME	DATETIME	,
        DW_UPDATE_DATETIME	DATETIME	,
        SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_PROCESS_ID VARCHAR(36),
        CONSTRAINT PK_F_FACT_CLAIM_EXTN_PROV_SEQ_CLAIM_ID_COMPANY_DIM_ID PRIMARY KEY (SEQ_CLAIM_ID,COMPANY_DIM_ID)
    );
COMMENT ON TABLE COMPACT.FACT_CLAIM_EXTN_PROV IS 'This table stores provider information submitted on the claim.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.COMPANY_DIM_ID IS 'Represents the link to DIM_COMPANY table that describes UHC Community and State Company created codes for every plan used in the Data Warehouse. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SEQ_CLAIM_ID IS 'Sequential unique claim id created when claims are inserted into the source system.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.CLAIM_NUMBER IS 'Number assigned to a Claim. Numbers are plan specific.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.PRIMARY_SVC_DATE IS 'The claim primary date when services were rendered for the whole claim.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.CONVERTED_FROM_DIM_ID IS 'Represents the link to DIM_CONVERTED_FROM table describing where the data source was converted from. i.e. CSP Facets, Diamond System or UHC Galaxy/Cosmos ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.MEMB_DIM_ID IS 'Represents the link to DIM_MEMBER table containing the member''s identification, company, eligibility, dob, dod, ssn, name, address, phone numbers, medicare no, medicaid no, Risk score and responsible party.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_DIM_ID IS 'Represents the link to DIM_PROVIDER table describing the provider who bill the service.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_NPI IS 'Billing provider''s NPI';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROVIDER_ID IS 'Billing provider ID ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_TAX_ID IS 'Billing provider''s IRS tax ID ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_SSN IS 'Billing provider''s SSN ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_TAXONOMY IS 'Billing provider''s taxonomy code ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_FIRST_NAME IS 'Billing provider''s first name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_LAST_NAME IS 'Billing provider''s last name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_ADDR1 IS 'Billing provider''s first address line ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_ADDR2 IS 'Billing provider''s second address line ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_CITY IS 'Billing provider''s city ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_STATE IS 'Billing provider''s state ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.BI_PROV_ZIP IS 'Billing provider''s zip code ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.AT_PROV_NPI IS 'Attending provider''s NPI';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.AT_PROV_TAXONOMY IS 'Attending provider''s taxonomy code ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.AT_PROV_FIRST_NAME IS 'Attending provider''s first name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.AT_PROV_LAST_NAME IS 'Attending provider''s last name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.RE_PROV_NPI IS 'Rendering provider''s NPI';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.RE_PROV_TAXONOMY IS 'Rendering provider''s taxonomy code ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.RE_PROV_FIRST_NAME IS 'Rendering provider''s first name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.RE_PROV_LAST_NAME IS 'Rendering provider''s last name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.OP_PROV_NPI IS 'Operating provider''s NPI';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.OP_PROV_FIRST_NAME IS 'Operating provider''s first name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.OP_PROV_LAST_NAME IS 'Operating provider''s last name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.OT_OP_PROV_NPI IS 'Other Operating provider''s NPI';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.OT_OP_PROV_FIRST_NAME IS 'Other Operating provider''s first name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.OT_OP_PROV_LAST_NAME IS 'Other Operating provider''s last name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.RF_PROV_NPI IS 'Referring provider''s NPI';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.RF_PROV_FIRST_NAME IS 'Referring provider''s first name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.RF_PROV_LAST_NAME IS 'Referring provider''s last name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SP_PROV_NPI IS 'Supervising provider''s NPI';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SP_PROV_FIRST_NAME IS 'Supervising provider''s first name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SP_PROV_LAST_NAME IS 'Supervising provider''s last name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SE_PROV_NPI IS 'Servicing provider''s NPI';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SE_PROV_FAC_ID IS 'Servicing provider''s facility ID';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SE_PROV_FIRST_NAME IS 'Servicing provider''s first name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SE_PROV_LAST_NAME IS 'Servicing provider''s last name ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SE_PROV_ADDR1 IS 'Servicing provider''s first address line ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SE_PROV_ADDR2 IS 'Servicing provider''s second address line ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SE_PROV_CITY IS 'Servicing provider''s city ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SE_PROV_STATE IS 'Servicing provider''s state ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM_EXTN_PROV.SE_PROV_ZIP IS 'Servicing provider''s zip code ';
