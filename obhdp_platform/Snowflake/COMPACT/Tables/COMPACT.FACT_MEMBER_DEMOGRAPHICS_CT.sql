CREATE TABLE IF NOT EXISTS COMPACT.FACT_MEMBER_DEMOGRAPHICS (
	MEMB_DEMOGRAPHICS_FULL_DATE DATE,
	MEMB_DIM_ID INTEGER,
	PLAN_DIM_ID INTEGER,
	COMPANY_DIM_ID INTEGER,
	AGE INTEGER,
	MEMBER_CNT INTEGER,
	DW_INSERT_DATETIME DATETIME,
	DW_UPDATE_DATETIME DATETIME,
	PROV_DIM_ID INTEGER,
	GEOGRAPHY_DIM_ID INTEGER,
	LINE_OF_BUSINESS_DIM_ID INTEGER DEFAULT -1 ,
	ELIG_STATUS_DIM_ID INTEGER,
	GROUP_DIM_ID INTEGER DEFAULT -1 ,
	PRODUCT_DIM_ID INTEGER DEFAULT -1 ,
	CONVERTED_FROM_DIM_ID INTEGER DEFAULT -1 ,
	COSMOS_CUST_SEG_DIM_ID INTEGER,
	CARE_LEVEL_DIM_ID INTEGER,
	CASE_MANAGER_DIM_ID INTEGER,
	COMORBIDITY_CNT INTEGER,
	PLAN_LOB_DIM_ID INTEGER DEFAULT -1,
	MONTH_DIM_ID INTEGER DEFAULT -1,
	MEDICARE_TYPE_DIM_ID INTEGER DEFAULT -1 ,
	LTC_IND_DIM_ID INTEGER DEFAULT -1 ,
	UNISON_PRODUCT_DIM_ID INTEGER,
	STATE_SPEC_COUNTY_DIM_ID INTEGER,
	RTCL_RATE_DIM_ID INTEGER,
	SYS1 VARCHAR(60),
	SYS2 VARCHAR(60),
	SYS3 VARCHAR(60),
	SYS4 VARCHAR(60),
	SOURCE_PRODUCT_DIM_ID INTEGER DEFAULT -1 ,
	CLASS_DIM_ID INTEGER DEFAULT -1 ,
	CLASS_PLAN_DIM_ID INTEGER DEFAULT -1,
	COA_DIM_ID INTEGER DEFAULT -1 ,
	PROGRAM_STATUS_DIM_ID INTEGER DEFAULT -1 ,
	SDA_DIM_ID INTEGER,
	SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
CONSTRAINT PK_F_FACT_MEMBER_DEMOGRAPHICS PRIMARY KEY (MEMB_DEMOGRAPHICS_FULL_DATE,MEMB_DIM_ID,PLAN_DIM_ID,COMPANY_DIM_ID,PROV_DIM_ID,GEOGRAPHY_DIM_ID,ELIG_STATUS_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_CLASS FOREIGN KEY (CLASS_DIM_ID) REFERENCES COMPACT.DIM_CLASS(CLASS_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_CLASS_PLAN FOREIGN KEY (CLASS_PLAN_DIM_ID) REFERENCES COMPACT.DIM_CLASS_PLAN(CLASS_PLAN_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_PLAN_LOB FOREIGN KEY (PLAN_LOB_DIM_ID) REFERENCES COMPACT.DIM_PLAN_LOB(PLAN_LOB_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_SDA FOREIGN KEY (SDA_DIM_ID) REFERENCES COMPACT.DIM_SDA(SDA_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_SRC_PRODUCT FOREIGN KEY (SOURCE_PRODUCT_DIM_ID) REFERENCES COMPACT.DIM_SOURCE_PRODUCT(SOURCE_PRODUCT_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_LOB FOREIGN KEY (LINE_OF_BUSINESS_DIM_ID) REFERENCES COMPACT.DIM_LINE_OF_BUSINESS(LINE_OF_BUSINESS_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_LTC_IND FOREIGN KEY (LTC_IND_DIM_ID) REFERENCES COMPACT.DIM_LTC_IND(LTC_IND_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_COSMOS_CUST FOREIGN KEY (COSMOS_CUST_SEG_DIM_ID) REFERENCES COMPACT.DIM_COSMOS_CUST_SEG(COSMOS_CUST_SEG_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_CONV_FROM FOREIGN KEY (CONVERTED_FROM_DIM_ID) REFERENCES COMPACT.DIM_CONVERTED_FROM(CONVERTED_FROM_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_COMPANY FOREIGN KEY (COMPANY_DIM_ID) REFERENCES COMPACT.DIM_COMPANY(COMPANY_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_DATE FOREIGN KEY (MEMB_DEMOGRAPHICS_FULL_DATE) REFERENCES COMPACT.DIM_DATE(FULL_DATE),
CONSTRAINT FK_FACT_MEM_DEM_DIM_DATE_MONTH FOREIGN KEY (MONTH_DIM_ID) REFERENCES COMPACT.DIM_DATE_MONTH(MONTH_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_ELIG_STATUS FOREIGN KEY (ELIG_STATUS_DIM_ID) REFERENCES COMPACT.DIM_ELIG_STATUS(ELIG_STATUS_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_GEO FOREIGN KEY (GEOGRAPHY_DIM_ID) REFERENCES COMPACT.DIM_GEOGRAPHY(GEOGRAPHY_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_GROUP FOREIGN KEY (GROUP_DIM_ID) REFERENCES COMPACT.DIM_GROUP(GROUP_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_MEMBER FOREIGN KEY (MEMB_DIM_ID) REFERENCES COMPACT.DIM_MEMBER(MEMB_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_PLAN FOREIGN KEY (PLAN_DIM_ID) REFERENCES COMPACT.DIM_PLAN(PLAN_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_PRODUCT FOREIGN KEY (PRODUCT_DIM_ID) REFERENCES COMPACT.DIM_PRODUCT(PRODUCT_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_PROVIDER FOREIGN KEY (PROV_DIM_ID) REFERENCES COMPACT.DIM_PROVIDER(PROV_DIM_ID),
CONSTRAINT FK_FACT_MEM_DEM_DIM_MEDICARE_TYPE FOREIGN KEY (MEDICARE_TYPE_DIM_ID) REFERENCES COMPACT.DIM_MEDICARE_TYPE(MEDICARE_TYPE_DIM_ID)
);
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.MEMB_DEMOGRAPHICS_FULL_DATE IS 'Represents the members demographics full date.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.MEMB_DIM_ID IS 'Represents the link to DIM_MEMBER table which stores all member information.This field is a system generated unique identifier for each member record.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.PLAN_DIM_ID IS 'Represents the link to DIM_PLAN table which stores detailed member benefit plan information.This field is a system generated unique identifier for each Plan record.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.COMPANY_DIM_ID IS 'Represents the link to DIM_COMPANY table which stores the UHC Community and State Company created codes for every plan used in the Data Warehouse.This field is a system generated unique identifier for each Company record.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.AGE IS 'Represents the member age calculated at the time of the members demographics full date.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.MEMBER_CNT IS 'Represents the member count. It is defaulted to 1. This field is no longer used.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.PROV_DIM_ID IS 'Represents the link to DIM_PROVIDER table which stores information regarding all providers .This field in fact_member_demographics table specifically contains  only PCP Provider Identifiers.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.GEOGRAPHY_DIM_ID IS 'Represents the link to DIM_GEOGRAPHY table which describes geographical location.This field is a system generated unique identifier for each geographical location.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.LINE_OF_BUSINESS_DIM_ID IS 'Represents the link to DIM_LINE_OF_BUSINESS table  which stores attributes that describe UHC Community and State created codes for every line of business used by the plans like DSNP, Medicaid, FHP, Evercare, Long Term Care and CHIP, MMP. This field is a system generated unique identifier for each Line of Business.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.ELIG_STATUS_DIM_ID IS 'Represents the link to DIM_ELIG_STATUS table which describes the members eligibility status. This field contains  only eligible members  for fact_member_demographics table so the status i.e. elig_status_dim_id = 1.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.GROUP_DIM_ID IS 'Represents the link to DIM_GROUP table which describes the group the member/claim belongs to. This field is used to include configuration information by the source.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.PRODUCT_DIM_ID IS 'Represents the link to DIM_PRODUCT table which stores the  PeopleSoft 8.0 version Financial Summary Database (FSDB) Product Information.This field is a system generated unique identifier for each Product record.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.CONVERTED_FROM_DIM_ID IS 'Represents the link to DIM_CONVERTED_FROM table which  describes the originating data source. This field in fact_member_demographics table identifies where  the membership is sourced from.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.COSMOS_CUST_SEG_DIM_ID IS 'Represents the link to DIM_COSMOS_CUST_SEG table which breaks down the company into COSMOS group segments.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.CARE_LEVEL_DIM_ID IS 'Represent the link to DIM_CARE_LEVEL table which describes the level of care needed for the patient. Applies to Managed Service Organization (MSO )members only. This field is no longer used.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.CASE_MANAGER_DIM_ID IS 'Represents the link to the DIM_CASE_MANAGER table  which contains the information of the Case Manager responsible for identifying and coordinating services necessary to meet service needs of the member.  This field is no longer used.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.COMORBIDITY_CNT IS 'Represents the number of coexistent disease or health conditions for a member at a given time. This field is no longer used.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.PLAN_LOB_DIM_ID IS 'Represents the link to DIM_PLAN_LOB table which stores the plan , line of business and Plan programs mapping. This field can be used to link to DIM_LINE_OF_BUSINESS, DIM_PLAN and DIM_PLAN_PROGRAM tables. This field is critical in deriving the Plan-LOB-Plan Program mapping.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.MONTH_DIM_ID IS 'Represents the link to the DIM_DATE_MONTH table which lists the month by number, quarter, year quarter, year number, begin and end dates for month, quarter and year, LMO, LYR and current month indicator.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.MEDICARE_TYPE_DIM_ID IS 'Represents the link to DIM_MEDICARE_TYPE table which describes the different types of Medicare i.e. Medicare Part A only, Medicare Part B only,  MEDICARE PART A AND B .This field can be used to Link to DIM_MEDICARE_GROUP and DIM_GROUP tables. Information on how this field is defined  based on source systems (COSMOS, CSP) will be added in future.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.LTC_IND_DIM_ID IS 'Represents the link to DIM_LTC_IND  table which indicates if the member is in Long Term Care services or not. Information on how this field is defined  based on source systems (COSMOS, CSP) will be added in future.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.UNISON_PRODUCT_DIM_ID IS 'Represents the Unison Facets Product (PDPD_ID). This field currently stores  CSP���s PDPD_ID. It is a legacy field and should no longer be used. Use Source Product Code (SOURCE_PRODUCT_DIM_ID) instead.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.STATE_SPEC_COUNTY_DIM_ID IS 'Represents the link to GEOGRAPHY_COUNTY_DIM_ID in DIM_GEOGRAPHY_COUNTY table which contains state specified county information from 834 file that may be different from national county code. This field is a system generated unique identifier for each state specified county record.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.RTCL_RATE_DIM_ID IS 'Represents the link to the DIM_RATECELL_RATE table where Rate cells are built based on age, gender, eligibility and medical status code. This field is populated only for  MMPs and DSNPs (Medicare)  and contains HPBP contract codes.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.SYS1 IS 'Field created as Place holders and to be used for ETL Purposes only.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.SYS2 IS 'Field created as Place holders and to be used for ETL Purposes only.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.SYS3 IS 'Field created as Place holders and to be used for ETL Purposes only.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.SYS4 IS 'Field created as Place holders and to be used for ETL Purposes only.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.SOURCE_PRODUCT_DIM_ID IS 'Represents the link to DIM_SOURCE_PRODUCT table which describes the CSP Facet products(PDPD_ID). A Source Product represents a benefit package for a particular health plan. CSP Product(PDPD_ID) is one of the components of the group structure.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.CLASS_DIM_ID IS 'Represents the link to DIM_CLASS table which describes the CSPs class ID (CSCS_ID). It is one of the components of the group structure and is often used in conjunction with Class Plan(CSPI_ID) / Source Product Code (PDPD_ID) to define distinctive service regions covered by a particular health plan.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.CLASS_PLAN_DIM_ID IS 'Represents the link to DIM_CLASS_PLAN table which describes the CSPs Plan ID (CSPI_ID).Each Class Plan ID is tied to a source product code. It is one of the components of the group structure and can be used to define a sub-category of source product code.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.COA_DIM_ID IS 'Represents the link to DIM_COA table which describes state specific category of aid information. This is a CSP specific field.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.PROGRAM_STATUS_DIM_ID IS 'Represents the link to DIM_PROGRAM_STATUS_CODE table which stores attributes that holds state specific status codes for a program. This is a CSP specific field.';
COMMENT ON COLUMN COMPACT.FACT_MEMBER_DEMOGRAPHICS.SDA_DIM_ID IS 'Represents the link to DIM_SDA table which breaks down the states into service delivery area. This is a CSP specific field. Currently SDA is defined for the states of Florida and Texas.';
