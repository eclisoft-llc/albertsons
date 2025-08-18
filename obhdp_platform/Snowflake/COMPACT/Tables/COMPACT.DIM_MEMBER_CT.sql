CREATE TABLE IF NOT EXISTS COMPACT.DIM_MEMBER (
MEMB_DIM_ID INTEGER NOT NULL CONSTRAINT PK_F_DIM_MEMBER_DIM_ID PRIMARY KEY,
SEQ_MEMB_ID INTEGER NOT NULL,
PERSON_NUMBER INTEGER,
COMPANY_DIM_ID INTEGER NOT NULL,
SEQ_SUBS_ID INTEGER,
SUBSCRIBER_ID VARCHAR(14),
MEMB_ELIG_STATUS VARCHAR(1),
DOB DATE,
DOD DATE,
GENDER VARCHAR(1),
ETHNIC VARCHAR(1),
SOCIAL_SEC_NO VARCHAR(9),
MEMB_FIRST_NAME VARCHAR(30),
MEMB_MIDDLE_INITIAL VARCHAR(1),
MEMB_LAST_NAME VARCHAR(35),
MEMB_ADDRESS_LINE_1 VARCHAR(55),
MEMB_ADDRESS_LINE_2 VARCHAR(55),
MEMB_CITY VARCHAR(30),
MEMB_STATE VARCHAR(6),
MEMB_ZIP VARCHAR(15),
HOME_PHONE_NUMBER VARCHAR(20),
BUS_PHONE_NUMBER VARCHAR(20),
DIAMOND_ID VARCHAR(12),
ORIGINAL_EFFECTIVE_DATE DATE,
DW_INSERT_DATETIME DATETIME,
DW_UPDATE_DATETIME DATETIME,
GEOGRAPHY_STATE_DIM_ID INTEGER NOT NULL,
GENDER_DIM_ID INTEGER NOT NULL,
GENDER_DESC VARCHAR(20),
CURR_PCP_DIM_ID INTEGER NOT NULL,
GEOGRAPHY_DIM_ID INTEGER NOT NULL,
MEMB_GEOG_ZIP VARCHAR(5),
MEMB_GEOG_STATE VARCHAR(6),
MEMB_GEOG_STATE_DESC VARCHAR(20),
CONVERTED_FROM_DIM_ID INTEGER NOT NULL,
MEDICARE_NO VARCHAR(12),
MEDICAID_NO VARCHAR(20),
CASE_ID VARCHAR(16),
CURR_CASE_MANAGER_DIM_ID INTEGER NOT NULL DEFAULT -1,
CURR_COMORBIDITY_CNT INTEGER,
PREV_SUBSCRIBER_ID VARCHAR(14),
PREV_SEQ_MEMB_ID INTEGER,
MC_PERSONAL_CARE_SPECIALIST VARCHAR(30),
LANGUAGE_DIM_ID INTEGER,
IPRO_RISK_SCORE NUMBER(12,4),
RISK_SCORE_3MO NUMBER(12,4),
PRETOTDL NUMBER(12,4),
PRETOTDL3M NUMBER(12,4),
RISK_SCORE_AGESEX NUMBER(12,4),
RISK_SCORE_ACTUARIAL NUMBER(12,2),
RISK_SCORE_LAB NUMBER(12,4),
RISK_SCORE_PRG NUMBER(12,4),
VERSION_DATE DATE,
GROUP_DIM_ID INTEGER NOT NULL DEFAULT -1,
MCARE_DUAL_ENROLLED_FLAG VARCHAR(1),
RES_PARTY_FIRST_NAME VARCHAR(30),
RES_PARTY_LAST_NAME VARCHAR(35),
RES_PARTY_ADDR1 VARCHAR(55),
RES_PARTY_ADDR2 VARCHAR(55),
RES_PARTY_ADDR3 VARCHAR(55),
RES_PARTY_CITY VARCHAR(30),
RES_PARTY_STATE VARCHAR(6),
RES_PARTY_ZIP VARCHAR(15),
RES_PARTY_GEO_DIM_ID INTEGER NOT NULL DEFAULT -1,
HCR_IND_DIM_ID INTEGER DEFAULT -1,
RECENT_TERM_DT DATE,
MEMB_ADDR_MAIL_DIM_ID INTEGER NOT NULL DEFAULT -1,
CELL_PHONE_NUMBER VARCHAR(20),
WORK_PHONE_NUMBER VARCHAR(20),
TERM_REASON_DIM_ID INTEGER,
MEDICARE_DUAL_FLAG_DIM_ID INTEGER DEFAULT -1,
ETHNIC_DIM_ID INTEGER NOT NULL DEFAULT -1,
QUALIFY_DATE DATE,
RES_PARTY_MID_INIT VARCHAR(1),
RES_PARTY_PHONE VARCHAR(25),
RES_PARTY_EMAIL VARCHAR(40),
RES_PARTY_LAST_EFF_DATE DATE,
RES_PARTY_LAST_TERM_DATE DATE,
RES_PARTY_ID VARCHAR(9),
MOTHER_MEMB_DIM_ID INTEGER NOT NULL DEFAULT -1,
HOMELESS_FLAG_DIM_ID INTEGER DEFAULT 0,
FAM_RELATIONSHIP VARCHAR(1),
FAM_LINK_ID VARCHAR(12),
IEX_MEMB_ID VARCHAR(80),
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36),
CONSTRAINT UK_F_DIM_MEMBER_SEQ_MEMB_ID_COMPANY_DIM_ID UNIQUE (SEQ_MEMB_ID,COMPANY_DIM_ID),
CONSTRAINT FK_DIM_MEMBER_DIM_COMPANY FOREIGN KEY (COMPANY_DIM_ID) REFERENCES COMPACT.DIM_COMPANY(COMPANY_DIM_ID),
CONSTRAINT FK_DIM_MEMBER_DIM_GEOGRAPHY FOREIGN KEY (GEOGRAPHY_DIM_ID) REFERENCES COMPACT.DIM_GEOGRAPHY(GEOGRAPHY_DIM_ID),
CONSTRAINT FK_DIM_MEMBER_DIM_LANGUAGE FOREIGN KEY (LANGUAGE_DIM_ID) REFERENCES COMPACT.DIM_LANGUAGE(LANGUAGE_DIM_ID)
);
COMMENT ON TABLE COMPACT.DIM_MEMBER IS 'This dimension table stores all member information such as: company, eligibility status, dob, dod, ssn, name, address, phone numbers, medicare no, medicaid no, Risk score and responsible party.A member is an individual that participates by being covered by a healthcare plan in order to take advantage of the benefits and services provided. An Individual could have multiple records in this table. i.e. he/she  could have multiple memb_dim_id with different line of business.';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_DIM_ID IS 'This field is a system generated unique identifier for each member record.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.SEQ_MEMB_ID IS 'Represents the Sequential member id created when members are inserted into the source system. This field is populated differently based on the source system. Populated as a system generated Id for CSP and populated with a dummy identifier for COSMOS.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.PERSON_NUMBER IS 'Represents the identifier used to distinguish between subscriber and dependents.  For the Subscriber, person_number will be 01 and for other members in the family (dependents) would then get assigned sequential numbers.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.COMPANY_DIM_ID IS 'Represents the link to DIM_COMPANY table which stores the UHC Community and State Company created codes for every plan used in the Data Warehouse.This field is a system generated unique identifier for each Company record.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.SEQ_SUBS_ID IS 'Represents the  SEQ_MEMB_ID of the Member who is also the Subscriber. It is a legacy field and currently populated with dummy identifiers for CSP and COSMOS	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.SUBSCRIBER_ID IS 'This field is used as an external facing identifier on the member ID card. Legacy subscriber IDs are prefixed with ~ indicating that these are old subscriber IDs and the members were loaded by Legacy and were not converted by CSP when the health plan moved from Legacy to CSP.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_ELIG_STATUS IS 'Represents the current eligibility status of the member. Eligible members will have this field populated as Y. 	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.DOB IS 'Represents the Date of birth of the Member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.DOD IS 'Represents the Date of death of the Member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.GENDER IS 'Represents of the Gender of the Member.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.ETHNIC IS 'Represents the code which describes the race/ethnicity  of the subscriber. This field is populated from source system.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.SOCIAL_SEC_NO IS 'Represents the Social Security number (SSN) of the Member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_FIRST_NAME IS 'Represents the First Name of the Member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_MIDDLE_INITIAL IS 'Represents the Middle Initial of the Member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_LAST_NAME IS 'Represents the Last Name of the Member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_ADDRESS_LINE_1 IS 'Represents the Address for the member, alias, responsible party, beneficiary or employer. This field captures home address for CSP members	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_ADDRESS_LINE_2 IS 'Represents the  Address for the member, alias, responsible party, beneficiary or employer. This field captures home address for CSP members	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_CITY IS 'Represents the City for the member, alias, responsible party, beneficiary or employer. This field captures home address for CSP members	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_STATE IS 'Represents the State for the member, alias, responsible party, beneficiary or employer. This field captures home address for CSP members	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_ZIP IS 'Represents the Zip code for the member, alias, responsible party, beneficiary or employer. This field captures home address for CSP members	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.HOME_PHONE_NUMBER IS 'Represents the Home Phone Number of the Member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.BUS_PHONE_NUMBER IS 'Represents the Work Phone Number of the Member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.DIAMOND_ID IS 'Represents the Unique identifier for the individual. This field is used to tie all member records for the same individual together irrespective of the source system or line of business.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.ORIGINAL_EFFECTIVE_DATE IS 'Represents the Members First effective date on the insurance plan.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.GEOGRAPHY_STATE_DIM_ID IS 'Represents the link to DIM_GEOGRAPHY_STATE table which breaks down the states by state code, state description, state latitude and longitude. Populated from source data.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.GENDER_DIM_ID IS 'Represents the link to DIM_GENDER table which describes the gender of the member. 	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.GENDER_DESC IS 'Represents the description of the Members  gender.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.CURR_PCP_DIM_ID IS 'Represents the link to the DIM_CURRENT_PCP table which stores attributes that describe the current Primary Care Provider used by the member. This table includes the name, address, medicaid number, drug enforcement agency number and location for the provider.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.GEOGRAPHY_DIM_ID IS 'Represents the link to DIM_GEOGRAPHY_STATE table which breaks down the states by state code, state description, state latitude and longitude. Populated from source data.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_GEOG_ZIP IS 'Represents the Members geographical zip code	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_GEOG_STATE IS 'Represents the Members geographical state abbreviation	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_GEOG_STATE_DESC IS 'Represents the Members geographical  state description	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.CONVERTED_FROM_DIM_ID IS 'Represents the link to DIM_CONVERTED_FROM table which  describes the originating data source. This field in DIM_MEMBER table identifies where  the membership is sourced from.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEDICARE_NO IS 'Represents the Members Medicare assigned number.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEDICAID_NO IS 'Represents the Members Medicaid assigned number.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.CASE_ID IS 'Represents the case number assigned by an insurance carrier to a claim. Case numbers are typically used by insurance carriers that provide third party liability coverage like auto insurance.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.CURR_CASE_MANAGER_DIM_ID IS 'Represents the link to the DIM_CASE_MANAGER table which stores information of the Case Manager the individual responsible for identifying and coordinating services necessary to meet service needs of the member. It is a legacy field and no longer used.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.CURR_COMORBIDITY_CNT IS 'Represents the current number of coexistent disease or health conditions for a member at a given time.It is a Legacy field. No longer used.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.PREV_SUBSCRIBER_ID IS 'Represents the Previous Subscriber Id if Subscriber Id was changed.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.PREV_SEQ_MEMB_ID IS 'Represents the Previous sequential member id if Seq_memb_id was changed.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MC_PERSONAL_CARE_SPECIALIST IS 'Represents the Code associated with the Medicare personal care specialist. It is a legacy field and no longer used.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.LANGUAGE_DIM_ID IS 'Represents the Members first language used. i.e. English or Spanish	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.IPRO_RISK_SCORE IS 'Represents the Members risk score calculated using standard risk model. This field is calculated based on relative risk of member for the most recent 12 months compared to other plan members with respect to total costs calculated.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RISK_SCORE_3MO IS 'Represents the Members risk score calculated using 3- Month risk model. This field is calculated based on relative risk of member for the next 3 months compared to other plan members with respect to total costs calculated.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.PRETOTDL IS 'Represents the Members expected future costs for the next 12 months computed using standard risk model. 	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.PRETOTDL3M IS 'Represents the Members expected future costs for the next 3 months computed using the 3-month risk model	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RISK_SCORE_AGESEX IS 'Represents the Members risk score calculated using Age/Sex risk model. This field is calculated based on relative risk of member compared to other plan members with respect to total costs calculated.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RISK_SCORE_ACTUARIAL IS '"Represents the Member risk score calculated based on 250K Medical + Pharmacy Underwriting risk model. This field is calculated based on relative risk of member compared to other plan members with respect tototal costs calculated."	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RISK_SCORE_LAB IS 'Represents the Member risk score calculated based on Lab Risk model. This field is calculated based on relative risk of the member compared to other plan members with respect to total costs calculated.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RISK_SCORE_PRG IS 'Represents the Member risk score calculated based on PRG model. This field is calculated based on relative risk of the member compared to other plan members with respect to total costs calculated	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.VERSION_DATE IS 'Represents the members latest risk score version date. This field defines the last day of the IPRO cycle.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.GROUP_DIM_ID IS 'Represents the link to DIM_GROUP table which describes the group the member/claim belongs to. This field is used to include configuration information by the source.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MCARE_DUAL_ENROLLED_FLAG IS 'Represents the Flag which indicates if the member is enrolled in both Medicare and Medicaid.It is a Legacy field. Used by COSMOS only.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RES_PARTY_FIRST_NAME IS 'Represents the First name of the responsible party for the member if applicable.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RES_PARTY_LAST_NAME IS 'Represents the Last name of the responsible party for the member if applicable.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RES_PARTY_ADDR1 IS 'Represents the First address of the responsible party for the member if applicable.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RES_PARTY_ADDR2 IS 'Represents the Additional address of the responsible party for the member if applicable.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RES_PARTY_ADDR3 IS 'Represents the Additional address of the responsible party for the member if applicable.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RES_PARTY_CITY IS 'Represents the city of the address of the responsible party for the member if applicable.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RES_PARTY_STATE IS 'Represents the state of the address of the responsible party for the member if applicable.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RES_PARTY_ZIP IS 'Represents the zip code of the state of the responsible party for the member if applicable.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RES_PARTY_GEO_DIM_ID IS 'Represents the geographical location of the responsible party for the member if applicable.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.HCR_IND_DIM_ID IS 'Represents an Indicator that identifies the members initial enrollment as per the Health Care Reform(HCR) act.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.RECENT_TERM_DT IS 'Represents the Members recent termination date from the insurance plan.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEMB_ADDR_MAIL_DIM_ID IS 'Represents the link to DIM_MEMBER_ADDRESS table which stores all addresses associated with the member. This field stores only the mailing address for the member.	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.CELL_PHONE_NUMBER IS 'Represents the  Cell Phone Number of the member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.WORK_PHONE_NUMBER IS 'Represents the  Work Phone Number of the member	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.TERM_REASON_DIM_ID IS 'Represents the link to DIM_REASON_CODE table which stores the Reason Codes explaining why the member was termed. 	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MEDICARE_DUAL_FLAG_DIM_ID IS 'Represents the link to DIM_MEDICARE_DUAL_FLAG table which describes whether the program types are Medicare Dual enrolled and dual eligible or not . This field is populated for CSP only. 	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.ETHNIC_DIM_ID IS 'Represent the link to DIM_ETHNIC_CODE table which stores the members ethnicity.  	 ';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.QUALIFY_DATE IS 'Member recertification date';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.MOTHER_MEMB_DIM_ID IS 'Represents the mothers memb_dim_id of this member.';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.HOMELESS_FLAG_DIM_ID IS 'Represent the link to DIM_Homeless_IND table which stores the members homeless flag.';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.FAM_RELATIONSHIP IS 'Represents the Members relationship to the subscriber.';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.FAM_LINK_ID IS ' An Auto genrated number common to members of a family unit.';
COMMENT ON COLUMN COMPACT.DIM_MEMBER.IEX_MEMB_ID IS 'This is the members Standard Unique Health Identifier (Standard Unique Health ID).';

