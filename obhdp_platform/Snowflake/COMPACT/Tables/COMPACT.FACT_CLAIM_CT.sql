CREATE TABLE IF NOT EXISTS COMPACT.FACT_CLAIM (
COMPANY_DIM_ID INTEGER,
SEQ_CLAIM_ID VARCHAR(50),
LINE_NUMBER INTEGER,
SUB_LINE_CODE CHAR(1),
PRIMARY_SVC_DATE DATE DEFAULT TO_DATE('9/9/9999', 'mm/dd/yyyy'),
PLAN_DIM_ID INTEGER DEFAULT -1,
CLAIM_NUMBER VARCHAR(36),
CLAIM_TYPE_DIM_ID INTEGER DEFAULT -1,
IO_FLAG_DIM_ID INTEGER DEFAULT -1,
CLAIM_STATUS_DIM_ID INTEGER DEFAULT -1,
PROCESSING_STATUS_DIM_ID INTEGER DEFAULT -1,
MED_DEF_CODE_DIM_ID INTEGER DEFAULT -1,
POST_DATE DATE DEFAULT TO_DATE('9/9/9999', 'mm/dd/yyyy'),
CHECK_DATE DATE,
CHECK_NUMBER VARCHAR(16),
CHECK_NUMBER_2 VARCHAR(15),
CPT_CODE_DIM_ID INTEGER DEFAULT -1,
CPT_MODIFIER_DIM_ID INTEGER DEFAULT -1,
REVENUE_CODE_DIM_ID INTEGER DEFAULT -1,
DATE_RECEIVED DATE DEFAULT TO_DATE('9/9/9999', 'mm/dd/yyyy'),
ADJUD_DATE DATE,
DETAIL_SVC_DATE DATE DEFAULT TO_DATE('9/9/9999', 'mm/dd/yyyy'),
SVC_TO_DATE DATE,
PROV_DIM_ID INTEGER DEFAULT -1,
MEMB_DIM_ID INTEGER DEFAULT -1,
ALLOWED_REASON_DIM_ID INTEGER DEFAULT -1,
NOT_COVERED_REASON_DIM_ID INTEGER DEFAULT -1,
ADJUSTMENT_REASON_DIM_ID INTEGER DEFAULT -1,
CLAIM_THRU_DATE DATE,
MEMBER_AGE NUMBER(12,1),
PCP_DIM_ID INTEGER DEFAULT -1,
ATT_PROV_DIM_ID INTEGER DEFAULT -1,
PROV_SPEC_DIM_ID INTEGER DEFAULT -1,
PROV_TYPE_DIM_ID INTEGER DEFAULT -1,
PLACE_OF_SVC_1_DIM_ID INTEGER DEFAULT -1,
PLACE_OF_SVC_2_DIM_ID INTEGER DEFAULT -1,
PLACE_OF_SVC_3_DIM_ID INTEGER DEFAULT -1,
DIAGNOSIS_1_DIM_ID INTEGER DEFAULT -1,
DIAGNOSIS_2_DIM_ID INTEGER DEFAULT -1,
DIAGNOSIS_3_DIM_ID INTEGER DEFAULT -1,
DIAGNOSIS_4_DIM_ID INTEGER DEFAULT -1,
DIAGNOSIS_5_DIM_ID INTEGER DEFAULT -1,
DRG_CODE_DIM_ID INTEGER DEFAULT -1,
ICD_PRIM_DIAG VARCHAR(4),
ICD_FLAG VARCHAR(5),
ICD_PROC_1_DIM_ID INTEGER DEFAULT -1,
ICD_PROC_2_DIM_ID INTEGER DEFAULT -1,
ICD_PROC_3_DIM_ID INTEGER DEFAULT -1,
ICD_PROC_4_DIM_ID INTEGER DEFAULT -1,
ICD_PROC_5_DIM_ID INTEGER DEFAULT -1,
PAT_CONTROL_NO VARCHAR(38),
ACTUAL_ADMISSION_DATE DATE,
ADMISSION_DATE DATE,
ADMIT_HOUR NUMBER(2,0),
ADMIT_SOURCE_DIM_ID INTEGER DEFAULT -1,
ADMIT_TYPE VARCHAR(1),
ADMITTING_DIAGNOSIS_DIM_ID INTEGER DEFAULT -1,
DISCHARGE_HOUR NUMBER(2,0),
MEDICAL_RECORD_NO VARCHAR(30),
EPSDT_IND VARCHAR(1),
HCPCS_MODIFIER_1_DIM_ID INTEGER DEFAULT -1,
HCPCS_MODIFIER_2_DIM_ID INTEGER DEFAULT -1,
HCPCS_MODIFIER_3_DIM_ID INTEGER DEFAULT -1,
HCPCS_MODIFIER_4_DIM_ID INTEGER DEFAULT -1,
INVALID_DATA_IND VARCHAR(1),
STATE_SUBMIT_DATE DATE,
EXTERNAL_CRN VARCHAR(15),
BILL_TYPE_DIM_ID INTEGER DEFAULT -1,
LINE_OF_BUSINESS_DIM_ID INTEGER DEFAULT -1,
PATIENT_STATUS VARCHAR(2),
DIAMOND_INSERT_DATE DATE,
VEND_DIM_ID INTEGER DEFAULT -1,
MEDIA_CODE_DIM_ID INTEGER DEFAULT -1,
AUTO_ADJUD_FLAG_DIM_ID INTEGER DEFAULT -1,
EDI_FLAG_DIM_ID INTEGER DEFAULT -1,
ER_FLAG_DIM_ID INTEGER DEFAULT -1,
UNCLEAN_FLAG_DIM_ID INTEGER DEFAULT -1,
QUANTITY NUMBER(15,1) DEFAULT 0,
OTHER_CARRIER_AMT NUMBER(18,2) DEFAULT 0,
BILLED_AMT NUMBER(18,2) DEFAULT 0,
ALLOWED_AMT NUMBER(18,2) DEFAULT 0,
NOT_COVERED_AMT NUMBER(18,2) DEFAULT 0,
NET_AMT NUMBER(18,2) DEFAULT 0,
PAID_NET_AMT NUMBER(18,2) DEFAULT 0,
DW_INSERT_DATETIME DATE,
DW_UPDATE_DATETIME DATE,
INTEREST_AMT NUMBER(18,2) DEFAULT 0,
WHOLE_CLAIM_STATUS_DIM_ID INTEGER DEFAULT -1,
PROVIDER_PAR_DIM_ID INTEGER DEFAULT -1,
COPAYMENT_1_AMT NUMBER(18,2) DEFAULT 0,
COPAYMENT_2_AMT NUMBER(18,2) DEFAULT 0,
DEDUCTIBLE_AMT NUMBER(18,2) DEFAULT 0,
CALC_ALLO_PAID_NET_AMT NUMBER(18,2) DEFAULT 0,
CALC_ALLO_LINE_NUMBER INTEGER DEFAULT 0,
COS_STL_4_DIM_ID INTEGER DEFAULT -1,
CONVERTED_FROM_DIM_ID INTEGER DEFAULT -1,
RX_GENERIC_BRAND_IND_DIM_ID INTEGER DEFAULT -1,
RX_SUPPLY_DAYS INTEGER DEFAULT 0,
CALC_ALLO_METHOD INTEGER,
CALC_VISITS INTEGER DEFAULT 0,
CALC_UNITS NUMBER(12,1) DEFAULT 0,
CALC_PROCEDURES INTEGER DEFAULT 0,
CALC_DAYS INTEGER DEFAULT 0,
CALC_ADMITS INTEGER DEFAULT 0,
DTL_COS_STL_4_DIM_ID INTEGER DEFAULT -1,
GROUP_DIM_ID INTEGER DEFAULT -1,
PRODUCT_DIM_ID INTEGER DEFAULT -1,
DIAGNOSIS_6_DIM_ID INTEGER DEFAULT -1,
DIAGNOSIS_7_DIM_ID INTEGER DEFAULT -1,
DIAGNOSIS_8_DIM_ID INTEGER DEFAULT -1,
DIAGNOSIS_9_DIM_ID INTEGER DEFAULT -1,
TOTAL_BILLED_AMT NUMBER(18,2) DEFAULT 0,
BATCH_NUMBER VARCHAR(9),
CALC_QUANTITY NUMBER(15,1) DEFAULT 0,
AUTH_NUMBER VARCHAR(16),
DUPLICATE_CLAIM_FLAG_DIM_ID INTEGER DEFAULT -1,
DISCOUNT_AMT NUMBER(18,2) DEFAULT 0,
WITHHOLD_AMT NUMBER(18,2) DEFAULT 0,
ORIG_CHECK_DATE DATE,
ORIG_CHECK_NUMBER VARCHAR(16),
RPS_FISCAL_DATE DATE,
COSMOS_CUST_SEG_DIM_ID INTEGER DEFAULT -1,
NDC_CODE_DIM_ID INTEGER DEFAULT -1,
RX_DISPENSING_FEE_AMT NUMBER(18,2),
RX_INGREDIENT_AMT NUMBER(18,2),
RX_FORMULARY_IND_DIM_ID INTEGER DEFAULT -1,
CARE_LEVEL_DIM_ID INTEGER,
DW_INSERT_PROCESS VARCHAR(12),
DW_UPDATE_PROCESS VARCHAR(12),
SYS1 VARCHAR(24),
SYS2 VARCHAR(24),
SYS3 VARCHAR(24),
SYS4 VARCHAR(24),
SYS5 VARCHAR(24),
SYS6 VARCHAR(60),
PLAN_LOB_DIM_ID INTEGER DEFAULT -1,
CONTRACT_DIM_ID INTEGER,
EOB_IND_DIM_ID NUMBER(38,0),
OC_ALLOWED_AMT NUMBER(18,2),
OC_PAID_REASON_DIM_ID NUMBER(38,0),
RX_DATE_PRESCRIPTION_WRITTEN DATE,
SEQ_AP_TRANS_ID NUMBER(9,0),
REF_PROV_DIM_ID NUMBER(38,0),
PCP_TYPE_DIM_ID NUMBER(38,0),
PCP_SPEC_DIM_ID NUMBER(38,0),
GL_TAG_DIM_ID NUMBER(38,0),
RX_PRESCRIPTION_NUMBER VARCHAR(12),
RX_ORIG_BILLED_AMT NUMBER(9,2),
ORIG_BATCH_NUMBER VARCHAR(18),
DIAGNOSIS_10_DIM_ID INTEGER,
DIAGNOSIS_11_DIM_ID INTEGER,
DIAGNOSIS_12_DIM_ID INTEGER,
DIAGNOSIS_13_DIM_ID INTEGER,
DIAGNOSIS_14_DIM_ID INTEGER,
DIAGNOSIS_15_DIM_ID INTEGER,
DIAGNOSIS_16_DIM_ID INTEGER,
DIAGNOSIS_17_DIM_ID INTEGER,
DIAGNOSIS_18_DIM_ID INTEGER,
CPT_MODIFIER_2_DIM_ID INTEGER,
CPT_MODIFIER_3_DIM_ID INTEGER,
CPT_MODIFIER_4_DIM_ID INTEGER,
COB_CARRIER_CODE_DIM_ID INTEGER,
EOB_ATTACHED VARCHAR(1),
ORIG_DRG_DIM_ID NUMBER(10,0),
ICD_PROC_6_DIM_ID INTEGER,
ICD_PROC_1_DATE DATE,
ICD_PROC_2_DATE DATE,
ICD_PROC_3_DATE DATE,
ICD_PROC_4_DATE DATE,
ICD_PROC_5_DATE DATE,
ICD_PROC_6_DATE DATE,
SOI_DIM_ID NUMBER(10,0),
ROM_DIM_ID NUMBER(10,0),
CLAIM_IMAGE_ID VARCHAR(30),
ORIG_SEQ_CLAIM_ID INTEGER,
DIAGNOSIS_1_POA VARCHAR(1),
DIAGNOSIS_2_POA VARCHAR(1),
DIAGNOSIS_3_POA VARCHAR(1),
DIAGNOSIS_4_POA VARCHAR(1),
DIAGNOSIS_5_POA VARCHAR(1),
DIAGNOSIS_6_POA VARCHAR(1),
DIAGNOSIS_7_POA VARCHAR(1),
DIAGNOSIS_8_POA VARCHAR(1),
DIAGNOSIS_9_POA VARCHAR(1),
DIAGNOSIS_10_POA VARCHAR(1),
DIAGNOSIS_11_POA VARCHAR(1),
DIAGNOSIS_12_POA VARCHAR(1),
DIAGNOSIS_13_POA VARCHAR(1),
DIAGNOSIS_14_POA VARCHAR(1),
DIAGNOSIS_15_POA VARCHAR(1),
DIAGNOSIS_16_POA VARCHAR(1),
DIAGNOSIS_17_POA VARCHAR(1),
DIAGNOSIS_18_POA VARCHAR(1),
FEE_SCHEDULE_DIM_ID INTEGER,
CLAIM_NUMBER_ADJ_FROM VARCHAR(36),
CLAIM_NUMBER_ADJ_TO VARCHAR(36),
BIRTH_WEIGHT VARCHAR(4),
RX_GPI_DIM_ID INTEGER,
RX_BILLING_ACCT_DIM_ID NUMBER,
MEDICARE_TYPE_DIM_ID INTEGER,
CONDITION_CODE_1_DIM_ID INTEGER,
CONDITION_CODE_2_DIM_ID INTEGER,
CONDITION_CODE_3_DIM_ID INTEGER,
CONDITION_CODE_4_DIM_ID INTEGER,
CONDITION_CODE_5_DIM_ID INTEGER,
CONDITION_CODE_6_DIM_ID INTEGER,
CONDITION_CODE_7_DIM_ID INTEGER,
CONDITION_CODE_8_DIM_ID INTEGER,
CONDITION_CODE_9_DIM_ID INTEGER,
CONDITION_CODE_10_DIM_ID INTEGER,
CONDITION_CODE_11_DIM_ID INTEGER,
ADJ_SEQ_CLAIM_ID INTEGER,
PRICER_BASE_REIMB_AMT NUMBER(18,2),
PRICER_OUTLIER_PAYMENTS_AMT NUMBER(18,2),
PRICER_ALT_LEVEL_CARE_PYMT_AMT NUMBER(18,2),
PRICER_TOTAL_REIMB_AMT NUMBER(18,2),
PRICER_OUTLIER_TYPE_DIM_ID INTEGER,
PRICER_AVERAGE_LOS NUMBER(10,4),
ADJUDICATION_METHOD_DIM_ID INTEGER,
EFT_TRANS_NUMBER VARCHAR(90),
ATT_PROV_NPI NUMBER(10,0),
PROVIDER_NPI NUMBER(10,0),
DIAGNOSIS_19_DIM_ID INTEGER,
DIAGNOSIS_20_DIM_ID INTEGER,
DIAGNOSIS_21_DIM_ID INTEGER,
DIAGNOSIS_22_DIM_ID INTEGER,
DIAGNOSIS_23_DIM_ID INTEGER,
DIAGNOSIS_24_DIM_ID INTEGER,
DIAGNOSIS_25_DIM_ID INTEGER,
DIAGNOSIS_19_POA VARCHAR(1),
DIAGNOSIS_20_POA VARCHAR(1),
DIAGNOSIS_21_POA VARCHAR(1),
DIAGNOSIS_22_POA VARCHAR(1),
DIAGNOSIS_23_POA VARCHAR(1),
DIAGNOSIS_24_POA VARCHAR(1),
DIAGNOSIS_25_POA VARCHAR(1),
ICD_PROC_7_DIM_ID INTEGER,
ICD_PROC_8_DIM_ID INTEGER,
ICD_PROC_9_DIM_ID INTEGER,
ICD_PROC_10_DIM_ID INTEGER,
ICD_PROC_11_DIM_ID INTEGER,
ICD_PROC_12_DIM_ID INTEGER,
ICD_PROC_13_DIM_ID INTEGER,
ICD_PROC_14_DIM_ID INTEGER,
ICD_PROC_15_DIM_ID INTEGER,
ICD_PROC_16_DIM_ID INTEGER,
ICD_PROC_17_DIM_ID INTEGER,
ICD_PROC_18_DIM_ID INTEGER,
ICD_PROC_19_DIM_ID INTEGER,
ICD_PROC_20_DIM_ID INTEGER,
ICD_PROC_21_DIM_ID INTEGER,
ICD_PROC_22_DIM_ID INTEGER,
ICD_PROC_23_DIM_ID INTEGER,
ICD_PROC_24_DIM_ID INTEGER,
ICD_PROC_25_DIM_ID INTEGER,
ICD_PROC_7_DATE DATE,
ICD_PROC_8_DATE DATE,
ICD_PROC_9_DATE DATE,
ICD_PROC_10_DATE DATE,
ICD_PROC_11_DATE DATE,
ICD_PROC_12_DATE DATE,
ICD_PROC_13_DATE DATE,
ICD_PROC_14_DATE DATE,
ICD_PROC_15_DATE DATE,
ICD_PROC_16_DATE DATE,
ICD_PROC_17_DATE DATE,
ICD_PROC_18_DATE DATE,
ICD_PROC_19_DATE DATE,
ICD_PROC_20_DATE DATE,
ICD_PROC_21_DATE DATE,
ICD_PROC_22_DATE DATE,
ICD_PROC_23_DATE DATE,
ICD_PROC_24_DATE DATE,
ICD_PROC_25_DATE DATE,
RTCL_RATE_DIM_ID INTEGER,
ICD_VER_IND VARCHAR(1),
SYS10 VARCHAR(60),
SYS11 VARCHAR(60),
SYS12 VARCHAR(60),
SYS13 VARCHAR(200),
SYS14 VARCHAR(60),
REF_PROVIDER_NPI VARCHAR(10),
SOURCE_PRODUCT_DIM_ID INTEGER DEFAULT -1,
CLASS_DIM_ID INTEGER DEFAULT -1,
CLASS_PLAN_DIM_ID INTEGER DEFAULT -1,
BILLED_QUANTITY NUMBER(15,1),
EOB_DIM_ID INTEGER DEFAULT -1,
DTL_EOB_DIM_ID INTEGER DEFAULT -1,
PROV_AGREEMENT_DIM_ID INTEGER DEFAULT -1,
SDA_DIM_ID INTEGER DEFAULT -1,
SRC_SERVICE_NPI VARCHAR(10),
SRC_BILLING_NPI VARCHAR(10),
SRC_ADMITTING_NPI VARCHAR(10),
SRC_OPERATING_NPI VARCHAR(10),
DIAG_POINTER_1 VARCHAR(2),
DIAG_POINTER_2 VARCHAR(2),
DIAG_POINTER_3 VARCHAR(2),
DIAG_POINTER_4 VARCHAR(2),
DIAG_POINTER_5 VARCHAR(2),
DIAG_POINTER_6 VARCHAR(2),
DIAG_POINTER_7 VARCHAR(2),
DIAG_POINTER_8 VARCHAR(2),
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36),
CONSTRAINT PK_F_FACT_CLAIM PRIMARY KEY (SEQ_CLAIM_ID,COMPANY_DIM_ID,LINE_NUMBER,SUB_LINE_CODE),
CONSTRAINT FK_FACT_CLAIM_DIM_BILL_TYPE FOREIGN KEY (BILL_TYPE_DIM_ID) REFERENCES COMPACT.DIM_BILL_TYPE(BILL_TYPE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_CLASS FOREIGN KEY (CLASS_DIM_ID) REFERENCES COMPACT.DIM_CLASS(CLASS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_CLAIM_STATUS FOREIGN KEY (CLAIM_STATUS_DIM_ID) REFERENCES COMPACT.DIM_CLAIM_STATUS(CLAIM_STATUS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_CLAIM_TYPE FOREIGN KEY (CLAIM_TYPE_DIM_ID) REFERENCES COMPACT.DIM_CLAIM_TYPE(CLAIM_TYPE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_COMPANY FOREIGN KEY (COMPANY_DIM_ID) REFERENCES COMPACT.DIM_COMPANY(COMPANY_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_COS_STL_4_ID FOREIGN KEY (COS_STL_4_DIM_ID) REFERENCES COMPACT.DIM_COS_STL_4(COS_STL_4_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_COS_STL_4_DTL FOREIGN KEY (DTL_COS_STL_4_DIM_ID) REFERENCES COMPACT.DIM_COS_STL_4(COS_STL_4_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_ADMT FOREIGN KEY (ADMITTING_DIAGNOSIS_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_10 FOREIGN KEY (DIAGNOSIS_10_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_11 FOREIGN KEY (DIAGNOSIS_11_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_12 FOREIGN KEY (DIAGNOSIS_12_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_13 FOREIGN KEY (DIAGNOSIS_13_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_14 FOREIGN KEY (DIAGNOSIS_14_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_15 FOREIGN KEY (DIAGNOSIS_15_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_16 FOREIGN KEY (DIAGNOSIS_16_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_17 FOREIGN KEY (DIAGNOSIS_17_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_18 FOREIGN KEY (DIAGNOSIS_18_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_1 FOREIGN KEY (DIAGNOSIS_1_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_2 FOREIGN KEY (DIAGNOSIS_2_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_3 FOREIGN KEY (DIAGNOSIS_3_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_4 FOREIGN KEY (DIAGNOSIS_4_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_5 FOREIGN KEY (DIAGNOSIS_5_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_PROC_1 FOREIGN KEY (ICD_PROC_1_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_PROC_2 FOREIGN KEY (ICD_PROC_2_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_PROC_3 FOREIGN KEY (ICD_PROC_3_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_PROC_4 FOREIGN KEY (ICD_PROC_4_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_PROC_5 FOREIGN KEY (ICD_PROC_5_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DIAGNOSIS_PROC_6 FOREIGN KEY (ICD_PROC_6_DIM_ID) REFERENCES COMPACT.DIM_DIAGNOSIS(DIAGNOSIS_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_DRG_CODE FOREIGN KEY (DRG_CODE_DIM_ID) REFERENCES COMPACT.DIM_DRG_CODE(DRG_CODE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_MEMBER FOREIGN KEY (MEMB_DIM_ID) REFERENCES COMPACT.DIM_MEMBER(MEMB_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_NDC_CODE FOREIGN KEY (NDC_CODE_DIM_ID) REFERENCES COMPACT.DIM_NDC_CODE(NDC_CODE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PLACE_OF_SVC_1 FOREIGN KEY (PLACE_OF_SVC_1_DIM_ID) REFERENCES COMPACT.DIM_PLACE_OF_SVC(PLACE_OF_SVC_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PLACE_OF_SVC_2 FOREIGN KEY (PLACE_OF_SVC_2_DIM_ID) REFERENCES COMPACT.DIM_PLACE_OF_SVC(PLACE_OF_SVC_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PLACE_OF_SVC_3 FOREIGN KEY (PLACE_OF_SVC_3_DIM_ID) REFERENCES COMPACT.DIM_PLACE_OF_SVC(PLACE_OF_SVC_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PLAN FOREIGN KEY (PLAN_DIM_ID) REFERENCES COMPACT.DIM_PLAN(PLAN_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_CODE_CPT FOREIGN KEY (CPT_CODE_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_CODE(PROCEDURE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_CODE_RVN FOREIGN KEY (REVENUE_CODE_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_CODE(PROCEDURE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_MODIFIER_CPT_2 FOREIGN KEY (CPT_MODIFIER_2_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_MODIFIER(MODIFIER_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_MODIFIER_CPT_3 FOREIGN KEY (CPT_MODIFIER_3_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_MODIFIER(MODIFIER_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_MODIFIER_CPT_4 FOREIGN KEY (CPT_MODIFIER_4_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_MODIFIER(MODIFIER_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_MODIFIER_CPT_1 FOREIGN KEY (CPT_MODIFIER_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_MODIFIER(MODIFIER_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_MODIFIER_HCPCS_1 FOREIGN KEY (HCPCS_MODIFIER_1_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_MODIFIER(MODIFIER_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_MODIFIER_HCPCS_2 FOREIGN KEY (HCPCS_MODIFIER_2_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_MODIFIER(MODIFIER_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_MODIFIER_HCPCS_3 FOREIGN KEY (HCPCS_MODIFIER_3_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_MODIFIER(MODIFIER_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROCEDURE_MODIFIER_HCPCS_4 FOREIGN KEY (HCPCS_MODIFIER_4_DIM_ID) REFERENCES COMPACT.DIM_PROCEDURE_MODIFIER(MODIFIER_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PRODUCT FOREIGN KEY (PRODUCT_DIM_ID) REFERENCES COMPACT.DIM_PRODUCT(PRODUCT_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROVIDER_PCP FOREIGN KEY (PCP_DIM_ID) REFERENCES COMPACT.DIM_PROVIDER(PROV_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROVIDER_PROV FOREIGN KEY (PROV_DIM_ID) REFERENCES COMPACT.DIM_PROVIDER(PROV_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROVIDER_ATT FOREIGN KEY (ATT_PROV_DIM_ID) REFERENCES COMPACT.DIM_PROVIDER(PROV_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROVIDER_PAR_STAT FOREIGN KEY (PROVIDER_PAR_DIM_ID) REFERENCES COMPACT.DIM_PROVIDER_PAR_STAT(PROVIDER_PAR_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROVIDER_TYPE_SPEC FOREIGN KEY (PCP_SPEC_DIM_ID) REFERENCES COMPACT.DIM_PROVIDER_TYPE(PROV_TYPE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROVIDER_TYPE_PCP_TYEP FOREIGN KEY (PCP_TYPE_DIM_ID) REFERENCES COMPACT.DIM_PROVIDER_TYPE(PROV_TYPE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROVIDER_TYPE_SPEC_P FOREIGN KEY (PROV_SPEC_DIM_ID) REFERENCES COMPACT.DIM_PROVIDER_TYPE(PROV_TYPE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_PROVIDER_TYPE FOREIGN KEY (PROV_TYPE_DIM_ID) REFERENCES COMPACT.DIM_PROVIDER_TYPE(PROV_TYPE_DIM_ID),
CONSTRAINT FK_FACT_CLAIM_DIM_VENDORE FOREIGN KEY (VEND_DIM_ID) REFERENCES COMPACT.DIM_VENDOR(VEND_DIM_ID)
);
COMMENT ON COLUMN COMPACT.FACT_CLAIM.COMPANY_DIM_ID IS 'Represents the link to DIM_COMPANY table which stores the UHC Community and State Market. This field is part of the Unique Key (company_dim_id, seq_claim_id, line_number, sub_line_code) used to identify claims and may improve performance when querying Fact_Claim table.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SEQ_CLAIM_ID IS 'Represents an identifier generated when claims are inserted from the source system. This field is part of the Unique Key (company_dim_id, seq_claim_id, line_number, sub_line_code) used to identify claims and may improve performance when querying Fact_Claim table.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.LINE_NUMBER IS 'Represents the claim line number for a rendered service. This field is part of the Unique Key (company_dim_id, seq_claim_id, line_number, sub_line_code) used to identify claims and may improve performance when querying Fact_Claim table.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SUB_LINE_CODE IS 'Represents a one character code identifying the status of a claim at the line/detail level. A = Adjusted; R = Reversed;  If claim line is not A or R, original claim will be a single space " ". Remaining values may be used for line level adjustments to allow many versions of adjustment of the line.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PRIMARY_SVC_DATE IS 'Represents the earliest service date indicating when services rendered for the whole claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PLAN_DIM_ID IS 'Represents detailed specific member benefit plan information and is directly linked to DIM_PLAN table. Please refer to Dim Plan for specific descriptions.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CLAIM_NUMBER IS 'Represents the original unique identifier assigned to a claim from source system. Each claim number is specific to a Market/Plan or Company.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CLAIM_TYPE_DIM_ID IS 'Represents the link to DIM_CLAIM_TYPE table with descriptions that identify types of claims.  For example: Rx, Prof, Inst, etc. Please refer to Dim_Claim_Type for specific descriptions.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.IO_FLAG_DIM_ID IS 'Represents Inpatient/Outpatient Status. Values are 1 = Inpatient; 2 = Outpatient; -1 = N/A and 0 = UNK. Please reference DTL_COS_STL_4_DIM_ID for further details.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CLAIM_STATUS_DIM_ID IS 'Represents link to DIM_CLAIM_STATUS table describing the status of a claim detail line. This field can be used to identify paid/denied/adjusted claims. In addition, the DIM_CLAIM_STATUS table can be further identified and grouped with Claim Status types: C = Capitated; F = Fee for Service; X = External/CarveOut.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PROCESSING_STATUS_DIM_ID IS 'Represents the link to the DIM_PROCESSING_STATUS table describing the processing status of a claim line.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.MED_DEF_CODE_DIM_ID IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.POST_DATE IS 'Represents the final adjudicated date at the line level.  It is also the date the claim was finalized (paid/denied). ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CHECK_DATE IS 'Represents the print date for the check associated with the claim.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CHECK_NUMBER IS 'Represents the check number associated with the payable transaction.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CHECK_NUMBER_2 IS 'This field to be used for ETL Purposes only.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CPT_CODE_DIM_ID IS 'Represents the link to DIM_PROCEDURE_CODE table that stores the unique codes and/or descriptions for the current procedural terminology (CPT) or HCPCS code data. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CPT_MODIFIER_DIM_ID IS 'Represents the link to DIM_PROCEDURE_CODE table that describes procedures performed for the member or supplies.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.REVENUE_CODE_DIM_ID IS 'Represents the link to DIM_PROCEDURE_CODE table that describes claim revenue information.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DATE_RECEIVED IS 'Represents the date when the claim was received into the original claims source system.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ADJUD_DATE IS 'Represents the date the claim line was finalized (not necessarily paid) in the source system.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DETAIL_SVC_DATE IS 'Represents the first service date of the claim detail line.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SVC_TO_DATE IS 'Represents the last service date of the claim detail line. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PROV_DIM_ID IS 'Represents direct link from DIM_PROVIDER table that describes Provider information (Service, Billing, Admitting, Operating).';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.MEMB_DIM_ID IS 'Represents direct link to DIM_MEMBER table which stores all member information.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ALLOWED_REASON_DIM_ID IS 'Represents the link to DIM_REASON_CODE table explaining why the claim line was allowed while processing. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.NOT_COVERED_REASON_DIM_ID IS 'Represents the link to DIM_REASON_CODE table explaining why the claim line was not covered while processing.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ADJUSTMENT_REASON_DIM_ID IS 'Represents the link to DIM_REASON_CODE table explaining why the claim line was adjusted while processing. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CLAIM_THRU_DATE IS 'Represents the last date of the claim at the header level.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.MEMBER_AGE IS 'Represents the member''s age at the time the service was rendered.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PCP_DIM_ID IS 'Represents the link to DIM_PROVIDER table which describes the Primary Care Provider (PCP) assigned to the member for the service provided. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ATT_PROV_DIM_ID IS 'Represents the link to DIM_PROVIDER table describing the Attending Provider (Institutional). For pharmacy claims, this field contains the prescribing provider information.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PROV_SPEC_DIM_ID IS 'Represents the link to DIM_PROVIDER_TYPE table which describes the type or specialty of the provider. i.e. Obstetrics , Optometry etc. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PROV_TYPE_DIM_ID IS 'Represents the link to DIM_PROVIDER_TYPE table which describes the type or specialty of the provider. i.e. Obstetrics , Optometry etc. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PLACE_OF_SVC_1_DIM_ID IS 'Represents the link to DIM_PLACE_OF_SERVICE table describing the place where services were rendered. i.e. Emergency Room, Clinic, Birthing Center and Ambulance. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PLACE_OF_SVC_2_DIM_ID IS 'Represents the link to DIM_PLACE_OF_SERVICE table describing the place where services were rendered. i.e. Emergency Room, Clinic, Birthing Center and Ambulance. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PLACE_OF_SVC_3_DIM_ID IS 'Represents the link to DIM_PLACE_OF_SERVICE table describing the place where services were rendered. i.e. Emergency Room, Clinic, Birthing Center and Ambulance. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_1_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_2_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_3_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_4_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_5_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DRG_CODE_DIM_ID IS 'Represents the link to DIM_DRG_CODE table describing Diagnostic Related Group codes.  SMART uses a tool called All Patients Refined Diagnosis Related Grouper (APR DRG-version 35.0) for medical and behavioral health (Institutional) claims. The APR DRG tool classifies patients according to their reason of admission, severity of illness, and risk of mortality.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PRIM_DIAG IS 'Represents a Legacy field. No longer used.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_FLAG IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_1_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_2_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_3_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_4_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_5_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PAT_CONTROL_NO IS 'Patient control number is the number assigned to the patient by the billing office of the hospital or the provider. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ACTUAL_ADMISSION_DATE IS 'Represents the actual admission date for the initial episode when the patient is first admitted to the hospital for inpatient care regardless of type of care (Institutional only).  Default value is 9/9/999. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ADMISSION_DATE IS 'Represents the date on which an inpatient or day case admission occurs (Institutional only). If actual admission date is available, use Actual Admission Date. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ADMIT_HOUR IS 'Represents the hour information when a patient is admitted for inpatient care (Institutional only) when available. Rarely populated.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ADMIT_SOURCE_DIM_ID IS 'Represents the link to DIM_ADMIT_SOURCE table describing the reason a member was admitted to the hospital (Institutional only).';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ADMIT_TYPE IS 'Represents the type of admission into the hospital. Valid values are: 1 - Emergency; 2 - Urgent; 3 - Elective; 4 - Newborn (Institutional only).';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ADMITTING_DIAGNOSIS_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the initial diagnosis of an admitting patient.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DISCHARGE_HOUR IS 'Represents the Hour information when the member was discharged from inpatient care (Institutional only). Rarely populated.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.MEDICAL_RECORD_NO IS 'Represents the medical record number for institutional claims. Rarely populated.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.EPSDT_IND IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.HCPCS_MODIFIER_1_DIM_ID IS 'Represents the link to DIM_PROCEDURE_MODIFIER table describing a modifier that works in conjunction with the procedures performed on the member or supplies. Modifiers are any codes used to enhance the description of a service or supply under certain circumstances. The table also includes an Anesthesia indicator and an Informational Flag of Y or N. (Professional Only).  Procedure modifier table (CPT, HCPS). ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.HCPCS_MODIFIER_2_DIM_ID IS 'Represents the link to DIM_PROCEDURE_MODIFIER table describing a modifier that works in conjunction with the procedures performed on the member or supplies. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. The table also includes an Anesthesia indicator and an Informational Flag of Y or N. (Professional Only).  Procedure modifier table (CPT, HCPS). ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.HCPCS_MODIFIER_3_DIM_ID IS 'Represents the link to DIM_PROCEDURE_MODIFIER table describing a modifier that works in conjunction with the procedures performed on the member or supplies. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. The table also includes an Anesthesia indicator and an Informational Flag of Y or N. (Professional Only).  Procedure modifier table (CPT, HCPS). ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.HCPCS_MODIFIER_4_DIM_ID IS 'Represents the link to DIM_PROCEDURE_MODIFIER table describing a modifier that works in conjunction with the procedures performed on the member or supplies. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. The table also includes an Anesthesia indicator and an Informational Flag of Y or N. (Professional Only).  Procedure modifier table (CPT, HCPS). ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.INVALID_DATA_IND IS 'Represents a Legacy field. No longer used.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.STATE_SUBMIT_DATE IS 'Represents the Date when claim was submitted to the state. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.EXTERNAL_CRN IS 'Represents the claim reference number used by the Provider for billing the member. It could be different from the claim number and column is dependent on the source. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.BILL_TYPE_DIM_ID IS 'Represents the link to DIM_BILL_TYPE table describing plan defined billing types used to determine the correct Level of Service that will be used to bill the insurance. Types of indicator include: Inpatient = I or Outpatient =  O. Institutional claim only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.LINE_OF_BUSINESS_DIM_ID IS 'Represents the link to DIM_LINE_OF_BUSINESS table describing UHC Community and State created codes for every line of business used by the plans. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PATIENT_STATUS IS 'Represents the status code of the Patient. These standard codes are taken from the Centers for Medicare and Medicaid Services (CMS). Codes include: 01 - Discharge to home or self-care; 02 - Discharge/transfer to short term hospital; 03 - Discharge/transfer to SNF; 04 - Discharge/transfer to ICF; 05 - Discharge/transfer to other institution; 06 - Discharge to home health services; 07 - Left against medical advice; 08 - Discharge to home, IV (intravenous);  09 - Admitted as patient; 10 19 (except 12)  - Discharged - Reason Defined at the State (RDS); 20 - Patient died; 21,23 and 28 - Expired, (RDS); 30 - Still patient; 31 - Admitted, first interim bill (Institutional Only); 32 - Still patient, (RDS); 40 - Expired at home (Medicare/Champus - Hospice Only); 41 - Expired at a medical facility (Medicare/Champus - Hospice Only); 42 -Expired - place unknown (Medicare/Champus - Hospice Only); 43 - Discharged/transferred to Federal Hospital; 50 - Discharge to Hospice - Home; 51 - Discharge to Hospice - Medical Facility; 61 - Discharged/transferred within institution to a Medicare appvd swing bed; 62 - Discharged/transferred to another rehab facility incl rehab distinct part units of a hosp; 63 - Discharged/transferred to a long term care hospital; 66 -Discharged/transferred to critical access Hospital; 70 - Discharged/transferred to other type of Healthcare Institution not elsewhere defined; 71 - Discharged/transferred/referred to another institution for outp svcs;  Please refer to the standard CMS Patient Discharge Status Codes. In addition,  a two-digit code that identifies where the patient is at the conclusion of a health care facility encounter.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAMOND_INSERT_DATE IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.VEND_DIM_ID IS 'Represents the link to DIM_VENDOR table describing Provider payer information such as address, phone number, vendor type, etc. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.MEDIA_CODE_DIM_ID IS 'Represents the link to DIM_MEDIA_CODE table describing the data outside media sources. Used for Vendor claims such as Vision, Dental, and Transportation.   ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.AUTO_ADJUD_FLAG_DIM_ID IS 'Represents link to DIM_AUTO_ADJUD_FLAG table describing whether the claim is adjudicated  by 1 = Auto-Adjudicated or 2 = Manual Adjudication.   ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.EDI_FLAG_DIM_ID IS 'Represents the link to DIM_EDI_FLAG table the Electronic Data Interchange flag describing how a claim is received. Values include:  1 = EDI, 2 = Paper, 0 = Unknown or -1 = N/A.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ER_FLAG_DIM_ID IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.UNCLEAN_FLAG_DIM_ID IS 'Represents the link to DIM_UNCLEAN_FLAG table describing whether the claim was in hold status of 1 = Unclean or 2 = Clean where the claim was never held.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.QUANTITY IS 'Represents a value or number indicating the quantity or unit of service provided. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.OTHER_CARRIER_AMT IS 'Represents the amount for the claim line that has been paid by another insurance carrier. The net amount of the claim detail line is reduced by this value.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.BILLED_AMT IS 'Represents charges or Billed Amount for each detail line of the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ALLOWED_AMT IS 'Represents the amount the insurance will allow for the service rendered.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.NOT_COVERED_AMT IS 'Represents the amount the insurance declines for the service rendered.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.NET_AMT IS 'Represents the calculated net monetary amount to be paid for this claim line. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PAID_NET_AMT IS 'Represents the amount paid for the claim line.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DW_INSERT_DATETIME IS 'Represents the date when the record was added to the Data Warehouse.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DW_UPDATE_DATETIME IS 'Represents the  date when the record was updated in the Data Warehouse.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.INTEREST_AMT IS 'Represents the interest amount for the claim line for any delay in the claim payment by the insurance company. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.WHOLE_CLAIM_STATUS_DIM_ID IS 'Represents the link to DIM_WHOLE_CLAIM_STATUS table describing if the entire claim is 1 = P Paid, 2 = D Denied, 0 = U Unknown or -1 = N/A.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PROVIDER_PAR_DIM_ID IS 'Represents the link to DIM_PROVIDER_PAR_STAT table describing if the provider on the claim is a participant in the health insurance plan. Values are 1 = P Participating, 2 = D Non-par, 0 = U Unknown or -1 = N/A.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.COPAYMENT_1_AMT IS 'Represents the co-payment amount based on the member benefit package. A copayment (or copay) is a fixed-dollar amount that a member pays each time for certain services. Coinsurance is a percent of the cost of your care. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.COPAYMENT_2_AMT IS 'Represents the second co-payment or coinsurance amount based on the member benefit package. Coinsurance is a percent of the cost of your care. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DEDUCTIBLE_AMT IS 'Represents the Member Out of Pocket amount paid towards deductible.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CALC_ALLO_PAID_NET_AMT IS 'Represents SMART logic used for Whole Claim Pricing (WCP) Paid Amounts. WCP paid amounts have been allocated from case-rate payments in the source system to the individual claim lines. For regular claims, this equals the Paid Net Amt.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CALC_ALLO_LINE_NUMBER IS 'Represents the SMART calculated paid amount for each line number allocated from (Whole Claim Pricing claims only). ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.COS_STL_4_DIM_ID IS 'Represents the SMART COS which links to DIM_COS_STL_4 table describing categories of services per detail line on the claim. Claim lines can be categorized as Inpatient, Outpatient, Physician or Pharmacy and then further categorized by a combination of Bill type, DRG code, ICD9 diagnosis, Procedure code, Revenue code and/or Provider Specialty. There are four levels of categorization and all the category tables link to each other. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONVERTED_FROM_DIM_ID IS 'Represents the link to DIM_CONVERTED_FROM table which describes a numeric value where the source data is being pull in SMART such as CSP Facets, Diamond System or UHC Galaxy/Cosmos. Please refer to dim_converted_from for further details. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_GENERIC_BRAND_IND_DIM_ID IS 'Represents the link to DIM_RX_GENERIC_BRAND_IND table describing if Prescription drugs are generic for Pharmacy claims. Values include 1 = 0 - Multi-source brand (more than one "branded" version of a drug available), 2 = 1 - Single-source brand(a brand-name drug only, no generic(s) available) , 3 = 2 - Generic, 0 = U - Unknown or -1 = - N/A.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_SUPPLY_DAYS IS 'Represents the number of days the supply of the dispensed medication will last. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CALC_ALLO_METHOD IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CALC_VISITS IS 'Represents the Data Mart calculated field for Outpatient or Physician visits only, set to 1 if claim is considered a Visit.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CALC_UNITS IS 'Represents the Data Mart calculated field for Outpatient or Physician units only.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CALC_PROCEDURES IS 'Represents the Data Mart calculated field for Outpatient or Physician only indicating an Outpatient or Physician Procedure count. (OP/Phys records only).';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CALC_DAYS IS 'Represents the Data Mart calculated field for Inpatient claims only which equals the number of days.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CALC_ADMITS IS 'Represents the Data Mart calculated field for inpatient claims admissions only, set to 1 if claim is considered an admission.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DTL_COS_STL_4_DIM_ID IS 'Represents the  link to DIM_COS_STL_4 table describing categories of services per detail line on the claim. Claim lines can be categorized as Inpatient, Outpatient, Physician or Pharmacy and then further categorized by a combination of Bill type, DRG code, ICD9 diagnosis, Procedure code, Revenue code and/or Provider Specialty. There are four levels of categorization and all the category tables link to each other.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.GROUP_DIM_ID IS 'Represents the link to DIM_GROUP table which describes the group the member/claim belongs to. Groups may consist of Medicaid, Medicare, CHIP or LTC. Groups are selected in conjunction with the COMPANY_DIM_ID. This field is also used to include configuration information by the source. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PRODUCT_DIM_ID IS 'Represents the link to DIM_PRODUCT table describing the Plan Product or Program. i.e. CHIP(Kids Care), FHP, SSI w/MCR and SSI w/o MCR. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_6_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_7_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_8_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_9_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). Diagnosis_code represents the ICD (International Classification of diseases) code number for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.TOTAL_BILLED_AMT IS 'Represents the total amount billed for the entire claim. It is equal to the sum of all the rows in the BILLED_AMT column. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.BATCH_NUMBER IS 'Represents a system assigned number to identify and track claims that are processed in batches. Not always populated.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CALC_QUANTITY IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.AUTH_NUMBER IS 'Represents the Authorization number that exists on the clinical/case management processing system/source system such as ICUE, CareOne, etc.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DUPLICATE_CLAIM_FLAG_DIM_ID IS 'Represents the link to DIM_DUPLICATE_CLAIM_FLAG table describing claim duplication flag. 1 = Not Duplicate, 2 = Duplicate, 0 = Unknown or -1 = N/A Not currently used.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DISCOUNT_AMT IS 'Represents the discount amount for the specific claim line. Used for payments made against Institutional vendors with negative balances. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.WITHHOLD_AMT IS 'Represents the dollar value of the withhold amount based on any withhold percentage for the procedure code or a withhold percentage defined on the Provider contract. In addition, this amount only applies to sequestration reduction amount or any Fee-for-Service payments to the Provider to cover performance guarantees or as incentives. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ORIG_CHECK_DATE IS 'Represents the Original check date for re-issued  checks. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ORIG_CHECK_NUMBER IS 'Represents the Original check number for re-issued checks. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RPS_FISCAL_DATE IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.COSMOS_CUST_SEG_DIM_ID IS 'Represents the link to DIM_COSMOS_CUST_SEG table for COSMOS claims. Please refer to DIM_COSMOS_CUST_SEG table for details. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.NDC_CODE_DIM_ID IS 'Represents the link to DIM_NDC_CODE table describing National Drug Codes established by the Food and Drug Administration to identify the labeler, product number, and package sizes of FDA-approved prescription drugs. The prescription drugs are further classified into specific therapeutic classifications for AHFS, Medco, Standard, Generic and Spec.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_DISPENSING_FEE_AMT IS 'Represents the dispensing fee amount for the pharmacy claim ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_INGREDIENT_AMT IS 'Represents the prescription ingredient amount for the pharmacy claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_FORMULARY_IND_DIM_ID IS 'Represents the link to DM_RX_FORMULARY_IND table that indicates if the Pharmacy claim prescription is on a list or formulary. Values include 0 = U UNKNOWN, 1 = Y Formulary, 2 = N Non-Formulary or -1 = - N/A.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CARE_LEVEL_DIM_ID IS 'Represent the link to DIM_CARE_LEVEL table listing the level of care needed for the patient. Applies to MSO members only.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DW_INSERT_PROCESS IS 'Represents descriptive data used by the Data Warehouse team during the insert process. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DW_UPDATE_PROCESS IS 'Represents date the record was updated in the Data Warehouse. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS1 IS 'Represents field to be used for ETL Purposes only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS2 IS 'Represents field to be used for ETL Purposes only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS3 IS 'Represents field to be used for ETL Purposes only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS4 IS 'Represents field to be used for ETL Purposes only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS5 IS 'Represents field to be used for ETL Purposes only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS6 IS 'Represents field to be used for ETL Purposes only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PLAN_LOB_DIM_ID IS 'Represents the link to DIM_PLAN_LOB table that brings the plan line of business and programs together this includes links to DIM_LINE_OF_BUSINESS, DIM_PLAN and DIM_PLAN_PROGRAM tables. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONTRACT_DIM_ID IS 'Represents the link to the DIM_PROVIDER_CONTRACT table which contains all provider contract information including contract type, provider line of business, contract effective and term dates, term reasons, primary care provider flag, tax id, vendor link and provider contract id.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.EOB_IND_DIM_ID IS 'Represents the link to the DIM_EOB_IND table which indicates if there was an Explanation of Benefits attached to the claim with values as 1 = N, 0 = N/A or -1 = Unknown. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.OC_ALLOWED_AMT IS 'Represents  Other Carrier or insurer allowed amount.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.OC_PAID_REASON_DIM_ID IS 'Represents the link to DIM_REASON_CODE table explaining why the claim was paid by the other carrier or insurance.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_DATE_PRESCRIPTION_WRITTEN IS 'Represents the prescription date for the pharmacy claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SEQ_AP_TRANS_ID IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.REF_PROV_DIM_ID IS 'Represents the link to DIM_PROVIDER table describing the Referring Provider listed on the claim. Link to the DIM_PROIVDER using the PROV_DIM_ID.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PCP_TYPE_DIM_ID IS 'Represents the link to DIM_PROVIDER_TYPE describing the type or specialty of the Primary Care Provider. i.e. Obstetrics, Optometry This table links to the DIM_PROVIDER using PROV_DIM_ID.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PCP_SPEC_DIM_ID IS 'Represents the link to DIM_PROVIDER_TYPE describing the type or specialty of the Primary Care Provider. i.e. Obstetrics, Optometry This table links to the DIM_PROVIDER using PROV_DIM_ID.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.GL_TAG_DIM_ID IS 'Represents the link to DIM_GL_TAG table describing the General Ledger tag values 1 - 50000 Physician Paid Claims, 2 = 50100 Outpatient Paid Claims, 3 = 50200 Inpatient Paid Claims, 4 = 50300 Pharmacy Paid Claims, 0 = N/A or -1 UNK unknown. Diamond only.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_PRESCRIPTION_NUMBER IS 'Represents the prescription number for the pharmacy claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_ORIG_BILLED_AMT IS 'Represents the pharmacy claim original billed amount. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ORIG_BATCH_NUMBER IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_10_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_11_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_12_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_13_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_14_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_15_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_16_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_17_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_18_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing the ICD code (International Classification of diseases) for classifying disease diagnosis for the claim. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CPT_MODIFIER_2_DIM_ID IS 'Represents the link to DIM_PROCEDURE_MODIFIER table describing a modifier that works in conjunction with the procedures performed on the member or supplies. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. The table also includes an Anesthesia indicator and an Informational Flag of Y or N. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CPT_MODIFIER_3_DIM_ID IS 'Represents the link to DIM_PROCEDURE_MODIFIER table describing a modifier that works in conjunction with the procedures performed on the member or supplies. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. The table also includes an Anesthesia indicator and an Informational Flag of Y or N. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CPT_MODIFIER_4_DIM_ID IS 'Represents the link to DIM_PROCEDURE_MODIFIER table describing a modifier that works in conjunction with the procedures performed on the member or supplies. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. The table also includes an Anesthesia indicator and an Informational Flag of Y or N. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.COB_CARRIER_CODE_DIM_ID IS 'Represents the Link to DIM_COB_CARRIER_CODE table describing Coordination of Benefits (COB). COB is the process of determining which health plan or insurance policy will pay first when a Medicaid consumer is covered by multiple health care insurers.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.EOB_ATTACHED IS 'Represents an indicator if the Explanation of Benefits has been attached to the claim.  Flag of:  Y = Yes; or N = No. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ORIG_DRG_DIM_ID IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_6_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_1_DATE IS 'ICD9 procedure date which corresponds to the ICD9 diagnosis code number. (Institutional only) ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_2_DATE IS 'ICD9 procedure date which corresponds to the ICD9 diagnosis code number. (Institutional only) ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_3_DATE IS 'ICD9 procedure date which corresponds to the ICD9 diagnosis code number. (Institutional only) ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_4_DATE IS 'ICD9 procedure date which corresponds to the ICD9 diagnosis code number. (Institutional only) ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_5_DATE IS 'ICD9 procedure date which corresponds to the ICD9 diagnosis code number. (Institutional only) ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_6_DATE IS 'ICD9 procedure date which corresponds to the ICD9 diagnosis code number. (Institutional only) ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SOI_DIM_ID IS 'Represents the link to the DIM_SEVERITY_OF_ILLNESS table describing the different levels of severity of illness a sub classification for the APRDRG. Values are 1 = Minor, 2 = Moderate, 3 = Major, 4 = Extreme, 0 UNK and -1 = N/A.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ROM_DIM_ID IS 'Represents the link to the DIM_RISK_OF_MORTALITY table describing the different levels of mortality risk sub calssification for the APRDRG. Values are 1 = Minor, 2 = Moderate, 3 = Major, 4 = Extreme, 0 UNK and -1 = N/A.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CLAIM_IMAGE_ID IS 'Paper claims may have a snapshot image of the HCFA or UB form attached to the claim. The claim image id number is stored here and seen by the processor on the MACESS software. (Diamond)';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ORIG_SEQ_CLAIM_ID IS 'Represents a Legacy field. No longer used.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_1_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_2_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_3_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_4_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_5_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_6_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_7_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_8_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_9_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_10_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_11_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_12_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_13_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_14_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_15_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_16_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_17_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_18_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.FEE_SCHEDULE_DIM_ID IS 'Represents the link to DIM_FEE_SCHEDULE table describing a plan created pricing schedule on a claim. i.e. SC Current Fee Schedule at 115%-pricing spec OB (SE115)';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CLAIM_NUMBER_ADJ_FROM IS 'Original claim number adjusted from.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CLAIM_NUMBER_ADJ_TO IS 'Claim number adjusted to.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.BIRTH_WEIGHT IS 'Newborn birth weight.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_GPI_DIM_ID IS 'Represents the link to DIM_RX_GPI table describing Pharmacy claims Generic Product Indicator code which can be up to 14-digits that identify a group of similar drugs. Each successive two digits of the code represents a more specific relationship of the drugs. Example, the first 2 digits indicate a general drug group. As 2-digit blocks are added to the selection, the group of drugs becomes more specific. The full 14-digits indicate a group of NDC numbers which are a specific drug, specific dose form and specific strength.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RX_BILLING_ACCT_DIM_ID IS 'Represents the link to company specific pharmacy billing account information.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.MEDICARE_TYPE_DIM_ID IS 'Represents the link to DIM_MEDICARE_TYPE describing the different types of Medicare i.e. Medicare Part B only and Medicare Part A only. Links to DIM_MEDICARE_GROUP and DIM_GROUP tables. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_1_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_2_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_3_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_4_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_5_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_6_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_7_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_8_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_9_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_10_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.CONDITION_CODE_11_DIM_ID IS 'Represents the link to the DIM_CONDITION_CODE table describing conditions or events that apply to an Institutional claim and is required for some billing situations. Condition Codes are not used for Medicare claims. i.e. Condition is Employment Related.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ADJ_SEQ_CLAIM_ID IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PRICER_BASE_REIMB_AMT IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PRICER_OUTLIER_PAYMENTS_AMT IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PRICER_ALT_LEVEL_CARE_PYMT_AMT IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PRICER_TOTAL_REIMB_AMT IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PRICER_OUTLIER_TYPE_DIM_ID IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PRICER_AVERAGE_LOS IS 'Represents a Legacy field. No longer used. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ADJUDICATION_METHOD_DIM_ID IS 'Represents the link to DIM_ADJUDICATION_METHOD table used for pricing and adjudicating the claim line. This can be a two-character field where first character indicates the pricing method and the second character indicates the adjudication method. A means automatic and M means manual.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.EFT_TRANS_NUMBER IS 'Electronic funds transfer (EFT) number for transferring of money from one account to another.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ATT_PROV_NPI IS 'For Institutional claims the Attending Provider National Provider ID. NPI is a Health Insurance Portability and Accountability ACT (HIPPA) Administrative Simplification Standard.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.PROVIDER_NPI IS 'The Provider''s National Provider ID. NPI is a Health Insurance Portability and Accountability ACT (HIPPA) Administrative Simplification Standard.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_19_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_20_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_21_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_22_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_23_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_24_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_25_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_19_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_20_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_21_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_22_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_23_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_24_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAGNOSIS_25_POA IS '(Optional) Diagnosis Present on Admission (POA) indicator includes Y - Diagnosis was present at time of inpatient admission, N - Diagnosis was not present at time of inpatient admission, U - Documentation insufficient to determine if condition was present at the time of inpatient admission, W - Clinically undetermined and 1 - Unreported/Not used. There is a POA for all 25 diagnosis codes. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_7_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_8_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_9_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_10_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_11_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_12_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_13_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_14_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_15_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_16_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_17_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_18_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_19_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_20_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_21_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_22_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_23_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_24_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_25_DIM_ID IS 'Represents the link to DIM_DIAGNOSIS table describing diagnosis(act of identifying a disease from its signs and symptoms). These codes are consolidated in the data mart into a single list of codes which are taken from ALL plans. In the Fact Claim table link the Diagnosis Codes or ICD9 Procedure Codes(International Classification of diseases, Ninth Revision) using DIAGNOSIS_DIM_ID to the DIM_DIAGNOSIS table. The table includes codes, descriptions, diagnosis class, patient gender, trauma flag, procedure class, diag MDC code and diagnosis firstname(basis code). Claims can have up to 25 Diagnosis or ICD9 Procedure codes. Diagnosis can be further classified into levels 1 and 2. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_7_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_8_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_9_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_10_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_11_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_12_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_13_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_14_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_15_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_16_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_17_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_18_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_19_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_20_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_21_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_22_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_23_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_24_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.ICD_PROC_25_DATE IS 'ICD9 procedure date that corresponds with the procedure codes numbered 1 - 25.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.RTCL_RATE_DIM_ID IS 'Represents the link to the DIM_RATECELL_RATE table.Rate cells are built based on the following information: age, gender, eligibility and medical status code.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS10 IS 'Represents field to be used for ETL Purposes only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS11 IS 'For OptumRx Claims - CONVERTED_FROM_DIM_ID - 63, it contains the HIOS ID -  Health Insurance Oversight System Identifier';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS12 IS 'Represents field to be used for ETL Purposes only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS13 IS 'For OptumRx Claims - CONVERTED_FROM_DIM_ID - 63, it contains Exchange Member fields delimited by | HIM_FROM_DATE,HIM_THRU_DATE,HIM_PLAN_ID,HIM_CSR_LEVEL,HIM_PLAN_METAL_IND,MEMBER_ETHNICITY_IND,MEMBER_APTC_IND,HIM_GRACE_PERIOD_EFF_DATE,HIM_GRACE_PERIOD_TERM_DATE,HIM_GRACE_PERIOD_IND';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SYS14 IS 'Represents field to be used for ETL Purposes only. ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.BILLED_QUANTITY IS 'Represents a number of units or quantity entered on the line item charged by the Provider directly from the claim source. This field is only populated for CSP claims.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.EOB_DIM_ID IS 'Represents Explanation Of Benefits (EOB) specific to CSP at the header level. Please refer to dim_reason_code for specific descriptions of EOB.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DTL_EOB_DIM_ID IS 'Represents Explanation Of Benefits (EOB) specific to CSP at the line level. Please refer to dim_reason_code for specific descriptions of EOB.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SRC_SERVICE_NPI IS 'Represents the National Provider Identifier(NPI) for Service Providers from the Source.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SRC_BILLING_NPI IS 'Represents the National Provider Identifier(NPI) for Billing Providers from the Source.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SRC_ADMITTING_NPI IS 'Represents the National Provider Identifier(NPI) for Admitting Providers from the Source.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.SRC_OPERATING_NPI IS 'Represents the National Provider Identifier(NPI) for Operating Providers from the Source.  ';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAG_POINTER_1 IS 'Primary Diagnosis Pointer; Diagnosis Pointers are used to link diagnosis codes to CPT codes/Procedure codes. They are only used in Professional Claims - sourced from Cosmos/CSP/CO.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAG_POINTER_2 IS 'First Additional Diagnosis Pointer; Diagnosis Pointers are used to link diagnosis codes to CPT codes/Procedure codes. They are only used in Professional Claims - sourced from Cosmos/CSP/CO.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAG_POINTER_3 IS 'Second Additional Diagnosis Pointer; Diagnosis Pointers are used to link diagnosis codes to CPT codes/Procedure codes. They are only used in Professional Claims - sourced from Cosmos/CSP/CO.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAG_POINTER_4 IS 'Third Additional Diagnosis Pointer; Diagnosis Pointers are used to link diagnosis codes to CPT codes/Procedure codes. They are only used in Professional Claims - sourced from Cosmos/CSP/CO.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAG_POINTER_5 IS 'Fourth Additional Diagnosis Pointer; Diagnosis Pointers are used to link diagnosis codes to CPT codes/Procedure codes. They are only used in Professional Claims - sourced from Cosmos/CSP/CO.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAG_POINTER_6 IS 'Fifth Additional Diagnosis Pointer; Diagnosis Pointers are used to link diagnosis codes to CPT codes/Procedure codes. They are only used in Professional Claims - sourced from Cosmos/CSP/CO.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAG_POINTER_7 IS 'Sixth Additional Diagnosis Pointer; Diagnosis Pointers are used to link diagnosis codes to CPT codes/Procedure codes. They are only used in Professional Claims - sourced from Cosmos/CSP/CO.';
COMMENT ON COLUMN COMPACT.FACT_CLAIM.DIAG_POINTER_8 IS 'Seventh Additional Diagnosis Pointer; Diagnosis Pointers are used to link diagnosis codes to CPT codes/Procedure codes. They are only used in Professional Claims - sourced from Cosmos/CSP/CO.';
