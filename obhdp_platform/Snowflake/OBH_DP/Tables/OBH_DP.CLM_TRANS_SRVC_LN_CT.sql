CREATE TABLE IF NOT EXISTS OBH_DP.CLM_TRANS_SRVC_LN CLUSTER BY (LEFT(CLM_TRANS_SYS_ID,7))(
CLM_TRANS_SYS_ID VARCHAR(100) NOT NULL CONSTRAINT PK_O_CLM_TRANS_SRVC_LN_SYS_ID PRIMARY KEY,
CLM_AUD_SRC_ID VARCHAR(50) NULL,
CLM_AUD_SRC_SYS_CD VARCHAR(8) NULL,
CLM_AUD_NBR VARCHAR(50) NULL,
CLM_AUD_LN_NBR VARCHAR(50) NULL,
CLM_AUD_TRANS_SEQ_NBR VARCHAR(1) NULL,
CLM_SRC_AUTH_NBR VARCHAR(50) NULL,
CLM_SRC_MBR_ACCT_NBR VARCHAR(30) NULL,
CLM_SRC_MBR_FAM_ID VARCHAR(20) NULL,
CLM_SRC_MBR_GDR_CD VARCHAR(20) NULL,
CLM_SRC_MBR_GRP_ID VARCHAR(20) NULL,
CLM_SRC_MBR_MKT_DERIV_NBR VARCHAR(30) NULL,
CLM_SRC_MBR_NON_UHG_EE_FLG VARCHAR(1) NULL,
CLM_SRC_MBR_PLN_ID VARCHAR(20) NULL,
CLM_SRC_MBR_PRDCT_CD VARCHAR(20) NULL,
CLM_SRC_MBR_REL_CD VARCHAR(2) NULL,
CLM_SRC_MBR_SBSCR_ID VARCHAR(15) NULL,
CLM_SRC_MBR_SYS_ID VARCHAR(39) NULL,
CLM_SRC_PROV_SRVC_CTY_NM VARCHAR(30) NULL,
CLM_SRC_PROV_SRVC_ST_CD VARCHAR(6) NULL,
CLM_SRC_PROV_SYS_ID VARCHAR(20) NULL,
CLM_TRANS_ADJ_CD VARCHAR(50) NULL,
CLM_TRANS_CLM_FM_TYP_CD VARCHAR(20) NULL,
CLM_TRANS_DIAG_1_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_1_CD_CATGY_LVL_1_DESC VARCHAR(100) NULL,
CLM_TRANS_DIAG_1_CD_TYP_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_2_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_2_CD_CATGY_LVL_1_DESC VARCHAR(100) NULL,
CLM_TRANS_DIAG_2_CD_TYP_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_3_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_3_CD_CATGY_LVL_1_DESC VARCHAR(100) NULL,
CLM_TRANS_DIAG_3_CD_TYP_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_4_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_4_CD_CATGY_LVL_1_DESC VARCHAR(100) NULL,
CLM_TRANS_DIAG_4_CD_TYP_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_5_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_5_CD_CATGY_LVL_1_DESC VARCHAR(100) NULL,
CLM_TRANS_DIAG_5_CD_TYP_CD VARCHAR(30) NULL,
CLM_TRANS_DIAG_MH_SA_CD VARCHAR(3) NULL,
CLM_TRANS_DIAG_SBSTNC_ABUS_FLG VARCHAR(1) NULL,
CLM_TRANS_DLOC_TXT VARCHAR(32) NULL,
CLM_TRANS_DLOC_SEQ_NBR VARCHAR(17) NULL,
CLM_TRANS_DLOC_CATGY_TXT VARCHAR(30) NULL,
CLM_TRANS_DT_ADJD_DT DATE NULL,
CLM_TRANS_DT_ENT_DT DATE NULL,
CLM_TRANS_DT_PD_DT DATE NULL,
CLM_TRANS_DT_RCVD_DT DATE NULL,
CLM_TRANS_DT_PST_DT DATE NULL,
CLM_TRANS_DT_SRVC_FROM_DT DATE NULL,
CLM_TRANS_DT_SRVC_THRU_DT DATE NULL,
CLM_TRANS_LVL_OF_SRVC_DERIV_CD VARCHAR(50) NULL,
CLM_TRANS_LVL_OF_SRVC_SRC_CD VARCHAR(25) NULL,
CLM_TRANS_MBR_IN_YR_AGE NUMBER NULL,
CLM_TRANS_PL_OF_SRVC_AMA_CD VARCHAR(16) NULL,
CLM_TRANS_PROC_MOD_CD VARCHAR(14) NULL,
CLM_TRANS_PROC_TYP_CD VARCHAR(8) NULL,
CLM_SRC_PROV_TAX_ID_NBR VARCHAR(15) NULL,
CLM_SRC_PROV_SRVC_ZIP_CD VARCHAR(15) NULL,
CLM_SRC_PROV_CLM_NPI_ID VARCHAR(10) NULL,
CLM_TRANS_RSN_TYP_CD VARCHAR(1) NULL,
CLM_TRANS_CLM_PRI_DIAG_CD VARCHAR(30) NULL,
CLM_TRANS_DT_ADMIT_DT DATE NULL,
CLM_TRANS_ADMIT_TYP_CD VARCHAR(2) NULL,
CLM_TRANS_ADMIT_SRC_CD VARCHAR(6) NULL,
CLM_TRANS_DT_CLM_FST_SRVC_DT DATE NULL,
CLM_TRANS_DT_CLM_LST_SRVC_DT DATE NULL,
CLM_TRANS_BILL_TYP_CD VARCHAR(4) NULL,
CLM_TRANS_DSCHRG_STS_CD VARCHAR(3) NULL,
CLM_TRANS_SYS_DIAG_REL_GRP_CD VARCHAR(6) NULL,
CLM_TRANS_ENT_DIAG_REL_GRP_CD VARCHAR(6) NULL,
CLM_TRANS_CLM_LVL_RSN_CD_DESC VARCHAR(254) NULL,
CLM_TRANS_EOB_RSN_CD_DESC VARCHAR(254) NULL,
CLM_TRANS_AUTH_NBR VARCHAR(50) NULL,
CLM_TRANS_SUPP_ORDR_IND VARCHAR(39) NULL,
CLM_SRC_PROV_PAYE_NM VARCHAR(55) NULL,
CLM_TRANS_DIAG_1_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_DIAG_2_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_DIAG_3_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_DIAG_4_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_DIAG_5_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_PROC_CD VARCHAR(10) NULL,
CLM_TRANS_PROC_DESC VARCHAR(250) NULL,
ICD_DIAG_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_RSN_DESC VARCHAR(500) NULL,
CLM_TRANS_DIAG_1_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_DIAG_2_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_DIAG_3_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_DIAG_4_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_DIAG_5_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_CLM_LVL_RSN_CD VARCHAR(12) NULL,
CLM_TRANS_PROC_MOD_2_CD VARCHAR(4) NULL,
CLM_TRANS_PROC_MOD_3_CD VARCHAR(4) NULL,
CLM_TRANS_PROC_MOD_4_CD VARCHAR(4) NULL,
CLM_TRANS_RVNU_CD VARCHAR(10) NULL,
CLM_TRANS_RVNU_DESC VARCHAR(250) NULL,
CLM_TRANS_SEC_PROC_CD VARCHAR(10) NULL,
CLM_TRANS_SEC_PROC_DESC VARCHAR(250) NULL,
CLM_TRANS_SEC_PROC_TYP_CD VARCHAR(10) NULL,
SRC_FINC_LIAB_CD VARCHAR(8) NULL,
DERIV_OBH_FINC_LIAB VARCHAR(5) NULL,
CLM_SRC_PROV_CLM_SRVC_NPI_ID VARCHAR(10) DEFAULT '' NULL,
CLM_SRC_PROV_CLM_REF_NPI_ID VARCHAR(10) DEFAULT '' NULL,
CLM_SRC_PROV_CLM_FAC_NPI_ID VARCHAR(10) DEFAULT '' NULL,
CLM_SRC_PROV_CLM_RNDR_NPI_ID VARCHAR(10) DEFAULT '' NULL,
CLM_SRC_PROV_CLM_BIL_NPI_ID VARCHAR(10) DEFAULT '' NULL,
CLM_SRC_PROV_CLM_PAYE_NPI_ID VARCHAR(10) DEFAULT '' NULL,
CLM_SRC_PROV_CLM_ADMIT_NPI_ID VARCHAR(10) DEFAULT '' NULL,
CLM_SRC_PROV_CLM_ATD_NPI_ID VARCHAR(10) DEFAULT '' NULL,
CLM_TRANS_RSN_CD VARCHAR(12) NULL,
CLM_TRANS_AGRMNT_ID VARCHAR(20) NULL,
CLM_TRANS_AGRMNT_DESC VARCHAR(100) NULL,
CLM_TRANS_RSN_TYP_DESC VARCHAR(20) NULL,
SRC_FINC_LIAB_DESC VARCHAR(30) NULL,
CLM_TRANS_DIAG_1_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
CLM_TRANS_DIAG_2_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
CLM_TRANS_DIAG_3_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
CLM_TRANS_DIAG_4_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
CLM_TRANS_DIAG_5_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
MBR_SYS_ID VARCHAR(39) NULL,
PERS_SYS_ID VARCHAR(250) NULL,
PKG_SYS_ID VARCHAR(20) NULL,
CLM_TRANS_ALLW_AMT NUMBER(18,2) NULL,
CLM_TRANS_ALLW_DERIV_AMT NUMBER(18,2) NULL,
CLM_TRANS_COB_AMT NUMBER(18,2) NULL,
CLM_TRANS_COINS_AMT NUMBER(18,2) NULL,
CLM_TRANS_CONTR_AMT NUMBER(18,2) NULL,
CLM_TRANS_COPAY_AMT NUMBER(18,2) NULL,
CLM_TRANS_DED_AMT NUMBER(18,2) NULL,
CLM_TRANS_DSALLW_AMT NUMBER(18,2) NULL,
CLM_TRANS_DSCNT_AMT NUMBER(18,2) NULL,
CLM_TRANS_MBR_RESP_AMT NUMBER(18,2) NULL,
CLM_TRANS_PD_AMT NUMBER(18,2) NULL,
CLM_TRANS_RSRV_AMT NUMBER(18,2) NULL,
CLM_TRANS_SBMT_AMT NUMBER(18,2) NULL,
CLM_TRANS_ALLWD_QTY NUMBER DEFAULT 0,
CLM_TRANS_BILL_QTY NUMBER DEFAULT 0,
CLM_TRANS_COV_VST_CNT NUMBER DEFAULT 0,
CLM_TRANS_COV_DAYS_CNT NUMBER DEFAULT 0,
CLM_TRANS_SRVC_UNT_CNT NUMBER DEFAULT 0,
CLM_TRANS_PROC_CNT NUMBER DEFAULT 0,
CLM_TRANS_ALLWD_ADMTS NUMBER DEFAULT 0,
SRC_CUST_CD VARCHAR(5),
SRC_CUST_NM VARCHAR(30),
CLM_TRANS_THER_CLASS_CD VARCHAR(8) NULL,
CLM_TRANS_THER_CLASS_DESC VARCHAR(100) NULL,
CLM_TRANS_PKG_CD VARCHAR(16) NULL,
CLM_TRANS_PKG_NM VARCHAR(30) NULL,
CLM_TRANS_MKT_PKG_CD VARCHAR(10) NULL,
CLM_TRANS_MKT_PKG_DESC VARCHAR(200) NULL,
CLM_TRANS_PLAN_CD VARCHAR(10) NULL,
CLM_TRANS_PLAN_DESC VARCHAR(70) NULL,
CLM_TRANS_PRDCT_TYP_DESC VARCHAR(8) NULL,
SRC_MBR_ID VARCHAR(12) NULL,
MEDCD_ID VARCHAR(20) NULL,
MEDCR_ID VARCHAR(20) NULL,
CLM_SRC_MBR_GRP_NAME VARCHAR(240) NULL,
CLM_TRANS_PL_OF_SRVC_AMA_DESC VARCHAR(60) NULL,
CLM_SRC_PROV_PAYE_TYP VARCHAR(1) NULL,
CLM_SRC_PROV_PAYE_TAX_ID VARCHAR(9) NULL,
CLM_TRANS_PROC_MOD_DESC VARCHAR(60) NULL,
CLM_TRANS_PROC_MOD_2_DESC VARCHAR(60) NULL,
CLM_TRANS_PROC_MOD_3_DESC VARCHAR(60) NULL,
CLM_TRANS_PROC_MOD_4_DESC VARCHAR(60) NULL,
CLM_TRANS_BILL_TYP_DESC VARCHAR(60) NULL,
CLM_TRANS_SYS_DIAG_REL_GRP_DESC VARCHAR(30) NULL,
CLM_TRANS_NDC_CD VARCHAR(12) NULL,
CLM_TRANS_NDC_GNRC_NM VARCHAR(100) NULL,
CLM_TRANS_COS_CD VARCHAR(4) NULL,
CLM_TRANS_COS_LVL1_DESC VARCHAR(20) NULL,
CLM_TRANS_COS_LVL2_DESC VARCHAR(20) NULL,
CLM_TRANS_COS_LVL3_DESC VARCHAR(20) NULL,
CLM_TRANS_COS_LVL4_DESC VARCHAR(40) NULL,
SRC_PROV_NTWK_CD VARCHAR(1) NULL,
SRC_PROV_NTWK_DESC VARCHAR(20) NULL,
CLM_SRVC_LN_STS_CD VARCHAR(1) NULL,
CLM_SRVC_LN_STS_DESC VARCHAR(30) NULL,
CLM_TRANS_PROC_CTGY_LVL_1_DESC VARCHAR(50) NULL,
CLM_TRANS_PROC_CTGY_LVL_2_DESC VARCHAR(500) NULL,
CLM_TRANS_PROC_CTGY_LVL_3_DESC VARCHAR(500) NULL,
CLM_PROV_EPIM_ID VARCHAR(20) NULL,
SRVC_PROV_EPIM_ID VARCHAR(20) NULL,
REF_PROV_EPIM_ID VARCHAR(20) NULL,
FAC_PROV_EPIM_ID VARCHAR(20) NULL,
RNDR_PROV_EPIM_ID VARCHAR(20) NULL,
BIL_PROV_EPIM_ID VARCHAR(20) NULL,
PAYEE_PROV_EPIM_ID VARCHAR(20) NULL,
ADM_PROV_EPIM_ID VARCHAR(20) NULL,
ATD_PROV_EPIM_ID VARCHAR(20) NULL,
IMDM_ID VARCHAR(10) NULL,
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36)
) ;

COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_SYS_ID IS 'Data warehouse unique surrogate key for a claim detail transaction.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_AUD_SRC_ID IS 'Uniquely identifies the record in the source claim system.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_AUD_SRC_SYS_CD IS 'Identifies source of the claim. .... COSMOS, UNET, RIOS.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_AUD_NBR IS 'An identification number assigned to each claim by the source system.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_AUD_LN_NBR IS 'A number referencing the line of the claim.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_AUD_TRANS_SEQ_NBR IS 'A system generated sequential number.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_AUTH_NBR IS 'Source System Authorization Number.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_ACCT_NBR IS 'Client Account Number (for Facets and PBH this is the Group ID)';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_FAM_ID IS 'A unique Id for family. A family consists of the subscriber and the subscribers dependents. Families roll up to a group. Family members select products that the group offers.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_GDR_CD IS 'The gender of the member.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_GRP_ID IS 'Derived from Fam ID format = 9 character subscriber ID - 5 character account ID - 3 character group ID.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_MKT_DERIV_NBR IS 'A market number generated by the data extract transform and load process within GALAXY which is created based upon the network to which the member belongs or the members zip code. For Unet Data the assignment is based on Members HMO ACCT Division (for HMO products only), CES Sold Market number or Zip code.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_NON_UHG_EE_FLG IS 'The flag used to indicate that the employee does not work for UHG.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_PLN_ID IS 'Unique number used to identify a grouping of specific details describing covered services and standard rules for a specific component.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_PRDCT_CD IS 'Identifies the product at the lowest level of detail.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_REL_CD IS 'Part of the Member Source System Key.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_SBSCR_ID IS 'Identifies the subscriber associated with the member.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_SYS_ID IS 'A sequence of numbers and/or characters used to identify the member in the downstream system.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_SRVC_CTY_NM IS 'City in which the services were rendered.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_SRVC_ST_CD IS 'Code for the state where the provider rendered the service';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_SYS_ID IS 'Number that is used to uniquely identify a provider in one of the source systems that submits a claim.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_ADJ_CD IS 'Indicates if any line item on the claim has been adjusted.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_CLM_FM_TYP_CD IS 'Determines whether the claim submission form is physician (2) or hospital (1).';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_1_CD IS 'The Diagnosis One Code represents the most important diagnosis (also known as Primary Diagnosis) responsible for the medical services. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_1_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_1_CD_TYP_CD IS 'Field denotes Diagnosis Description Type. (ICD9, DSM, EAP)';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_2_CD IS 'The Diagnosis Two Code represents the second most important diagnosis (also known as Secondary Diagnosis) responsible for the medical services. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_2_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_2_CD_TYP_CD IS 'Field denotes Diagnosis Description Type. (ICD9, DSM, EAP)';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_3_CD IS 'The Diagnosis Three Code represents the third most important diagnosis (also known as Tertiary Diagnosis) responsible for the medical services. Data Placement: left justified, space filled ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_3_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_4_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_5_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_3_CD_TYP_CD IS 'Field denotes Diagnosis Description Type. (ICD9, DSM, EAP)';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_MH_SA_CD IS 'Indicates if the primary diagnosis code is a MH or SA diagnosis. This column is populated from the MH SA indicator from the Diagnosis table. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_SBSTNC_ABUS_FLG IS 'Set to Y when any of the diagnosis codes has an MH/SA value of "SA" .';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DLOC_TXT IS 'Derived Level of Care (DLOC) description that is appropriate to the Claim transaction.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DLOC_SEQ_NBR IS 'Level of care hierarchy. One ("1") is the highest level of care.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DLOC_CATGY_TXT IS 'Level of care common category. A description representing a common level of care category across all source systems.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_ADJD_DT IS 'Most rececent adjudication date for any of the claim line items.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_ENT_DT IS 'The date of data entry into the source system.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_PD_DT IS 'Most recent paid date for this line item.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_RCVD_DT IS 'The date the claim was posted to the General Ledger';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_PST_DT IS 'Date the claim was posted to the general ledger. For RIOS this is the check date. (Valid Values: Valid date)';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_SRVC_FROM_DT IS 'From date of service for this line item.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_SRVC_THRU_DT IS 'Thru date of service for this line item.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_LVL_OF_SRVC_DERIV_CD IS 'Three letter code indicating a common service level code accross all source systems.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_LVL_OF_SRVC_SRC_CD IS 'Three letter code indicating a common service level code accross all source systems.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_MBR_IN_YR_AGE IS 'The age of the claimant as of the earliest service date on the claim. This field will be populated with the age of the claimant as if the earliest service date on the claim for COSMOS claims if the Benefit Contract Calendar Year Indicator (on the Customer Segment Coverage table) = N, indicating that the members benefits are based on a calendar year. If the Benefit Contract Calendar Year Indicator (on the Customer Segment Coverage table) = Y, indicating that the members benefits are based on a contract year, this field will default to 999 and the age will be populated in the Member Contract Age fiel';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PL_OF_SRVC_AMA_CD IS 'American Medical Association (AMA) code that specifies where the healthcare service was rendered.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_MOD_CD IS 'Procedure code modifier associated with the procedure code. The actual procedure code is 2 positions in length, this field hold up to 2 occurrences.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_TYP_CD IS 'Derived via lookup to DW Procedure Code Table.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_TAX_ID_NBR IS 'Claim Service Provider Tax Identifier: This field will contain the LATEST Tax Identification Number for which the MCTN_TYPE=E (Employer Identification Number) for the entity. It is also known as TIN.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_SRVC_ZIP_CD IS 'Provider Service Zip Code: the ZIP code for this provider service address. ZIP code can be 5, 9, or 11 digits in length.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_CLM_NPI_ID IS 'This is the NPI submitted on the claim. Claim Service Provider National Provider Identifier: Unique eight digit alphanumeric identifier, with a two digit location suffix, assigned by HCFA to each individual, group and organization of health care services for Medicare beneficiaries.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_RSN_TYP_CD IS 'Code: Indicates whether a claim was paid, denied, or adj. (RIOS)';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_RSN_TYP_DESC IS 'Description: Indicates whether a claim was paid, denied, or adj. (RIOS)';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_CLM_PRI_DIAG_CD IS 'Claim Primary Diagnosis Code. Based on the first diagnosis code in the claim line.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_ADMIT_DT IS 'Date of Inpatient Admission.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_ADMIT_TYP_CD IS 'Identifies the priority of the admission.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_ADMIT_SRC_CD IS 'Identifies how the inpatient stay was initiated. Examples include physician referral, transfer from a hospital, and emergency room.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_CLM_FST_SRVC_DT IS 'Earliest Date of Service';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DT_CLM_LST_SRVC_DT IS 'The Latest Service Date represents the latest Service Through Date among all services that are detailed in the claim.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_BILL_TYP_CD IS 'Created by HCFA and provides three specific pieces of information. The first character identifies the type of facility. The second classifies the type of care. The third indicates the sequence of this bill in this particular episode of care.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DSCHRG_STS_CD IS 'The patient status from the UB92 form. SDW: Identifies the status of the members inpatient stay as of the last service date on the claim.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_SYS_DIAG_REL_GRP_CD IS 'DRG (Diagnostic Related Group) code created by the claim payment system. No RIOS.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_ENT_DIAG_REL_GRP_CD IS 'DRG (Diagnostic Related Group) code submitted on the claim. No RIOS.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_CLM_LVL_RSN_CD_DESC IS 'Claim Level Reason Code Description (applies to the entire claim, not the claim line).';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_EOB_RSN_CD_DESC IS 'Description of the EOB code';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_1_CD_DESC IS 'Field denotes Diagnosis Code Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_2_CD_DESC IS 'Field denotes Diagnosis Code Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_3_CD_DESC IS 'Field denotes Diagnosis Code Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_CD IS 'A code to represent the services billed. Practitioner services are usually billed using a CPT or DSM or HCPC code. Facility claims will usually use a Revenue Code, but can use any of the others as well.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_DESC IS 'Describes the services billed.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_PAYE_NM IS 'The name of the provider that received payment.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_1_DSM_TYP_CD IS 'A code that denotes if DSMIV or DSM5 descriptors';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_2_DSM_TYP_CD IS 'A code that denotes if DSMIV or DSM5 descriptors';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.ICD_DIAG_TYP_CD IS 'Code that denotes whether the diagnosis code is a ICD9 or ICD10 version';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_3_DSM_TYP_CD IS 'A code that denotes if DSMIV or DSM5 descriptors';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_CLM_LVL_RSN_CD IS 'Claim Level Reason Code (applies to the entire claim, not the claim line). Garth clean vs dirty claims. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_RSN_DESC IS 'Derscription of the reason code.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_MOD_2_CD IS 'Procedure code modifier associated with the procedure code. The actual procedure code is 2 positions in length, this field hold up to 2 occurrences.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_MOD_3_CD IS 'Procedure code modifier associated with the procedure code. The actual procedure code is 2 positions in length, this field hold up to 2 occurrences.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_MOD_4_CD IS 'Procedure code modifier associated with the procedure code. The actual procedure code is 2 positions in length, this field hold up to 2 occurrences.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_RVNU_CD IS 'Claim Service Revenue Code: The industry standard revenue code found on hospital claims to identify the type of service performed.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_RVNU_DESC IS 'Describes the services billed.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_SEC_PROC_CD IS 'Secondary procedure code found on a claim';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_SEC_PROC_DESC IS 'Describes the services billed.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_SEC_PROC_TYP_CD IS 'Derived via lookup to DW Procedure Code Table.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_CLM_SRVC_NPI_ID IS 'Claim Servicing Provider NPI Number';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_CLM_REF_NPI_ID IS 'Claim Referring Provider NPI Number';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_CLM_FAC_NPI_ID IS 'Claim Facility Provider NPI Number';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_CLM_RNDR_NPI_ID IS 'Claim Rendering Provider NPI Number';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_CLM_BIL_NPI_ID IS 'Claim Billing Provider NPI Number';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_CLM_PAYE_NPI_ID IS 'Claim Payee Provider NPI Number';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_CLM_ADMIT_NPI_ID IS 'Claim Admitting Provider NPI Number';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_CLM_ATD_NPI_ID IS 'Claim Attending Provider NPI Number';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.DERIV_OBH_FINC_LIAB IS 'True/False indicator that indicates whether BH is liable for this claim';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.SRC_FINC_LIAB_CD IS 'Code that indicates whether BH is liable for this claim';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.SRC_FINC_LIAB_DESC IS 'Description that indicates whether BH is liable for this claim';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_1_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_2_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_3_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_4_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DIAG_5_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_SYS_ID IS 'Data warehouse unique surrogate key for a claim detail transaction.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.MBR_SYS_ID IS 'Data warehouse unique surrogate key for member.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.PERS_SYS_ID IS 'Data warehouse unique surrogate key for person.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.PKG_SYS_ID IS 'Data Warehouse unique surrogate key for Package.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_ALLW_AMT IS 'An adjustment made to the dollar amount that another carrier has paid for the service.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_ALLW_DERIV_AMT IS 'Derived allowed amount.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COB_AMT IS 'An adjustment made to the dollar amount that another carrier has paid for the service.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COINS_AMT IS 'Coinsurance Amount';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_CONTR_AMT IS 'Contract Amount';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COPAY_AMT IS 'The amount of eligible expenses the covered person is financially responsible for, according to a fixed dollar amount for a specified service.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DED_AMT IS 'The amount of eligible expenses the covered person is financially responsible for, according to a fixed dollar amount for a specified service.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DSALLW_AMT IS 'Amount of submitted charges not eligible for reimbursement due to contract agreements, COB, non-covered services, and etc. Amount disallowed from payment.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_DSCNT_AMT IS 'The amount of actual savings based either on the U&C or the contracted rate. Example: $100 submitted, contracted rate is $80, discount amount is $20.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_MBR_RESP_AMT IS 'Member Responsibility Amount';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PD_AMT IS 'The amount that will be compensated for services rendered.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_RSRV_AMT IS 'The "at-risk" portion that is deducted and withheld by the health plan before payment is made to a participating physician as an incentive for appropriate utilization and quality of care.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_SBMT_AMT IS 'The dollar amount the provider requested to be reimbursed for the service they provided. This amount is what was entered into the source system and is also referred to as the claimed amount or the source charge amount.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_ALLWD_QTY IS 'Represents a value or number indicating the quantity or unit of service provided was allowed.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_BILL_QTY IS 'Represents a number of units or quantity entered on the line item charged by the Provider directly from the claim source as of when the claim was submitted';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COV_VST_CNT IS 'The number of covered visits for the outpatient care';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COV_DAYS_CNT IS 'The number of covered days for the inpatient stay';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_SRVC_UNT_CNT IS 'Numeric value for the service rendered';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_CNT IS 'Numeric value for the service rendered per CPT code';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_ALLWD_ADMTS IS 'Represents a value or number indicating the number of admit days were allowed as per coverage. Admit is considered as 1 if the stay is for consecutive days [Admit date does not vary]';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.SRC_CUST_CD IS 'Represents the UHC Community and State Company created codes for every plan used in the Data Warehouse.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.SRC_CUST_NM IS 'Represents the long description of the UHC Community and State Company created codes.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_THER_CLASS_CD IS 'Code for therapeutic classification. i.e. Brand name in NDC table = Tylenol Cold and Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_THER_CLASS_DESC IS 'Description for therapeutic classification. i.e. Brand name in NDC table = Tylenol Cold and Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PKG_CD IS 'Text code for a package of benefit plans.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PKG_NM IS 'Text Name for a package of benefit plans.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_MKT_PKG_CD IS 'Text code representing a package of financial products offered to employer groups or individuals. Default Values: U';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_MKT_PKG_DESC IS 'Text description representing a package of financial products offered to employer groups or individuals. Default Values: U';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PLAN_CD IS 'Class Plan Identifier for each plan or program from the Facet Class Plan table CMC_PLDS_PLAN_DESC e.g. KSKCSDD or KSLTAWN.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PLAN_DESC IS 'Class Plan Description for each plan or program from the Facet Class Plan table CMC_PLDS_PLAN_DESC e.g. KS KanCare Spenddown Members Dual or KS LTC Autism Waiver.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PRDCT_TYP_DESC IS 'A text description rollup of plan benefits, such as: Commercial, Medicare, Medicaid. Valid Values: Commercial, Medicaid, Medicare, U. Default Values: U';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.SRC_MBR_ID IS 'Represents the Unique identifier for the individual. This field is used to tie all member records for the same individual together irrespective of the source system or line of business.';

COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_MBR_GRP_NAME IS  'Represents the Plan description  ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PL_OF_SRVC_AMA_DESC IS 'Describes the place where services were rendered. i.e. Emergency Room, Clinic, Birthing Center and Ambulance. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_PAYE_TYP IS 'Identifies the vendor as either Medical or Other.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRC_PROV_PAYE_TAX_ID IS  'Internal Revenue ServiceTax ID code assigned to the vendor.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_MOD_DESC IS  'Describes the Procedure Modifier that works in conjunction with the Procedure Code. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_MOD_2_DESC IS  'Describes the Procedure Modifier that works in conjunction with the Procedure Code. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_MOD_3_DESC IS  'Describes the Procedure Modifier that works in conjunction with the Procedure Code. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_MOD_4_DESC IS  'Describes the Procedure Modifier that works in conjunction with the Procedure Code. Modifiers are codes that are used to enhance the description of a service or supply under certain circumstances. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_BILL_TYP_DESC IS 'Describes the billing type used to determine the correct level of service that will be used to bill the insurance. i.e. Home Health Inpatient Adjustment of Prior Claim. (Institutional only).';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_SYS_DIAG_REL_GRP_DESC IS   'Diagnostic Related Group code short description';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_NDC_CD IS 'Code that represents the National Drug Code for prescriptions.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_NDC_GNRC_NM IS  'Generic prescription name to use instead of the brand name.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COS_CD IS 'Code that represents the category of service.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COS_LVL1_DESC IS 'Describes the category of service for the entire claim.SMART COS level 1 is derived first by Bill type to classify the claim into basic categories of Inpatient, Outpatient or Physician. Farther levels of categorization can use a combination of Bill type, DRG code, ICD9 diagnosis, Procedure code, Revenue code and/or Provider Specialty to place claims into detailed COS categories. i.e. Level 1 = Outpatient, Level 2 = Facility, Level 3 = OP Surgery and Level 4 = LAP Chole.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COS_LVL2_DESC IS 'Describes the category of service for the entire claim.SMART COS level 1 is derived first by Bill type to classify the claim into basic categories of Inpatient, Outpatient or Physician. Farther levels of categorization can use a combination of Bill type, DRG code, ICD9 diagnosis, Procedure code, Revenue code and/or Provider Specialty to place claims into detailed COS categories. i.e. Level 1 = Outpatient, Level 2 = Facility, Level 3 = OP Surgery and Level 4 = LAP Chole.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COS_LVL3_DESC  IS  'Describes the category of service for the entire claim.SMART COS level 1 is derived first by Bill type to classify the claim into basic categories of Inpatient, Outpatient or Physician. Farther levels of categorization can use a combination of Bill type, DRG code, ICD9 diagnosis, Procedure code, Revenue code and/or Provider Specialty to place claims into detailed COS categories. i.e. Level 1 = Outpatient, Level 2 = Facility, Level 3 = OP Surgery and Level 4 = LAP Chole.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_COS_LVL4_DESC IS 'Describes the category of service for the entire claim.SMART COS level 1 is derived first by Bill type to classify the claim into basic categories of Inpatient, Outpatient or Physician. Farther levels of categorization can use a combination of Bill type, DRG code, ICD9 diagnosis, Procedure code, Revenue code and/or Provider Specialty to place claims into detailed COS categories. i.e. Level 1 = Outpatient, Level 2 = Facility, Level 3 = OP Surgery and Level 4 = LAP Chole.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.SRC_PROV_NTWK_DESC IS 'Describes if the provider on the claim is a participant in the health insurance plan. Values are Participating, Non-par, Unknown or N/A.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRVC_LN_STS_CD IS 'Claim status codes represent the status of a claim by detail line including 1 = ''P''ayable, 2 = ''D''enied, 3 = ''A''djustment, 4 = ''N''o check (adjustment), 5 = ''C''apitated, 6 = ''S''taff, 7 = ''F''inal, 8 = ''I''nformational, -1 = N/A and 0 = UNK. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_SRVC_LN_STS_DESC IS 'Describes the status of a claim by detail line including ''P''ayable, ''D''enied, ''A''djustment, ''N''o check (adjustment), ''C''apitated, ''S''taff, ''F''inal, Informational, N/A and UNK. ';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_CTGY_LVL_1_DESC IS 'Describes the CPT category further defining the procedure codes i.e. Motion Analysis Level 3, Neurology ''&'' Neuromuscul Level 2 with Medicine Level 1.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_CTGY_LVL_2_DESC IS 'Describes the CPT category further defining the procedure codes i.e. Motion Analysis Level 3, Neurology ''&'' Neuromuscul Level 2 with Medicine Level 1.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.CLM_TRANS_PROC_CTGY_LVL_3_DESC IS 'Describes the CPT category further defining the procedure codes i.e. Motion Analysis Level 3, Neurology ''&'' Neuromuscul Level 2 with Medicine Level 1.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.MEDCD_ID IS 'Identifier assigned by CMS for a individual enrolled in Community & State [C&S] coverage.';
COMMENT ON COLUMN OBH_DP.CLM_TRANS_SRVC_LN.MEDCR_ID IS 'Identifier assigned by CMS for a individual enrolled in Medicare & Retirement [M&R] coverage.';
