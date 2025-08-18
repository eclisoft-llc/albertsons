CREATE TABLE IF NOT EXISTS COMPACT.DIM_CLM_TRANS CLUSTER BY (LEFT(CLM_TRANS_SYS_ID,7))(
CLM_TRANS_SYS_ID VARCHAR(100) NOT NULL CONSTRAINT PK_F_DIM_CLM_TRANS_SYS_ID PRIMARY KEY,
CLM_AUD_SRC_ID VARCHAR(50) NULL,
CLM_AUD_SRC_SYS_CD VARCHAR(8) NULL,
CLM_AUD_NBR VARCHAR(50) NULL,
CLM_AUD_LN_NBR VARCHAR(50) NULL,
CLM_AUD_TRANS_SEQ_NBR VARCHAR(1) NULL,
CLM_SRC_AUTH_NBR VARCHAR(50) NULL,
CLM_SRC_COSM_DIV_CD VARCHAR(3) NULL,
CLM_SRC_DOC_CNTL_NBR NUMBER NULL,
CLM_SRC_LEG_ENTY_NBR VARCHAR(20) NULL,
CLM_SRC_MBR_ACCT_NBR VARCHAR(30) NULL,
CLM_SRC_MBR_CUST_SEG_NBR VARCHAR(30) NULL,
CLM_SRC_MBR_DEPN_CD VARCHAR(3) NULL,
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
CLM_SRC_RIOS_BUS_UNIT_ID VARCHAR(3) NULL,
CLM_SRC_UNET_CO_CD VARCHAR(2) NULL,
CLM_TRANS_ACCOM_FLG VARCHAR(1) NULL,
CLM_TRANS_ADJ_CD VARCHAR(50) NULL,
CLM_TRANS_BEN_LVL_DERIV_CD VARCHAR(2) NULL,
CLM_TRANS_BEN_LVL_SRC_CD VARCHAR(2) NULL,
CLM_TRANS_CHK_NBR VARCHAR(13) NULL,
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
CLM_TRANS_PL_OF_SRVC_SRC_CD VARCHAR(60) NULL,
CLM_TRANS_PROC_MOD_CD VARCHAR(14) NULL,
CLM_TRANS_PROC_TYP_CD VARCHAR(8) NULL,
CLM_TRANS_PROC_UNIT_OF_MSR_TXT VARCHAR(16) NULL,
CLM_TRANS_PROV_NTWK_DERIV_CD VARCHAR(1) NULL,
CLM_TRANS_UNET_CAP_FND_CD VARCHAR(1) NULL,
CLM_TRANS_UNET_CAP_FND_DRIV_CD VARCHAR(1) NULL,
CLM_SRC_MBR_JOB_CATGY_CD VARCHAR(50) NULL,
CLM_SRC_MBR_JOB_SITE_CD VARCHAR(50) NULL,
CLM_SRC_PROV_TAX_ID_NBR VARCHAR(15) NULL,
CLM_SRC_PROV_SRVC_ZIP_CD VARCHAR(15) NULL,
CLM_SRC_PROV_CLM_NPI_ID VARCHAR(10) NULL,
CLM_TRANS_UNET_OBLG_CD VARCHAR(2) NULL,
CLM_TRANS_UNET_SHR_ARNG_CD VARCHAR(2) NULL,
CLM_TRANS_UNET_BEN_LVL_CAUS_CD VARCHAR(1) NULL,
CLM_TRANS_ADJD_ID VARCHAR(9) NULL,
CLM_TRANS_PROC_ADJD_OFC_DESC VARCHAR(35) NULL,
CLM_TRANS_PYMT_OVRIDE_CD VARCHAR(3) NULL,
CLM_TRANS_PYMT_OVRIDE_FLG VARCHAR(1) NULL,
CLM_TRANS_SBMT_SRC_DESC VARCHAR(64) NULL,
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
CLM_TRANS_UNET_HE_MBR_MKT_NBR VARCHAR(7) NULL,
CLM_TRANS_UNET_CES_FINC_ARNG_CD VARCHAR(1) NULL,
CLM_TRANS_UNET_CES_CUST_NBR VARCHAR(7) NULL,
CLM_TRANS_UNET_CES_GRP_NBR VARCHAR(7) NULL,
CLM_TRANS_UNET_CLM_MKT_SEG_CD VARCHAR(1) NULL,
CLM_TRANS_UNET_LD_PRTN_CD VARCHAR(5) NULL,
CLM_TRANS_UNET_MHSA_PRDCT_CD VARCHAR(1) NULL,
CLM_TRANS_UNET_MHSA_PRDCT_DESC VARCHAR(25) NULL,
CLM_TRANS_UNET_PLN_VAR_SUBDIV_CD VARCHAR(4) NULL,
CLM_TRANS_UNET_RPT_CD_BR_CD VARCHAR(4) NULL,
CLM_TRANS_EOB_RSN_CD_DESC VARCHAR(254) NULL,
CLM_TRANS_CAC_AUTH_SYS_ID NUMBER NULL,
CLM_TRANS_AUTH_NBR VARCHAR(50) NULL,
CLM_TRANS_SUPP_ORDR_IND VARCHAR(39) NULL,
CLM_TRANS_EXTR_BEN_NBR VARCHAR(3) NULL,
CLM_TRANS_EXTR_AGCY_ID VARCHAR(5) NULL,
CLM_TRANS_EXTR_DEPN_NBR VARCHAR(2) NULL,
CLM_ETL_DW_SYS_ID NUMBER NULL,
CLM_ETL_DW_FROM_DT DATE NULL,
REC_CRT_DTTM TIMESTAMP NULL,
REC_UPDT_DTTM TIMESTAMP NULL,
CLM_SRC_PROV_PAYE_NM VARCHAR(55) NULL,
CLM_TRANS_PRI_PAYE_CD VARCHAR(1) NULL,
CLM_TRANS_PRI_PAYE_CD_DESC VARCHAR(35) NULL,
CLM_TRANS_DIAG_1_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_DIAG_2_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_DIAG_3_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_DIAG_4_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_DIAG_5_CD_DESC VARCHAR(250) NULL,
CLM_TRANS_PROC_CD VARCHAR(10) NULL,
CLM_TRANS_PROC_DESC VARCHAR(250) NULL,
ICD_DIAG_TYP_CD VARCHAR(10) NULL,
BIL_RVNU_CD VARCHAR(4) NULL,
CLM_TRANS_RSN_DESC VARCHAR(500) NULL,
CLM_TRANS_UNET_RDUC_RSN_CD VARCHAR(5) NULL,
CLM_TRANS_UNET_RDUC_RSN_DESC VARCHAR(500) NULL,
CLM_TRANS_DIAG_1_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_DIAG_2_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_DIAG_3_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_DIAG_4_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_DIAG_5_DSM_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_CLM_LVL_RSN_CD VARCHAR(12) NULL,
CLM_TRANS_EOB_RSN_CD VARCHAR(30) NULL,
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
PND_CLM_FLG VARCHAR(6) DEFAULT 'False' NULL,
CLM_TRANS_BIL_PROC_CD VARCHAR(10) NULL,
CLM_TRANS_BIL_PROC_DESC VARCHAR(250) NULL,
CLM_TRANS_BIL_PROC_TYP_CD VARCHAR(10) NULL,
CLM_TRANS_CLM_ST_OF_ISS VARCHAR(2) NULL,
CLM_TRANS_RSN_CD VARCHAR(12) NULL,
CLM_TRANS_AGRMNT_ID VARCHAR(20) NULL,
CLM_TRANS_AGRMNT_DESC VARCHAR(100) NULL,
CSP_NTWX_PRCNG_CLM_BASED_ON VARCHAR(30) NULL,
CSP_NTWX_PRCNG_CLM_BASED_DESC VARCHAR(30) NULL,
CLM_TRANS_CLM_ACT_RATE_SRC VARCHAR(50) NULL,
CLM_TRANS_OON_MTHDLGY VARCHAR(70) NULL,
CLM_TRANS_RSN_TYP_DESC VARCHAR(20) NULL,
SRC_FINC_LIAB_DESC VARCHAR(30) NULL,
CLM_TRANS_DIAG_1_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
CLM_TRANS_DIAG_2_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
CLM_TRANS_DIAG_3_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
CLM_TRANS_DIAG_4_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
CLM_TRANS_DIAG_5_CD_CATGY_LVL_2_DESC VARCHAR(200) NULL,
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36)
) ;
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_SYS_ID IS 'Data warehouse unique surrogate key for a claim detail transaction.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_AUD_SRC_ID IS 'Uniquely identifies the record in the source claim system.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_AUD_SRC_SYS_CD IS 'Identifies source of the claim. .... COSMOS, UNET, RIOS.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_AUD_NBR IS 'An identification number assigned to each claim by the source system.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_AUD_LN_NBR IS 'A number referencing the line of the claim.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_AUD_TRANS_SEQ_NBR IS 'A system generated sequential number.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_AUTH_NBR IS 'Source System Authorization Number.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_COSM_DIV_CD IS 'Derived from Claim Audit Number for Cosmos claims.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_DOC_CNTL_NBR IS 'Document number assigned when the claim is received. Used to track the claim through its lifecycle.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_LEG_ENTY_NBR IS 'The number assigned by United Health Group to uniquely identify a legal entity. It identifies an organization or company that assumes financial risk for the payment of health care services. The entity that signs the contract with the Health Plan (i.e., employer group, health plan). The legal entity is at financial risk for all services. Examples of legal entities include health plans, third party administrators, and self-funded customers.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_ACCT_NBR IS 'Client Account Number (for Facets and PBH this is the Group ID)';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_CUST_SEG_NBR IS 'Also known as Account Identifier. Derived from the Member Source System Key. The number assigned to an entity that has purchased products and/or services from UHG. For UNET this equates to policy number. For IMCS this equates to Group.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_DEPN_CD IS 'Derived from Mem ID format is 9 character subscriber ID - 2 character dependent code.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_FAM_ID IS 'A unique Id for family. A family consists of the subscriber and the subscribers dependents. Families roll up to a group. Family members select products that the group offers.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_GDR_CD IS 'The gender of the member.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_GRP_ID IS 'Derived from Fam ID format = 9 character subscriber ID - 5 character account ID - 3 character group ID.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_MKT_DERIV_NBR IS 'A market number generated by the data extract transform and load process within GALAXY which is created based upon the network to which the member belongs or the members zip code. For Unet Data the assignment is based on Members HMO ACCT Division (for HMO products only), CES Sold Market number or Zip code.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_NON_UHG_EE_FLG IS 'The flag used to indicate that the employee does not work for UHG.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_PLN_ID IS 'Unique number used to identify a grouping of specific details describing covered services and standard rules for a specific component.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_PRDCT_CD IS 'Identifies the product at the lowest level of detail.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_REL_CD IS 'Part of the Member Source System Key.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_SBSCR_ID IS 'Identifies the subscriber associated with the member.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_SYS_ID IS 'A sequence of numbers and/or characters used to identify the member in the downstream system.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_SRVC_CTY_NM IS 'City in which the services were rendered.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_SRVC_ST_CD IS 'Code for the state where the provider rendered the service';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_SYS_ID IS 'Number that is used to uniquely identify a provider in one of the source systems that submits a claim.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_RIOS_BUS_UNIT_ID IS 'Identifies business unit in RIOS.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_UNET_CO_CD IS 'A code denoting the funding arrangement, legal entity, and reinsurance combination.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_ACCOM_FLG IS 'Accommodation Indicator Flag';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_ADJ_CD IS 'Indicates if any line item on the claim has been adjusted.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_BEN_LVL_DERIV_CD IS 'Derived from benefit level.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_BEN_LVL_SRC_CD IS 'Benefit level of payment.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_CHK_NBR IS 'Check Number';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_CLM_FM_TYP_CD IS 'Determines whether the claim submission form is physician (2) or hospital (1).';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_1_CD IS 'The Diagnosis One Code represents the most important diagnosis (also known as Primary Diagnosis) responsible for the medical services. ';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_1_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_1_CD_TYP_CD IS 'Field denotes Diagnosis Description Type. (ICD9, DSM, EAP)';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_2_CD IS 'The Diagnosis Two Code represents the second most important diagnosis (also known as Secondary Diagnosis) responsible for the medical services. ';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_2_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_2_CD_TYP_CD IS 'Field denotes Diagnosis Description Type. (ICD9, DSM, EAP)';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_3_CD IS 'The Diagnosis Three Code represents the third most important diagnosis (also known as Tertiary Diagnosis) responsible for the medical services. Data Placement: left justified, space filled ';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_3_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_4_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_5_CD_CATGY_LVL_1_DESC IS 'Field denotes Diagnosis Category 1 Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_3_CD_TYP_CD IS 'Field denotes Diagnosis Description Type. (ICD9, DSM, EAP)';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_MH_SA_CD IS 'Indicates if the primary diagnosis code is a MH or SA diagnosis. This column is populated from the MH SA indicator from the Diagnosis table. ';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_SBSTNC_ABUS_FLG IS 'Set to Y when any of the diagnosis codes has an MH/SA value of "SA" .';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DLOC_TXT IS 'Derived Level of Care (DLOC) description that is appropriate to the Claim transaction.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DLOC_SEQ_NBR IS 'Level of care hierarchy. One ("1") is the highest level of care.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DLOC_CATGY_TXT IS 'Level of care common category. A description representing a common level of care category across all source systems.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_ADJD_DT IS 'Most rececent adjudication date for any of the claim line items.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_ENT_DT IS 'The date of data entry into the source system.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_PD_DT IS 'Most recent paid date for this line item.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_RCVD_DT IS 'The date the claim was posted to the General Ledger';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_PST_DT IS 'Date the claim was posted to the general ledger. For RIOS this is the check date. (Valid Values: Valid date)';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_SRVC_FROM_DT IS 'From date of service for this line item.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_SRVC_THRU_DT IS 'Thru date of service for this line item.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_LVL_OF_SRVC_DERIV_CD IS 'Three letter code indicating a common service level code accross all source systems.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_LVL_OF_SRVC_SRC_CD IS 'Three letter code indicating a common service level code accross all source systems.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_MBR_IN_YR_AGE IS 'The age of the claimant as of the earliest service date on the claim. This field will be populated with the age of the claimant as if the earliest service date on the claim for COSMOS claims if the Benefit Contract Calendar Year Indicator (on the Customer Segment Coverage table) = N, indicating that the members benefits are based on a calendar year. If the Benefit Contract Calendar Year Indicator (on the Customer Segment Coverage table) = Y, indicating that the members benefits are based on a contract year, this field will default to 999 and the age will be populated in the Member Contract Age fiel';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PL_OF_SRVC_AMA_CD IS 'American Medical Association (AMA) code that specifies where the healthcare service was rendered.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PL_OF_SRVC_SRC_CD IS 'AMA Place of Service Code. ';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROC_MOD_CD IS 'Procedure code modifier associated with the procedure code. The actual procedure code is 2 positions in length, this field hold up to 2 occurrences.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROC_TYP_CD IS 'Derived via lookup to DW Procedure Code Table.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROC_UNIT_OF_MSR_TXT IS 'For some anicillary codes, this is the unit of measure used for units billed.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROV_NTWK_DERIV_CD IS 'Derived network code.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_CAP_FND_CD IS 'Identifies capitated services, the type of capitation, and the account that a capitated service is being charged to or paid from.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_CAP_FND_DRIV_CD IS 'Identifies capitated services, the type of capitation, and the account that a capitated service is being charged to or paid from.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_JOB_CATGY_CD IS 'Claim Service Member Job Category Code';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_MBR_JOB_SITE_CD IS 'Claim Service Member Job Site Code';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_TAX_ID_NBR IS 'Claim Service Provider Tax Identifier: This field will contain the LATEST Tax Identification Number for which the MCTN_TYPE=E (Employer Identification Number) for the entity. It is also known as TIN.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_SRVC_ZIP_CD IS 'Provider Service Zip Code: the ZIP code for this provider service address. ZIP code can be 5, 9, or 11 digits in length.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_CLM_NPI_ID IS 'This is the NPI submitted on the claim. Claim Service Provider National Provider Identifier: Unique eight digit alphanumeric identifier, with a two digit location suffix, assigned by HCFA to each individual, group and organization of health care services for Medicare beneficiaries.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_OBLG_CD IS 'UNET Claim Service Obligor Code: UNET only. Identifies which organization "is responsible" to provide the products and Services in a shared arrangement i.e. UHG has the responsibilty of providing service and products if the Obligore ID is 01. Medica is responsible if it is 02, Harvard Pilgrim if it is 03, Multiple Obligore Ids indicate shared responsibility. Obligor IDs are coded on a "Plan" level.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_SHR_ARNG_CD IS 'UNET Claim Service Shared Arrangement Code: UNET only. A code used to indicate that an arrangement exists between UHG an employer group and an external Health plan insurer (s) to jointly sell and service products. Each code describes a specific arrangement and is not unique i.e. Medica and Harvard Pilgrim have an "O3" Shared Arrangement with UHG. The Obligor ID specifically identifies the Health Plan. The Shared Arrangement is coded on a "Policy" level.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_BEN_LVL_CAUS_CD IS 'UNET ...Identifies the reason the patient sought medical care.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_ADJD_ID IS 'UNET: The identification number of the person who entered the service into the source system. (Adjuster Number)';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROC_ADJD_OFC_DESC IS 'team within an office (e.g. Albany Non-HMO). For COSMOS data, it is based on the Site Code. For UNet data, it is based on Adjudication Office Code and TOPS Engine Code. ';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PYMT_OVRIDE_CD IS 'Identifies that the approver overrode the system-generated payment amount. For COSMOS, identifies what part of the payment was overridden (copay, contracted amount, total manual calculation, etc.). For UNET and IMCS, identifies the reason the approver overrode the system (claim related to detoxification, pay benefit from credit-reserve, rejected line item, etc.). ';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_SBMT_SRC_DESC IS 'Identifies how the claim was input into the source system. Different descriptions are used for each of the source systems (Cosm, Unet, Rios).';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_RSN_TYP_CD IS 'Code: Indicates whether a claim was paid, denied, or adj. (RIOS)';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_RSN_TYP_DESC IS 'Description: Indicates whether a claim was paid, denied, or adj. (RIOS)';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_CLM_PRI_DIAG_CD IS 'Claim Primary Diagnosis Code. Based on the first diagnosis code in the claim line.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_ADMIT_DT IS 'Date of Inpatient Admission.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_ADMIT_TYP_CD IS 'Identifies the priority of the admission.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_ADMIT_SRC_CD IS 'Identifies how the inpatient stay was initiated. Examples include physician referral, transfer from a hospital, and emergency room.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_CLM_FST_SRVC_DT IS 'Earliest Date of Service';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DT_CLM_LST_SRVC_DT IS 'The Latest Service Date represents the latest Service Through Date among all services that are detailed in the claim.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_BILL_TYP_CD IS 'Created by HCFA and provides three specific pieces of information. The first character identifies the type of facility. The second classifies the type of care. The third indicates the sequence of this bill in this particular episode of care.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DSCHRG_STS_CD IS 'The patient status from the UB92 form. SDW: Identifies the status of the members inpatient stay as of the last service date on the claim.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_SYS_DIAG_REL_GRP_CD IS 'DRG (Diagnostic Related Group) code created by the claim payment system. No RIOS.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_ENT_DIAG_REL_GRP_CD IS 'DRG (Diagnostic Related Group) code submitted on the claim. No RIOS.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_CLM_LVL_RSN_CD_DESC IS 'Claim Level Reason Code Description (applies to the entire claim, not the claim line).';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_HE_MBR_MKT_NBR IS 'UNET Only. Reflects either the Members Market based on the product, their PCP (Primary Care Physician) Market, a Customer Specific Market, or the Members Residential Market based on their ZIP Code.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_CES_FINC_ARNG_CD IS 'UNET only. Denotes a high level financial arrangement (i.e. ASO, Fully Insured, Risk).';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_CES_CUST_NBR IS 'UNET only. A unique number that is used within CES to access correct case structure information and relate multiple cases together under a single identifier';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_CES_GRP_NBR IS 'UNET only. Used as a key into CES with the CES Customer Number to uniquely identify case structure.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_LD_PRTN_CD IS 'UNET Only. Code to indicate who the Lead Partner is in a multi-alliance customer arrangement.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_MHSA_PRDCT_CD IS 'UNET Only. Indicates if the customer segment has a mental health substance abuse network arrangement in place.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_MHSA_PRDCT_DESC IS 'UNET Only. Indicates if the customer segment has a mental health substance abuse network arrangement in place.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_PLN_VAR_SUBDIV_CD IS 'UNET Only. This is used as a key in other subject areas. For TOPS policies, this equates to Benefit Structure One Code (Plan Variation).';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_RPT_CD_BR_CD IS 'UNET Only. This is used as a key in for other subject areas. For TOPS policies, this equates to Benefit Structure Two Code (Report Code). For IMCS policies, this equates to Subgroup Two Code (Branch Code).';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_EOB_RSN_CD_DESC IS 'Description of the EOB code';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_ETL_DW_SYS_ID IS 'This will allow a quick trace back to the DW using the DW sys ids for audit purposes. Note the source system code would need to be used in conjunction with the ETL_DW_SYS_ID to point to the correct table.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_1_CD_DESC IS 'Field denotes Diagnosis Code Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_2_CD_DESC IS 'Field denotes Diagnosis Code Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_3_CD_DESC IS 'Field denotes Diagnosis Code Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROC_CD IS 'A code to represent the services billed. Practitioner services are usually billed using a CPT or DSM or HCPC code. Facility claims will usually use a Revenue Code, but can use any of the others as well.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROC_DESC IS 'Describes the services billed.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_ETL_DW_FROM_DT IS 'This field would contain the same value as CLM_TRANS_DT_SRVC_FROM_DT for all sources except Cosmos. Cosmos does not do a straight move from the warehouse, but instead will move the First Service Date when it is a Facilities Claim. The performance tuning requires this value to be consistently mapped for all sources.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.REC_CRT_DTTM IS 'The create datetime of a record into the table.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.REC_UPDT_DTTM IS 'The update datetime of a record into the table.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_PAYE_NM IS 'The name of the provider that received payment.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PRI_PAYE_CD IS 'Code that Identifies who is assigned to receive claim payment.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PRI_PAYE_CD_DESC IS 'Description that Identifies who is assigned to receive claim payment.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.BIL_RVNU_CD IS 'Billed revenue code derived from the UB92 tableSTG_GALA_UNET_UB92_REVENUE_CODE_STATISTICAL_SERVICE';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_1_DSM_TYP_CD IS 'A code that denotes if DSMIV or DSM5 descriptors';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_2_DSM_TYP_CD IS 'A code that denotes if DSMIV or DSM5 descriptors';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.ICD_DIAG_TYP_CD IS 'Code that denotes whether the diagnosis code is a ICD9 or ICD10 version';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_3_DSM_TYP_CD IS 'A code that denotes if DSMIV or DSM5 descriptors';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_CLM_LVL_RSN_CD IS 'Claim Level Reason Code (applies to the entire claim, not the claim line). Garth clean vs dirty claims. ';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_RSN_DESC IS 'Derscription of the reason code.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_RDUC_RSN_CD IS 'Code that identifies the reason for an adjustment in UNET. The new reason code values will be: 00 - Other, 01 - Coordination of Benefits, 02 - System BPL or Service Code Change, 03 - System/Provider Contract Change, 04 - System/Other, 05 - CSA Processor, 06 - Provider, 07 - Enrollment, 08 - Health Plan, 09 - Contract/Benefit';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_UNET_RDUC_RSN_DESC IS 'Describes the reason for an adjustment in UNET. The new reason code values will be: 00 - Other, 01 - Coordination of Benefits, 02 - System BPL or Service Code Change, 03 - System/Provider Contract Change, 04 - System/Other, 05 - CSA Processor, 06 - Provider, 07 - Enrollment, 08 - Health Plan, 09 - Contract/Benefit';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_BIL_PROC_CD IS 'Procedure Code describes the type of procedure performed or service provided. This procedure code is usually a CPT-4 OR HCPCS Code';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_BIL_PROC_DESC IS 'Description of the services billed';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_BIL_PROC_TYP_CD IS 'Derived via lookup to DW Procedure Code Table';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_CLM_ST_OF_ISS IS 'State of Issue Code at the CUSTOMER_SEGMENT_GROUP_STRUCTURE level';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_EOB_RSN_CD IS 'The user defined value that describes a situation associated with the corresponding line item within the claim. Facets only, put in for IBM billing error - X00';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROC_MOD_2_CD IS 'Procedure code modifier associated with the procedure code. The actual procedure code is 2 positions in length, this field hold up to 2 occurrences.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROC_MOD_3_CD IS 'Procedure code modifier associated with the procedure code. The actual procedure code is 2 positions in length, this field hold up to 2 occurrences.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_PROC_MOD_4_CD IS 'Procedure code modifier associated with the procedure code. The actual procedure code is 2 positions in length, this field hold up to 2 occurrences.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_RVNU_CD IS 'Claim Service Revenue Code: The industry standard revenue code found on hospital claims to identify the type of service performed.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_RVNU_DESC IS 'Describes the services billed.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_SEC_PROC_CD IS 'Secondary procedure code found on a claim';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_SEC_PROC_DESC IS 'Describes the services billed.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_SEC_PROC_TYP_CD IS 'Derived via lookup to DW Procedure Code Table.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_CLM_SRVC_NPI_ID IS 'Claim Servicing Provider NPI Number';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_CLM_REF_NPI_ID IS 'Claim Referring Provider NPI Number';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_CLM_FAC_NPI_ID IS 'Claim Facility Provider NPI Number';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_CLM_RNDR_NPI_ID IS 'Claim Rendering Provider NPI Number';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_CLM_BIL_NPI_ID IS 'Claim Billing Provider NPI Number';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_CLM_PAYE_NPI_ID IS 'Claim Payee Provider NPI Number';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_CLM_ADMIT_NPI_ID IS 'Claim Admitting Provider NPI Number';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_SRC_PROV_CLM_ATD_NPI_ID IS 'Claim Attending Provider NPI Number';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.PND_CLM_FLG IS 'True/False indicator that indicates this claim came from the Pended Claim Table and was a payable claim.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.DERIV_OBH_FINC_LIAB IS 'True/False indicator that indicates whether BH is liable for this claim';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.SRC_FINC_LIAB_CD IS 'Code that indicates whether BH is liable for this claim';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.SRC_FINC_LIAB_DESC IS 'Description that indicates whether BH is liable for this claim';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_1_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_2_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_3_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_4_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
COMMENT ON COLUMN COMPACT.DIM_CLM_TRANS.CLM_TRANS_DIAG_5_CD_CATGY_LVL_2_DESC IS 'Field denotes Diagnosis Category 2 Description.';
