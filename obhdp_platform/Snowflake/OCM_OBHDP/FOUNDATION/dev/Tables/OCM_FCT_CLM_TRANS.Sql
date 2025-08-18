CREATE TABLE IF NOT EXISTS FOUNDATION.OCM_FCT_CLM_TRANS CLUSTER BY (LEFT(CLM_TRANS_SYS_ID,7))(
CLM_TRANS_SYS_ID VARCHAR(100) NOT NULL CONSTRAINT PK_F_OCM_FCT_CLM_TRANS_SYS_ID PRIMARY KEY,
CLM_AUD_SRC_ID VARCHAR(50) NULL,
CLM_AUD_LN_NBR VARCHAR(50) NULL,
CLM_AUD_NBR VARCHAR(50) NULL,
CLM_AUD_TRANS_SEQ_NBR VARCHAR(1) NULL,
CARE_MGT_MKT_SYS_ID NUMBER NULL,
GRP_SYS_ID NUMBER NULL,
MBR_ELIG_SYS_ID NUMBER NULL,
MBR_SYS_ID VARCHAR(39) NULL,
PERS_SYS_ID NUMBER NULL,
PKG_SYS_ID VARCHAR(20) NULL,
PROV_SYS_ID NUMBER NULL,
SALE_CHNL_SYS_ID NUMBER NULL,
CLM_MBR_ATTR_SYS_ID NUMBER NULL,
RPT_CUST_SYS_ID NUMBER DEFAULT -1 NULL,
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
CLM_TRANS_UNIT_CNT NUMBER(12,1) NULL,
CLM_TRANS_UNIT_DERIV_CNT NUMBER NULL,
REC_CRT_DTTM  TIMESTAMP NULL,
REC_UPDT_DTTM TIMESTAMP NULL,
CSP_NTWX_PRCNG_CLM_VOL_TYP VARCHAR(30) NULL,
CSP_NTWX_PRCNG_CLM_RT VARCHAR(60) NULL,
CSP_NTWX_PRCNG_CLM_RT_TYP VARCHAR(30) NULL,
CLM_TRANS_CLM_ACT_RATE NUMBER(9,2) NULL,
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36)
) ;
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_AUD_NBR IS 'An identification number assigned to each claim by the source system.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CARE_MGT_MKT_SYS_ID IS 'Data Warehouse unique surrogate key for Care Management Location.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_SYS_ID IS 'Data warehouse unique surrogate key for a claim detail transaction.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.GRP_SYS_ID IS 'Data Warehouse unique surrogate key for group.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.MBR_ELIG_SYS_ID IS 'Data warehouse unique surrogate key for member eligibility.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.MBR_SYS_ID IS 'Data warehouse unique surrogate key for member.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.PERS_SYS_ID IS 'Data warehouse unique surrogate key for person.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.PKG_SYS_ID IS 'Data Warehouse unique surrogate key for Package.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.PROV_SYS_ID IS 'Data warehouse unique surrogate key for a practice (organization) or a provider (individual).';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.SALE_CHNL_SYS_ID IS 'Data warehouse unique surrogate key for sales channel.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_MBR_ATTR_SYS_ID IS 'Data Warehouse unique surrogate key for claim member attribute.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.RPT_CUST_SYS_ID IS 'Data Warehouse unique surrogate key for a reporting customer. A reporting customer is comprised of a combination of grp_sys_id, pkg_sys_id, and care_mgmt_mkt_sys_id values.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_ALLW_AMT IS 'An adjustment made to the dollar amount that another carrier has paid for the service.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_ALLW_DERIV_AMT IS 'Derived allowed amount.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_COB_AMT IS 'An adjustment made to the dollar amount that another carrier has paid for the service.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_COINS_AMT IS 'Coinsurance Amount';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_CONTR_AMT IS 'Contract Amount';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_COPAY_AMT IS 'The amount of eligible expenses the covered person is financially responsible for, according to a fixed dollar amount for a specified service.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_DED_AMT IS 'The amount of eligible expenses the covered person is financially responsible for, according to a fixed dollar amount for a specified service.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_DSALLW_AMT IS 'Amount of submitted charges not eligible for reimbursement due to contract agreements, COB, non-covered services, and etc. Amount disallowed from payment.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_DSCNT_AMT IS 'The amount of actual savings based either on the U&C or the contracted rate. Example: $100 submitted, contracted rate is $80, discount amount is $20.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_MBR_RESP_AMT IS 'Member Responsibility Amount';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_PD_AMT IS 'The amount that will be compensated for services rendered.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_RSRV_AMT IS 'The "at-risk" portion that is deducted and withheld by the health plan before payment is made to a participating physician as an incentive for appropriate utilization and quality of care.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_SBMT_AMT IS 'The dollar amount the provider requested to be reimbursed for the service they provided. This amount is what was entered into the source system and is also referred to as the claimed amount or the source charge amount.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_UNIT_CNT IS 'Number of times the listed procedure (code) was performed.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.CLM_TRANS_UNIT_DERIV_CNT IS 'Derived number of units.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.REC_CRT_DTTM IS 'The create datetime of a record into the table.';
COMMENT ON COLUMN FOUNDATION.OCM_FCT_CLM_TRANS.REC_UPDT_DTTM IS 'The update datetime of a record into the table.';
