PHRM_DRG_CLM

STM.STM_GALA_NDC_DRUG = ETL.BHP_STM_GALA_NDC_DRUG 
STG.STG_PSRX_CLM_GOVT =  
STM.STM_PSRX_CLM_PROV = 
STM.STM_PSRX_CARR_MBR_ALT = 
STG.STG_PSRX_CLM_COMM = 
STM.STM_COSM_GUM  = 
DW.TB_MBR_ELIG_PBH_WRK = 
STM.STM_PBH_CMC_MEME_MEMBER = 
STM.STM_FCET_CMC_SBSB_SUBSC = 
STM.STM_FCET_CMC_MEME_MEMBER = 
STM.STM_FCET_CMC_MEPE_PRCS_ELIG = 
STM.STM_FCET_CMC_GRGR_GROUP = 
STM.STM_CSP_CMC_SBSB_SUBSC = 
STM.STM_CSP_CMC_MEME_MEMBER =
STM.STM_CSP_CMC_MEPE_PRCS_ELIG = 
STM.STM_CSP_CMC_GRGR_GROUP = 
STM.STM_COSM_MEM = 
STG.STG_PSRX_CLM_XCHG = 
STM.STM_ELI_T_CES_MEMBER = 
STG.STG_PSRX_CLM_COMM_FRESHSTART = 
DW.TB_MEM_MBR = 
DW.TB_MEM_MBR_ALT = 


create or repalce temporary table temp_ndc as 
select NDC1.*,rank() over ( partition by NDC order by NDC_DRG_ROW_EFF_DT DESC ) as rnk
from STM.STM_GALA_NDC_DRUG NDC1;


create or replace temporary table extSTG_PSRX_CLM_GOVT as 
SELECT   date(ndc.NDC_DRG_ROW_EFF_DT )as NDC_DRG_ROW_EFF_DT,
         REC_ID,
         PRCSR_ID_NUM,
         PRCSR_BATCH_NUM,
         govt.SRVC_PROV_ID AS SRVC_PROV_ID,
         govt.SRVC_PROV_ID_QLFYR AS SRVC_PROV_ID_QLFYR,
         CLM_ORGNTN_FLAG,
         PHRMCY_NTWRK_CD,
         govt.SRVC_PROV_NAME SRVC_PROV_NAME,
         SRVC_PROV_REL_CD,
         govt.SRVC_PROV_DSPNSR_CLS SRVC_PROV_DSPNSR_CLS,
         govt.SRVC_PROV_DSPNSR_TYPE SRVC_PROV_DSPNSR_TYPE,
         RX_SRVC_REF_NUM,
         govt.FILL_DT FILL_DT,
         PROD_SRVC_ID,
         PROD_ID_QLFYR,
         PROD_LABEL_NAME_WTH_DSG,
         PROD_MFTR,
         NEW_REFL_CD,
         MTRC_DCML_QTY,
         DAYS_SUPLY,
         DRUG_RX_OTC_IND,
         DRUG_STGTH,
         UOM,
         ROA,
         DSG_FORM,
         HCPCS_CD,
         BASIS_OF_REIMBRSMT,
         BASIS_OF_CST_DTRMNTN,
         CST_TYPE_CD_APRVD_CST_SRC,
         DRUG_UNIT_CST,
         PYMT_TYPE_FLAG,
         INGRDNT_CST_BILLED,
         INGRDNT_CST_SBMTD,
         INGRDNT_CST_CALCD,
         DSPNSNG_FEE_BILLED,
         OUT_OF_NTWRK_PNLTY,
         PTNT_PAY_AMT,
         SLS_TAX_BILLED,
         CLNT_COPAY,
         NET_PTNT_PAY,
         PLAN_CO_PAY,
         PROD_SLCTN_DFRNTL,
         PROFNL_SRVC_FEE_BILLED,
         INCNTV_AMT_BILLED,
         CLM_ADJSTMT_WTHLD_AMT,
         TOT_AMT_BILLED,
         OTHR_PYR_AMT_RECOGNIZED,
         OTHR_AMT_BILLED,
         PTNT_PAY_AMT_ATTRD_TO_DDCTBL,
         USL_AND_CUSTMRY_BLG_AMT,
         trim(PTNT_FNAME) PTNT_FNAME,
         Trim(PTNT_LNAME) PTNT_LNAME,
         PTNT_MI,
         PTNT_DOB,
         GNDR,
         trim(MMBR_ID_NUM) as MMBR_ID_NUM,
         MMBR_ZIP,
         PRSN_CD,
         PTNT_REL_TO_CARDHLDR,
         PRSCRBR_ID,
         PRSCRBR_ID_QLFYR,
         PRSCRBR_DEA_NUM,
         PRSCRBR_LAST_NAME,
         PRSCRBR_FIRST_NAME,
         PRSCRBR_SPCLTY_CD,
         PRSCRBR_NTWRK_ID,
         DGNS_CD,
         DGNS_CD_QLFYR,
         MDCL_CRTFCTN_QLFYR,
         PRIOR_AUTHRZTN_ID,
         PRIOR_AUTH_RSN_CD,
         CARDHLDR_ID,
         CARDHLDR_LNAME,
         CARDHLDR_FNAME,
         CARDHLDR_MI,
         CARDHLDR_DOB,
         CARDHLDR_ADR_1,
         CARDHLDR_ADR_2,
         CARDHLDR_CITY,
         CARDHLDR_STATE,
         CARDHLDR_ZIP,
         POS,
         RX_ORGN,
         PTNT_RSDNC,
         DT_RX_WRTN,
         PROD_SLCTN_CD,
         OTHR_CVRG_CD,
         TP_RSTCTN_CD,
         SPCLTY_FLAG,
         CMPND_CD,
         FRMLRY_STUS,
         PLAN_DRUG_STUS,
         NUM_OF_REFLS_AUTHRZD,
         MLT_SRC_CD,
         BRND_NAME_CD,
         DRUG_DEA_CLS_CD,
         PRMRY_CARE_PROV_ID_CD,
         CARE_FAC_ID_CD,
         CARE_QLFYR_CD,
         PLAN_CD,
         govt.UNIT_DOSE_IND UNIT_DOSE_IND,
         MNTNC_DRUG_CD,
         AHFS_CD,
         GPI,
         DRUG_GNRC_NAME,
         GCN,
         GCN_SEQ_NUM,
         trim( COALESCE( CARRIER,' ')) AS CARRIER,
         ACCOUNT,
         GRP_ID,
         trim(SSN) SSN,
         SBMT_DT_OF_CLM,
         SBMT_TIME_OF_CLM,
         RX_CLM_NUM,
         SQNC_NUM_OF_CLM,
         CLM_STUS,
         govt.TRANS_CD TRANS_CD,
         BRND_GNRC_IND,
         CLM_MSG,
         BNFT_MAX_FLAG,
         CLNT_FLAG,
         OOP_MAX_APLD,
         BNFT_MAX_APLD,
         PROC_CYC_DT,
         FRMLRY_CLM_FLAG,
         MDCL_CLM_FLAG,
         PRIOR_AUTH_CLM_FLAG,
         TRNSPLNT_FLAG,
         INJCTBL_PROD_FLAG,
         RXSOL_FRMLRY_FLAG,
         HALF_TAB,
         RSRVD,
         RSRVD2,
         RMTNC_TYPE,
         govt.CHK_DT CHK_DT,
         CHK_NUM,
         PLN_TYPE_CD,
         CLNT_PROD_CD,
         CLNT_RIDER_CD,
         CLNT_BNFT_CD,
         PLAN_QLFYR_CD,
         TIER_VAL,
         HLTH_REIMBRSMT_AMT,
         ALTRNT_INSRNC_FLAG,
         ADMIN_FEE,
         RGNL_DSTR_OVRD,
         CLM_ADJSTMT,
         govt.PRCSR_RSRVD PRCSR_RSRVD,
         govt.IS_DEL IS_DEL,
         govt.REC_CRT_DT REC_CRT_DT,
         govt.REC_CRT_TM REC_CRT_TM,
         govt.REC_UPDT_DT REC_UPDT_DT,
         govt.REC_UPDT_TM REC_UPDT_TM,
         govt.REC_XTRCT_SRC_CD REC_XTRCT_SRC_CD,
         govt.REC_INPUT_SRC_CD REC_INPUT_SRC_CD,
         govt.CYCLE_DT CYCLE_DT,
         prv.SRVC_PROV_NCPDP_ID_NUM as SRVC_PROV_NCPDP_ID_NUM,
         Left(trim(govt.MMBR_ID_NUM), 9) as MMBR_ID_NUM_JNKEY,
         left( trim(govt.ACCOUNT), 3) as GUM_SITE_KEY,
         ( case When ((len(trim(govt.MMBR_ID_NUM))=11 or len(trim(govt.MMBR_ID_NUM))=9) and trim(CARRIER)='MPDOVA' ) then 'COSM' else 'N/A' end ) as EDWELIG,
         alt1.Mbr_Alt_Id_Logic_Cd AS Mbr_Alt_Id_Logic_Cd,
         alt2.Mbr_Alt_Id_Logic_Cd AS Mbr_Alt_Id_Logic_Cd2,
		 ( case When (trim(CARRIER) in ('PSI2501','MPDOVA')) then 'FCET' else 'N/A' end ) as EDWELIG2
FROM     STG.STG_PSRX_CLM_GOVT govt 
         Inner join temp_ndc ndc on trim(govt.PROD_SRVC_ID) = trim(ndc.NDC) and rnk =1 and  govt.FILL_DT BETWEEN ndc.NDC_DRG_ROW_EFF_DT and ndc.NDC_DRG_ROW_END_DT and 

         ( trim(ndc.AHFS_THERAPEUTIC_CLSS_CD) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000') AND 
         substr(trim(ndc.EXT_AHFS_THRPTC_CLSS_CD),1,6) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000') AND 
         trim(ndc.GNRC_NM) not in ( 'ACETAMINOPHEN/BUTALBITAL', 'ACETAMINOPHEN/CAFFEINE/BUTALB', 'ASPIRIN/BUTALBITAL', 'ASPIRIN/CAFFEINE/BUTALBITAL', 'ASPIRIN/MEPROBAMATE', 'BROMOCRIPTINE MESYLATE', 'PERGOLIDE MESYLATE', 'BIPERIDEN HCL', 'PROCYCLIDINE HCL', 'SILDENAFIL CITRATE', 'TRIHEXYPHENIDYL HCL', 'BENZTROPINE MESYLATE', 'DIPHENHYDRAMINE HCL') ) 

         left outer join STM.STM_PSRX_CLM_PROV prv on trim(govt.SRVC_PROV_ID) = trim(prv.SRVC_PROV_ID)
         Left Outer Join STM.STM_PSRX_CARR_MBR_ALT alt1 ON  trim(alt1.Carrier_id) = trim(govt.CARRIER) AND alt1.EDW_ELIG = EDWELIG1
         Left Outer Join STM.STM_PSRX_CARR_MBR_ALT alt2 ON  trim(alt2.Carrier_id) = trim(govt.CARRIER) AND alt2.EDW_ELIG = EDWELIG2
WHERE  
 		govt.REC_UPDT_DT >= '#RUN_FROMDT#'
AND      govt.REC_UPDT_DT <= '#RUN_TODT#' 
-- or govt.FILL_DT >= '2017-01-01' 
;



create or replace temporary table ds_GOVT_COSM_LOGCD3 as 
select 
	 	 NDC_DRG_ROW_EFF_DT,
         REC_ID,
         PRCSR_ID_NUM,
         PRCSR_BATCH_NUM,
         SRVC_PROV_ID,
         SRVC_PROV_ID_QLFYR,
         CLM_ORGNTN_FLAG,
         PHRMCY_NTWRK_CD,
         SRVC_PROV_NAME,
         SRVC_PROV_REL_CD,
         SRVC_PROV_DSPNSR_CLS,
         SRVC_PROV_DSPNSR_TYPE,
         RX_SRVC_REF_NUM,
         FILL_DT,
         PROD_SRVC_ID,
         PROD_ID_QLFYR,
         PROD_LABEL_NAME_WTH_DSG,
         PROD_MFTR,
         NEW_REFL_CD,
         MTRC_DCML_QTY,
         DAYS_SUPLY,
         DRUG_RX_OTC_IND,
         DRUG_STGTH,
         UOM,
         ROA,
         DSG_FORM,
         HCPCS_CD,
         BASIS_OF_REIMBRSMT,
         BASIS_OF_CST_DTRMNTN,
         CST_TYPE_CD_APRVD_CST_SRC,
         DRUG_UNIT_CST,
         PYMT_TYPE_FLAG,
         INGRDNT_CST_BILLED,
         INGRDNT_CST_SBMTD,
         INGRDNT_CST_CALCD,
         DSPNSNG_FEE_BILLED,
         OUT_OF_NTWRK_PNLTY,
         PTNT_PAY_AMT,
         SLS_TAX_BILLED,
         CLNT_COPAY,
         NET_PTNT_PAY,
         PLAN_CO_PAY,
         PROD_SLCTN_DFRNTL,
         PROFNL_SRVC_FEE_BILLED,
         INCNTV_AMT_BILLED,
         CLM_ADJSTMT_WTHLD_AMT,
         TOT_AMT_BILLED,
         OTHR_PYR_AMT_RECOGNIZED,
         OTHR_AMT_BILLED,
         PTNT_PAY_AMT_ATTRD_TO_DDCTBL,
         USL_AND_CUSTMRY_BLG_AMT,
         PTNT_FNAME,
         PTNT_LNAME,
         PTNT_MI,
         PTNT_DOB,
         GNDR,
         MMBR_ID_NUM,
         MMBR_ZIP,
         PRSN_CD,
         PTNT_REL_TO_CARDHLDR,
         PRSCRBR_ID,
         PRSCRBR_ID_QLFYR,
         PRSCRBR_DEA_NUM,
         PRSCRBR_LAST_NAME,
         PRSCRBR_FIRST_NAME,
         PRSCRBR_SPCLTY_CD,
         PRSCRBR_NTWRK_ID,
         DGNS_CD,
         DGNS_CD_QLFYR,
         MDCL_CRTFCTN_QLFYR,
         PRIOR_AUTHRZTN_ID,
         PRIOR_AUTH_RSN_CD,
         CARDHLDR_ID,
         CARDHLDR_LNAME,
         CARDHLDR_FNAME,
         CARDHLDR_MI,
         CARDHLDR_DOB,
         CARDHLDR_ADR_1,
         CARDHLDR_ADR_2,
         CARDHLDR_CITY,
         CARDHLDR_STATE,
         CARDHLDR_ZIP,
         POS,
         RX_ORGN,
         PTNT_RSDNC,
         DT_RX_WRTN,
         PROD_SLCTN_CD,
         OTHR_CVRG_CD,
         TP_RSTCTN_CD,
         SPCLTY_FLAG,
         CMPND_CD,
         FRMLRY_STUS,
         PLAN_DRUG_STUS,
         NUM_OF_REFLS_AUTHRZD,
         MLT_SRC_CD,
         BRND_NAME_CD,
         DRUG_DEA_CLS_CD,
         PRMRY_CARE_PROV_ID_CD,
         CARE_FAC_ID_CD,
         CARE_QLFYR_CD,
         PLAN_CD,
         UNIT_DOSE_IND,
         MNTNC_DRUG_CD,
         AHFS_CD,
         GPI,
         DRUG_GNRC_NAME,
         GCN,
         GCN_SEQ_NUM,
         CARRIER,
         ACCOUNT,
         GRP_ID,
         SSN,
         SBMT_DT_OF_CLM,
         SBMT_TIME_OF_CLM,
         RX_CLM_NUM,
         SQNC_NUM_OF_CLM,
         CLM_STUS,
         TRANS_CD,
         BRND_GNRC_IND,
         CLM_MSG,
         BNFT_MAX_FLAG,
         CLNT_FLAG,
         OOP_MAX_APLD,
         BNFT_MAX_APLD,
         PROC_CYC_DT,
         FRMLRY_CLM_FLAG,
         MDCL_CLM_FLAG,
         PRIOR_AUTH_CLM_FLAG,
         TRNSPLNT_FLAG,
         INJCTBL_PROD_FLAG,
         RXSOL_FRMLRY_FLAG,
         HALF_TAB,
         RSRVD,
         RSRVD2,
         RMTNC_TYPE,
         CHK_DT,
         CHK_NUM,
         PLN_TYPE_CD,
         CLNT_PROD_CD,
         CLNT_RIDER_CD,
         CLNT_BNFT_CD,
         PLAN_QLFYR_CD,
         TIER_VAL,
         HLTH_REIMBRSMT_AMT,
         ALTRNT_INSRNC_FLAG,
         ADMIN_FEE,
         RGNL_DSTR_OVRD,
         CLM_ADJSTMT,
         PRCSR_RSRVD,
         IS_DEL,
         REC_CRT_DT,
         REC_CRT_TM,
         REC_UPDT_DT,
         REC_UPDT_TM,
         REC_XTRCT_SRC_CD,
         REC_INPUT_SRC_CD,
         CYCLE_DT,
         SRVC_PROV_NCPDP_ID_NUM,
         MMBR_ID_NUM_JNKEY,
         GUM_SITE_KEY,
         EDWELIG,
         Mbr_Alt_Id_Logic_Cd
		 from extSTG_PSRX_CLM_GOVT where Mbr_Alt_Id_Logic_Cd = 3;
 


create or replace temporary table ds_GOVT_PBH_LOGCD4 as 
select 
	 	 NDC_DRG_ROW_EFF_DT,
         REC_ID,
         PRCSR_ID_NUM,
         PRCSR_BATCH_NUM,
         SRVC_PROV_ID,
         SRVC_PROV_ID_QLFYR,
         CLM_ORGNTN_FLAG,
         PHRMCY_NTWRK_CD,
         SRVC_PROV_NAME,
         SRVC_PROV_REL_CD,
         SRVC_PROV_DSPNSR_CLS,
         SRVC_PROV_DSPNSR_TYPE,
         RX_SRVC_REF_NUM,
         FILL_DT,
         PROD_SRVC_ID,
         PROD_ID_QLFYR,
         PROD_LABEL_NAME_WTH_DSG,
         PROD_MFTR,
         NEW_REFL_CD,
         MTRC_DCML_QTY,
         DAYS_SUPLY,
         DRUG_RX_OTC_IND,
         DRUG_STGTH,
         UOM,
         ROA,
         DSG_FORM,
         HCPCS_CD,
         BASIS_OF_REIMBRSMT,
         BASIS_OF_CST_DTRMNTN,
         CST_TYPE_CD_APRVD_CST_SRC,
         DRUG_UNIT_CST,
         PYMT_TYPE_FLAG,
         INGRDNT_CST_BILLED,
         INGRDNT_CST_SBMTD,
         INGRDNT_CST_CALCD,
         DSPNSNG_FEE_BILLED,
         OUT_OF_NTWRK_PNLTY,
         PTNT_PAY_AMT,
         SLS_TAX_BILLED,
         CLNT_COPAY,
         NET_PTNT_PAY,
         PLAN_CO_PAY,
         PROD_SLCTN_DFRNTL,
         PROFNL_SRVC_FEE_BILLED,
         INCNTV_AMT_BILLED,
         CLM_ADJSTMT_WTHLD_AMT,
         TOT_AMT_BILLED,
         OTHR_PYR_AMT_RECOGNIZED,
         OTHR_AMT_BILLED,
         PTNT_PAY_AMT_ATTRD_TO_DDCTBL,
         USL_AND_CUSTMRY_BLG_AMT,
         PTNT_FNAME,
         PTNT_LNAME,
         PTNT_MI,
         PTNT_DOB,
         GNDR,
         MMBR_ID_NUM,
         MMBR_ZIP,
         PRSN_CD,
         PTNT_REL_TO_CARDHLDR,
         PRSCRBR_ID,
         PRSCRBR_ID_QLFYR,
         PRSCRBR_DEA_NUM,
         PRSCRBR_LAST_NAME,
         PRSCRBR_FIRST_NAME,
         PRSCRBR_SPCLTY_CD,
         PRSCRBR_NTWRK_ID,
         DGNS_CD,
         DGNS_CD_QLFYR,
         MDCL_CRTFCTN_QLFYR,
         PRIOR_AUTHRZTN_ID,
         PRIOR_AUTH_RSN_CD,
         CARDHLDR_ID,
         CARDHLDR_LNAME,
         CARDHLDR_FNAME,
         CARDHLDR_MI,
         CARDHLDR_DOB,
         CARDHLDR_ADR_1,
         CARDHLDR_ADR_2,
         CARDHLDR_CITY,
         CARDHLDR_STATE,
         CARDHLDR_ZIP,
         POS,
         RX_ORGN,
         PTNT_RSDNC,
         DT_RX_WRTN,
         PROD_SLCTN_CD,
         OTHR_CVRG_CD,
         TP_RSTCTN_CD,
         SPCLTY_FLAG,
         CMPND_CD,
         FRMLRY_STUS,
         PLAN_DRUG_STUS,
         NUM_OF_REFLS_AUTHRZD,
         MLT_SRC_CD,
         BRND_NAME_CD,
         DRUG_DEA_CLS_CD,
         PRMRY_CARE_PROV_ID_CD,
         CARE_FAC_ID_CD,
         CARE_QLFYR_CD,
         PLAN_CD,
         UNIT_DOSE_IND,
         MNTNC_DRUG_CD,
         AHFS_CD,
         GPI,
         DRUG_GNRC_NAME,
         GCN,
         GCN_SEQ_NUM,
         CARRIER,
         ACCOUNT,
         GRP_ID,
         SSN,
         SBMT_DT_OF_CLM,
         SBMT_TIME_OF_CLM,
         RX_CLM_NUM,
         SQNC_NUM_OF_CLM,
         CLM_STUS,
         TRANS_CD,
         BRND_GNRC_IND,
         CLM_MSG,
         BNFT_MAX_FLAG,
         CLNT_FLAG,
         OOP_MAX_APLD,
         BNFT_MAX_APLD,
         PROC_CYC_DT,
         FRMLRY_CLM_FLAG,
         MDCL_CLM_FLAG,
         PRIOR_AUTH_CLM_FLAG,
         TRNSPLNT_FLAG,
         INJCTBL_PROD_FLAG,
         RXSOL_FRMLRY_FLAG,
         HALF_TAB,
         RSRVD,
         RSRVD2,
         RMTNC_TYPE,
         CHK_DT,
         CHK_NUM,
         PLN_TYPE_CD,
         CLNT_PROD_CD,
         CLNT_RIDER_CD,
         CLNT_BNFT_CD,
         PLAN_QLFYR_CD,
         TIER_VAL,
         HLTH_REIMBRSMT_AMT,
         ALTRNT_INSRNC_FLAG,
         ADMIN_FEE,
         RGNL_DSTR_OVRD,
         CLM_ADJSTMT,
         PRCSR_RSRVD,
         IS_DEL,
         REC_CRT_DT,
         REC_CRT_TM,
         REC_UPDT_DT,
         REC_UPDT_TM,
         REC_XTRCT_SRC_CD,
         REC_INPUT_SRC_CD,
         CYCLE_DT,
         SRVC_PROV_NCPDP_ID_NUM,
         MMBR_ID_NUM_JNKEY,
         GUM_SITE_KEY,
         EDWELIG,
         Mbr_Alt_Id_Logic_Cd
		 from extSTG_PSRX_CLM_GOVT where Mbr_Alt_Id_Logic_Cd = 4;


create or replace temporary table ds_GOVT_COSM_LOGCD5 as 
select 
	 	 NDC_DRG_ROW_EFF_DT,
         REC_ID,
         PRCSR_ID_NUM,
         PRCSR_BATCH_NUM,
         SRVC_PROV_ID,
         SRVC_PROV_ID_QLFYR,
         CLM_ORGNTN_FLAG,
         PHRMCY_NTWRK_CD,
         SRVC_PROV_NAME,
         SRVC_PROV_REL_CD,
         SRVC_PROV_DSPNSR_CLS,
         SRVC_PROV_DSPNSR_TYPE,
         RX_SRVC_REF_NUM,
         FILL_DT,
         PROD_SRVC_ID,
         PROD_ID_QLFYR,
         PROD_LABEL_NAME_WTH_DSG,
         PROD_MFTR,
         NEW_REFL_CD,
         MTRC_DCML_QTY,
         DAYS_SUPLY,
         DRUG_RX_OTC_IND,
         DRUG_STGTH,
         UOM,
         ROA,
         DSG_FORM,
         HCPCS_CD,
         BASIS_OF_REIMBRSMT,
         BASIS_OF_CST_DTRMNTN,
         CST_TYPE_CD_APRVD_CST_SRC,
         DRUG_UNIT_CST,
         PYMT_TYPE_FLAG,
         INGRDNT_CST_BILLED,
         INGRDNT_CST_SBMTD,
         INGRDNT_CST_CALCD,
         DSPNSNG_FEE_BILLED,
         OUT_OF_NTWRK_PNLTY,
         PTNT_PAY_AMT,
         SLS_TAX_BILLED,
         CLNT_COPAY,
         NET_PTNT_PAY,
         PLAN_CO_PAY,
         PROD_SLCTN_DFRNTL,
         PROFNL_SRVC_FEE_BILLED,
         INCNTV_AMT_BILLED,
         CLM_ADJSTMT_WTHLD_AMT,
         TOT_AMT_BILLED,
         OTHR_PYR_AMT_RECOGNIZED,
         OTHR_AMT_BILLED,
         PTNT_PAY_AMT_ATTRD_TO_DDCTBL,
         USL_AND_CUSTMRY_BLG_AMT,
         PTNT_FNAME,
         PTNT_LNAME,
         PTNT_MI,
         PTNT_DOB,
         GNDR,
         MMBR_ID_NUM,
         MMBR_ZIP,
         PRSN_CD,
         PTNT_REL_TO_CARDHLDR,
         PRSCRBR_ID,
         PRSCRBR_ID_QLFYR,
         PRSCRBR_DEA_NUM,
         PRSCRBR_LAST_NAME,
         PRSCRBR_FIRST_NAME,
         PRSCRBR_SPCLTY_CD,
         PRSCRBR_NTWRK_ID,
         DGNS_CD,
         DGNS_CD_QLFYR,
         MDCL_CRTFCTN_QLFYR,
         PRIOR_AUTHRZTN_ID,
         PRIOR_AUTH_RSN_CD,
         CARDHLDR_ID,
         CARDHLDR_LNAME,
         CARDHLDR_FNAME,
         CARDHLDR_MI,
         CARDHLDR_DOB,
         CARDHLDR_ADR_1,
         CARDHLDR_ADR_2,
         CARDHLDR_CITY,
         CARDHLDR_STATE,
         CARDHLDR_ZIP,
         POS,
         RX_ORGN,
         PTNT_RSDNC,
         DT_RX_WRTN,
         PROD_SLCTN_CD,
         OTHR_CVRG_CD,
         TP_RSTCTN_CD,
         SPCLTY_FLAG,
         CMPND_CD,
         FRMLRY_STUS,
         PLAN_DRUG_STUS,
         NUM_OF_REFLS_AUTHRZD,
         MLT_SRC_CD,
         BRND_NAME_CD,
         DRUG_DEA_CLS_CD,
         PRMRY_CARE_PROV_ID_CD,
         CARE_FAC_ID_CD,
         CARE_QLFYR_CD,
         PLAN_CD,
         UNIT_DOSE_IND,
         MNTNC_DRUG_CD,
         AHFS_CD,
         GPI,
         DRUG_GNRC_NAME,
         GCN,
         GCN_SEQ_NUM,
         CARRIER,
         ACCOUNT,
         GRP_ID,
         SSN,
         SBMT_DT_OF_CLM,
         SBMT_TIME_OF_CLM,
         RX_CLM_NUM,
         SQNC_NUM_OF_CLM,
         CLM_STUS,
         TRANS_CD,
         BRND_GNRC_IND,
         CLM_MSG,
         BNFT_MAX_FLAG,
         CLNT_FLAG,
         OOP_MAX_APLD,
         BNFT_MAX_APLD,
         PROC_CYC_DT,
         FRMLRY_CLM_FLAG,
         MDCL_CLM_FLAG,
         PRIOR_AUTH_CLM_FLAG,
         TRNSPLNT_FLAG,
         INJCTBL_PROD_FLAG,
         RXSOL_FRMLRY_FLAG,
         HALF_TAB,
         RSRVD,
         RSRVD2,
         RMTNC_TYPE,
         CHK_DT,
         CHK_NUM,
         PLN_TYPE_CD,
         CLNT_PROD_CD,
         CLNT_RIDER_CD,
         CLNT_BNFT_CD,
         PLAN_QLFYR_CD,
         TIER_VAL,
         HLTH_REIMBRSMT_AMT,
         ALTRNT_INSRNC_FLAG,
         ADMIN_FEE,
         RGNL_DSTR_OVRD,
         CLM_ADJSTMT,
         PRCSR_RSRVD,
         IS_DEL,
         REC_CRT_DT,
         REC_CRT_TM,
         REC_UPDT_DT,
         REC_UPDT_TM,
         REC_XTRCT_SRC_CD,
         REC_INPUT_SRC_CD,
         CYCLE_DT,
         SRVC_PROV_NCPDP_ID_NUM,
         MMBR_ID_NUM_JNKEY,
         GUM_SITE_KEY,
         EDWELIG,
         Mbr_Alt_Id_Logic_Cd
		 from extSTG_PSRX_CLM_GOVT where Mbr_Alt_Id_Logic_Cd = 5;



create or replace temporary table ds_GOVT_COSM_LOGCD6_7 as 
select 
	 	 NDC_DRG_ROW_EFF_DT,
         REC_ID,
         PRCSR_ID_NUM,
         PRCSR_BATCH_NUM,
         SRVC_PROV_ID,
         SRVC_PROV_ID_QLFYR,
         CLM_ORGNTN_FLAG,
         PHRMCY_NTWRK_CD,
         SRVC_PROV_NAME,
         SRVC_PROV_REL_CD,
         SRVC_PROV_DSPNSR_CLS,
         SRVC_PROV_DSPNSR_TYPE,
         RX_SRVC_REF_NUM,
         FILL_DT,
         PROD_SRVC_ID,
         PROD_ID_QLFYR,
         PROD_LABEL_NAME_WTH_DSG,
         PROD_MFTR,
         NEW_REFL_CD,
         MTRC_DCML_QTY,
         DAYS_SUPLY,
         DRUG_RX_OTC_IND,
         DRUG_STGTH,
         UOM,
         ROA,
         DSG_FORM,
         HCPCS_CD,
         BASIS_OF_REIMBRSMT,
         BASIS_OF_CST_DTRMNTN,
         CST_TYPE_CD_APRVD_CST_SRC,
         DRUG_UNIT_CST,
         PYMT_TYPE_FLAG,
         INGRDNT_CST_BILLED,
         INGRDNT_CST_SBMTD,
         INGRDNT_CST_CALCD,
         DSPNSNG_FEE_BILLED,
         OUT_OF_NTWRK_PNLTY,
         PTNT_PAY_AMT,
         SLS_TAX_BILLED,
         CLNT_COPAY,
         NET_PTNT_PAY,
         PLAN_CO_PAY,
         PROD_SLCTN_DFRNTL,
         PROFNL_SRVC_FEE_BILLED,
         INCNTV_AMT_BILLED,
         CLM_ADJSTMT_WTHLD_AMT,
         TOT_AMT_BILLED,
         OTHR_PYR_AMT_RECOGNIZED,
         OTHR_AMT_BILLED,
         PTNT_PAY_AMT_ATTRD_TO_DDCTBL,
         USL_AND_CUSTMRY_BLG_AMT,
         PTNT_FNAME,
         PTNT_LNAME,
         PTNT_MI,
         PTNT_DOB,
         GNDR,
         MMBR_ID_NUM,
         MMBR_ZIP,
         PRSN_CD,
         PTNT_REL_TO_CARDHLDR,
         PRSCRBR_ID,
         PRSCRBR_ID_QLFYR,
         PRSCRBR_DEA_NUM,
         PRSCRBR_LAST_NAME,
         PRSCRBR_FIRST_NAME,
         PRSCRBR_SPCLTY_CD,
         PRSCRBR_NTWRK_ID,
         DGNS_CD,
         DGNS_CD_QLFYR,
         MDCL_CRTFCTN_QLFYR,
         PRIOR_AUTHRZTN_ID,
         PRIOR_AUTH_RSN_CD,
         CARDHLDR_ID,
         CARDHLDR_LNAME,
         CARDHLDR_FNAME,
         CARDHLDR_MI,
         CARDHLDR_DOB,
         CARDHLDR_ADR_1,
         CARDHLDR_ADR_2,
         CARDHLDR_CITY,
         CARDHLDR_STATE,
         CARDHLDR_ZIP,
         POS,
         RX_ORGN,
         PTNT_RSDNC,
         DT_RX_WRTN,
         PROD_SLCTN_CD,
         OTHR_CVRG_CD,
         TP_RSTCTN_CD,
         SPCLTY_FLAG,
         CMPND_CD,
         FRMLRY_STUS,
         PLAN_DRUG_STUS,
         NUM_OF_REFLS_AUTHRZD,
         MLT_SRC_CD,
         BRND_NAME_CD,
         DRUG_DEA_CLS_CD,
         PRMRY_CARE_PROV_ID_CD,
         CARE_FAC_ID_CD,
         CARE_QLFYR_CD,
         PLAN_CD,
         UNIT_DOSE_IND,
         MNTNC_DRUG_CD,
         AHFS_CD,
         GPI,
         DRUG_GNRC_NAME,
         GCN,
         GCN_SEQ_NUM,
         CARRIER,
         ACCOUNT,
         GRP_ID,
         SSN,
         SBMT_DT_OF_CLM,
         SBMT_TIME_OF_CLM,
         RX_CLM_NUM,
         SQNC_NUM_OF_CLM,
         CLM_STUS,
         TRANS_CD,
         BRND_GNRC_IND,
         CLM_MSG,
         BNFT_MAX_FLAG,
         CLNT_FLAG,
         OOP_MAX_APLD,
         BNFT_MAX_APLD,
         PROC_CYC_DT,
         FRMLRY_CLM_FLAG,
         MDCL_CLM_FLAG,
         PRIOR_AUTH_CLM_FLAG,
         TRNSPLNT_FLAG,
         INJCTBL_PROD_FLAG,
         RXSOL_FRMLRY_FLAG,
         HALF_TAB,
         RSRVD,
         RSRVD2,
         RMTNC_TYPE,
         CHK_DT,
         CHK_NUM,
         PLN_TYPE_CD,
         CLNT_PROD_CD,
         CLNT_RIDER_CD,
         CLNT_BNFT_CD,
         PLAN_QLFYR_CD,
         TIER_VAL,
         HLTH_REIMBRSMT_AMT,
         ALTRNT_INSRNC_FLAG,
         ADMIN_FEE,
         RGNL_DSTR_OVRD,
         CLM_ADJSTMT,
         PRCSR_RSRVD,
         IS_DEL,
         REC_CRT_DT,
         REC_CRT_TM,
         REC_UPDT_DT,
         REC_UPDT_TM,
         REC_XTRCT_SRC_CD,
         REC_INPUT_SRC_CD,
         CYCLE_DT,
         SRVC_PROV_NCPDP_ID_NUM,
         MMBR_ID_NUM_JNKEY,
         GUM_SITE_KEY,
         EDWELIG,
         Mbr_Alt_Id_Logic_Cd
		 from extSTG_PSRX_CLM_GOVT where (Mbr_Alt_Id_Logic_Cd = 6 or Mbr_Alt_Id_Logic_Cd = 7 ) ;



create or replace temporary table ds_GOVT_FCETLOGCD8 as 
select 
	 	 NDC_DRG_ROW_EFF_DT,
         REC_ID,
         PRCSR_ID_NUM,
         PRCSR_BATCH_NUM,
         SRVC_PROV_ID,
         SRVC_PROV_ID_QLFYR,
         CLM_ORGNTN_FLAG,
         PHRMCY_NTWRK_CD,
         SRVC_PROV_NAME,
         SRVC_PROV_REL_CD,
         SRVC_PROV_DSPNSR_CLS,
         SRVC_PROV_DSPNSR_TYPE,
         RX_SRVC_REF_NUM,
         FILL_DT,
         PROD_SRVC_ID,
         PROD_ID_QLFYR,
         PROD_LABEL_NAME_WTH_DSG,
         PROD_MFTR,
         NEW_REFL_CD,
         MTRC_DCML_QTY,
         DAYS_SUPLY,
         DRUG_RX_OTC_IND,
         DRUG_STGTH,
         UOM,
         ROA,
         DSG_FORM,
         HCPCS_CD,
         BASIS_OF_REIMBRSMT,
         BASIS_OF_CST_DTRMNTN,
         CST_TYPE_CD_APRVD_CST_SRC,
         DRUG_UNIT_CST,
         PYMT_TYPE_FLAG,
         INGRDNT_CST_BILLED,
         INGRDNT_CST_SBMTD,
         INGRDNT_CST_CALCD,
         DSPNSNG_FEE_BILLED,
         OUT_OF_NTWRK_PNLTY,
         PTNT_PAY_AMT,
         SLS_TAX_BILLED,
         CLNT_COPAY,
         NET_PTNT_PAY,
         PLAN_CO_PAY,
         PROD_SLCTN_DFRNTL,
         PROFNL_SRVC_FEE_BILLED,
         INCNTV_AMT_BILLED,
         CLM_ADJSTMT_WTHLD_AMT,
         TOT_AMT_BILLED,
         OTHR_PYR_AMT_RECOGNIZED,
         OTHR_AMT_BILLED,
         PTNT_PAY_AMT_ATTRD_TO_DDCTBL,
         USL_AND_CUSTMRY_BLG_AMT,
         PTNT_FNAME,
         PTNT_LNAME,
         PTNT_MI,
         PTNT_DOB,
         GNDR,
         MMBR_ID_NUM,
         MMBR_ZIP,
         PRSN_CD,
         PTNT_REL_TO_CARDHLDR,
         PRSCRBR_ID,
         PRSCRBR_ID_QLFYR,
         PRSCRBR_DEA_NUM,
         PRSCRBR_LAST_NAME,
         PRSCRBR_FIRST_NAME,
         PRSCRBR_SPCLTY_CD,
         PRSCRBR_NTWRK_ID,
         DGNS_CD,
         DGNS_CD_QLFYR,
         MDCL_CRTFCTN_QLFYR,
         PRIOR_AUTHRZTN_ID,
         PRIOR_AUTH_RSN_CD,
         CARDHLDR_ID,
         CARDHLDR_LNAME,
         CARDHLDR_FNAME,
         CARDHLDR_MI,
         CARDHLDR_DOB,
         CARDHLDR_ADR_1,
         CARDHLDR_ADR_2,
         CARDHLDR_CITY,
         CARDHLDR_STATE,
         CARDHLDR_ZIP,
         POS,
         RX_ORGN,
         PTNT_RSDNC,
         DT_RX_WRTN,
         PROD_SLCTN_CD,
         OTHR_CVRG_CD,
         TP_RSTCTN_CD,
         SPCLTY_FLAG,
         CMPND_CD,
         FRMLRY_STUS,
         PLAN_DRUG_STUS,
         NUM_OF_REFLS_AUTHRZD,
         MLT_SRC_CD,
         BRND_NAME_CD,
         DRUG_DEA_CLS_CD,
         PRMRY_CARE_PROV_ID_CD,
         CARE_FAC_ID_CD,
         CARE_QLFYR_CD,
         PLAN_CD,
         UNIT_DOSE_IND,
         MNTNC_DRUG_CD,
         AHFS_CD,
         GPI,
         DRUG_GNRC_NAME,
         GCN,
         GCN_SEQ_NUM,
         CARRIER,
         ACCOUNT,
         GRP_ID,
         SSN,
         SBMT_DT_OF_CLM,
         SBMT_TIME_OF_CLM,
         RX_CLM_NUM,
         SQNC_NUM_OF_CLM,
         CLM_STUS,
         TRANS_CD,
         BRND_GNRC_IND,
         CLM_MSG,
         BNFT_MAX_FLAG,
         CLNT_FLAG,
         OOP_MAX_APLD,
         BNFT_MAX_APLD,
         PROC_CYC_DT,
         FRMLRY_CLM_FLAG,
         MDCL_CLM_FLAG,
         PRIOR_AUTH_CLM_FLAG,
         TRNSPLNT_FLAG,
         INJCTBL_PROD_FLAG,
         RXSOL_FRMLRY_FLAG,
         HALF_TAB,
         RSRVD,
         RSRVD2,
         RMTNC_TYPE,
         CHK_DT,
         CHK_NUM,
         PLN_TYPE_CD,
         CLNT_PROD_CD,
         CLNT_RIDER_CD,
         CLNT_BNFT_CD,
         PLAN_QLFYR_CD,
         TIER_VAL,
         HLTH_REIMBRSMT_AMT,
         ALTRNT_INSRNC_FLAG,
         ADMIN_FEE,
         RGNL_DSTR_OVRD,
         CLM_ADJSTMT,
         PRCSR_RSRVD,
         IS_DEL,
         REC_CRT_DT,
         REC_CRT_TM,
         REC_UPDT_DT,
         REC_UPDT_TM,
         REC_XTRCT_SRC_CD,
         REC_INPUT_SRC_CD,
         CYCLE_DT,
         SRVC_PROV_NCPDP_ID_NUM,
         MMBR_ID_NUM_JNKEY,
         GUM_SITE_KEY,
         EDWELIG2 as EDWELIG,
         Mbr_Alt_Id_Logic_Cd2 as Mbr_Alt_Id_Logic_Cd
		 from extSTG_PSRX_CLM_GOVT where ( Mbr_Alt_Id_Logic_Cd2 = 8 or Mbr_Alt_Id_Logic_Cd = 8 ) ;
		 
		 
		
create or replace temporary table cpy_dropped_carr as 
select 
	 	 NDC_DRG_ROW_EFF_DT,
         REC_ID,
         PRCSR_ID_NUM,
         PRCSR_BATCH_NUM,
         SRVC_PROV_ID,
         SRVC_PROV_ID_QLFYR,
         CLM_ORGNTN_FLAG,
         PHRMCY_NTWRK_CD,
         SRVC_PROV_NAME,
         SRVC_PROV_REL_CD,
         SRVC_PROV_DSPNSR_CLS,
         SRVC_PROV_DSPNSR_TYPE,
         RX_SRVC_REF_NUM,
         FILL_DT,
         PROD_SRVC_ID,
         PROD_ID_QLFYR,
         PROD_LABEL_NAME_WTH_DSG,
         PROD_MFTR,
         NEW_REFL_CD,
         MTRC_DCML_QTY,
         DAYS_SUPLY,
         DRUG_RX_OTC_IND,
         DRUG_STGTH,
         UOM,
         ROA,
         DSG_FORM,
         HCPCS_CD,
         BASIS_OF_REIMBRSMT,
         BASIS_OF_CST_DTRMNTN,
         CST_TYPE_CD_APRVD_CST_SRC,
         DRUG_UNIT_CST,
         PYMT_TYPE_FLAG,
         INGRDNT_CST_BILLED,
         INGRDNT_CST_SBMTD,
         INGRDNT_CST_CALCD,
         DSPNSNG_FEE_BILLED,
         OUT_OF_NTWRK_PNLTY,
         PTNT_PAY_AMT,
         SLS_TAX_BILLED,
         CLNT_COPAY,
         NET_PTNT_PAY,
         PLAN_CO_PAY,
         PROD_SLCTN_DFRNTL,
         PROFNL_SRVC_FEE_BILLED,
         INCNTV_AMT_BILLED,
         CLM_ADJSTMT_WTHLD_AMT,
         TOT_AMT_BILLED,
         OTHR_PYR_AMT_RECOGNIZED,
         OTHR_AMT_BILLED,
         PTNT_PAY_AMT_ATTRD_TO_DDCTBL,
         USL_AND_CUSTMRY_BLG_AMT,
         PTNT_FNAME,
         PTNT_LNAME,
         PTNT_MI,
         PTNT_DOB,
         GNDR,
         MMBR_ID_NUM,
         MMBR_ZIP,
         PRSN_CD,
         PTNT_REL_TO_CARDHLDR,
         PRSCRBR_ID,
         PRSCRBR_ID_QLFYR,
         PRSCRBR_DEA_NUM,
         PRSCRBR_LAST_NAME,
         PRSCRBR_FIRST_NAME,
         PRSCRBR_SPCLTY_CD,
         PRSCRBR_NTWRK_ID,
         DGNS_CD,
         DGNS_CD_QLFYR,
         MDCL_CRTFCTN_QLFYR,
         PRIOR_AUTHRZTN_ID,
         PRIOR_AUTH_RSN_CD,
         CARDHLDR_ID,
         CARDHLDR_LNAME,
         CARDHLDR_FNAME,
         CARDHLDR_MI,
         CARDHLDR_DOB,
         CARDHLDR_ADR_1,
         CARDHLDR_ADR_2,
         CARDHLDR_CITY,
         CARDHLDR_STATE,
         CARDHLDR_ZIP,
         POS,
         RX_ORGN,
         PTNT_RSDNC,
         DT_RX_WRTN,
         PROD_SLCTN_CD,
         OTHR_CVRG_CD,
         TP_RSTCTN_CD,
         SPCLTY_FLAG,
         CMPND_CD,
         FRMLRY_STUS,
         PLAN_DRUG_STUS,
         NUM_OF_REFLS_AUTHRZD,
         MLT_SRC_CD,
         BRND_NAME_CD,
         DRUG_DEA_CLS_CD,
         PRMRY_CARE_PROV_ID_CD,
         CARE_FAC_ID_CD,
         CARE_QLFYR_CD,
         PLAN_CD,
         UNIT_DOSE_IND,
         MNTNC_DRUG_CD,
         AHFS_CD,
         GPI,
         DRUG_GNRC_NAME,
         GCN,
         GCN_SEQ_NUM,
         CARRIER,
         ACCOUNT,
         GRP_ID,
         SSN,
         SBMT_DT_OF_CLM,
         SBMT_TIME_OF_CLM,
         RX_CLM_NUM,
         SQNC_NUM_OF_CLM,
         CLM_STUS,
         TRANS_CD,
         BRND_GNRC_IND,
         CLM_MSG,
         BNFT_MAX_FLAG,
         CLNT_FLAG,
         OOP_MAX_APLD,
         BNFT_MAX_APLD,
         PROC_CYC_DT,
         FRMLRY_CLM_FLAG,
         MDCL_CLM_FLAG,
         PRIOR_AUTH_CLM_FLAG,
         TRNSPLNT_FLAG,
         INJCTBL_PROD_FLAG,
         RXSOL_FRMLRY_FLAG,
         HALF_TAB,
         RSRVD,
         RSRVD2,
         RMTNC_TYPE,
         CHK_DT,
         CHK_NUM,
         PLN_TYPE_CD,
         CLNT_PROD_CD,
         CLNT_RIDER_CD,
         CLNT_BNFT_CD,
         PLAN_QLFYR_CD,
         TIER_VAL,
         HLTH_REIMBRSMT_AMT,
         ALTRNT_INSRNC_FLAG,
         ADMIN_FEE,
         RGNL_DSTR_OVRD,
         CLM_ADJSTMT,
         PRCSR_RSRVD,
         IS_DEL,
         REC_CRT_DT,
         REC_CRT_TM,
         REC_UPDT_DT,
         REC_UPDT_TM,
         REC_XTRCT_SRC_CD,
         REC_INPUT_SRC_CD,
         CYCLE_DT,
         SRVC_PROV_NCPDP_ID_NUM,
         MMBR_ID_NUM_JNKEY,
         GUM_SITE_KEY,
         EDWELIG,
         Mbr_Alt_Id_Logic_Cd
		 from extSTG_PSRX_CLM_GOVT where Mbr_Alt_Id_Logic_Cd = 0 ;
		 
		 

create or replace temporary table extSTG_PSRX_CLM_COMM as 
		SELECT 

		date(ndc.NDC_DRG_ROW_EFF_DT )as NDC_DRG_ROW_EFF_DT , 

		REC_ID,PRCSR_ID_NUM,PRCSR_BATCH_NUM,comm.SRVC_PROV_ID,comm.SRVC_PROV_ID_QLFYR,CLM_ORGNTN_FLAG,
		PHRMCY_NTWRK_CD,comm.SRVC_PROV_NAME,SRVC_PROV_REL_CD,comm.SRVC_PROV_DSPNSR_CLS,comm.SRVC_PROV_DSPNSR_TYPE,RX_SRVC_REF_NUM,

		comm.FILL_DT,PROD_SRVC_ID,PROD_ID_QLFYR,PROD_LABEL_NAME_WTH_DSG,PROD_MFTR,NEW_REFL_CD,MTRC_DCML_QTY,
		DAYS_SUPLY,DRUG_RX_OTC_IND,DRUG_STGTH,UOM,ROA,DSG_FORM,HCPCS_CD,BASIS_OF_REIMBRSMT,BASIS_OF_CST_DTRMNTN,CST_TYPE_CD_APRVD_CST_SRC,
		DRUG_UNIT_CST,PYMT_TYPE_FLAG,INGRDNT_CST_BILLED,INGRDNT_CST_SBMTD,INGRDNT_CST_CALCD,DSPNSNG_FEE_BILLED,OUT_OF_NTWRK_PNLTY,
		PTNT_PAY_AMT,SLS_TAX_BILLED,CLNT_COPAY,NET_PTNT_PAY,PLAN_CO_PAY,PROD_SLCTN_DFRNTL,

		PROFNL_SRVC_FEE_BILLED,

		INCNTV_AMT_BILLED,

		CLM_ADJSTMT_WTHLD_AMT,

		TOT_AMT_BILLED,

		OTHR_PYR_AMT_RECOGNIZED,

		OTHR_AMT_BILLED,

		PTNT_PAY_AMT_ATTRD_TO_DDCTBL,

		USL_AND_CUSTMRY_BLG_AMT,

		trim(PTNT_FNAME) PTNT_FNAME ,trim(PTNT_LNAME) PTNT_LNAME,PTNT_MI,PTNT_DOB,GNDR,MMBR_ID_NUM,MMBR_ZIP,PRSN_CD,PTNT_REL_TO_CARDHLDR,PRSCRBR_ID,

		PRSCRBR_ID_QLFYR,PRSCRBR_DEA_NUM,PRSCRBR_LAST_NAME,PRSCRBR_FIRST_NAME,PRSCRBR_SPCLTY_CD,

		PRSCRBR_NTWRK_ID,

		DGNS_CD,

		DGNS_CD_QLFYR,

		MDCL_CRTFCTN_QLFYR,

		PRIOR_AUTHRZTN_ID,

		PRIOR_AUTH_RSN_CD,CARDHLDR_ID,CARDHLDR_LNAME,CARDHLDR_FNAME,

		CARDHLDR_MI,CARDHLDR_DOB,CARDHLDR_ADR_1,CARDHLDR_ADR_2,CARDHLDR_CITY,CARDHLDR_STATE,CARDHLDR_ZIP,POS,RX_ORGN,PTNT_RSDNC,
		DT_RX_WRTN,PROD_SLCTN_CD,OTHR_CVRG_CD,TP_RSTCTN_CD,SPCLTY_FLAG,CMPND_CD,FRMLRY_STUS,PLAN_DRUG_STUS,NUM_OF_REFLS_AUTHRZD,
		MLT_SRC_CD,BRND_NAME_CD,DRUG_DEA_CLS_CD,PRMRY_CARE_PROV_ID_CD,CARE_FAC_ID_CD,CARE_QLFYR_CD,PLAN_CD,

		comm.UNIT_DOSE_IND,MNTNC_DRUG_CD,AHFS_CD,GPI,DRUG_GNRC_NAME,GCN,GCN_SEQ_NUM,trim( COALESCE( CARRIER,' ')) as CARRIER
		,ACCOUNT,GRP_ID,SSN,SBMT_DT_OF_CLM,
		SBMT_TIME_OF_CLM,RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS,

		comm.TRANS_CD,BRND_GNRC_IND,CLM_MSG,BNFT_MAX_FLAG,CLNT_FLAG,OOP_MAX_APLD,BNFT_MAX_APLD,PROC_CYC_DT,THRPTC_GRP_IND,
		FRMLRY_CLM_FLAG,MDCL_CLM_FLAG,PRIOR_AUTH_CLM_FLAG,TRNSPLNT_FLAG,INJCTBL_PROD_FLAG,RXSOL_FRMLRY_FLAG,HALF_TAB,RSRVD,RSRVD2,RMTNC_TYPE,

		comm.CHK_DT,CHK_NUM,PLN_TYPE_CD,CLNT_PROD_CD,CLNT_RIDER_CD,CLNT_BNFT_CD,PLAN_QLFYR_CD,TIER_VAL,HLTH_REIMBRSMT_AMT,
		ALTRNT_INSRNC_FLAG,ADMIN_FEE,RGNL_DSTR_OVRD,CLM_ADJSTMT,comm.PRCSR_RSRVD,

		comm.IS_DEL,

		comm.REC_CRT_DT,

		comm.REC_CRT_TM,

		comm.REC_UPDT_DT,

		comm.REC_UPDT_TM,

		comm.REC_XTRCT_SRC_CD,

		comm.REC_INPUT_SRC_CD,

		comm.CYCLE_DT ,

		prv.SRVC_PROV_NCPDP_ID_NUM  ,
		MBR_ALT.Mbr_Alt_Id_Logic_Cd,

		left(trim(comm.grp_id),4)||'0' as GRP_IDP1,

		'AI'||left(trim(comm.grp_id),4)||'0' as GRP_IDP2
		

		FROM 

		STG.STG_PSRX_CLM_COMM comm

		Inner join  temp_ndc ndc on

		trim(comm.PROD_SRVC_ID) = trim(ndc.NDC) and rnk=1 and 

		comm.FILL_DT BETWEEN ndc.NDC_DRG_ROW_EFF_DT and ndc.NDC_DRG_ROW_END_DT and 

		(

		trim(ndc.AHFS_THERAPEUTIC_CLSS_CD)  not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000')    AND

		substr(trim(ndc.EXT_AHFS_THRPTC_CLSS_CD),1,6) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000')  and  

		trim(ndc.GNRC_NM) not in ( 'ACETAMINOPHEN/BUTALBITAL', 'ACETAMINOPHEN/CAFFEINE/BUTALB', 'ASPIRIN/BUTALBITAL', 'ASPIRIN/CAFFEINE/BUTALBITAL', 'ASPIRIN/MEPROBAMATE', 'BROMOCRIPTINE MESYLATE', 'PERGOLIDE MESYLATE', 'BIPERIDEN HCL', 'PROCYCLIDINE HCL', 'SILDENAFIL CITRATE', 'TRIHEXYPHENIDYL HCL', 'BENZTROPINE MESYLATE', 'DIPHENHYDRAMINE HCL')  )

		left outer join STM.STM_PSRX_CLM_PROV prv on

		trim(comm.SRVC_PROV_ID) = trim(prv.SRVC_PROV_ID)
		left outer join STM.STM_PSRX_CARR_MBR_ALT MBR_ALT ON TRIM(MBR_ALT.Carrier_id) = CARRIER AND TRIM(MBR_ALT.EDW_ELIG) =  'N/A'
		where 
		comm.REC_UPDT_DT  >=  '#RUN_FROMDT#'  and comm.REC_UPDT_DT  <=   '#RUN_TODT#'  
		-- and comm.FILL_DT >= '2017-01-01' 
		;
		 
		 
	create or replace temporary table ds_COMM_CES_LOGCD1 as 
	SELECT * FROM extSTG_PSRX_CLM_COMM WHERE Mbr_Alt_Id_Logic_Cd   = 1 ;
	 
	create or replace temporary table ds_COMM_PBH_LOGCD2 as 
	SELECT * FROM extSTG_PSRX_CLM_COMM WHERE Mbr_Alt_Id_Logic_Cd   = 2 ;
	
	create or replace temporary table ds_COMM_PBH_LOGCD9 as 
	SELECT * FROM extSTG_PSRX_CLM_COMM WHERE Mbr_Alt_Id_Logic_Cd   = 9 ;
	
	
	create or replace temporary table cpy_dropped as 
	SELECT RX_CLM_NUM FROM extSTG_PSRX_CLM_COMM WHERE Mbr_Alt_Id_Logic_Cd   = 0 ;
	
	
	create or replace temporary table ds_COMMFS_CES_LOGCD_10 as 
	SELECT * FROM extSTG_PSRX_CLM_COMM WHERE Mbr_Alt_Id_Logic_Cd   = 10 ;
	
	
	create or replace temporary table ds_COMMFS_CES_LOGCD_15 as 
	SELECT * FROM extSTG_PSRX_CLM_COMM WHERE Mbr_Alt_Id_Logic_Cd   = 15 ;		 
		
	create or replace temporary table ds_STM_COSM_GUM as 	
	select 
	GUM_SITE_ID ,
	GUM_SITE_NBR
	from
    STM.STM_COSM_GUM ;	
		
		
	create or replace temporary table extSTM_COSM_MEM as 
	select
	govt.RX_CLM_NUM as RX_CLM_NUM, 		
	govt.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	govt.CLM_STUS as CLM_STUS ,
	date(govt.FILL_DT)  as FILL_DT  ,
	mem.MEM_MEMSUB as      MEM_MEMSUB ,
	mem.MEM_SITE_NBR as    MEM_SITE_NBR ,
	mem.MEM_EFF_YMD as     MEM_EFF_YMD,
	mem.MEM_EXP_YMD as    MEM_EXP_YMD,
	upper(trim(mem.MEM_L_NAME)) as       MEM_L_NAME,
	upper(trim(mem.MEM_F_NAME))  as      MEM_F_NAME,
	date(mem.MEM_DOB_YMD)   as   MEM_DOB_YMD, 
	mem.MEM_MEMDEP as      MEM_MEMDEP,
	mem.MEM_MEMGRP as      MEM_MEMGRP,
	length(trim(govt.MMBR_ID_NUM)) as  MMBR_ID_NUM_LEN,
	date(mem.REC_UPDT_DTTM)  as REC_UPDT_DTTM
	from STG.STG_PSRX_CLM_GOVT govt 
	Inner join  temp_ndc ndc on
	trim(govt.PROD_SRVC_ID) = trim(ndc.NDC) and rnk =1 and 
	govt.FILL_DT BETWEEN ndc.NDC_DRG_ROW_EFF_DT and ndc.NDC_DRG_ROW_END_DT and 
	(
	trim(ndc.AHFS_THERAPEUTIC_CLSS_CD)  not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000')    AND  
	substr(trim(ndc.EXT_AHFS_THRPTC_CLSS_CD),1,6)  not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000')  AND  
	trim(ndc.GNRC_NM) not in ( 'ACETAMINOPHEN/BUTALBITAL', 'ACETAMINOPHEN/CAFFEINE/BUTALB', 'ASPIRIN/BUTALBITAL', 'ASPIRIN/CAFFEINE/BUTALBITAL', 'ASPIRIN/MEPROBAMATE', 'BROMOCRIPTINE MESYLATE', 'PERGOLIDE MESYLATE', 'BIPERIDEN HCL', 'PROCYCLIDINE HCL', 'SILDENAFIL CITRATE', 'TRIHEXYPHENIDYL HCL', 'BENZTROPINE MESYLATE', 'DIPHENHYDRAMINE HCL')  )
	inner join #STMSchema#.STM_COSM_MEM mem on 
	upper(trim(govt.PTNT_LNAME))= upper(trim(mem.MEM_L_NAME)) and
	date(govt.PTNT_DOB) = date(mem.MEM_DOB_YMD) 
	where
	govt.REC_UPDT_DT  >=  '#RUN_FROMDT#'  and govt.REC_UPDT_DT  <=  '#RUN_TODT#'  
	-- and govt.FILL_DT >= '2017-01-01' 
	;
		
	
	create or replace temporary table ds_STM_COSM_MEM_PSRX_GOVT_3_ref as
	SELECT
	RX_CLM_NUM, 		
	SQNC_NUM_OF_CLM ,
	CLM_STUS ,
	FILL_DT  ,
	MEM_MEMSUB ,
	MEM_SITE_NBR ,
	MEM_EFF_YMD,
	MEM_EXP_YMD,
	MEM_L_NAME,
	MEM_F_NAME,
	MEM_DOB_YMD, 
	MEM_MEMDEP,
	MEM_MEMGRP,
	REC_UPDT_DTTM
    FROM extSTM_COSM_MEM WHERE MMBR_ID_NUM_LEN = 11 ;	
		
		
	create or replace temporary table ds_STM_COSM_MEM_PSRX_GOVT_5_ref as
	SELECT
	RX_CLM_NUM, 		
	SQNC_NUM_OF_CLM ,
	CLM_STUS ,
	FILL_DT  ,
	MEM_MEMSUB ,
	MEM_SITE_NBR ,
	MEM_EFF_YMD,
	MEM_EXP_YMD,
	MEM_L_NAME,
	MEM_F_NAME,
	MEM_DOB_YMD, 
	MEM_MEMDEP,
	MEM_MEMGRP,
	REC_UPDT_DTTM
    FROM extSTM_COSM_MEM WHERE 
	FILL_DT  between MEM_EFF_YMD and MEM_EXP_YMD and MEM_MEMDEP = 0 ;
		
		
	create or replace temporary table ds_STM_PBH_CMC_SBSB_SUBSC4_ref as	
	SELECT  
	pbh.MEME_SFX  as MEME_SFX , 
	trim(pbh.MBR_ALT_ID)  as MBR_ALT_ID0 ,
	trim(pbh.MBR_ALT_ID) as MBR_ALT_ID1 ,
	pbh.MEPE_EFF_DT as  MEPE_EFF_DT,
	pbh.REC_UPDT_DTTM as REC_UPDT_DTTM, 
	govt.RX_CLM_NUM as RX_CLM_NUM, 		
	govt.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	govt.CLM_STUS as CLM_STUS ,
	Case When left(govt.MMBR_ID_NUM,7)=trim(pbh.SBSB_ID) Then 1 
	          When '00'||left(trim(govt.MMBR_ID_NUM),7)=trim(pbh.SBSB_ID) Then 2 
	           Else 3 End  as SBSCR_RANKING
	FROM  STG.STG_PSRX_CLM_GOVT govt 
	Inner join  temp_ndc ndc on
	trim(govt.PROD_SRVC_ID) = trim(ndc.NDC) and rnk =1 and 
	govt.FILL_DT BETWEEN ndc.NDC_DRG_ROW_EFF_DT and ndc.NDC_DRG_ROW_END_DT and 
	(
	trim(ndc.AHFS_THERAPEUTIC_CLSS_CD) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000')    AND 
	substr(trim(ndc.EXT_AHFS_THRPTC_CLSS_CD),1,6)  not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000')  AND  
	trim(ndc.GNRC_NM)  not in ( 'ACETAMINOPHEN/BUTALBITAL', 'ACETAMINOPHEN/CAFFEINE/BUTALB', 'ASPIRIN/BUTALBITAL', 'ASPIRIN/CAFFEINE/BUTALBITAL', 'ASPIRIN/MEPROBAMATE', 'BROMOCRIPTINE MESYLATE', 'PERGOLIDE MESYLATE', 'BIPERIDEN HCL', 'PROCYCLIDINE HCL', 'SILDENAFIL CITRATE', 'TRIHEXYPHENIDYL HCL', 'BENZTROPINE MESYLATE', 'DIPHENHYDRAMINE HCL')  )
	inner join  DW.TB_MBR_ELIG_PBH_WRK pbh
	on   ( '00'||left(trim(govt.MMBR_ID_NUM),7)=trim(pbh.SBSB_ID) or left(govt.MMBR_ID_NUM,7)=trim(pbh.SBSB_ID) )
	AND       date(govt.FILL_DT)  between date(pbh.MEPE_EFF_DT)  and date(pbh.MEPE_TERM_DT)
	AND (  ( pbh.MEME_SFX=0 ) OR ( pbh.MEME_SFX=1) ) 
	inner join  STM.STM_PBH_CMC_MEME_MEMBER mem
	on        pbh.MEME_CK=mem.MEME_CK
	AND    upper(trim(govt.PTNT_LNAME)) = upper(trim(mem.MEME_LAST_NAME))
	AND    upper(trim(govt.PTNT_FNAME)) = upper(trim(mem.MEME_FIRST_NAME))
	where
	govt.REC_UPDT_DT  >=  '#RUN_FROMDT#'  and govt.REC_UPDT_DT  <=  '#RUN_TODT#'   
	-- and govt.FILL_DT >= '2017-01-01'  
	;


   create or replace temporary table ds_STM_CMC_MEME8_ref as
   SELECT   grp.GRGR_ID as GRGR_ID,
            subsc.SBSB_ID as SBSB_ID,
            mem.MEME_SFX as MEME_SFX,
            rx.RX_CLM_NUM as RX_CLM_NUM,
            rx.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM,
            rx.CLM_STUS as CLM_STUS,
            subsc.SBSB_ORIG_EFF_DT as SBSB_ORIG_EFF_DT,
            subsc.REC_CRT_DTTM as SBSB_REC_CRT_DTTM,
            mem.MEME_ORIG_EFF_DT as MEME_ORIG_EFF_DT,  
            mem.REC_CRT_DTTM as MEME_REC_CRT_DTTM,  
            elig.MEPE_EFF_DT as MEPE_EFF_DT,  
            elig.MEPE_CREATE_DTM as MEPE_CREATE_DTM
   FROM     STG.STG_PSRX_CLM_GOVT rx 
            Inner join temp_ndc ndc on trim(rx.PROD_SRVC_ID) = trim(ndc.NDC) and rnk =1 and  rx.FILL_DT BETWEEN ndc.NDC_DRG_ROW_EFF_DT and ndc.NDC_DRG_ROW_END_DT and
			( trim(ndc.AHFS_THERAPEUTIC_CLSS_CD) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000') AND 
             substr(trim(ndc.EXT_AHFS_THRPTC_CLSS_CD),1,6) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000') AND 
              trim(ndc.GNRC_NM) not in ( 'ACETAMINOPHEN/BUTALBITAL', 'ACETAMINOPHEN/CAFFEINE/BUTALB', 'ASPIRIN/BUTALBITAL', 'ASPIRIN/CAFFEINE/BUTALBITAL', 'ASPIRIN/MEPROBAMATE', 'BROMOCRIPTINE MESYLATE', 'PERGOLIDE MESYLATE', 'BIPERIDEN HCL', 'PROCYCLIDINE HCL', 'SILDENAFIL CITRATE', 'TRIHEXYPHENIDYL HCL', 'BENZTROPINE MESYLATE', 'DIPHENHYDRAMINE HCL') ) 
			inner join STM.STM_FCET_CMC_SBSB_SUBSC subsc on trim(rx.SSN) =trim(subsc.SBSB_ID) 
            inner join STM.STM_FCET_CMC_MEME_MEMBER mem on subsc.SBSB_CK=mem.SBSB_CK and upper(trim(rx.PTNT_LNAME)) = upper(trim(mem.MEME_LAST_NAME)) and upper(trim(rx.PTNT_FNAME)) = upper(trim(mem.MEME_FIRST_NAME)) and date(rx.PTNT_DOB)=date(mem.MEME_BIRTH_DT) 
            left outer join STM.STM_FCET_CMC_MEPE_PRCS_ELIG elig on mem.MEME_CK=elig.MEME_CK and ( date(rx.FILL_DT) between date(elig.MEPE_EFF_DT) and date(elig.MEPE_TERM_DT ) ) 
            left outer join STM.STM_FCET_CMC_GRGR_GROUP grp on elig.GRGR_CK=grp.GRGR_CK
   WHERE   
    rx.REC_UPDT_DT >= '#RUN_FROMDT#'
   AND      rx.REC_UPDT_DT <= '#RUN_TODT#' 
   -- and rx.FILL_DT >= '2017-01-01' 
	qualify row_number() over (partition by RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS
                           order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC, 
						   SBSB_ORIG_EFF_DT DESC, SBSB_REC_CRT_DTTM DESC, MEME_ORIG_EFF_DT DESC,
						   MEME_REC_CRT_DTTM DESC, MEPE_EFF_DT DESC NULLS LAST, MEPE_CREATE_DTTM DESC NULLS LAST) = 1 ;
		
		
		
		
	create or replace temporary table CSP_REF as					   
	SELECT   grp.GRGR_ID as GRGR_ID,
	         subsc.SBSB_ID as SBSB_ID,
	         mem.MEME_SFX as MEME_SFX,
	         rx.RX_CLM_NUM as RX_CLM_NUM,
	         rx.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM,
	         rx.CLM_STUS as CLM_STUS,
	         subsc.SBSB_ORIG_EFF_DT as SBSB_ORIG_EFF_DT,
	         subsc.REC_CRT_DTTM as SBSB_REC_CRT_DTTM,
	         mem.MEME_ORIG_EFF_DT as MEME_ORIG_EFF_DT,  
	         mem.REC_CRT_DTTM as MEME_REC_CRT_DTTM,  
	         elig.MEPE_EFF_DT as MEPE_EFF_DT,  
	         elig.MEPE_CREATE_DTM as MEPE_CREATE_DTM
	FROM     STG.STG_PSRX_CLM_GOVT rx 
	         Inner join temp_ndc ndc on trim(rx.PROD_SRVC_ID) = trim(ndc.NDC) and rnk =1 and  rx.FILL_DT BETWEEN ndc.NDC_DRG_ROW_EFF_DT and ndc.NDC_DRG_ROW_END_DT and 
	         ( trim(ndc.AHFS_THERAPEUTIC_CLSS_CD) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000') AND
	         substr(trim(ndc.EXT_AHFS_THRPTC_CLSS_CD),1,6) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000') AND
	          trim(ndc.GNRC_NM) not in ( 'ACETAMINOPHEN/BUTALBITAL', 'ACETAMINOPHEN/CAFFEINE/BUTALB', 'ASPIRIN/BUTALBITAL', 'ASPIRIN/CAFFEINE/BUTALBITAL', 'ASPIRIN/MEPROBAMATE', 'BROMOCRIPTINE MESYLATE', 'PERGOLIDE MESYLATE', 'BIPERIDEN HCL', 'PROCYCLIDINE HCL', 'SILDENAFIL CITRATE', 'TRIHEXYPHENIDYL HCL', 'BENZTROPINE MESYLATE', 'DIPHENHYDRAMINE HCL') ) 
	         inner join STM.STM_CSP_CMC_SBSB_SUBSC subsc on LEFT(rx.MMBR_ID_NUM,9)  =trim(subsc.SBSB_ID) 
	         inner join STM.STM_CSP_CMC_MEME_MEMBER mem on subsc.SBSB_CK=mem.SBSB_CK and upper(trim(rx.PTNT_LNAME)) = upper(trim(mem.MEME_LAST_NAME)) and upper(trim(rx.PTNT_FNAME)) = upper(trim(mem.MEME_FIRST_NAME)) and date(rx.PTNT_DOB)=date(mem.MEME_BIRTH_DT) 
	         left outer join STM.STM_CSP_CMC_MEPE_PRCS_ELIG elig on mem.MEME_CK=elig.MEME_CK and ( date(rx.FILL_DT) between date(elig.MEPE_EFF_DT) and date(elig.MEPE_TERM_DT ) ) 
	         left outer join STM.STM_CSP_CMC_GRGR_GROUP grp on elig.GRGR_CK=grp.GRGR_CK
	WHERE   
	 rx.REC_UPDT_DT >=  '#RUN_FROMDT#' AND  rx.REC_UPDT_DT <= '#RUN_TODT#' AND rx.REC_INPUT_SRC_CD = 'RXSD' 
	 -- and rx.FILL_DT >= '2017-01-01'  
 	qualify row_number() over (partition by RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS
                            order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC, 
 						   SBSB_ORIG_EFF_DT DESC, SBSB_REC_CRT_DTTM DESC, MEME_ORIG_EFF_DT DESC,
 						   MEME_REC_CRT_DTTM DESC, MEPE_EFF_DT DESC NULLS LAST, MEPE_CREATE_DTTM DESC NULLS LAST) = 1 ;
		
		
	create or replace temporary table ds_STM_COSM_MEM_PSRX_GOVT_3_PBHtoCOSM as	
	SELECT
	govt.RX_CLM_NUM as RX_CLM_NUM, 		
	govt.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	govt.CLM_STUS as CLM_STUS ,
	trim(govt.MMBR_ID_NUM) as MMBR_ID_NUM,
	mem.MEM_MEMGRP as      MEM_MEMGRP_PBH,
	mem.MEM_MEMDEP as      MEM_MEMDEP_PBH,
	mem.MEM_SITE_NBR as    GUM_SITE_NBR_PBH,
	mem.MEM_MEMSUB as      MEM_MEMSUB_PBH ,
	mem.MEM_SITE_NBR ||'-'|| mem.MEM_MEMGRP ||'-'|| mem.MEM_MEMSUB  ||'-'|| mem.MEM_MEMDEP as MEM_ALT_ID_PBH,
	mem.MEM_EFF_YMD,
	mem.REC_UPDT_DTTM
	FROM STG.STG_PSRX_CLM_GOVT govt 
	INNER JOIN  temp_ndc ndc on
	trim(govt.PROD_SRVC_ID) = trim(ndc.NDC) and ndc.rnk =1 and 
	govt.FILL_DT BETWEEN ndc.NDC_DRG_ROW_EFF_DT and ndc.NDC_DRG_ROW_END_DT  and 
	(
	trim(ndc.AHFS_THERAPEUTIC_CLSS_CD) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000')    AND   
	substr(trim(ndc.EXT_AHFS_THRPTC_CLSS_CD),1,6) not in ('280808','280812','281204','281208','281292','281604','281608','282004','282092','282404','282408','282492','282800','882800','289200','241212','281000','282000')  AND
	trim(ndc.GNRC_NM) not in ( 'ACETAMINOPHEN/BUTALBITAL', 'ACETAMINOPHEN/CAFFEINE/BUTALB', 'ASPIRIN/BUTALBITAL', 'ASPIRIN/CAFFEINE/BUTALBITAL', 'ASPIRIN/MEPROBAMATE', 'BROMOCRIPTINE MESYLATE', 'PERGOLIDE MESYLATE', 'BIPERIDEN HCL', 'PROCYCLIDINE HCL', 'SILDENAFIL CITRATE', 'TRIHEXYPHENIDYL HCL', 'BENZTROPINE MESYLATE', 'DIPHENHYDRAMINE HCL')  )
	INNER JOIN  STM.STM_COSM_MEM mem on 
	upper(trim(govt.PTNT_LNAME))= upper(trim(mem.MEM_L_NAME)) and 
	upper(trim(govt.PTNT_FNAME))= upper(trim(mem.MEM_F_NAME)) and 
	date(govt.PTNT_DOB) = date(mem.MEM_DOB_YMD) and
	govt.FILL_DT between mem.MEM_EFF_YMD and mem.MEM_EXP_YMD and
	govt.MMBR_ID_NUM=mem.MEM_ALT_ID and
	length(trim(govt.MMBR_ID_NUM))  = 9
	WHERE 
	govt.REC_UPDT_DT  >=  '#RUN_FROMDT#'  and govt.REC_UPDT_DT  <=  '#RUN_TODT#' 
	-- and govt.FILL_DT >= '2017-01-01'	
 	qualify row_number() over (partition by RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS
                            order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC, 
 						   MEM_EFF_YMD DESC, REC_UPDT_DTTM DESC, MEM_MEMGRP_PBH DESC NULLS FIRST) = 1 ;	
		
		
	create or replace temporary table CSP_XCHG_REF as		
	SELECT grp.GRGR_ID as GRGR_ID,
	         subsc.SBSB_ID as SBSB_ID,
	         mem.MEME_SFX as MEME_SFX,
	         rx.RX_CLM_NUM as RX_CLM_NUM,
	         rx.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM,
	         rx.CLM_STUS as CLM_STUS,
	         subsc.SBSB_ORIG_EFF_DT as SBSB_ORIG_EFF_DT,
	         subsc.REC_CRT_DTTM as SBSB_REC_CRT_DTTM,
	         mem.MEME_ORIG_EFF_DT as MEME_ORIG_EFF_DT,
	         mem.REC_CRT_DTTM as MEME_REC_CRT_DTTM,
	         elig.MEPE_EFF_DT as MEPE_EFF_DT,
	         elig.MEPE_CREATE_DTM as MEPE_CREATE_DTM 
	FROM STG.STG_PSRX_CLM_XCHG rx 
	         Inner join temp_ndc ndc on trim(rx.PROD_SRVC_ID) = trim(ndc.NDC) and rnk =1 and rx.FILL_DT BETWEEN ndc.NDC_DRG_ROW_EFF_DT and ndc.NDC_DRG_ROW_END_DT 
	         inner join STM.STM_CSP_CMC_SBSB_SUBSC subsc on LEFT(rx.MMBR_ID_NUM,9) =trim(subsc.SBSB_ID) 
	         inner join STM.STM_CSP_CMC_MEME_MEMBER mem on subsc.SBSB_CK=mem.SBSB_CK and upper(trim(rx.PTNT_LNAME)) = upper(trim(mem.MEME_LAST_NAME)) and upper(trim(rx.PTNT_FNAME)) = upper(trim(mem.MEME_FIRST_NAME)) and date(rx.PTNT_DOB)=date(mem.MEME_BIRTH_DT) 
	         left outer join STM.STM_CSP_CMC_MEPE_PRCS_ELIG elig on mem.MEME_CK=elig.MEME_CK and ( date(rx.FILL_DT) between date(elig.MEPE_EFF_DT) and date(elig.MEPE_TERM_DT ) ) 
	         left outer join STM.STM_CSP_CMC_GRGR_GROUP grp on elig.GRGR_CK=grp.GRGR_CK
	WHERE    rx.REC_UPDT_DT >= '#RUN_FROMDT#'
	AND      rx.REC_UPDT_DT <= '#RUN_TODT#'
	AND      rx.REC_INPUT_SRC_CD = 'RXSC'	
 	qualify row_number() over (partition by RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS
                             order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC, 
  						   SBSB_ORIG_EFF_DT DESC, SBSB_REC_CRT_DTTM DESC, MEME_ORIG_EFF_DT DESC,
  						   MEME_REC_CRT_DTTM DESC, MEPE_EFF_DT DESC NULLS LAST, MEPE_CREATE_DTTM DESC NULLS LAST) = 1 ;	
		
		
		
	create or replace temporary table extSTM_ELI_T_CES_MEMBER as	
	select 
	ces.MBR_SEQ_NBR as  MBR_SEQ_NBR,
	comm.RX_CLM_NUM as RX_CLM_NUM, 		
	comm.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	comm.CLM_STUS as CLM_STUS 
	from 
	STG.STG_PSRX_CLM_COMM comm
	left outer join STM.STM_ELI_T_CES_MEMBER ces on
	trim(ces.POLICY_NBR) =left(trim(comm.GRP_ID),7)and
	trim(ces.SUBSCRIBER_NBR)=trim(comm.SSN)  and 
	ces.CES_RELATIONSHIP_CODE =( case when comm.PTNT_REL_TO_CARDHLDR='1' then '0'
						when comm.PTNT_REL_TO_CARDHLDR='2' then '1'
							else  '2' 
							END   ) and 
	upper(trim(ces.FIRST_NAME)) 	= 	upper(trim(comm.PTNT_FNAME)) and 
	date(ces.BIRTH_DATE)		=	date(comm.PTNT_DOB  )
	where  comm.REC_UPDT_DT >=   '#RUN_FROMDT#'  and comm.REC_UPDT_DT <= '#RUN_TODT#'	;
	
	
	create or replace temporary table ds_COMM_CES_LOGCD1_1 as
	(SELECT * FROM 
	ds_COMM_CES_LOGCD1
 	qualify row_number() over (partition by RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS
                             order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC, 
  						       NDC_DRG_ROW_EFF_DT DESC) = 1 ) ;
		
	
	create or replace temporary table join_cesmem as
	(SELECT A.*, B.MBR_SEQ_NBR FROM 
	ds_COMM_CES_LOGCD1_1 A
	LEFT OUTER JOIN extSTM_ELI_T_CES_MEMBER B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM AND	
	A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM AND
	A.CLM_STUS = B.CLM_STUS) ; 
		
	create or replace temporary table t1xfrm_mapping as 
	(select 
	SUBSTR(trim(NullToEmpty(DGNS_CD)),1,5) PHRM_DIAG_CD,
	DRUG_UNIT_CST PHRM_AVG_WHLSL_AMT,
	CLNT_COPAY PHRM_COPAY_AMT,
	PTNT_PAY_AMT_ATTRD_TO_DDCTBL PHRM_DED_AMT,
	DSPNSNG_FEE_BILLED PHRM_DSPNS_FEE_AMT,
	INGRDNT_CST_BILLED PHRM_INGR_CST_AMT,
	CHK_DT PHRM_CHK_DT,
	FILL_DT PHRM_FILL_DT,
	DAYS_SUPLY PHRM_DAY_SPL_CNT,
	PROD_SLCTN_CD PHRM_DSPNS_AS_WRT_CD,
	SUBSTR(trim(NullToEmpty(PROD_SRVC_ID)),1,11) PHRM_NDC_CD,
	NDC_DRG_ROW_EFF_DT PHRM_NDC_ROW_EFF_DT,
	CASE WHEN Trim(NullToEmpty(NEW_REFL_CD)) = '00' then 'Y' else 'N' END AS PHRM_FST_FILL_IND_CD,
	FRMLRY_STUS PHRM_FRMLRY_IND_CD,
	CASE WHEN SUBSTR(trim(NullToEmpty(in_t1xfrm_mapping.PLAN_QLFYR_CD)),1,1)  =  '2' then '3'  
	WHEN SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '3' then '4'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '4' then '8'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '5' then '9' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '6' then 'E' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '7' then 'F' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '8' then 'G' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '9' then 'H' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '10' then 'A' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '11' then 'B' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '12' then 'C' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '13' then 'D' else
	SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1) END AS 
	PHRM_FRMLRY_TYP_CD,
	Right('000000000000'||( NullToZero(RX_SRVC_REF_NUM)), 12)  PHRM_PRSC_NBR,
	AsInteger(NEW_REFL_CD)  PHRM_PRSC_RFL_NBR,
	MTRC_DCML_QTY PHRM_QTY_CNT,
	CASE WHEN SRVC_PROV_ID_QLFYR = '07'  then SRVC_PROV_ID 
	WHEN SRVC_PROV_ID_QLFYR = '01'  then SRVC_PROV_NCPDP_ID_NUM
	else '' END AS PHRM_NBR,
	PRIOR_AUTHRZTN_ID PHRM_PRR_AUTH_NBR,
	case when trim(NullToEmpty(SRVC_PROV_ID_QLFYR ) )  =  '01' 
	then substr(trim(NullToEmpty(SRVC_PROV_ID) ),1,10) else ''
	end as PHRM_NPI_PROV_ID,
	case when trim( NullToEmpty(PRSCRBR_ID_QLFYR ))  =  '01' 
	then substr(trim(NullToEmpty(in_t1xfrm_mapping.PRSCRBR_ID )),1,10) else ''
	end as PHRM_PRSC_NPI_PROV_ID,
	substr(trim(NullToEmpty(PRSCRBR_DEA_NUM)),1,11) PHRM_PROV_DEA_NBR,
	ACCOUNT PHRM_ACCT_ID,
	CARRIER PHRM_CARR_ID,
	substr(trim( NullToEmpty(OTHR_CVRG_CD)),1,1) PHRM_COB_CD,
	RX_CLM_NUM RX_CLM_NUM,
	SQNC_NUM_OF_CLM SQNC_NUM_OF_CLM,
	CLM_STUS CLM_STUS,
	trim(REC_INPUT_SRC_CD)||'-'|| 
	Convert('.','',Trim( (RX_CLM_NUM))||
	'-'||Trim( (SQNC_NUM_OF_CLM)))||
	'-'||trim(CLM_STUS) PHRM_DRG_CLM_ALT_ID,
	
	CASE WHEN isNull(MBR_SEQ_NBR) then -1 
	WHEN ( Left(trim(GRP_ID),7)||'-'||trim(SSN)||'-'||
	(CASE WHEN PTNT_REL_TO_CARDHLDR = '1' then '0'
	WHEN PTNT_REL_TO_CARDHLDR = '2' then '1'
	else '2' END
	)||'-'||trim((MBR_SEQ_NBR) ) ) end as varMBRALTID,
	
	case when trim(varMBRALTID) = '0' then  '-1' else  trim(varMBRALTID) END AS  MBR_ALT_ID,
	PRSCRBR_DEA_NUM PROV_ALT_ID,
	'' as SITE_CD,
	trim(SSN) SBSCR_NBR,
	Right('000'||trim(nullToEmpty(MBR_SEQ_NBR)),3) DEPN_NBR,
	'' as GRP_NBR,
	Left(trim(GRP_ID),7) CUST_SEG_NBR,
	PTNT_DOB MBR_BTH_DT,
	PTNT_FNAME MBR_FST_NM,
	PTNT_LNAME MBR_LST_NM,
	PRMRY_CARE_PROV_ID_CD MBR_PRI_PHYSN_NBR,
	CLNT_BNFT_CD BEN_PLN_NBR,
	substr(Trim(NullToEmpty(CLNT_PROD_CD)),1,5) PRDCT_CD,
	TRANS_CD TRAN_CD,
	REC_XTRCT_SRC_CD REC_XTRCT_SRC_CD,
	REC_INPUT_SRC_CD REC_INPUT_SRC_CD,
	CYCLE_DT CYCLE_DT,
	MMBR_ZIP MBR_ZIP_CD,
	DT_RX_WRTN PHRM_PRSCN_WRT_DT,
	POS PHRM_POS_CD,
	NUM_OF_REFLS_AUTHRZD PHRM_AUTH_RFL_CNT,
	GNDR MBR_GDR_CD,
	DGNS_CD_QLFYR DGNS_CD_QLFYR,
	SETNULL() AS CRR_MEMGROUPID
	from join_cesmem ) ;
	
	
	
	create or replace temporary table ds_COMM_PBH_LOGCD2_1 as
	(SELECT * FROM 
	ds_COMM_PBH_LOGCD2
 	qualify row_number() over (partition by RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS,NDC_DRG_ROW_EFF_DT
                             order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC, 
  						       NDC_DRG_ROW_EFF_DT DESC) = 1 ) ;
	
	
	create or replace temporary table extSTM_PBH_CMC_SBSB_SUBSC as
	(SELECT   
	pbh.MBR_ALT_ID as  MBR_ALT_ID ,
	clm.RX_CLM_NUM as RX_CLM_NUM, 		
	clm.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	clm.CLM_STUS as CLM_STUS ,
	 Convert(varchar,date(pbh.MEPE_EFF_DT) ) as MEPE_EFF_DT,
	Convert(varchar,date(pbh.REC_UPDT_DTTM))  as REC_UPDT_DTTM
	FROM  STG.STG_PSRX_CLM_COMM clm
	inner join DW.TB_MBR_ELIG_PBH_WRK pbh 
	on    '00'||left(trim(clm.MMBR_ID_NUM),7)=trim(pbh.SBSB_ID)
	AND         Convert(int, trim(clm.PRSN_CD) )=pbh.MEME_SFX
	AND         left(trim(clm.grp_id),6)=trim(pbh.GRGR_ID)
	AND         date(clm.FILL_DT)  between date(pbh.MEPE_EFF_DT)  and  date(pbh.MEPE_TERM_DT)
	AND   ( trim(clm.PRSN_CD)  like '[0-9]' OR  trim(clm.PRSN_CD)  like '[0-9][0-9]' OR  trim(clm.PRSN_CD)  like '[0-9][0-9][0-9]' ) 
 	qualify row_number() over (partition by RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS
                             order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC, 
  						     MEPE_EFF_DT DESC,  REC_UPDT_DTTM DESC) = 1 ) ;
	
	
	
	create or replace temporary table join_cms_sbsb as
	(SELECT A.*, B.MBR_ALT_ID FROM 
	ds_COMM_PBH_LOGCD2_1 A
	LEFT OUTER JOIN extSTM_PBH_CMC_SBSB_SUBSC B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM AND	
	A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM AND
	A.CLM_STUS = B.CLM_STUS) ;
	
	
	create or replace temporary table extSTM_PBH_CMC_SBSB_SUBSC_pass2 as
	(SELECT   
	pbh.MBR_ALT_ID as  MBR_ALT_ID2 ,
	clm.RX_CLM_NUM as RX_CLM_NUM, 		
	clm.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	clm.CLM_STUS as CLM_STUS ,
	 Convert(varchar,date(pbh.MEPE_EFF_DT) ) as MEPE_EFF_DT,
	Convert(varchar,date(pbh.REC_UPDT_DTTM))  as REC_UPDT_DTTM,
	1 as PASS1_EXISTS
	FROM  STG.STG_PSRX_CLM_COMM clm
	INNER JOIN  DW.TB_MBR_ELIG_PBH_WRK pbh 
	ON  ( '00'||left(trim(clm.MMBR_ID_NUM),7)=trim(pbh.SBSB_ID) AND  Convert(int,clm.PRSN_CD)=pbh.MEME_SFX )
	INNER JOIN STM.STM_PBH_CMC_MEME_MEMBER mem
	ON ( pbh.MEME_CK=mem.MEME_CK AND clm.PTNT_LNAME=upper(mem.MEME_LAST_NAME) AND clm.PTNT_FNAME=upper(mem.MEME_FIRST_NAME) )
	WHERE (PRSN_CD like '[0-9]' OR PRSN_CD like '[0-9][0-9]' OR
	PRSN_CD like '[0-9][0-9][0-9]')
	AND     date(clm.FILL_DT)  between date(pbh.MEPE_EFF_DT)  and  date(pbh.MEPE_TERM_DT) 
 	qualify row_number() over (partition by RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS
                             order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC, 
  						     MEPE_EFF_DT DESC,  REC_UPDT_DTTM DESC) = 1 ) ;
	
	
	
	create or replace temporary table Join_pass2 as
	(SELECT A.*, B.MBR_ALT_ID2, B.PASS1_EXISTS FROM 
	join_cms_sbsb A
	LEFT OUTER JOIN extSTM_PBH_CMC_SBSB_SUBSC_pass2 B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM AND	
	A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM AND
	A.CLM_STUS = B.CLM_STUS) ;
	
	
	
	create or replace temporary table t2xfrm_mapping as 
	(select 
	SUBSTR(trim(NullToEmpty(DGNS_CD)),1,5) PHRM_DIAG_CD,
	DRUG_UNIT_CST PHRM_AVG_WHLSL_AMT,
	CLNT_COPAY PHRM_COPAY_AMT,
	PTNT_PAY_AMT_ATTRD_TO_DDCTBL PHRM_DED_AMT,
	DSPNSNG_FEE_BILLED PHRM_DSPNS_FEE_AMT,
	INGRDNT_CST_BILLED PHRM_INGR_CST_AMT,
	CHK_DT PHRM_CHK_DT,
	FILL_DT PHRM_FILL_DT,
	DAYS_SUPLY PHRM_DAY_SPL_CNT,
	PROD_SLCTN_CD PHRM_DSPNS_AS_WRT_CD,
	SUBSTR(trim(NullToEmpty(PROD_SRVC_ID)),1,11) PHRM_NDC_CD,
	NDC_DRG_ROW_EFF_DT PHRM_NDC_ROW_EFF_DT,
	CASE WHEN Trim(NullToEmpty(NEW_REFL_CD)) = '00' then 'Y' else 'N' END AS PHRM_FST_FILL_IND_CD,
	FRMLRY_STUS PHRM_FRMLRY_IND_CD,
	CASE WHEN SUBSTR(trim(NullToEmpty(in_t1xfrm_mapping.PLAN_QLFYR_CD)),1,1)  =  '2' then '3'  
	WHEN SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '3' then '4'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '4' then '8'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '5' then '9' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '6' then 'E' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '7' then 'F' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '8' then 'G' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '9' then 'H' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '10' then 'A' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '11' then 'B' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '12' then 'C' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '13' then 'D' else
	SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1) END AS 
	PHRM_FRMLRY_TYP_CD,
	Right('000000000000'||( NullToZero(RX_SRVC_REF_NUM)), 12)  PHRM_PRSC_NBR,
	CASE WHEN IsNull(NEW_REFL_CD) Then '' Else AsInteger(NEW_REFL_CD) END AS  PHRM_PRSC_RFL_NBR,
	MTRC_DCML_QTY PHRM_QTY_CNT,
	CASE WHEN SRVC_PROV_ID_QLFYR = '07'  then SRVC_PROV_ID 
	WHEN SRVC_PROV_ID_QLFYR = '01'  then SRVC_PROV_NCPDP_ID_NUM
	else '' END AS PHRM_NBR,
	PRIOR_AUTHRZTN_ID PHRM_PRR_AUTH_NBR,
	case when trim(NullToEmpty(SRVC_PROV_ID_QLFYR ) )  =  '01' 
	then substr(trim(NullToEmpty(SRVC_PROV_ID) ),1,10) else ''
	end as PHRM_NPI_PROV_ID,
	case when trim( NullToEmpty(PRSCRBR_ID_QLFYR ))  =  '01' 
	then substr(trim(NullToEmpty(in_t1xfrm_mapping.PRSCRBR_ID )),1,10) else ''
	end as PHRM_PRSC_NPI_PROV_ID,
	substr(trim(NullToEmpty(PRSCRBR_DEA_NUM)),1,11) PHRM_PROV_DEA_NBR,
	ACCOUNT PHRM_ACCT_ID,
	CARRIER PHRM_CARR_ID,
	substr(trim( NullToEmpty(OTHR_CVRG_CD)),1,1) PHRM_COB_CD,
	RX_CLM_NUM RX_CLM_NUM,
	SQNC_NUM_OF_CLM SQNC_NUM_OF_CLM,
	CLM_STUS CLM_STUS,
	trim(REC_INPUT_SRC_CD)||'-'|| 
	Convert('.','',Trim( (RX_CLM_NUM))||
	'-'||Trim( (SQNC_NUM_OF_CLM)))||
	'-'||trim(CLM_STUS) PHRM_DRG_CLM_ALT_ID,
	
	CASE WHEN IsNull(MBR_ALT_ID) Then (CASE WHEN IsNull(MBR_ALT_ID2) 
	then '-1' else MBR_ALT_ID2 END) Else MBR_ALT_ID end as varMBRALTID,
	
	case when trim(varMBRALTID) = '0' then  '-1' else  trim(varMBRALTID) END AS  MBR_ALT_ID,
	PRSCRBR_DEA_NUM PROV_ALT_ID,
	'' as SITE_CD,
	Right('000000000'||left(trim(in_t2xfrm_mapping.MMBR_ID_NUM),7),9) SBSCR_NBR,
	Right('000'||right(trim(NullToEmpty(PRSN_CD)),2),3 ) DEPN_NBR,
	CASE WHEN IsNull(GRP_ID) Then 
	(CASE WHEN IsNull(PASS1_EXISTS)  
	Then '' Else '' END) Else  Left(trim(GRP_ID),6) END as GRP_NBR,
	'' CUST_SEG_NBR,
	PTNT_DOB MBR_BTH_DT,
	PTNT_FNAME MBR_FST_NM,
	PTNT_LNAME MBR_LST_NM,
	PRMRY_CARE_PROV_ID_CD MBR_PRI_PHYSN_NBR,
	CLNT_BNFT_CD BEN_PLN_NBR,
	substr(Trim(NullToEmpty(CLNT_PROD_CD)),1,5) PRDCT_CD,
	TRANS_CD TRAN_CD,
	REC_XTRCT_SRC_CD REC_XTRCT_SRC_CD,
	REC_INPUT_SRC_CD REC_INPUT_SRC_CD,
	CYCLE_DT CYCLE_DT,
	MMBR_ZIP MBR_ZIP_CD,
	DT_RX_WRTN PHRM_PRSCN_WRT_DT,
	POS PHRM_POS_CD,
	NUM_OF_REFLS_AUTHRZD PHRM_AUTH_RFL_CNT,
	GNDR MBR_GDR_CD,
	DGNS_CD_QLFYR DGNS_CD_QLFYR,
	SETNULL() AS CRR_MEMGROUPID
	from Join_pass2 ) ;
	
	
	
	create or replace temporary table ds_TB_PHRM_DRG_CLM_COSM12 AS 
	( SELECT * FROM t1xfrm_mapping
	UNION 
	SELECT * FROM t2xfrm_mapping ) ;
	
	
	create or replace temporary table COMM_PBH_LOGCD9_1 as
	(SELECT * FROM 
	COMM_PBH_LOGCD9
 	qualify row_number() over (partition by GRP_IDP1,RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS,NDC_DRG_ROW_EFF_DT
                             order by GRP_IDP1 ASC, RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC, 
  						       NDC_DRG_ROW_EFF_DT DESC) = 1 ) ;
	
	
	create or replace temporary table extSTM_PBH_CMC_SBSB_SUBSC2 as
	(SELECT   
	trim(pbh.MBR_ALT_ID)  as MBR_ALT_IDP1,
	trim(pbh.MBR_ALT_ID)  as MBR_ALT_IDP2,
	trim(pbh.GRGR_ID)  as GRGR_IDP1,
	trim(pbh.GRGR_ID)  as GRGR_IDP2,
	clm.RX_CLM_NUM as RX_CLM_NUM, 		
	clm.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	clm.CLM_STUS as CLM_STUS 
	FROM   STG.STG_PSRX_CLM_COMM clm
	inner join  DW.TB_MBR_ELIG_PBH_WRK pbh
	on     left(trim(clm.MMBR_ID_NUM),9)=trim(pbh.SBSB_ID)  and
	 trim(clm.PRSN_CD) =pbh.MEME_SFX and 
	( trim(clm.PRSN_CD)   like '[0-9]' OR  trim(clm.PRSN_CD)  like '[0-9][0-9]' OR  trim(clm.PRSN_CD)  like '[0-9][0-9][0-9]')   
	----                left(clm.grp_id,4)||'0'= pbh.GRGR_ID and
	----                'AI'||left(clm.grp_id,4)||'0'=pbh.GRGR_ID and
	and  date(clm.FILL_DT) between date(pbh.MEPE_EFF_DT)  and date(pbh.MEPE_TERM_DT)	
 	qualify row_number() over (partition by GRP_IDP1,RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS
                             order by GRP_IDP1 ASC, RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC ) = 1 ) ;
		
		
		
		
	create or replace temporary table join2_cms_sbsb as
	(SELECT A.*, B.MBR_ALT_IDP1 FROM 
	COMM_PBH_LOGCD9_1 A LEFT OUTER JOIN extSTM_PBH_CMC_SBSB_SUBSC2 B
	ON A.GRP_IDP1 = B.GRP_IDP1 AND 
	A.RX_CLM_NUM = B.RX_CLM_NUM AND 
	A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM AND 
	A.CLM_STUS = B.CLM_STUS AND )	;
		
		
		
	create or replace temporary table join3_cms_sbsb as
	(SELECT A.*, B.MBR_ALT_IDP2 FROM 
	join2_cms_sbsb A LEFT OUTER JOIN extSTM_PBH_CMC_SBSB_SUBSC2 B
	ON A.GRP_IDP2 = B.GRP_IDP2 AND 
	A.RX_CLM_NUM = B.RX_CLM_NUM AND 
	A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM AND 
	A.CLM_STUS = B.CLM_STUS AND )	;	
		
	
	create or replace temporary table extSTM_PBH_CMC_SBSB_SUBSC_pass2 as	
	(SELECT   
	pbh.MBR_ALT_ID as  MBR_ALT_ID2 ,
	clm.RX_CLM_NUM as RX_CLM_NUM, 		
	trim(pbh.GRGR_ID)  as GRGR_ID,
	clm.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	clm.CLM_STUS as CLM_STUS ,
	 Convert(varchar,date(pbh.MEPE_EFF_DT) ) as MEPE_EFF_DT,
	Convert(varchar,date(pbh.REC_UPDT_DTTM))  as REC_UPDT_DTTM,
	1 as PASS1_EXISTS
	FROM  STG.STG_PSRX_CLM_COMM clm
	INNER JOIN DW.TB_MBR_ELIG_PBH_WRK pbh 
	ON  (  left(trim(clm.MMBR_ID_NUM),9)=trim(pbh.SBSB_ID)  AND  trim(clm.PRSN_CD)=pbh.MEME_SFX )
	INNER JOIN STM.STM_PBH_CMC_MEME_MEMBER mem
	ON ( pbh.MEME_CK=mem.MEME_CK AND clm.PTNT_LNAME=upper(mem.MEME_LAST_NAME) AND clm.PTNT_FNAME=upper(mem.MEME_FIRST_NAME) )
	WHERE  ( trim(clm.PRSN_CD)  like '[0-9]' OR trim(clm.PRSN_CD) like '[0-9][0-9]' OR  trim(clm.PRSN_CD)  like '[0-9][0-9][0-9]')  
	               AND     date(clm.FILL_DT)  between date(pbh.MEPE_EFF_DT)  and  date(pbh.MEPE_TERM_DT)	
  	qualify row_number() over (partition by RX_CLM_NUM,SQNC_NUM_OF_CLM,CLM_STUS
                              order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC 
							  MEPE_EFF_DT DESC, REC_UPDT_DTTM DESC) = 1  ) ;
		
	
	
	create or replace temporary table join_td as
	(SELECT A.*, B.MBR_ALT_ID2, B.PASS1_EXISTS, B.GRGR_ID FROM 
	join3_cms_sbsb A LEFT OUTER JOIN extSTM_PBH_CMC_SBSB_SUBSC_pass2 B
	ON 
	A.RX_CLM_NUM = B.RX_CLM_NUM AND 
	A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM AND 
	A.CLM_STUS = B.CLM_STUS AND )	;	
		
		
	
	create or replace temporary table ds_TB_PHRM_DRG_CLM_COSM9 as
	(SELECT 
	SUBSTR(trim(NullToEmpty(DGNS_CD)),1,5) PHRM_DIAG_CD,
	DRUG_UNIT_CST PHRM_AVG_WHLSL_AMT,
	CLNT_COPAY PHRM_COPAY_AMT,
	PTNT_PAY_AMT_ATTRD_TO_DDCTBL PHRM_DED_AMT,
	DSPNSNG_FEE_BILLED PHRM_DSPNS_FEE_AMT,
	INGRDNT_CST_BILLED PHRM_INGR_CST_AMT,
	CHK_DT PHRM_CHK_DT,
	FILL_DT PHRM_FILL_DT,
	DAYS_SUPLY PHRM_DAY_SPL_CNT,
	PROD_SLCTN_CD PHRM_DSPNS_AS_WRT_CD,
	SUBSTR(trim(NullToEmpty(PROD_SRVC_ID)),1,11) PHRM_NDC_CD,
	NDC_DRG_ROW_EFF_DT PHRM_NDC_ROW_EFF_DT,
	CASE WHEN Trim(NullToEmpty(NEW_REFL_CD)) = '00' then 'Y' else 'N' END AS PHRM_FST_FILL_IND_CD,
	FRMLRY_STUS PHRM_FRMLRY_IND_CD,
	CASE WHEN SUBSTR(trim(NullToEmpty(in_t1xfrm_mapping.PLAN_QLFYR_CD)),1,1)  =  '2' then '3'  
	WHEN SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '3' then '4'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '4' then '8'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '5' then '9' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '6' then 'E' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '7' then 'F' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '8' then 'G' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '9' then 'H' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '10' then 'A' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '11' then 'B' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '12' then 'C' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '13' then 'D' else
	SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1) END AS 
	PHRM_FRMLRY_TYP_CD,
	Right('000000000000'||( NullToZero(RX_SRVC_REF_NUM)), 12)  PHRM_PRSC_NBR,
	AsInteger(NEW_REFL_CD) END AS  PHRM_PRSC_RFL_NBR,
	MTRC_DCML_QTY PHRM_QTY_CNT,
	CASE WHEN SRVC_PROV_ID_QLFYR = '07'  then SRVC_PROV_ID 
	WHEN SRVC_PROV_ID_QLFYR = '01'  then SRVC_PROV_NCPDP_ID_NUM
	else '' END AS PHRM_NBR,
	PRIOR_AUTHRZTN_ID PHRM_PRR_AUTH_NBR,
	case when trim(NullToEmpty(SRVC_PROV_ID_QLFYR ) )  =  '01' 
	then substr(trim(NullToEmpty(SRVC_PROV_ID) ),1,10) else ''
	end as PHRM_NPI_PROV_ID,
	case when trim( NullToEmpty(PRSCRBR_ID_QLFYR ))  =  '01' 
	then substr(trim(NullToEmpty(in_t1xfrm_mapping.PRSCRBR_ID )),1,10) else ''
	end as PHRM_PRSC_NPI_PROV_ID,
	substr(trim(NullToEmpty(PRSCRBR_DEA_NUM)),1,11) PHRM_PROV_DEA_NBR,
	ACCOUNT PHRM_ACCT_ID,
	CARRIER PHRM_CARR_ID,
	substr(trim( NullToEmpty(OTHR_CVRG_CD)),1,1) PHRM_COB_CD,
	RX_CLM_NUM RX_CLM_NUM,
	SQNC_NUM_OF_CLM SQNC_NUM_OF_CLM,
	CLM_STUS CLM_STUS,
	trim(REC_INPUT_SRC_CD)||'-'|| 
	Convert('.','',Trim( (RX_CLM_NUM))||
	'-'||Trim( (SQNC_NUM_OF_CLM)))||
	'-'||trim(CLM_STUS) PHRM_DRG_CLM_ALT_ID,
	CASE WHEN IsNull(MBR_ALT_IDP1) 
	then (CASE WHEN isNull(MBR_ALT_IDP2)  
	then (CASE WHEN IsNull(MBR_ALT_ID2) 
	Then '-1' else MBR_ALT_ID2 END)
	Else MBR_ALT_IDP2 END)
	else MBR_ALT_IDP1 END AS 
	  MBR_ALT_ID,
	PRSCRBR_DEA_NUM PROV_ALT_ID,
	'' as SITE_CD,
	left(trim(MMBR_ID_NUM),9)  SBSCR_NBR,
	Right('000'||right(trim(NullToEmpty(PRSN_CD)),2),3 ) DEPN_NBR,
	CASE WHEN IsNull(MBR_ALT_IDP1) then  
	(CASE WHEN IsNull(MBR_ALT_IDP2)  
	Then (CASE WHEN IsNull(PASS1_EXISTS) 
	Then '' Else GRGR_ID END)
	Else  GRP_IDP2 END ) else  GRP_IDP1 END 
	 as GRP_NBR,
	'' CUST_SEG_NBR,
	PTNT_DOB MBR_BTH_DT,
	PTNT_FNAME MBR_FST_NM,
	PTNT_LNAME MBR_LST_NM,
	PRMRY_CARE_PROV_ID_CD MBR_PRI_PHYSN_NBR,
	CLNT_BNFT_CD BEN_PLN_NBR,
	substr(Trim(NullToEmpty(CLNT_PROD_CD)),1,5) PRDCT_CD,
	TRANS_CD TRAN_CD,
	REC_XTRCT_SRC_CD REC_XTRCT_SRC_CD,
	REC_INPUT_SRC_CD REC_INPUT_SRC_CD,
	CYCLE_DT CYCLE_DT,
	MMBR_ZIP MBR_ZIP_CD,
	DT_RX_WRTN PHRM_PRSCN_WRT_DT,
	POS PHRM_POS_CD,
	NUM_OF_REFLS_AUTHRZD PHRM_AUTH_RFL_CNT,
	GNDR MBR_GDR_CD,
	DGNS_CD_QLFYR DGNS_CD_QLFYR,
	SETNULL() AS CRR_MEMGROUPID
	FROM join_td ) ;
	
	
	
	create or replace temporary table t1xfm_drvjoinkeys as 
	(select 
	NDC_DRG_ROW_EFF_DT,
	SRVC_PROV_ID,
	SRVC_PROV_ID_QLFYR,
	RX_SRVC_REF_NUM,
	FILL_DT,
	PROD_SRVC_ID,
	NEW_REFL_CD,
	MTRC_DCML_QTY,
	DAYS_SUPLY,
	DRUG_UNIT_CST,
	INGRDNT_CST_BILLED,
	DSPNSNG_FEE_BILLED,
	CLNT_COPAY,
	PTNT_PAY_AMT_ATTRD_TO_DDCTBL,
	UPPER(PTNT_FNAME) AS PTNT_FNAME,
	UPPER(PTNT_LNAME) AS PTNT_LNAME,
	PTNT_DOB,
	GNDR,
	MMBR_ID_NUM,
	MMBR_ZIP,
	PRSCRBR_ID,
	PRSCRBR_ID_QLFYR,
	PRSCRBR_DEA_NUM,
	DGNS_CD,
	PRIOR_AUTHRZTN_ID,
	POS,
	DT_RX_WRTN,
	PROD_SLCTN_CD,
	OTHR_CVRG_CD,
	FRMLRY_STUS,
	NUM_OF_REFLS_AUTHRZD,
	PRMRY_CARE_PROV_ID_CD,
	CARRIER,
	ACCOUNT,
	GRP_ID,
	CASE WHEN num(NullToZero(SSN)) then AsInteger((SSN)) else 0  SSN,
	RX_CLM_NUM,
	SQNC_NUM_OF_CLM,
	CLM_STUS,
	TRANS_CD,
	CHK_DT,
	CLNT_PROD_CD,
	CLNT_BNFT_CD,
	PLAN_QLFYR_CD,
	REC_XTRCT_SRC_CD,
	REC_INPUT_SRC_CD,
	CYCLE_DT,
	SRVC_PROV_NCPDP_ID_NUM,
	MMBR_ID_NUM_JNKEY,
	AsInteger( (Right(Trim((ACCOUNT)),5)))   MEM_MEMGRP,
	AsInteger( (Right(Trim((MMBR_ID_NUM)),2)))  MEM_MEMDEP,
	SUBSTR(Trim(NullToEmpty(ACCOUNT)),7,3) GUM_SITE_ID,
	DGNS_CD_QLFYR,
	Mbr_Alt_Id_Logic_Cd
	FROM ds_GOVT_COSM_LOGCD3 ) ;
	
	
	create or replace temporary table ds_STM_COSM_GUM_1 as
	(SELECT * FROM ds_STM_COSM_GUM
  	qualify row_number() over (partition by GUM_SITE_ID
                              order by GUM_SITE_ID ASC) = 1 ) ;
	
	
	create or replace temporary table Copy_of_join_gum as 
	(select 
	A.*, B.GUM_SITE_NBR FROM 
	t1xfm_drvjoinkeys A LEFT OUTER JOIN ds_STM_COSM_GUM_1 B
	ON A.GUM_SITE_ID = B.GUM_SITE_ID ) ;
	
	
	
	create or replace temporary table Transformer_557_1 AS 
	(SELECT 
	RX_CLM_NUM,
	SQNC_NUM_OF_CLM,
	CLM_STUS,
	MEM_MEMSUB MMBR_ID_NUM_JNKEY,
	MEM_SITE_NBR GUM_SITE_NBR,
	MEM_SITE_NBR GUM_SITE_NBR_MBR_NUM,
	MEM_EFF_YMD,
	MEM_MEMDEP,
	MEM_MEMGRP,
	REC_UPDT_DTTM
	FROM ds_STM_COSM_MEM_PSRX_GOVT_3
	) ;
	
	
	create or replace temporary table Transformer_557_2 AS 
	(SELECT 
	RX_CLM_NUM,
	SQNC_NUM_OF_CLM,
	CLM_STUS,
	MEM_MEMSUB SSN,
	MEM_SITE_NBR GUM_SITE_NBR,
	MEM_SITE_NBR GUM_SITE_NBR_SSN,
	MEM_EFF_YMD,
	MEM_MEMDEP,
	MEM_MEMGRP,
	REC_UPDT_DTTM
	FROM ds_STM_COSM_MEM_PSRX_GOVT_3
	) ;
	
	
	create or replace temporary table Transformer_557_3 AS 
	(SELECT 
	MEM_L_NAME,
	MEM_DOB_YMD,
	MEM_SITE_NBR GUM_SITE_NBR,
	MEM_SITE_NBR GUM_SITE_NBR_PASS3,
	MEM_EFF_YMD,
	MEM_MEMDEP,
	MEM_MEMGRP,
	RX_CLM_NUM,
	SQNC_NUM_OF_CLM,
	CLM_STUS,
	MEM_MEMSUB,
	REC_UPDT_DTTM
	FROM ds_STM_COSM_MEM_PSRX_GOVT_3
	) ;
	
	
	create or replace temporary table join1mbralt as 
	(select 
	A.*, B.GUM_SITE_NBR_MBR_NUM FROM 
	Copy_of_join_gum A LEFT OUTER JOIN Transformer_557_1 B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM 
	and A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM 
	and A.CLM_STUS = B.CLM_STUS 
	and A.MMBR_ID_NUM_JNKEY = B.MMBR_ID_NUM_JNKEY 
	and A.MEM_MEMGRP = B.MEM_MEMGRP 
	and A.MEM_MEMDEP = B.MEM_MEMDEP 
	and A.GUM_SITE_NBR = B.GUM_SITE_NBR ) ;
	
	
	
	create or replace temporary table join2mbralt as 
	(select 
	A.*, B.GUM_SITE_NBR_SSN FROM 
	join1mbralt A LEFT OUTER JOIN Transformer_557_2 B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM 
	and A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM 
	and A.CLM_STUS = B.CLM_STUS 
	and A.SSN = B.SSN 
	and A.MEM_MEMGRP = B.MEM_MEMGRP 
	and A.MEM_MEMDEP = B.MEM_MEMDEP 
	and A.GUM_SITE_NBR = B.GUM_SITE_NBR ) ;
	
	
	
	create or replace temporary table Join3mbralt as 
	(select 
	A.*, B.MEM_L_NAME, B.MEM_DOB_YMD, B.GUM_SITE_NBR_PASS3, B.MEM_EFF_YMD,
	B.MEM_MEMSUB, B.REC_UPDT_DTTM  
	FROM 
	join2mbralt A LEFT OUTER JOIN Transformer_557_3 B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM 
	and A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM 
	and A.CLM_STUS = B.CLM_STUS 
	and A.MEM_MEMGRP = B.MEM_MEMGRP 
	and A.MEM_MEMDEP = B.MEM_MEMDEP 
	and A.GUM_SITE_NBR = B.GUM_SITE_NBR ) ;
	
	
	
	create or replace temporary table Join4mbralt as 
	(select 
	A.*, B.MEM_MEMGRP_PBH, B.MEM_MEMDEP_PBH, B.GUM_SITE_NBR_PBH,
	B.MEM_MEMSUB_PBH, B.MEM_ALT_ID_PBH
	FROM 
	Join3mbralt A LEFT OUTER JOIN ds_STM_COSM_MEM_PSRX_GOVT_3_PBHtoCOSM B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM 
	and A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM 
	and A.CLM_STUS = B.CLM_STUS 
	and A.MMBR_ID_NUM = B.MMBR_ID_NUM ) ;
	
	
	
	
	create or replace temporary table ds_TB_PHRM_DRG_CLM_GOVT3 as
	(SELECT 
	CASE WHEN Num(Left(Trim(NullToEmpty(MMBR_ID_NUM)),9) ) 
	then Left(Trim(NullToEmpty(MMBR_ID_NUM)),9) else -1 END AS varMBRALTID7,
	CASE WHEN IsNull(GUM_SITE_NBR_MBR_NUM)  
	Then (CASE WHEN IsNull(GUM_SITE_NBR_SSN) Then  
	(CASE WHEN IsNull(GUM_SITE_NBR_PASS3) then '-1'  
	Else NullToEmpty(MEM_MEMSUB) END)
	Else NullToEmpty(SSN) END) Else  varMBRALTID7 END AS varPARTIALMBRALTID,
	CASE WHEN Num( Right(Trim(NullToEmpty(MMBR_ID_NUM)),2))  
	then Right(Trim(NullToEmpty(MMBR_ID_NUM)),2) else -1 END AS varMBRIDNUMint,
	CASE WHEN Num(Right(Trim(NullToEmpty(ACCOUNT)),5)) 
	then Right(Trim(NullToEmpty(in_t1xfrm_mapping.ACCOUNT)),5) else -1 END AS varACCTN,
	CASE WHEN ( varPARTIALMBRALTID  = '-1'  or varMBRIDNUMint = -1)    then '-1'  
	 else  varACCTN||'-'||varPARTIALMBRALTID||'-'||varMBRIDNUMint END AS varMBRALTID,
 	CASE WHEN Isnull(GUM_SITE_NBR_MBR_NUM) then 
	(CASE WHEN isnull(GUM_SITE_NBR_SSN)  then  
	(CASE WHEN IsNull(GUM_SITE_NBR_PASS3) Then 0 Else GUM_SITE_NBR_PASS3 END)
	else GUM_SITE_NBR_SSN END)
	else GUM_SITE_NBR_MBR_NUM END AS varGUMSITENBR,
		SUBSTR(trim(NullToEmpty(DGNS_CD)),1,5) PHRM_DIAG_CD,
		DRUG_UNIT_CST PHRM_AVG_WHLSL_AMT,
		CLNT_COPAY PHRM_COPAY_AMT,
		PTNT_PAY_AMT_ATTRD_TO_DDCTBL PHRM_DED_AMT,
		DSPNSNG_FEE_BILLED PHRM_DSPNS_FEE_AMT,
		INGRDNT_CST_BILLED PHRM_INGR_CST_AMT,
		CHK_DT PHRM_CHK_DT,
		FILL_DT PHRM_FILL_DT,
		DAYS_SUPLY PHRM_DAY_SPL_CNT,
		PROD_SLCTN_CD PHRM_DSPNS_AS_WRT_CD,
		SUBSTR(trim(NullToEmpty(PROD_SRVC_ID)),1,11) PHRM_NDC_CD,
		NDC_DRG_ROW_EFF_DT PHRM_NDC_ROW_EFF_DT,
		CASE WHEN Trim(NullToEmpty(NEW_REFL_CD)) = '00' then 'Y' else 'N' END AS PHRM_FST_FILL_IND_CD,
		FRMLRY_STUS PHRM_FRMLRY_IND_CD,
		CASE WHEN SUBSTR(trim(NullToEmpty(in_t1xfrm_mapping.PLAN_QLFYR_CD)),1,1)  =  '2' then '3'  
		WHEN SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '3' then '4'  
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '4' then '8'  
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '5' then '9' 
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '6' then 'E' 
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '7' then 'F' 
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '8' then 'G' 
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '9' then 'H' 
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '10' then 'A' 
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '11' then 'B' 
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '12' then 'C' 
		WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '13' then 'D' else
		SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1) END AS 
		PHRM_FRMLRY_TYP_CD,
		Right('000000000000'||( NullToZero(RX_SRVC_REF_NUM)), 12)  PHRM_PRSC_NBR,
		AsInteger(NEW_REFL_CD) END AS  PHRM_PRSC_RFL_NBR,
		MTRC_DCML_QTY PHRM_QTY_CNT,
		CASE WHEN SRVC_PROV_ID_QLFYR = '07'  then SRVC_PROV_ID 
		WHEN SRVC_PROV_ID_QLFYR = '01'  then SRVC_PROV_NCPDP_ID_NUM
		else '' END AS PHRM_NBR,
		PRIOR_AUTHRZTN_ID PHRM_PRR_AUTH_NBR,
		case when trim(NullToEmpty(SRVC_PROV_ID_QLFYR ) )  =  '01' 
		then substr(trim(NullToEmpty(SRVC_PROV_ID) ),1,10) else ''
		end as PHRM_NPI_PROV_ID,
		case when trim( NullToEmpty(PRSCRBR_ID_QLFYR ))  =  '01' 
		then substr(trim(NullToEmpty(in_t1xfrm_mapping.PRSCRBR_ID )),1,10) else ''
		end as PHRM_PRSC_NPI_PROV_ID,
		substr(trim(NullToEmpty(PRSCRBR_DEA_NUM)),1,11) PHRM_PROV_DEA_NBR,
		ACCOUNT PHRM_ACCT_ID,
		CARRIER PHRM_CARR_ID,
		substr(trim( NullToEmpty(OTHR_CVRG_CD)),1,1) PHRM_COB_CD,
		RX_CLM_NUM RX_CLM_NUM,
		SQNC_NUM_OF_CLM SQNC_NUM_OF_CLM,
		CLM_STUS CLM_STUS,
		trim(REC_INPUT_SRC_CD)||'-'|| 
		Convert('.','',Trim( (RX_CLM_NUM))||
		'-'||Trim( (SQNC_NUM_OF_CLM)))||
		'-'||trim(CLM_STUS) PHRM_DRG_CLM_ALT_ID,
		CASE WHEN (LEN(trim(NullToEmpty(MEM_ALT_ID_PBH))) >= 9) 
		then trim(NullToEmpty(MEM_ALT_ID_PBH)) 
		else (CASE WHEN (varGUMSITENBR = 0 )  or trim( varMBRALTID) = '-1' )  
		then '-1' else varGUMSITENBR ||'-'|| varMBRALTID END) END AS
		MBR_ALT_ID,
		PRSCRBR_DEA_NUM PROV_ALT_ID,
		CASE WHEN (LEN(trim(NullToEmpty(MEM_ALT_ID_PBH))) >= 9) 
		then trim(NullToEmpty(GUM_SITE_NBR_PBH)) 
		else (CASE WHEN  (  (varGUMSITENBR = 0 )  or trim( varMBRALTID) = '-1' )  then setnull() 
		else SUBSTR(trim(NullToEmpty(ACCOUNT)),7,3) END ) END 
		 as SITE_CD,
		CASE WHEN (LEN(trim(NullToEmpty(MEM_ALT_ID_PBH))) >= 9) 
		then trim(NullToEmpty(MEM_MEMSUB_PBH)) 
			else (CASE WHEN  (  (varGUMSITENBR = 0 )  or trim( varMBRALTID) = '-1' )  then '' 
				else (CASE WHEN  IsNull(GUM_SITE_NBR_MBR_NUM)   then 
		    			(CASE WHEN isNull(GUM_SITE_NBR_SSN)       then  
		     				(CASE WHEN IsNull(MEM_MEMSUB)  Then '' 
		     				Else      Trim(NullToEmpty(MEM_MEMSUB)) END)
		    			else  Trim(NullToEmpty(SSN))  END )
		else  varMBRALTID7 END) END) END AS
		  SBSCR_NBR,
		CASE WHEN (LEN(trim(NullToEmpty(MEM_ALT_ID_PBH))) >= 9) 
		then trim(NullToEmpty(MEM_MEMDEP_PBH)) 
		else (CASE WHEN (  (varGUMSITENBR = 0 )  or trim( varMBRALTID) = '-1' )  then '' 
		else varMBRIDNUMint END) END AS 
		 DEPN_NBR,
		CASE WHEN (LEN(trim(NullToEmpty(MEM_ALT_ID_PBH))) >= 9) 
		then trim(NullToEmpty(MEM_MEMGRP_PBH)) 
		else (CASE WHEN (  (varGUMSITENBR = 0 )  or trim( varMBRALTID) = '-1' )  then setnull() 
		else Right(trim(NullToEmpty(ACCOUNT)),5) END) END AS 
		 GRP_NBR,
		'' CUST_SEG_NBR,
		PTNT_DOB MBR_BTH_DT,
		PTNT_FNAME MBR_FST_NM,
		PTNT_LNAME MBR_LST_NM,
		PRMRY_CARE_PROV_ID_CD MBR_PRI_PHYSN_NBR,
		CLNT_BNFT_CD BEN_PLN_NBR,
		substr(Trim(NullToEmpty(CLNT_PROD_CD)),1,5) PRDCT_CD,
		TRANS_CD TRAN_CD,
		REC_XTRCT_SRC_CD REC_XTRCT_SRC_CD,
		REC_INPUT_SRC_CD REC_INPUT_SRC_CD,
		CYCLE_DT CYCLE_DT,
		MMBR_ZIP MBR_ZIP_CD,
		DT_RX_WRTN PHRM_PRSCN_WRT_DT,
		POS PHRM_POS_CD,
		NUM_OF_REFLS_AUTHRZD PHRM_AUTH_RFL_CNT,
		GNDR MBR_GDR_CD,
		SUBSTR(Trim(NullToEmpty(ACCOUNT)),7,3) GUM_SITE_ID,
		CASE WHEN Isnull(GUM_SITE_NBR_MBR_NUM) then 
		(CASE WHEN isnull(GUM_SITE_NBR_SSN)  then  
		(CASE WHEN IsNull(GUM_SITE_NBR_PASS3) Then 0 Else in_t1xfrm_mapping.GUM_SITE_NBR_PASS3 END)
		else GUM_SITE_NBR_SSN END )
		else GUM_SITE_NBR_MBR_NUM END AS 
		GUM_SITE_NBR,
		DGNS_CD_QLFYR,
		MBR_ALT_ID_LOGIC_CD,
		CASE WHEN (LEN(trim(NullToEmpty(MEM_ALT_ID_PBH))) >= 9) 
		then 1 else (CASE WHEN (  (varGUMSITENBR = 0 )  or trim( varMBRALTID) = '-1' )  then 0 else  1 END) END AS
		MBR_ALT_ID_YN,
		SETNULL() AS CRR_MEMGROUPID
		FROM Join4mbralt ) ;
	
	
	
	create or replace temporary table ds_GOVT_FCETLOGCD8_1 as
	(SELECT * FROM ds_GOVT_FCETLOGCD8
  	qualify row_number() over (partition by RX_CLM_NUM, SQNC_NUM_OF_CLM, CLM_STUS
                              order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC,
							  NDC_DRG_ROW_EFF_DT DESC) = 1 ) ;
	
	
	
	create or replace temporary table join_cesmem_govt as 
	(select 
	A.*, B.GRGR_ID, B.SBSB_ID, B.MEME_SFX
	 FROM 
	ds_GOVT_FCETLOGCD8_1 A LEFT OUTER JOIN ds_STM_CMC_MEME8_ref B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM
	AND A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM
	AND A.CLM_STUS = B.CLM_STUS ) ;
	
	
	
	
	
	create or replace temporary table ds_TB_PHRM_DRG_CLM_GOVT8 AS 
	(SELECT 
	CASE WHEN  ( isNull(GRGR_ID)  or  isNull(SBSB_ID)  
	or isNull(MEME_SFX )    or  isNull(GRGR_ID)  )  
	then '-1' 
	else trim( (GRGR_ID))||'-'||trim( (SBSB_ID))||'-'||( (MEME_SFX) ) END AS MbrAltID,
	
	SUBSTR(trim(NullToEmpty(DGNS_CD)),1,5) PHRM_DIAG_CD,
	DRUG_UNIT_CST PHRM_AVG_WHLSL_AMT,
	CLNT_COPAY PHRM_COPAY_AMT,
	PTNT_PAY_AMT_ATTRD_TO_DDCTBL PHRM_DED_AMT,
	DSPNSNG_FEE_BILLED PHRM_DSPNS_FEE_AMT,
	INGRDNT_CST_BILLED PHRM_INGR_CST_AMT,
	CHK_DT PHRM_CHK_DT,
	FILL_DT PHRM_FILL_DT,
	DAYS_SUPLY PHRM_DAY_SPL_CNT,
	PROD_SLCTN_CD PHRM_DSPNS_AS_WRT_CD,
	SUBSTR(trim(NullToEmpty(PROD_SRVC_ID)),1,11) PHRM_NDC_CD,
	NDC_DRG_ROW_EFF_DT PHRM_NDC_ROW_EFF_DT,
	CASE WHEN Trim(NullToEmpty(NEW_REFL_CD)) = '00' then 'Y' else 'N' END AS PHRM_FST_FILL_IND_CD,
	FRMLRY_STUS PHRM_FRMLRY_IND_CD,
	CASE WHEN SUBSTR(trim(NullToEmpty(in_t1xfrm_mapping.PLAN_QLFYR_CD)),1,1)  =  '2' then '3'  
	WHEN SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '3' then '4'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '4' then '8'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '5' then '9' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '6' then 'E' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '7' then 'F' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '8' then 'G' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '9' then 'H' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '10' then 'A' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '11' then 'B' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '12' then 'C' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '13' then 'D' else
	SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1) END AS 
	PHRM_FRMLRY_TYP_CD,
	Right('000000000000'||( NullToZero(RX_SRVC_REF_NUM)), 12)  PHRM_PRSC_NBR,
	AsInteger(NEW_REFL_CD) END AS  PHRM_PRSC_RFL_NBR,
	MTRC_DCML_QTY PHRM_QTY_CNT,
	CASE WHEN SRVC_PROV_ID_QLFYR = '07'  then SRVC_PROV_ID 
	WHEN SRVC_PROV_ID_QLFYR = '01'  then SRVC_PROV_NCPDP_ID_NUM
	else '' END AS PHRM_NBR,
	PRIOR_AUTHRZTN_ID PHRM_PRR_AUTH_NBR,
	case when trim(NullToEmpty(SRVC_PROV_ID_QLFYR ) )  =  '01' 
	then substr(trim(NullToEmpty(SRVC_PROV_ID) ),1,10) else ''
	end as PHRM_NPI_PROV_ID,
	case when trim( NullToEmpty(PRSCRBR_ID_QLFYR ))  =  '01' 
	then substr(trim(NullToEmpty(in_t1xfrm_mapping.PRSCRBR_ID )),1,10) else ''
	end as PHRM_PRSC_NPI_PROV_ID,
	substr(trim(NullToEmpty(PRSCRBR_DEA_NUM)),1,11) PHRM_PROV_DEA_NBR,
	ACCOUNT PHRM_ACCT_ID,
	CARRIER PHRM_CARR_ID,
	substr(trim( NullToEmpty(OTHR_CVRG_CD)),1,1) PHRM_COB_CD,
	RX_CLM_NUM RX_CLM_NUM,
	SQNC_NUM_OF_CLM SQNC_NUM_OF_CLM,
	CLM_STUS CLM_STUS,
	trim(REC_INPUT_SRC_CD)||'-'|| 
	Convert('.','',Trim( (RX_CLM_NUM))||
	'-'||Trim( (SQNC_NUM_OF_CLM)))||
	'-'||trim(CLM_STUS) PHRM_DRG_CLM_ALT_ID,
	MbrAltID AS MBR_ALT_ID,
	PRSCRBR_DEA_NUM PROV_ALT_ID,
	SETNULL() as SITE_CD,
	CASE WHEN (MbrAltID = '-1') then '' else trim(SSN)  END AS  SBSCR_NBR,
	CASE WHEN (isNull(MEME_SFX) or MbrAltID = '-1')  then '' else  AsInteger((MEME_SFX)) END AS 
	 DEPN_NBR,
	CASE WHEN ( isNull(GRGR_ID) or MbrAltID = '-1') then setnull() else   trim(GRGR_ID) END AS
	 as GRP_NBR,
	'' CUST_SEG_NBR,
	PTNT_DOB MBR_BTH_DT,
	PTNT_FNAME MBR_FST_NM,
	PTNT_LNAME MBR_LST_NM,
	PRMRY_CARE_PROV_ID_CD MBR_PRI_PHYSN_NBR,
	CLNT_BNFT_CD BEN_PLN_NBR,
	substr(Trim(NullToEmpty(CLNT_PROD_CD)),1,5) PRDCT_CD,
	TRANS_CD TRAN_CD,
	REC_XTRCT_SRC_CD REC_XTRCT_SRC_CD,
	REC_INPUT_SRC_CD REC_INPUT_SRC_CD,
	CYCLE_DT CYCLE_DT,
	MMBR_ZIP MBR_ZIP_CD,
	DT_RX_WRTN PHRM_PRSCN_WRT_DT,
	POS PHRM_POS_CD,
	NUM_OF_REFLS_AUTHRZD PHRM_AUTH_RFL_CNT,
	GNDR MBR_GDR_CD,
	DGNS_CD_QLFYR DGNS_CD_QLFYR,
	Mbr_Alt_Id_Logic_Cd,
	CASE WHEN (MbrAltID = '-1') then 0 else 1 END AS MBR_ALT_ID_YN ,
	SETNULL() AS CRR_MEMGROUPID
	FROM join_cesmem_govt ) ;
	
	
	create or replace temporary table ds_GOVT_PBH_LOGCD4_1 as
	(SELECT * FROM ds_GOVT_PBH_LOGCD4
  	qualify row_number() over (partition by RX_CLM_NUM, SQNC_NUM_OF_CLM, CLM_STUS
                              order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC,
							  NDC_DRG_ROW_EFF_DT DESC) = 1 ) ;
							  
	
	create or replace temporary table flt_sfx_1 AS
	(SELECT 
	MEME_SFX,
	MBR_ALT_ID0,
	MEPE_EFF_DT,
	REC_UPDT_DTTM,
	RX_CLM_NUM,
	SQNC_NUM_OF_CLM,
	CLM_STUS,
	SBSCR_RANKING
	FROM ds_STM_PBH_CMC_SBSB_SUBSC4_ref) ;
	
	
	create or replace temporary table flt_sfx_2 AS
	(SELECT 
	MEME_SFX,
	MBR_ALT_ID1,
	MEPE_EFF_DT,
	REC_UPDT_DTTM,
	RX_CLM_NUM,
	SQNC_NUM_OF_CLM,
	CLM_STUS,
	SBSCR_RANKING
	FROM ds_STM_PBH_CMC_SBSB_SUBSC4_ref) ;
	
	
	create or replace temporary table join_sbsc_govt as 
	(select 
	A.*, B.GRGR_ID, B.MBR_ALT_ID0, B.SBSCR_RANKING SBSCR_RANKING1
	 FROM 
	ds_GOVT_PBH_LOGCD4_1 A LEFT OUTER JOIN flt_sfx_1 B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM
	AND A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM
	AND A.CLM_STUS = B.CLM_STUS ) ;
	
	
	
	
	create or replace temporary table join2_sbsc_govt as 
	(select 
	A.*, B.GRGR_ID, B.MBR_ALT_ID1, B.SBSCR_RANKING
	 FROM 
	join_sbsc_govt A LEFT OUTER JOIN flt_sfx_2 B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM
	AND A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM
	AND A.CLM_STUS = B.CLM_STUS ) ;
	
	
	
	
	create or replace temporary table ds_TB_PHRM_DRG_CLM_GOVT4 AS
	(SELECT 
	
	CASE WHEN Num( Right(Trim(NullToEmpty(MMBR_ID_NUM)),2))  
	then  (StringToDecimal(Right(Trim(NullToEmpty(MMBR_ID_NUM)),2)))   
	else -1 END AS varMBRIDNUMint,
	
	SUBSTR(trim(NullToEmpty(DGNS_CD)),1,5) PHRM_DIAG_CD,
	DRUG_UNIT_CST PHRM_AVG_WHLSL_AMT,
	CLNT_COPAY PHRM_COPAY_AMT,
	PTNT_PAY_AMT_ATTRD_TO_DDCTBL PHRM_DED_AMT,
	DSPNSNG_FEE_BILLED PHRM_DSPNS_FEE_AMT,
	INGRDNT_CST_BILLED PHRM_INGR_CST_AMT,
	CHK_DT PHRM_CHK_DT,
	FILL_DT PHRM_FILL_DT,
	DAYS_SUPLY PHRM_DAY_SPL_CNT,
	PROD_SLCTN_CD PHRM_DSPNS_AS_WRT_CD,
	SUBSTR(trim(NullToEmpty(PROD_SRVC_ID)),1,11) PHRM_NDC_CD,
	NDC_DRG_ROW_EFF_DT PHRM_NDC_ROW_EFF_DT,
	CASE WHEN Trim(NullToEmpty(NEW_REFL_CD)) = '00' then 'Y' else 'N' END AS PHRM_FST_FILL_IND_CD,
	FRMLRY_STUS PHRM_FRMLRY_IND_CD,
	CASE WHEN SUBSTR(trim(NullToEmpty(in_t1xfrm_mapping.PLAN_QLFYR_CD)),1,1)  =  '2' then '3'  
	WHEN SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '3' then '4'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '4' then '8'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '5' then '9' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '6' then 'E' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '7' then 'F' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '8' then 'G' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '9' then 'H' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '10' then 'A' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '11' then 'B' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '12' then 'C' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '13' then 'D' else
	SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1) END AS 
	PHRM_FRMLRY_TYP_CD,
	Right('000000000000'||( NullToZero(RX_SRVC_REF_NUM)), 12)  PHRM_PRSC_NBR,
	AsInteger(NEW_REFL_CD) END AS  PHRM_PRSC_RFL_NBR,
	MTRC_DCML_QTY PHRM_QTY_CNT,
	CASE WHEN SRVC_PROV_ID_QLFYR = '07'  then SRVC_PROV_ID 
	WHEN SRVC_PROV_ID_QLFYR = '01'  then SRVC_PROV_NCPDP_ID_NUM
	else '' END AS PHRM_NBR,
	PRIOR_AUTHRZTN_ID PHRM_PRR_AUTH_NBR,
	case when trim(NullToEmpty(SRVC_PROV_ID_QLFYR ) )  =  '01' 
	then substr(trim(NullToEmpty(SRVC_PROV_ID) ),1,10) else ''
	end as PHRM_NPI_PROV_ID,
	case when trim( NullToEmpty(PRSCRBR_ID_QLFYR ))  =  '01' 
	then substr(trim(NullToEmpty(in_t1xfrm_mapping.PRSCRBR_ID )),1,10) else ''
	end as PHRM_PRSC_NPI_PROV_ID,
	substr(trim(NullToEmpty(PRSCRBR_DEA_NUM)),1,11) PHRM_PROV_DEA_NBR,
	ACCOUNT PHRM_ACCT_ID,
	CARRIER PHRM_CARR_ID,
	substr(trim( NullToEmpty(OTHR_CVRG_CD)),1,1) PHRM_COB_CD,
	RX_CLM_NUM RX_CLM_NUM,
	SQNC_NUM_OF_CLM SQNC_NUM_OF_CLM,
	CLM_STUS CLM_STUS,
	trim(REC_INPUT_SRC_CD)||'-'|| 
	Trim( (RX_CLM_NUM)||'-'||
	Trim( (SQNC_NUM_OF_CLM)))||'-'||
	trim(CLM_STUS) PHRM_DRG_CLM_ALT_ID,
	case when  isNull(MBR_ALT_ID0) 
	then  (case when isNull(MBR_ALT_ID1)  
	then '-1' else  (MBR_ALT_ID1) end) else (MBR_ALT_ID0) end
	 AS MBR_ALT_ID,
	PRSCRBR_DEA_NUM PROV_ALT_ID,
	SETNULL() as SITE_CD,
	case when ( NullToValue(SBSCR_RANKING,5)=1   
	or NullToValue(SBSCR_RANKING1,5)=1 ) 
	Then Left(Trim(nullToEmpty(MMBR_ID_NUM)),7)  
	Else (case when  ( NullTOValue(SBSCR_RANKING,5)=2   
	or NullToValue(SBSCR_RANKING1,5)=2 ) 
	Then '00'||Left(Trim(nullToEmpty(MMBR_ID_NUM)),7) Else '' end) end as
	 SBSCR_NBR,
	varMBRIDNUMint DEPN_NBR,
	'' GRP_NBR,
	'' CUST_SEG_NBR,
	PTNT_DOB MBR_BTH_DT,
	PTNT_FNAME MBR_FST_NM,
	PTNT_LNAME MBR_LST_NM,
	PRMRY_CARE_PROV_ID_CD MBR_PRI_PHYSN_NBR,
	CLNT_BNFT_CD BEN_PLN_NBR,
	substr(Trim(NullToEmpty(CLNT_PROD_CD)),1,5) PRDCT_CD,
	TRANS_CD TRAN_CD,
	REC_XTRCT_SRC_CD REC_XTRCT_SRC_CD,
	REC_INPUT_SRC_CD REC_INPUT_SRC_CD,
	CYCLE_DT CYCLE_DT,
	MMBR_ZIP MBR_ZIP_CD,
	DT_RX_WRTN PHRM_PRSCN_WRT_DT,
	POS PHRM_POS_CD,
	NUM_OF_REFLS_AUTHRZD PHRM_AUTH_RFL_CNT,
	GNDR MBR_GDR_CD,
	DGNS_CD_QLFYR DGNS_CD_QLFYR,
	SETNULL() AS CRR_MEMGROUPID
	FROM join2_sbsc_govt ) ;
	
	
	
	
	create or replace temporary table extSTM_ELI_T_CES_MEMBER_1 as 
	(select 
	ces.MBR_SEQ_NBR as  MBR_SEQ_NBR,
	commfs.RX_CLM_NUM as RX_CLM_NUM, 		
	commfs.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	commfs.CLM_STUS as CLM_STUS ,
	Convert(varchar(26),ces.UPDATE_DATE) as UPDATE_DATE ,
	trim(ces.POLICY_NBR)
	from 
	STG.STG_PSRX_CLM_COMM_FRESHSTART commfs
	left outer join STM.STM_ELI_T_CES_MEMBER ces on
	substr(trim(ces.POLICY_NBR),2,6) = substr(left(trim(commfs.GRP_ID),7),2,6) and
	trim(ces.SUBSCRIBER_NBR)=trim(commfs.SSN)  and 
	ces.CES_RELATIONSHIP_CODE =( case when commfs.PTNT_REL_TO_CARDHLDR='1' then '0'
									when commfs.PTNT_REL_TO_CARDHLDR='2' then '1'
											else  '2' 
											END   ) and 
	upper(trim(ces.FIRST_NAME)) 	= 	upper(trim(commfs.PTNT_FNAME)) and 
	date(ces.BIRTH_DATE)		=	date(commfs.PTNT_DOB  )
	where  commfs.REC_UPDT_DT >= '#RUN_FROMDT#'  and commfs.REC_UPDT_DT <= '#RUN_TODT#' 
  	qualify row_number() over (partition by RX_CLM_NUM, SQNC_NUM_OF_CLM, CLM_STUS
                              order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC,
							  UPDATE_DATE DESC) = 1 ) ;


	
	create or replace temporary table join_cesmem_x as 
	(select 
	A.*, B.MBR_SEQ_NBR, B.POLICY_NBR
	 FROM 
	ds_COMM_CES_LOGCD1_1 A LEFT OUTER JOIN extSTM_ELI_T_CES_MEMBER_1 B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM
	AND A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM
	AND A.CLM_STUS = B.CLM_STUS ) ;
	
	
	
	create or replace temporary table ds_TB_PHRM_DRG_CLM_COMMFS_10 as 
	(select 
	CASE when isNull(MBR_SEQ_NBR) then -1 else 
	( NullToValue(POLICY_NBR,'')||'-'||trim(SSN)||'-'||
	(case when PTNT_REL_TO_CARDHLDR = '1' then '0'
	else (case when PTNT_REL_TO_CARDHLDR = '2' then '1'
	elsE '2' end)end)||'-'||trim((MBR_SEQ_NBR) ) ) end as varMBRALTID,
	
	SUBSTR(trim(NullToEmpty(DGNS_CD)),1,5) PHRM_DIAG_CD,
	DRUG_UNIT_CST PHRM_AVG_WHLSL_AMT,
	CLNT_COPAY PHRM_COPAY_AMT,
	PTNT_PAY_AMT_ATTRD_TO_DDCTBL PHRM_DED_AMT,
	DSPNSNG_FEE_BILLED PHRM_DSPNS_FEE_AMT,
	INGRDNT_CST_BILLED PHRM_INGR_CST_AMT,
	CHK_DT PHRM_CHK_DT,
	FILL_DT PHRM_FILL_DT,
	DAYS_SUPLY PHRM_DAY_SPL_CNT,
	PROD_SLCTN_CD PHRM_DSPNS_AS_WRT_CD,
	SUBSTR(trim(NullToEmpty(PROD_SRVC_ID)),1,11) PHRM_NDC_CD,
	NDC_DRG_ROW_EFF_DT PHRM_NDC_ROW_EFF_DT,
	CASE WHEN Trim(NullToEmpty(NEW_REFL_CD)) = '00' then 'Y' else 'N' END AS PHRM_FST_FILL_IND_CD,
	FRMLRY_STUS PHRM_FRMLRY_IND_CD,
	CASE WHEN SUBSTR(trim(NullToEmpty(in_t1xfrm_mapping.PLAN_QLFYR_CD)),1,1)  =  '2' then '3'  
	WHEN SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '3' then '4'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '4' then '8'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '5' then '9' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '6' then 'E' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '7' then 'F' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '8' then 'G' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '9' then 'H' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '10' then 'A' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '11' then 'B' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '12' then 'C' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '13' then 'D' else
	SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1) END AS 
	PHRM_FRMLRY_TYP_CD,
	Right('000000000000'||( NullToZero(RX_SRVC_REF_NUM)), 12)  PHRM_PRSC_NBR,
	AsInteger(NEW_REFL_CD) END AS  PHRM_PRSC_RFL_NBR,
	MTRC_DCML_QTY PHRM_QTY_CNT,
	CASE WHEN SRVC_PROV_ID_QLFYR = '07'  then SRVC_PROV_ID 
	WHEN SRVC_PROV_ID_QLFYR = '01'  then SRVC_PROV_NCPDP_ID_NUM
	else '' END AS PHRM_NBR,
	PRIOR_AUTHRZTN_ID PHRM_PRR_AUTH_NBR,
	case when trim(NullToEmpty(SRVC_PROV_ID_QLFYR ) )  =  '01' 
	then substr(trim(NullToEmpty(SRVC_PROV_ID) ),1,10) else ''
	end as PHRM_NPI_PROV_ID,
	case when trim( NullToEmpty(PRSCRBR_ID_QLFYR ))  =  '01' 
	then substr(trim(NullToEmpty(in_t1xfrm_mapping.PRSCRBR_ID )),1,10) else ''
	end as PHRM_PRSC_NPI_PROV_ID,
	substr(trim(NullToEmpty(PRSCRBR_DEA_NUM)),1,11) PHRM_PROV_DEA_NBR,
	ACCOUNT PHRM_ACCT_ID,
	CARRIER PHRM_CARR_ID,
	substr(trim( NullToEmpty(OTHR_CVRG_CD)),1,1) PHRM_COB_CD,
	RX_CLM_NUM RX_CLM_NUM,
	SQNC_NUM_OF_CLM SQNC_NUM_OF_CLM,
	CLM_STUS CLM_STUS,
	trim(REC_INPUT_SRC_CD)||'-'|| 
	Trim( (RX_CLM_NUM)||'-'||
	Trim( (SQNC_NUM_OF_CLM)))||'-'||
	trim(CLM_STUS) PHRM_DRG_CLM_ALT_ID,
	case when trim(varMBRALTID) = '0' then  '-1' else  trim(varMBRALTID)  
	AS MBR_ALT_ID,
	PRSCRBR_DEA_NUM PROV_ALT_ID,
	'' as SITE_CD,
	trim(SSN)
	 SBSCR_NBR,
	Right('000':trim(nullToEmpty(MBR_SEQ_NBR)) ,3 ) DEPN_NBR,
	'' GRP_NBR,
	Left(trim(GRP_ID),7) CUST_SEG_NBR,
	PTNT_DOB MBR_BTH_DT,
	PTNT_FNAME MBR_FST_NM,
	PTNT_LNAME MBR_LST_NM,
	PRMRY_CARE_PROV_ID_CD MBR_PRI_PHYSN_NBR,
	CLNT_BNFT_CD BEN_PLN_NBR,
	substr(Trim(NullToEmpty(CLNT_PROD_CD)),1,5) PRDCT_CD,
	TRANS_CD TRAN_CD,
	REC_XTRCT_SRC_CD REC_XTRCT_SRC_CD,
	REC_INPUT_SRC_CD REC_INPUT_SRC_CD,
	CYCLE_DT CYCLE_DT,
	MMBR_ZIP MBR_ZIP_CD,
	DT_RX_WRTN PHRM_PRSCN_WRT_DT,
	POS PHRM_POS_CD,
	NUM_OF_REFLS_AUTHRZD PHRM_AUTH_RFL_CNT,
	GNDR MBR_GDR_CD,
	DGNS_CD_QLFYR DGNS_CD_QLFYR,
	SETNULL() AS CRR_MEMGROUPID	
	from join_cesmem ) ;	
	
	
	
	
	create or replace temporary table ds_COMMFS_CES_LOGCD_15_1 as
	(SELECT * FROM ds_COMMFS_CES_LOGCD_15
  	qualify row_number() over (partition by RX_CLM_NUM, SQNC_NUM_OF_CLM, CLM_STUS
                              order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC,
							  NDC_DRG_ROW_EFF_DT DESC) = 1 ) ;
	
	
	
	
	create or replace temporary table ext_STG_PSRX_CLM_COMM_FRESHSTART as
	(SELECT  mbralt.MBR_ALT_ID,
	rx.RX_CLM_NUM as RX_CLM_NUM, 		
	rx.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	rx.CLM_STUS as CLM_STUS ,
	rx.CYCLE_DT ,
	SUBSTR(mbralt.MBR_ALT_ID, CHARINDEX('-',mbralt.MBR_ALT_ID) + 1, 
	CHARINDEX('-',SUBSTR(mbralt.MBR_ALT_ID, CHARINDEX('-',mbralt.MBR_ALT_ID) + 1))-1) as CRR_MEMGROUPID
	FROM STG.STG_PSRX_CLM_COMM_FRESHSTART rx,
	DW.TB_MEM_MBR mbr,
	DW.TB_MEM_MBR_ALT mbralt
	WHERE rx.MMBR_ID_NUM = mbr.SBSCR_ID_CRD_SRL_NBR
	AND mbr.REC_INPUT_SRC_CD = 'CRR'
	AND mbralt.MBR_SYS_ID = mbr.MBR_SYS_ID
	AND mbralt.REC_INPUT_SRC_CD = 'CRR'
	AND rx.REC_INPUT_SRC_CD = 'BK2C' 
	AND CARRIER   in    (#CARRIER_ID_15#) 
  	qualify row_number() over (partition by RX_CLM_NUM, SQNC_NUM_OF_CLM, CLM_STUS
                              order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC) = 1 ) ;
	
	
	
	create or replace temporary table STG_PSRX_CLM_COMM_FRESHSTART_subscriber_depn as
	(select LEFT(rx.MMBR_ID_NUM,9)  as SBSCR_NBR,
	RIGHT(rx.MMBR_ID_NUM,2) as DEPN_NBR,
	rx.RX_CLM_NUM as RX_CLM_NUM, 		
	rx.SQNC_NUM_OF_CLM as SQNC_NUM_OF_CLM ,
	rx.CLM_STUS as CLM_STUS  
	from STG.STG_PSRX_CLM_COMM_FRESHSTART rx
	where CARRIER   in (#CARRIER_ID_15#)    AND rx.REC_INPUT_SRC_CD = 'BK2C' 
  	qualify row_number() over (partition by RX_CLM_NUM, SQNC_NUM_OF_CLM, CLM_STUS
                              order by RX_CLM_NUM ASC, SQNC_NUM_OF_CLM ASC, CLM_STUS ASC) = 1 ) ;
	
	
	
	
	
	create or replace temporary table join_cesmem_y as 
	(select 
	A.*, B.MBR_SEQ_NBR, B.MBR_ALT_ID, B.CYCLE_DATE, B.CRR_MEMGROUPID
	 FROM 
	ds_COMMFS_CES_LOGCD_15_1 A LEFT OUTER JOIN ext_STG_PSRX_CLM_COMM_FRESHSTART B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM
	AND A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM
	AND A.CLM_STUS = B.CLM_STUS ) ;
	
	
	
	create or replace temporary table Join_Depn_Subscr as 
	(select 
	A.*, B.MBR_SEQ_NBR, B.SBSCR_NBR, B.DEPN_NBR
	 FROM 
	join_cesmem_y A LEFT OUTER JOIN STG_PSRX_CLM_COMM_FRESHSTART_subscriber_depn B
	ON A.RX_CLM_NUM = B.RX_CLM_NUM
	AND A.SQNC_NUM_OF_CLM = B.SQNC_NUM_OF_CLM
	AND A.CLM_STUS = B.CLM_STUS ) ;
	
	
	
	create or replace temporary table Join_Depn_Subscr ds_TB_PHRM_DRG_CLM_COMMFS_CRR_15 AS 
	(SELECT 
	SUBSTR(trim(NullToEmpty(DGNS_CD)),1,5) PHRM_DIAG_CD,
	DRUG_UNIT_CST PHRM_AVG_WHLSL_AMT,
	CLNT_COPAY PHRM_COPAY_AMT,
	PTNT_PAY_AMT_ATTRD_TO_DDCTBL PHRM_DED_AMT,
	DSPNSNG_FEE_BILLED PHRM_DSPNS_FEE_AMT,
	INGRDNT_CST_BILLED PHRM_INGR_CST_AMT,
	CHK_DT PHRM_CHK_DT,
	FILL_DT PHRM_FILL_DT,
	DAYS_SUPLY PHRM_DAY_SPL_CNT,
	PROD_SLCTN_CD PHRM_DSPNS_AS_WRT_CD,
	SUBSTR(trim(NullToEmpty(PROD_SRVC_ID)),1,11) PHRM_NDC_CD,
	NDC_DRG_ROW_EFF_DT PHRM_NDC_ROW_EFF_DT,
	CASE WHEN Trim(NullToEmpty(NEW_REFL_CD)) = '00' then 'Y' else 'N' END AS PHRM_FST_FILL_IND_CD,
	FRMLRY_STUS PHRM_FRMLRY_IND_CD,
	CASE WHEN SUBSTR(trim(NullToEmpty(in_t1xfrm_mapping.PLAN_QLFYR_CD)),1,1)  =  '2' then '3'  
	WHEN SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '3' then '4'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '4' then '8'  
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '5' then '9' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '6' then 'E' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '7' then 'F' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '8' then 'G' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,1)  =  '9' then 'H' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '10' then 'A' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '11' then 'B' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '12' then 'C' 
	WHEN SUBSTR( trim(NullToEmpty(PLAN_QLFYR_CD)),1,2)  =  '13' then 'D' else
	SUBSTR(trim(NullToEmpty(PLAN_QLFYR_CD)),1,1) END AS 
	PHRM_FRMLRY_TYP_CD,
	Right('000000000000'||( NullToZero(RX_SRVC_REF_NUM)), 12)  PHRM_PRSC_NBR,
	AsInteger(NEW_REFL_CD) END AS  PHRM_PRSC_RFL_NBR,
	MTRC_DCML_QTY PHRM_QTY_CNT,
	CASE WHEN SRVC_PROV_ID_QLFYR = '07'  then SRVC_PROV_ID 
	WHEN SRVC_PROV_ID_QLFYR = '01'  then SRVC_PROV_NCPDP_ID_NUM
	else '' END AS PHRM_NBR,
	PRIOR_AUTHRZTN_ID PHRM_PRR_AUTH_NBR,
	case when trim(NullToEmpty(SRVC_PROV_ID_QLFYR ) )  =  '01' 
	then substr(trim(NullToEmpty(SRVC_PROV_ID) ),1,10) else ''
	end as PHRM_NPI_PROV_ID,
	case when trim( NullToEmpty(PRSCRBR_ID_QLFYR ))  =  '01' 
	then substr(trim(NullToEmpty(in_t1xfrm_mapping.PRSCRBR_ID )),1,10) else ''
	end as PHRM_PRSC_NPI_PROV_ID,
	substr(trim(NullToEmpty(PRSCRBR_DEA_NUM)),1,11) PHRM_PROV_DEA_NBR,
	ACCOUNT PHRM_ACCT_ID,
	CARRIER PHRM_CARR_ID,
	substr(trim( NullToEmpty(OTHR_CVRG_CD)),1,1) PHRM_COB_CD,
	RX_CLM_NUM RX_CLM_NUM,
	SQNC_NUM_OF_CLM SQNC_NUM_OF_CLM,
	CLM_STUS CLM_STUS,
	trim(REC_INPUT_SRC_CD)||'-'|| 
	Trim( (RX_CLM_NUM)||'-'||
	Trim( (SQNC_NUM_OF_CLM)))||'-'||
	trim(CLM_STUS) PHRM_DRG_CLM_ALT_ID,
	MBR_ALT_ID,
	PRSCRBR_DEA_NUM PROV_ALT_ID,
	'' as SITE_CD,
	SBSCR_NBR,
	DEPN_NBR,
	'' GRP_NBR,
	Left(trim(GRP_ID),7) CUST_SEG_NBR,
	PTNT_DOB MBR_BTH_DT,
	PTNT_FNAME MBR_FST_NM,
	PTNT_LNAME MBR_LST_NM,
	PRMRY_CARE_PROV_ID_CD MBR_PRI_PHYSN_NBR,
	CLNT_BNFT_CD BEN_PLN_NBR,
	substr(Trim(NullToEmpty(CLNT_PROD_CD)),1,5) PRDCT_CD,
	TRANS_CD TRAN_CD,
	REC_XTRCT_SRC_CD REC_XTRCT_SRC_CD,
	REC_INPUT_SRC_CD REC_INPUT_SRC_CD,
	CYCLE_DT CYCLE_DT,
	MMBR_ZIP MBR_ZIP_CD,
	DT_RX_WRTN PHRM_PRSCN_WRT_DT,
	POS PHRM_POS_CD,
	NUM_OF_REFLS_AUTHRZD PHRM_AUTH_RFL_CNT,
	GNDR MBR_GDR_CD,
	DGNS_CD_QLFYR DGNS_CD_QLFYR,
	CRR_MEMGROUPID
	FROM Join_Depn_Subscr ) ;
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
		