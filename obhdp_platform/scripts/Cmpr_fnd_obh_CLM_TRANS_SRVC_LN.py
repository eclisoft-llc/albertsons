import pandas as pd
import argparse
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from datetime import date

### Main ###

parser = argparse.ArgumentParser(description='Validation Script')
parser.add_argument("--sfact", action="store", dest="sf_act_name", required=True, help="Snowflake Account Name")
parser.add_argument("--sfuser", action="store", dest="sf_username", required=True, help="Snowflake User Name")
parser.add_argument("--pkeypath", action="store", dest="keypath", required=True, help="P8 key path")
parser.add_argument("--sfrole", action="store", dest="sf_role", required=True, help="Snowflake role")
parser.add_argument("--sfwh", action="store", dest="sf_wh", required=True, help="Snowflake warehouse")
parser.add_argument("--sfdb", action="store", dest="sf_db", required=True, help="Snowflake database")
parser.add_argument("--psphrs", action="store", dest="passphrase", required=True, help="Passphrase")
parser.add_argument("--diffpath", action="store", dest="diffpath", required=True, help="Path for difference files")
args = parser.parse_args()

with open (args.keypath, "rb") as key:
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=args.passphrase.encode(),
        backend=default_backend()
    )
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

sf_conn = snowflake.connector.connect(
    account=args.sf_act_name,
    user=args.sf_username,
    private_key=pkb,
    role=args.sf_role,
    warehouse=args.sf_wh,
    database=args.sf_db
)


diff_path = args.diffpath
tgt_tb_name = 'CLM_TRANS_SRVC_LN'
tgt_schema_name = 'OBH_DP'


def execute_compare(query1, query2, yearn, monthn, dayn):
    try:

        s1 = pd.read_sql(query1, con=sf_conn)
        s2 = pd.read_sql(query2, con=sf_conn)

        s1 = s1.replace([None], [''], regex=True)
        s2 = s2.replace([None], [''], regex=True)

        s1 = s1.fillna('')
        s2 = s2.fillna('')

        df_diff2 = pd.concat([s2, s1], keys=['Snowflake-obhdp', 'Snowflake-Foundation']).drop_duplicates(keep=False)

        if (df_diff2.size > 0):
            df_diff2.to_csv(diff_path + tgt_tb_name + '_' + yearn + '_' + monthn + '_' + dayn + '_Foundation_obhdp_Diffs.csv')


    except Exception as e:
        print(e, e.args)
        pass


# update these 4 values to target specific dates by year, month and d, where "d" = Day of the month to start validation
# and endday = the day of the month to validate (up-to-this-day), example: validate every day from 8-11-2021 through 8-15-2021
yearNumber = 2022
month = 1
day = 1
endday = 1
# you can increase the amount of records to validate per day with the next value, but I found 2000 is the sweet spot where
# performance is not greatly affected, yet the amount of records is high enough for accuracy
rowcomparison = '4000'
try:

    while day <= endday:
        try:
            print('generating report for ' + 'CLM_TRANS_SRVC_LN' + ' ' + str(yearNumber) + ' ' + str(month) + ' ' + str(day))
            q1 = "SELECT CLM_TRANS_SYS_ID ,CLM_AUD_SRC_ID,CLM_AUD_SRC_SYS_CD,CLM_AUD_NBR,CLM_AUD_LN_NBR,CLM_AUD_TRANS_SEQ_NBR,CLM_SRC_AUTH_NBR," \
                 "CLM_SRC_MBR_ACCT_NBR,CLM_SRC_MBR_FAM_ID,CLM_SRC_MBR_GDR_CD,CLM_SRC_MBR_GRP_ID,CLM_SRC_MBR_MKT_DERIV_NBR,CLM_SRC_MBR_NON_UHG_EE_FLG," \
                 "CLM_SRC_MBR_PLN_ID,CLM_SRC_MBR_PRDCT_CD,CLM_SRC_MBR_REL_CD," \
                 "CLM_SRC_MBR_SBSCR_ID,CLM_SRC_MBR_SYS_ID,CLM_SRC_PROV_SRVC_CTY_NM,CLM_SRC_PROV_SRVC_ST_CD,CLM_SRC_PROV_SYS_ID," \
                 "CLM_TRANS_ADJ_CD,CLM_TRANS_CLM_FM_TYP_CD,CLM_TRANS_DIAG_1_CD,CLM_TRANS_DIAG_1_CD_CATGY_LVL_1_DESC,CLM_TRANS_DIAG_1_CD_TYP_CD," \
                 "CLM_TRANS_DIAG_2_CD,CLM_TRANS_DIAG_2_CD_CATGY_LVL_1_DESC," \
                 "CLM_TRANS_DIAG_2_CD_TYP_CD,CLM_TRANS_DIAG_3_CD,CLM_TRANS_DIAG_3_CD_CATGY_LVL_1_DESC,CLM_TRANS_DIAG_3_CD_TYP_CD,CLM_TRANS_DIAG_4_CD,CLM_TRANS_DIAG_4_CD_CATGY_LVL_1_DESC,CLM_TRANS_DIAG_4_CD_TYP_CD," \
                 "CLM_TRANS_DIAG_5_CD,CLM_TRANS_DIAG_5_CD_CATGY_LVL_1_DESC,CLM_TRANS_DIAG_5_CD_TYP_CD,CLM_TRANS_DIAG_MH_SA_CD,CLM_TRANS_DIAG_SBSTNC_ABUS_FLG,CLM_TRANS_DLOC_TXT,CLM_TRANS_DLOC_SEQ_NBR,CLM_TRANS_DLOC_CATGY_TXT ,CLM_TRANS_DT_ADJD_DT," \
                 "CLM_TRANS_DT_ENT_DT,CLM_TRANS_DT_PD_DT,CLM_TRANS_DT_RCVD_DT,CLM_TRANS_DT_PST_DT,CLM_TRANS_DT_SRVC_FROM_DT,CLM_TRANS_DT_SRVC_THRU_DT," \
                 "CLM_TRANS_LVL_OF_SRVC_DERIV_CD,CLM_TRANS_LVL_OF_SRVC_SRC_CD,CLM_TRANS_MBR_IN_YR_AGE,CLM_TRANS_PL_OF_SRVC_AMA_CD," \
                 "CLM_TRANS_PROC_MOD_CD,CLM_TRANS_PROC_TYP_CD,CLM_SRC_PROV_TAX_ID_NBR,CLM_SRC_PROV_SRVC_ZIP_CD,CLM_SRC_PROV_CLM_NPI_ID," \
                 "CLM_TRANS_RSN_TYP_CD,CLM_TRANS_CLM_PRI_DIAG_CD,CLM_TRANS_DT_ADMIT_DT,CLM_TRANS_ADMIT_TYP_CD,CLM_TRANS_ADMIT_SRC_CD," \
                 "CLM_TRANS_DT_CLM_FST_SRVC_DT,CLM_TRANS_DT_CLM_LST_SRVC_DT,CLM_TRANS_BILL_TYP_CD," \
                 "CLM_TRANS_DSCHRG_STS_CD,CLM_TRANS_SYS_DIAG_REL_GRP_CD,CLM_TRANS_ENT_DIAG_REL_GRP_CD," \
                 "CLM_TRANS_CLM_LVL_RSN_CD_DESC, CLM_TRANS_EOB_RSN_CD_DESC,CLM_TRANS_AUTH_NBR," \
                 "CLM_TRANS_SUPP_ORDR_IND,CLM_SRC_PROV_PAYE_NM,CLM_TRANS_DIAG_1_CD_DESC," \
                 "CLM_TRANS_DIAG_2_CD_DESC,CLM_TRANS_DIAG_3_CD_DESC,CLM_TRANS_DIAG_4_CD_DESC," \
                 "CLM_TRANS_DIAG_5_CD_DESC,CLM_TRANS_PROC_CD,CLM_TRANS_PROC_DESC," \
                 "ICD_DIAG_TYP_CD,CLM_TRANS_RSN_DESC,CLM_TRANS_DIAG_1_DSM_TYP_CD," \
                 "CLM_TRANS_DIAG_2_DSM_TYP_CD,CLM_TRANS_DIAG_3_DSM_TYP_CD,CLM_TRANS_DIAG_4_DSM_TYP_CD," \
                 "CLM_TRANS_DIAG_5_DSM_TYP_CD,CLM_TRANS_CLM_LVL_RSN_CD,CLM_TRANS_PROC_MOD_2_CD," \
                 "CLM_TRANS_PROC_MOD_3_CD,CLM_TRANS_PROC_MOD_4_CD,CLM_TRANS_RVNU_CD," \
                 "CLM_TRANS_RVNU_DESC,CLM_TRANS_SEC_PROC_CD,CLM_TRANS_SEC_PROC_DESC," \
                 "CLM_TRANS_SEC_PROC_TYP_CD,SRC_FINC_LIAB_CD,DERIV_OBH_FINC_LIAB," \
                 "CLM_SRC_PROV_CLM_SRVC_NPI_ID,CLM_SRC_PROV_CLM_REF_NPI_ID,CLM_SRC_PROV_CLM_FAC_NPI_ID," \
                 "CLM_SRC_PROV_CLM_RNDR_NPI_ID,CLM_SRC_PROV_CLM_BIL_NPI_ID,CLM_SRC_PROV_CLM_PAYE_NPI_ID," \
                 "CLM_SRC_PROV_CLM_ADMIT_NPI_ID,CLM_SRC_PROV_CLM_ATD_NPI_ID,CLM_TRANS_RSN_CD," \
                 "CLM_TRANS_AGRMNT_ID,CLM_TRANS_AGRMNT_DESC,CLM_TRANS_RSN_TYP_DESC," \
                 "SRC_FINC_LIAB_DESC,CLM_TRANS_DIAG_1_CD_CATGY_LVL_2_DESC,CLM_TRANS_DIAG_2_CD_CATGY_LVL_2_DESC," \
                 "CLM_TRANS_DIAG_3_CD_CATGY_LVL_2_DESC,CLM_TRANS_DIAG_4_CD_CATGY_LVL_2_DESC,CLM_TRANS_DIAG_5_CD_CATGY_LVL_2_DESC," \
                 "MBR_SYS_ID,TO_VARCHAR(CASE WHEN PRS.PERS_SYS_ID IS NULL THEN '-1' WHEN PRS.PERS_COUNT = 1 THEN PRS.PERS_SYS_ID ELSE '-2' END) PERS_SYS_ID,PKG_SYS_ID," \
                 "CLM_TRANS_ALLW_AMT,CLM_TRANS_ALLW_DERIV_AMT,CLM_TRANS_COB_AMT,CLM_TRANS_COINS_AMT," \
                 "CLM_TRANS_CONTR_AMT,CLM_TRANS_COPAY_AMT,CLM_TRANS_DED_AMT,CLM_TRANS_DSALLW_AMT," \
                 "CLM_TRANS_DSCNT_AMT,CLM_TRANS_MBR_RESP_AMT,CLM_TRANS_PD_AMT,CLM_TRANS_RSRV_AMT," \
                 "CLM_TRANS_SBMT_AMT,CLM_TRANS_ALLWD_QTY,CLM_TRANS_BILL_QTY,CLM_TRANS_COV_VST_CNT," \
                 "CLM_TRANS_COV_DAYS_CNT,CLM_TRANS_SRVC_UNT_CNT,CLM_TRANS_PROC_CNT,CLM_TRANS_ALLWD_ADMTS," \
                 "SRC_CUST_CD,SRC_CUST_NM,CLM_TRANS_THER_CLASS_CD,CLM_TRANS_THER_CLASS_DESC,CLM_TRANS_PKG_CD," \
                 "CLM_TRANS_PKG_NM,CLM_TRANS_MKT_PKG_CD,CLM_TRANS_MKT_PKG_DESC,CLM_TRANS_PLAN_CD," \
                 "CLM_TRANS_PLAN_DESC,CLM_TRANS_PRDCT_TYP_DESC,CLM_SRC_MBR_GRP_NAME,CLM_TRANS_PL_OF_SRVC_AMA_DESC," \
                 "CLM_SRC_PROV_PAYE_TYP,CLM_SRC_PROV_PAYE_TAX_ID,CLM_TRANS_PROC_MOD_DESC,CLM_TRANS_PROC_MOD_2_DESC," \
                 "CLM_TRANS_PROC_MOD_3_DESC,CLM_TRANS_PROC_MOD_4_DESC,CLM_TRANS_BILL_TYP_DESC," \
                 "CLM_TRANS_SYS_DIAG_REL_GRP_DESC,CLM_TRANS_NDC_CD,CLM_TRANS_NDC_GNRC_NM,CLM_TRANS_COS_CD," \
                 "CLM_TRANS_COS_LVL1_DESC,CLM_TRANS_COS_LVL2_DESC,CLM_TRANS_COS_LVL3_DESC,CLM_TRANS_COS_LVL4_DESC," \
                 "SRC_PROV_NTWK_CD,SRC_PROV_NTWK_DESC,CLM_SRVC_LN_STS_CD,CLM_SRVC_LN_STS_DESC,CLM_TRANS_PROC_CTGY_LVL_1_DESC," \
                 "CLM_TRANS_PROC_CTGY_LVL_2_DESC,CLM_TRANS_PROC_CTGY_LVL_3_DESC,SRC_MBR_ID,MEDCD_ID,MEDCR_ID" \
                 "  FROM FOUNDATION.CLM_TRANS_SRVC_LN CLM " \
                " LEFT OUTER JOIN FOUNDATION.IMDM_EDW_INDV_KEY_DEDUP INDV ON CLM.CLM_SRC_MBR_SYS_ID=INDV.INDV_KEY_VAL" \
                " LEFT OUTER JOIN FOUNDATION.IMDM_EDW_INDV_PERS_DEDUP PRS ON INDV.IMDM_ID=PRS.INDV_SRC_ID" \
                " WHERE EXTRACT(YEAR FROM CLM_TRANS_DT_SRVC_FROM_DT) = " \
                 + str(yearNumber) + " AND EXTRACT(MONTH FROM CLM_TRANS_DT_SRVC_FROM_DT) = " + str(month) \
                 + " AND EXTRACT(DAY FROM CLM_TRANS_DT_SRVC_FROM_DT) = " + str(day) \
                 + " ORDER BY CLM_TRANS_SYS_ID FETCH FIRST " + rowcomparison + " ROWS ONLY"

            q2 = "SELECT CLM_TRANS_SYS_ID ,CLM_AUD_SRC_ID,CLM_AUD_SRC_SYS_CD,CLM_AUD_NBR,CLM_AUD_LN_NBR,CLM_AUD_TRANS_SEQ_NBR,CLM_SRC_AUTH_NBR," \
                 "CLM_SRC_MBR_ACCT_NBR,CLM_SRC_MBR_FAM_ID,CLM_SRC_MBR_GDR_CD,CLM_SRC_MBR_GRP_ID,CLM_SRC_MBR_MKT_DERIV_NBR,CLM_SRC_MBR_NON_UHG_EE_FLG," \
                 "CLM_SRC_MBR_PLN_ID,CLM_SRC_MBR_PRDCT_CD,CLM_SRC_MBR_REL_CD," \
                 "CLM_SRC_MBR_SBSCR_ID,CLM_SRC_MBR_SYS_ID,CLM_SRC_PROV_SRVC_CTY_NM,CLM_SRC_PROV_SRVC_ST_CD,CLM_SRC_PROV_SYS_ID," \
                 "CLM_TRANS_ADJ_CD,CLM_TRANS_CLM_FM_TYP_CD,CLM_TRANS_DIAG_1_CD,CLM_TRANS_DIAG_1_CD_CATGY_LVL_1_DESC,CLM_TRANS_DIAG_1_CD_TYP_CD," \
                 "CLM_TRANS_DIAG_2_CD,CLM_TRANS_DIAG_2_CD_CATGY_LVL_1_DESC," \
                 "CLM_TRANS_DIAG_2_CD_TYP_CD,CLM_TRANS_DIAG_3_CD,CLM_TRANS_DIAG_3_CD_CATGY_LVL_1_DESC,CLM_TRANS_DIAG_3_CD_TYP_CD,CLM_TRANS_DIAG_4_CD,CLM_TRANS_DIAG_4_CD_CATGY_LVL_1_DESC,CLM_TRANS_DIAG_4_CD_TYP_CD," \
                 "CLM_TRANS_DIAG_5_CD,CLM_TRANS_DIAG_5_CD_CATGY_LVL_1_DESC,CLM_TRANS_DIAG_5_CD_TYP_CD,CLM_TRANS_DIAG_MH_SA_CD,CLM_TRANS_DIAG_SBSTNC_ABUS_FLG,CLM_TRANS_DLOC_TXT,CLM_TRANS_DLOC_SEQ_NBR,CLM_TRANS_DLOC_CATGY_TXT ,CLM_TRANS_DT_ADJD_DT," \
                 "CLM_TRANS_DT_ENT_DT,CLM_TRANS_DT_PD_DT,CLM_TRANS_DT_RCVD_DT,CLM_TRANS_DT_PST_DT,CLM_TRANS_DT_SRVC_FROM_DT,CLM_TRANS_DT_SRVC_THRU_DT," \
                 "CLM_TRANS_LVL_OF_SRVC_DERIV_CD,CLM_TRANS_LVL_OF_SRVC_SRC_CD,CLM_TRANS_MBR_IN_YR_AGE,CLM_TRANS_PL_OF_SRVC_AMA_CD," \
                 "CLM_TRANS_PROC_MOD_CD,CLM_TRANS_PROC_TYP_CD,CLM_SRC_PROV_TAX_ID_NBR,CLM_SRC_PROV_SRVC_ZIP_CD,CLM_SRC_PROV_CLM_NPI_ID," \
                 "CLM_TRANS_RSN_TYP_CD,CLM_TRANS_CLM_PRI_DIAG_CD,CLM_TRANS_DT_ADMIT_DT,CLM_TRANS_ADMIT_TYP_CD,CLM_TRANS_ADMIT_SRC_CD," \
                 "CLM_TRANS_DT_CLM_FST_SRVC_DT,CLM_TRANS_DT_CLM_LST_SRVC_DT,CLM_TRANS_BILL_TYP_CD," \
                 "CLM_TRANS_DSCHRG_STS_CD,CLM_TRANS_SYS_DIAG_REL_GRP_CD,CLM_TRANS_ENT_DIAG_REL_GRP_CD," \
                 "CLM_TRANS_CLM_LVL_RSN_CD_DESC, CLM_TRANS_EOB_RSN_CD_DESC,CLM_TRANS_AUTH_NBR," \
                 "CLM_TRANS_SUPP_ORDR_IND,CLM_SRC_PROV_PAYE_NM,CLM_TRANS_DIAG_1_CD_DESC," \
                 "CLM_TRANS_DIAG_2_CD_DESC,CLM_TRANS_DIAG_3_CD_DESC,CLM_TRANS_DIAG_4_CD_DESC," \
                 "CLM_TRANS_DIAG_5_CD_DESC,CLM_TRANS_PROC_CD,CLM_TRANS_PROC_DESC," \
                 "ICD_DIAG_TYP_CD,CLM_TRANS_RSN_DESC,CLM_TRANS_DIAG_1_DSM_TYP_CD," \
                 "CLM_TRANS_DIAG_2_DSM_TYP_CD,CLM_TRANS_DIAG_3_DSM_TYP_CD,CLM_TRANS_DIAG_4_DSM_TYP_CD," \
                 "CLM_TRANS_DIAG_5_DSM_TYP_CD,CLM_TRANS_CLM_LVL_RSN_CD,CLM_TRANS_PROC_MOD_2_CD," \
                 "CLM_TRANS_PROC_MOD_3_CD,CLM_TRANS_PROC_MOD_4_CD,CLM_TRANS_RVNU_CD," \
                 "CLM_TRANS_RVNU_DESC,CLM_TRANS_SEC_PROC_CD,CLM_TRANS_SEC_PROC_DESC," \
                 "CLM_TRANS_SEC_PROC_TYP_CD,SRC_FINC_LIAB_CD,DERIV_OBH_FINC_LIAB," \
                 "CLM_SRC_PROV_CLM_SRVC_NPI_ID,CLM_SRC_PROV_CLM_REF_NPI_ID,CLM_SRC_PROV_CLM_FAC_NPI_ID," \
                 "CLM_SRC_PROV_CLM_RNDR_NPI_ID,CLM_SRC_PROV_CLM_BIL_NPI_ID,CLM_SRC_PROV_CLM_PAYE_NPI_ID," \
                 "CLM_SRC_PROV_CLM_ADMIT_NPI_ID,CLM_SRC_PROV_CLM_ATD_NPI_ID,CLM_TRANS_RSN_CD," \
                 "CLM_TRANS_AGRMNT_ID,CLM_TRANS_AGRMNT_DESC,CLM_TRANS_RSN_TYP_DESC," \
                 "SRC_FINC_LIAB_DESC,CLM_TRANS_DIAG_1_CD_CATGY_LVL_2_DESC,CLM_TRANS_DIAG_2_CD_CATGY_LVL_2_DESC," \
                 "CLM_TRANS_DIAG_3_CD_CATGY_LVL_2_DESC,CLM_TRANS_DIAG_4_CD_CATGY_LVL_2_DESC,CLM_TRANS_DIAG_5_CD_CATGY_LVL_2_DESC," \
                 "MBR_SYS_ID,PERS_SYS_ID,PKG_SYS_ID," \
                 "CLM_TRANS_ALLW_AMT,CLM_TRANS_ALLW_DERIV_AMT,CLM_TRANS_COB_AMT,CLM_TRANS_COINS_AMT," \
                 "CLM_TRANS_CONTR_AMT,CLM_TRANS_COPAY_AMT,CLM_TRANS_DED_AMT,CLM_TRANS_DSALLW_AMT," \
                 "CLM_TRANS_DSCNT_AMT,CLM_TRANS_MBR_RESP_AMT,CLM_TRANS_PD_AMT,CLM_TRANS_RSRV_AMT," \
                 "CLM_TRANS_SBMT_AMT,CLM_TRANS_ALLWD_QTY,CLM_TRANS_BILL_QTY,CLM_TRANS_COV_VST_CNT," \
                 "CLM_TRANS_COV_DAYS_CNT,CLM_TRANS_SRVC_UNT_CNT,CLM_TRANS_PROC_CNT,CLM_TRANS_ALLWD_ADMTS," \
                 "SRC_CUST_CD,SRC_CUST_NM,CLM_TRANS_THER_CLASS_CD,CLM_TRANS_THER_CLASS_DESC,CLM_TRANS_PKG_CD," \
                 "CLM_TRANS_PKG_NM,CLM_TRANS_MKT_PKG_CD,CLM_TRANS_MKT_PKG_DESC,CLM_TRANS_PLAN_CD," \
                 "CLM_TRANS_PLAN_DESC,CLM_TRANS_PRDCT_TYP_DESC,CLM_SRC_MBR_GRP_NAME,CLM_TRANS_PL_OF_SRVC_AMA_DESC," \
                 "CLM_SRC_PROV_PAYE_TYP,CLM_SRC_PROV_PAYE_TAX_ID,CLM_TRANS_PROC_MOD_DESC,CLM_TRANS_PROC_MOD_2_DESC," \
                 "CLM_TRANS_PROC_MOD_3_DESC,CLM_TRANS_PROC_MOD_4_DESC,CLM_TRANS_BILL_TYP_DESC," \
                 "CLM_TRANS_SYS_DIAG_REL_GRP_DESC,CLM_TRANS_NDC_CD,CLM_TRANS_NDC_GNRC_NM,CLM_TRANS_COS_CD," \
                 "CLM_TRANS_COS_LVL1_DESC,CLM_TRANS_COS_LVL2_DESC,CLM_TRANS_COS_LVL3_DESC,CLM_TRANS_COS_LVL4_DESC," \
                 "SRC_PROV_NTWK_CD,SRC_PROV_NTWK_DESC,CLM_SRVC_LN_STS_CD,CLM_SRVC_LN_STS_DESC,CLM_TRANS_PROC_CTGY_LVL_1_DESC," \
                 "CLM_TRANS_PROC_CTGY_LVL_2_DESC,CLM_TRANS_PROC_CTGY_LVL_3_DESC,SRC_MBR_ID,MEDCD_ID,MEDCR_ID" \
                 "  FROM " + tgt_schema_name + '.' + \
                 tgt_tb_name + " WHERE EXTRACT(YEAR FROM CLM_TRANS_DT_SRVC_FROM_DT) = " \
                 + str(yearNumber) + " AND EXTRACT(MONTH FROM CLM_TRANS_DT_SRVC_FROM_DT) = " + str(month) \
                 + " AND EXTRACT(DAY FROM CLM_TRANS_DT_SRVC_FROM_DT) = " + str(day) \
                 + " ORDER BY CLM_TRANS_SYS_ID FETCH FIRST " + rowcomparison + " ROWS ONLY"
            execute_compare(q1, q2, str(yearNumber), str(month), str(day))
        except Exception as e:
            print(e, e.args)
            pass
        day += 1
except Exception as e:
    print(e, e.args)
    pass
finally:
    sf_conn.close()

    