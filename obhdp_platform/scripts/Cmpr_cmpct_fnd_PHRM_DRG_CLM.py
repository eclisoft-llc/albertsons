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

tgt_tb_name = 'PHRM_DRG_CLM'
tgt_schema_name = 'FOUNDATION'


def execute_compare(query1, query2, yearn, monthn, dayn):
    try:

        s1 = pd.read_sql(query1, con=sf_conn)
        s2 = pd.read_sql(query2, con=sf_conn)

        s1 = s1.replace([None], [''], regex=True)
        s2 = s2.replace([None], [''], regex=True)

        s1 = s1.fillna('')
        s2 = s2.fillna('')

        df_diff2 = pd.concat([s2, s1], keys=['Snowflake-Foundation', 'Snowflake-Compact']).drop_duplicates(keep=False)

        if (df_diff2.size > 0):
            df_diff2.to_csv(diff_path + tgt_tb_name + '_' + yearn + '_' + monthn + '_' + dayn + '_Diffs.csv')

    except Exception as e:
        print(e, e.args)
        pass

# update these 4 values to target specific dates by year, month and d, where "d" = Day of the month to start validation
# and endday = the day of the month to validate (up-to-this-day), example: validate every day from 8-11-2021 through 8-15-2021
yearNumber = 2022
month = 1
day = 1
endday = 2
# you can increase the amount of records to validate per day with the next value, but I found 2000 is the sweet spot where
# performance is not greatly affected, yet the amount of records is high enough for accuracy
rowcomparison = '6000'
try:
    while day <= endday:
        try:
            print('generating report for ' + 'PHRM_DRG_CLM' + ' ' + str(yearNumber) + ' ' + str(month) + ' ' + str(
                day))
            q1 = "SELECT " + " CONCAT(CASE WHEN COF.CONVERTED_FROM = 'PSI-TXT' THEN 'ORX' ELSE COF.CONVERTED_FROM END,'-',CMP.COMPANY_CODE,'-',CLM.CLAIM_NUMBER,'-',CLM.LINE_NUMBER, CASE WHEN TRIM(CLM.SUB_LINE_CODE) ='R' THEN '-X' WHEN TRIM(CLM.SUB_LINE_CODE) = '' THEN '-P' ELSE CONCAT('-',TRIM(CLM.SUB_LINE_CODE)) END) PHRM_DRG_CLM_SYS_ID," \
                             "CRX.PLAN_COPAY_AMT PHRM_COPAY_AMT,CRX.PATIENT_DEDUCTIBLE_AMT PHRM_DED_AMT,CLM.RX_DISPENSING_FEE_AMT PHRM_DSPNS_FEE_AMT," \
                             "CLM.RX_INGREDIENT_AMT PHRM_INGR_CST_AMT,CLM.PAID_NET_AMT PHRM_PD_AMT,CLM.CHECK_DATE PHRM_CHK_DT,CLM.DETAIL_SVC_DATE PHRM_FILL_DT," \
                             "CLM.RX_SUPPLY_DAYS PHRM_DAY_SPL_CNT,FOI.RX_FORMULARY_IND PHRM_DSPNS_AS_WRT_CD,NDC.NDC_CODE PHRM_NDC_CD," \
                             "CASE WHEN CRX.REFILL_CODE_DIM_ID = 2 THEN 'Y' ELSE 'N' END PHRM_FST_FILL_IND_CD,FOI.RX_FORMULARY_IND PHRM_FRMLRY_IND_CD,QLFR.PLAN_QUALIFIER_CODE PHRM_FRMLRY_TYP_CD," \
                             "QLFR.PLAN_QUALIFIER_CODE_DESC PHRM_FRMLRY_TYP_DESC,CRX.MAIL_ORDER_RX_INDICATOR PHRM_MAIL_ORDR_IND_CD,FIL.REFILL_CODE PHRM_PRSC_RFL_NBR," \
                             "CLM.QUANTITY PHRM_QTY_CNT,CASE WHEN PST.PROVIDER_PAR_STAT = '-' THEN '' WHEN PST.PROVIDER_PAR_STAT = 'P' THEN 'Y' WHEN PST.PROVIDER_PAR_STAT = 'D' THEN 'N' ELSE PST.PROVIDER_PAR_STAT END PHRM_IN_NTWK_CD," \
                             "PRV.NABP_NBR PHRM_NBR,TRIM(CLM.PROVIDER_NPI) PHRM_NPI_PROV_ID," \
                             "TRIM(CLM.ATT_PROV_NPI) PHRM_PRSC_NPI_PROV_ID,ATP.DEA_NBR PHRM_PROV_DEA_NBR," \
                             "CRX.SYS7 PHRM_ACCT_ID,CRX.SYS9 PHRM_CARR_ID,COB.OTHER_COVERAGE_CODE PHRM_COB_CD,COB.OTHER_COVERAGE_CODE_DESC PHRM_COB_DESC," \
                             "CONCAT((CASE WHEN COF.CONVERTED_FROM = 'PSI-TXT' THEN 'ORX' ELSE COF.CONVERTED_FROM END),'-',CMP.COMPANY_CODE,'-',CLM.CLAIM_NUMBER,'-',CLM.LINE_NUMBER,CASE WHEN TRIM(CLM.SUB_LINE_CODE) ='R' THEN '-X' WHEN TRIM(CLM.SUB_LINE_CODE) = '' THEN '-P' ELSE CONCAT('-',TRIM(CLM.SUB_LINE_CODE)) END ) PHRM_DRG_CLM_ALT_ID," \
                             "NULL MBR_ALT_ID,CLM.SYS3 PROV_ALT_ID,COS.COSMOS_CUST_SEG CUST_SEG_NBR,CRX.TRANSACTION_CODE TRAN_CD," \
                             "CASE WHEN COF.CONVERTED_FROM = 'PSI-TXT' THEN 'ORX' ELSE COF.CONVERTED_FROM END SRC_REC_CD,CLM.DW_INSERT_DATETIME REC_CRT_DT," \
                             "CLM.DW_UPDATE_DATETIME REC_UPD_DT,MBR.DOB MBR_BTH_DT,MBR.MEMB_FIRST_NAME MBR_FST_NM,MBR.MEMB_LAST_NAME MBR_LST_NM," \
                             "LEFT(MBR.MEMB_ZIP,5) MBR_ZIP_CD,CRX.REFILLS_AUTHORIZED PHRM_AUTH_RFL_CNT,CLM.RX_DATE_PRESCRIPTION_WRITTEN PHRM_PRSCN_WRT_DT," \
                             "CRX.CARDHOLDER_ID SBSCR_NBR,MBR.MEDICAID_NO MEDCD_ID,MBR.MEDICARE_NO MEDCR_ID,MBR.GENDER MBR_GDR_CD," \
                             "CLM.SYS4 PHRM_DIAG_CD,CLM.RX_PRESCRIPTION_NUMBER PHRM_PRSC_NBR,CTP.CLAIM_TYPE_DESC SRC_DRG_CLM_TYP," \
                             "CASE WHEN CLM.CLAIM_TYPE_DIM_ID = 8  THEN 'TRUE' ELSE 'FALSE' END DERIV_OBH_FINC_LIAB," \
                             "CMP.COMPANY_CODE SRC_CUST_CD,CMP.COMPANY_DESC SRC_CUST_NM,MBR.DIAMOND_ID SRC_MBR_ID,NULL OBH_SCR_RL_LST," \
                             "LIB.LINE_OF_BUSINESS_DESC CLM_SRC_MBR_ACCT_NBR,CLM.BILLED_AMT CLM_TRANS_SBMT_AMT,AHFS.AHFS_THER_CLASS_DESC CLM_TRANS_THER_CLASS_DESC," \
                             "LOBTYP.LINE_OF_BUSINESS_TYPE_DESC CLM_TRANS_PRDCT_TYP_DESC" \
                             " FROM COMPACT.FACT_CLAIM CLM " \
                             " LEFT JOIN COMPACT.DIM_CONVERTED_FROM COF ON COF.CONVERTED_FROM_DIM_ID = CLM.CONVERTED_FROM_DIM_ID " \
                             " LEFT JOIN COMPACT.FACT_CLAIM_RX_ADDITION CRX ON CRX.COMPANY_DIM_ID = CLM.COMPANY_DIM_ID AND CRX.LINE_NUMBER = CLM.LINE_NUMBER AND CRX.SUB_LINE_CODE = CLM.SUB_LINE_CODE AND TO_VARCHAR(CRX.SEQ_CLAIM_ID) = CLM.SEQ_CLAIM_ID " \
                             " LEFT JOIN COMPACT.DIM_COMPANY CMP ON CMP.COMPANY_DIM_ID = CLM.COMPANY_DIM_ID " \
                             " LEFT JOIN COMPACT.DIM_RX_FORMULARY_IND FOI ON FOI.RX_FORMULARY_IND_DIM_ID = CLM.RX_FORMULARY_IND_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_RX_PLAN_QUALIFIER_CODE QLFR ON QLFR.PLAN_QUALIFIER_CODE_DIM_ID = CRX.PLAN_QUALIFIER_CODE_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_RX_OTHER_COVERAGE_CODE COB ON COB.OTHER_COVERAGE_CODE_DIM_ID = CRX.OTHER_COVERAGE_CODE_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_MEMBER MBR ON MBR.MEMB_DIM_ID = CLM.MEMB_DIM_ID AND MBR.COMPANY_DIM_ID = CLM.COMPANY_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_CLAIM_TYPE CTP ON CTP.CLAIM_TYPE_DIM_ID = CLM.CLAIM_TYPE_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_COSMOS_CUST_SEG COS ON COS.COSMOS_CUST_SEG_DIM_ID = CLM.COSMOS_CUST_SEG_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_RX_REFILL_CODE FIL ON FIL.REFILL_CODE_DIM_ID = CRX.REFILL_CODE_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_PROVIDER ATP ON ATP.PROV_DIM_ID = CLM.ATT_PROV_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_PROVIDER PRV ON PRV.PROV_DIM_ID = CLM.PROV_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_NDC_CODE NDC ON NDC.NDC_CODE_DIM_ID = CLM.NDC_CODE_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_AHFS_THER_CLASS_CODE AHFS ON AHFS.AHFS_THER_CLASS_DIM_ID = NDC.AHFS_THER_CLASS_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_PROVIDER_PAR_STAT PST ON PST.PROVIDER_PAR_DIM_ID = CLM.PROVIDER_PAR_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_LINE_OF_BUSINESS LIB ON LIB.LINE_OF_BUSINESS_DIM_ID = CLM.LINE_OF_BUSINESS_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_PLAN_LOB PLB ON PLB.PLAN_LOB_DIM_ID = CLM.PLAN_LOB_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_LINE_OF_BUSINESS LOB ON LOB.LINE_OF_BUSINESS_DIM_ID = PLB.LINE_OF_BUSINESS_DIM_ID" \
                             " LEFT JOIN COMPACT.DIM_LINE_OF_BUSINESS_TYPE LOBTYP ON LOBTYP.LINE_OF_BUSINESS_TYPE_DIM_ID = LOB.LINE_OF_BUSINESS_TYPE_DIM_ID" \
                             " WHERE EXTRACT(YEAR FROM CLM.DETAIL_SVC_DATE) = " \
                            + str(yearNumber) + " AND EXTRACT(MONTH FROM CLM.DETAIL_SVC_DATE) = " + str(month) \
                            + " AND EXTRACT(DAY FROM CLM.DETAIL_SVC_DATE) = " + str(day) \
                            + " AND CLM.CLAIM_TYPE_DIM_ID IN (3,8)  AND NOT ( COF.CONVERTED_FROM_DIM_ID = 102 AND CLM.CHECK_DATE = '9999-09-09') " \
                            + " ORDER BY  CONCAT(CASE WHEN COF.CONVERTED_FROM = 'PSI-TXT' THEN 'ORX' ELSE COF.CONVERTED_FROM END,'-',CMP.COMPANY_CODE,'-',CLM.CLAIM_NUMBER,'-',CLM.LINE_NUMBER, CASE WHEN TRIM(CLM.SUB_LINE_CODE) ='R' THEN '-X' WHEN TRIM(CLM.SUB_LINE_CODE) = '' THEN '-P' ELSE CONCAT('-',TRIM(CLM.SUB_LINE_CODE)) END)   FETCH FIRST " + rowcomparison + ' ROWS ONLY'

            q2 = 'SELECT PHRM_DRG_CLM_SYS_ID,' \
                 'PHRM_COPAY_AMT,PHRM_DED_AMT,PHRM_DSPNS_FEE_AMT,' \
                 'PHRM_INGR_CST_AMT,PHRM_PD_AMT,PHRM_CHK_DT,PHRM_FILL_DT,' \
                 'PHRM_DAY_SPL_CNT,PHRM_DSPNS_AS_WRT_CD,PHRM_NDC_CD,PHRM_FST_FILL_IND_CD,PHRM_FRMLRY_IND_CD,PHRM_FRMLRY_TYP_CD,' \
                 'PHRM_FRMLRY_TYP_DESC,PHRM_MAIL_ORDR_IND_CD,PHRM_PRSC_RFL_NBR,' \
                 'PHRM_QTY_CNT,PHRM_IN_NTWK_CD,PHRM_NBR,PHRM_NPI_PROV_ID,PHRM_PRSC_NPI_PROV_ID,PHRM_PROV_DEA_NBR,' \
                 'PHRM_ACCT_ID,PHRM_CARR_ID,PHRM_COB_CD,PHRM_COB_DESC,PHRM_DRG_CLM_ALT_ID,' \
                 'MBR_ALT_ID,PROV_ALT_ID,CUST_SEG_NBR,TRAN_CD,SRC_REC_CD,REC_CRT_DT,REC_UPD_DT,MBR_BTH_DT,MBR_FST_NM,MBR_LST_NM,' \
                 'MBR_ZIP_CD,PHRM_AUTH_RFL_CNT,PHRM_PRSCN_WRT_DT,SBSCR_NBR,MEDCD_ID,MEDCR_ID,MBR_GDR_CD,PHRM_DIAG_CD,PHRM_PRSC_NBR,SRC_DRG_CLM_TYP,' \
                 'DERIV_OBH_FINC_LIAB,SRC_CUST_CD,SRC_CUST_NM,SRC_MBR_ID,OBH_SCR_RL_LST,CLM_SRC_MBR_ACCT_NBR,CLM_TRANS_SBMT_AMT,CLM_TRANS_THER_CLASS_DESC,CLM_TRANS_PRDCT_TYP_DESC ' \
                 '  FROM ' + tgt_schema_name + '.' + \
                 tgt_tb_name + ' WHERE EXTRACT(YEAR FROM PHRM_FILL_DT) = ' \
                 + str(yearNumber) + ' AND EXTRACT(MONTH FROM PHRM_FILL_DT) = ' + str(month) \
                 + ' AND EXTRACT(DAY FROM PHRM_FILL_DT) = ' + str(day) \
                 + ' ORDER BY PHRM_DRG_CLM_SYS_ID FETCH FIRST ' + rowcomparison + ' ROWS ONLY'

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

    