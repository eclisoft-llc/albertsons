# Author: Mary Jasline Vinodh
# Date: 04/20/2022
# Description: Compare a data set from compact tables in Snowflake against Foundation table PHRM_DRG_CLM in Snowflake, document the differences

import cx_Oracle
import pandas as pd
import snowflake.connector

sf_conn = snowflake.connector.connect(
    user='your-username',
    role='your-role',
    warehouse = 'your-warehouse',
    database = 'your-database',
    schema = 'OBH_DP',
    account='your-account',
    authenticator='externalbrowser'
)

sf_conn1 = snowflake.connector.connect(
    user='your-username',
    role='your-role',
    warehouse = 'your-warehouse',
    database = 'your-database',
    schema = 'FOUNDATION',
    account='your-account',
    authenticator='externalbrowser'
)

diff_path = 'C:/Users/mxavier6/Documents/PythonScripts/'
tgt_tb_name = 'PHRM_DRG_CLM'
tgt_schema_name = 'OBH_DP'


def execute_compare(query1, query2, yearn, monthn, dayn):
    try:

        s1 = pd.read_sql(query1, con=sf_conn1)
        s2 = pd.read_sql(query2, con=sf_conn)

        s1 = s1.replace([None], [''], regex=True)
        s2 = s2.replace([None], [''], regex=True)

        s1 = s1.fillna('')
        s2 = s2.fillna('')

        df_diff2 = pd.concat([s2, s1], keys=['OBHDP_Snowflake', 'FOUNDATION_Snowflake']).drop_duplicates(keep=False)

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
endday = 2
# you can increase the amount of records to validate per day with the next value, but I found 2000 is the sweet spot where
# performance is not greatly affected, yet the amount of records is high enough for accuracy
rowcomparison = '2000'
try:
    while day <= endday:
        try:
            print('generating report for ' + 'PHRM_DRG_CLM' + ' ' + str(yearNumber) + ' ' + str(month) + ' ' + str(
                day))
            q1 = "SELECT PHRM_DRG_CLM_SYS_ID," \
                 "PHRM_COPAY_AMT,PHRM_DED_AMT,PHRM_DSPNS_FEE_AMT," \
                 "PHRM_INGR_CST_AMT,PHRM_PD_AMT,PHRM_CHK_DT,PHRM_FILL_DT," \
                 "PHRM_DAY_SPL_CNT,PHRM_DSPNS_AS_WRT_CD,PHRM_NDC_CD,PHRM_FST_FILL_IND_CD,PHRM_FRMLRY_IND_CD,PHRM_FRMLRY_TYP_CD," \
                 "PHRM_FRMLRY_TYP_DESC,PHRM_MAIL_ORDR_IND_CD,PHRM_PRSC_RFL_NBR," \
                 "PHRM_QTY_CNT,PHRM_IN_NTWK_CD,PHRM_NBR,PHRM_NPI_PROV_ID,PHRM_PRSC_NPI_PROV_ID,PHRM_PROV_DEA_NBR," \
                 "PHRM_ACCT_ID,PHRM_CARR_ID,PHRM_COB_CD,PHRM_COB_DESC,PHRM_DRG_CLM_ALT_ID," \
                 "MBR_ALT_ID,PROV_ALT_ID,CUST_SEG_NBR,TRAN_CD,SRC_REC_CD,REC_CRT_DT,REC_UPD_DT,MBR_BTH_DT,MBR_FST_NM,MBR_LST_NM," \
                 "MBR_ZIP_CD,PHRM_AUTH_RFL_CNT,PHRM_PRSCN_WRT_DT,SBSCR_NBR,MEDCD_ID,MEDCR_ID,MBR_GDR_CD,PHRM_DIAG_CD,PHRM_PRSC_NBR,SRC_DRG_CLM_TYP," \
                 "DERIV_OBH_FINC_LIAB,SRC_CUST_CD,SRC_CUST_NM,SRC_MBR_ID,OBH_SCR_RL_LST,CLM_SRC_MBR_ACCT_NBR,CLM_TRANS_SBMT_AMT,CLM_TRANS_THER_CLASS_DESC,CLM_TRANS_PRDCT_TYP_DESC," \
                 "CASE WHEN INDV_EDW.PERS_COUNT=1 THEN INDV_EDW.EDWPRSID WHEN INDV_EDW.PERS_COUNT IS NULL THEN '-1' ELSE '-2' END PERS_SYS_ID," \
                 "CASE WHEN PRSC.EPIM_ID_CNT=1 THEN PRSC.EPIM_ID WHEN PRSC.EPIM_ID_CNT IS NULL THEN '-1' ELSE '-2' END PHRM_PRSC_EPIM_PROV_ID," \
                 "CASE WHEN PROV.EPIM_ID_CNT=1 THEN PROV.EPIM_ID WHEN PROV.EPIM_ID_CNT IS NULL THEN '-1' ELSE '-2' END  PHRM_EPIM_PROV_ID," \
                 "CASE WHEN PHRM.EPIM_ID_CNT=1 THEN PHRM.EPIM_ID WHEN PHRM.EPIM_ID_CNT IS NULL THEN '-1' ELSE '-2' END PHRM_DEA_EPIM_PROV_ID," \
                 "CASE WHEN INDV_IMDM.IMDM_COUNT=1 THEN INDV_IMDM.IMDM_ID   WHEN INDV_IMDM.IMDM_COUNT IS NULL THEN '-1'  ELSE '-2' END IMDM_ID"\
                 "  FROM FOUNDATION.PHRM_DRG_CLM CLM" \
                 "  LEFT JOIN FOUNDATION.IMDM_INDV_EDWPERSID_DEDUP INDV_EDW ON  CLM.SRC_MBR_ID=INDV_EDW.SMRTPRSID " \
                 "  LEFT JOIN FOUNDATION.IMDM_INDV_IMDMID_DEDUP INDV_IMDM ON  CLM.SRC_MBR_ID=INDV_IMDM.SMRTPRSID " \
                 "  LEFT JOIN FOUNDATION.EPIM_PROV_DEDUP PROV ON TRIM(PROV.IDENTIFIER)= TRIM(CLM.PHRM_NPI_PROV_ID) " \
                 "  AND TO_CHAR(CLM.PHRM_FILL_DT,'YYYY-MM-DD') BETWEEN PROV.START_DATE AND PROV.END_DATE " \
                 "  LEFT JOIN FOUNDATION.EPIM_PROV_DEDUP PRSC ON TRIM(PRSC.IDENTIFIER)= TRIM(CLM.PHRM_PRSC_NPI_PROV_ID)" \
                 " AND TO_CHAR(CLM.PHRM_PRSCN_WRT_DT,'YYYY-MM-DD') BETWEEN PRSC.START_DATE AND PRSC.END_DATE " \
                 " LEFT JOIN FOUNDATION.EPIM_PROV_DEA_DEDUP PHRM ON TRIM(PHRM.IDENTIFIER)= TRIM(CLM.PHRM_PROV_DEA_NBR) AND TO_CHAR(CLM.PHRM_PRSCN_WRT_DT,'YYYY-MM-DD') BETWEEN PHRM.START_DATE AND PHRM.END_DATE " \
                 " WHERE EXTRACT(YEAR FROM PHRM_FILL_DT) = " \
                 + str(yearNumber) + " AND EXTRACT(MONTH FROM PHRM_FILL_DT) = " + str(month) \
                 + " AND EXTRACT(DAY FROM PHRM_FILL_DT) = " + str(day) \
                 + " ORDER BY PHRM_DRG_CLM_SYS_ID FETCH FIRST " + rowcomparison + " ROWS ONLY"

            q2 = "SELECT PHRM_DRG_CLM_SYS_ID," \
                 "PHRM_COPAY_AMT,PHRM_DED_AMT,PHRM_DSPNS_FEE_AMT," \
                 "PHRM_INGR_CST_AMT,PHRM_PD_AMT,PHRM_CHK_DT,PHRM_FILL_DT," \
                 "PHRM_DAY_SPL_CNT,PHRM_DSPNS_AS_WRT_CD,PHRM_NDC_CD,PHRM_FST_FILL_IND_CD,PHRM_FRMLRY_IND_CD,PHRM_FRMLRY_TYP_CD," \
                 "PHRM_FRMLRY_TYP_DESC,PHRM_MAIL_ORDR_IND_CD,PHRM_PRSC_RFL_NBR," \
                 "PHRM_QTY_CNT,PHRM_IN_NTWK_CD,PHRM_NBR,PHRM_NPI_PROV_ID,PHRM_PRSC_NPI_PROV_ID,PHRM_PROV_DEA_NBR," \
                 "PHRM_ACCT_ID,PHRM_CARR_ID,PHRM_COB_CD,PHRM_COB_DESC,PHRM_DRG_CLM_ALT_ID," \
                 "MBR_ALT_ID,PROV_ALT_ID,CUST_SEG_NBR,TRAN_CD,SRC_REC_CD,REC_CRT_DT,REC_UPD_DT,MBR_BTH_DT,MBR_FST_NM,MBR_LST_NM," \
                 "MBR_ZIP_CD,PHRM_AUTH_RFL_CNT,PHRM_PRSCN_WRT_DT,SBSCR_NBR,MEDCD_ID,MEDCR_ID,MBR_GDR_CD,PHRM_DIAG_CD,PHRM_PRSC_NBR,SRC_DRG_CLM_TYP," \
                 "DERIV_OBH_FINC_LIAB,SRC_CUST_CD,SRC_CUST_NM,SRC_MBR_ID,OBH_SCR_RL_LST,CLM_SRC_MBR_ACCT_NBR,CLM_TRANS_SBMT_AMT,CLM_TRANS_THER_CLASS_DESC,CLM_TRANS_PRDCT_TYP_DESC, " \
                 "PERS_SYS_ID,PHRM_PRSC_EPIM_PROV_ID,PHRM_EPIM_PROV_ID,PHRM_DEA_EPIM_PROV_ID,IMDM_ID "\
                 "  FROM " + tgt_schema_name + "." + \
                 tgt_tb_name + " WHERE EXTRACT(YEAR FROM PHRM_FILL_DT) = " \
                 + str(yearNumber) + " AND EXTRACT(MONTH FROM PHRM_FILL_DT) = " + str(month) \
                 + " AND EXTRACT(DAY FROM PHRM_FILL_DT) = " + str(day) \
                 + " ORDER BY PHRM_DRG_CLM_SYS_ID FETCH FIRST " + rowcomparison + " ROWS ONLY"


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
    sf_conn1.close()
