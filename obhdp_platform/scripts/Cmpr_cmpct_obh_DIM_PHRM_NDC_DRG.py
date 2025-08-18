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
tgt_tb_name = 'DIM_PHRM_NDC_DRG'


def execute_compare(query1, query2):
    try:

        s1 = pd.read_sql(query1, con=sf_conn)
        s2 = pd.read_sql(query2, con=sf_conn)

        s1 = s1.replace([None], [''], regex=True)
        s2 = s2.replace([None], [''], regex=True)

        s1 = s1.fillna('')
        s2 = s2.fillna('')

        df_diff2 = pd.concat([s2, s1], keys=['Snowflake-obhdp', 'Snowflake-compact']).drop_duplicates(keep=False)

        if (df_diff2.size > 0):
            df_diff2.to_csv(diff_path + tgt_tb_name + '_Diffs.csv')

    except Exception as e:
        print(e, e.args)
        pass



rowcomparison = '3000'


try:
        print('generating report for ' + 'DIM_PHRM_NDC_DRG')

        q1 = "SELECT " \
         "NDC.NDC_CODE NDC_DRG_SYS_ID,NDC.NDC_CODE PHRM_NDC_CD,TO_DATE(NDC.DW_UPDATE_DATETIME) NDC_ROW_EFF_DT," \
         "TO_DATE('9999-12-31') NDC_ROW_END_DT,ATCC.AHFS_THER_CLASS_CODE AHFS_THRPTC_CLSS_CD,NDC.BRAND_NAME NDC_BRND_NM," \
         "NDC.DEA_DRUG_ABUSE_CODE NDC_DEA_DRG_ABUS_CD,NULL NDC_DSTRB_ID,NDC.DSTRB_NAME NDC_DSTRB_NM," \
         "NDC.DOSAGE_FM_DESC NDC_DOSE_FM_DESC,NDC.DRUG_ADMIN_RTE_TXT NDC_ADMIN_RTE_TXT,NDC.DRUG_CLASS NDC_AVL_CD," \
         "NDC.DRUG_CATEGORY_CODE NDC_CATGY_CD,NDC.DRUG_FM_CODE NDC_FM_CD,NULL NDC_OBSLT_DT,NULL NDC_PRC_TYP_CD," \
         "CASE WHEN TRIM(NDC.DRUG_STRENGTH_DESC) LIKE '0%' THEN NULL WHEN NDC.DRUG_STRENGTH_DESC=' ' THEN TRIM(NDC.DRUG_STRENGTH_DESC) ELSE TRIM(NDC.DRUG_STRENGTH_DESC) END  NDC_STRGTH_DESC," \
         "CASE WHEN NDC.DRUG_STRENGTH_NBR LIKE '0%' THEN NULL ELSE NDC.DRUG_STRENGTH_NBR END NDC_STRGTH_NBR,NDC.DRUG_STRENGTH_UNIT_DESC NDC_STRGTH_UNIT_DESC," \
         "NULL NDC_STRGTH_VOL_NBR,NULL NDC_STRGTH_VOL_UNIT_DESC,NULL NDC_XPND_DRG_DESC,NULL NDC_XPND_ORAG_BK_CD," \
         "NDC.GENERIC_IND NDC_GNRC_IND_CD,NDC.GENERIC_NAME NDC_GNRC_NM,NULL NDC_GNRC_NBR,NULL NDC_GNRC_SEQ_NBR," \
         "NULL NDC_GDR_SPEC_DRG_CD,GTCC.GNRC_THER_CLASS_CODE NDC_GNRC_THRPTC_CLSS_CD,NULL NDC_HCFA_APRV_DT," \
         "NULL NDC_HIER_INGR_NBR,NULL NDC_HIER_INGR_SEQ_NBR,NULL NDC_INNOV_IND_CD,NDC.NDC_LABEL_CODE NDC_LBL_CD," \
         "NDC.NDC_PACKAGE_CODE NDC_PKG_CD,NDC.NDC_PRODUCT_CODE NDC_PRDCT_CD,NULL NDC_PKG_DESC," \
         "NULL NDC_PKG_SZ_EQ_NBR,NULL NDC_PKG_SZ_NBR,NULL NDC_PREG_PCAUT_NBR,NULL NDC_REPKG_IND_CD," \
         "NULL NDC_SIDE_EFF_NBR,STCC.SPEC_THER_CLASS_CODE NDC_SPEC_THRPTC_CLSS_CD,NULL NDC_STD_PKG_IND_CD," \
         "STDTCC.STD_THER_CLASS_CODE NDC_STD_THRPTC_CLSS_CD,NULL NDC_UNIT_DOSE_IND_CD," \
         "NULL NDC_UNIT_OF_USE_IND_CD,NDC.BRAND_NAME NDC_COMPRS_BRND_NM,MTCC.MEDCO_THER_CLASS_CODE NDC_MEDCO_THRPTC_CLSS_CD," \
         "ATCC.AHFS_THER_CLASS_CODE EXT_AHFS_THRPTC_CLSS_CD,NULL SRC_REC_CD,TO_DATE(NDC.DW_INSERT_DATETIME) REC_CRT_DT," \
         "NDC.DW_INSERT_DATETIME REC_CRT_TM,TO_DATE(NDC.DW_UPDATE_DATETIME) REC_UPD_DT,NDC.DW_UPDATE_DATETIME REC_UPD_TM," \
         "NDC.NDC_PROCEDURE_CODE NDC_PROC_CD,NDC.GPI_CODE NDC_GPI_CD,NULL OBH_SCR_RL_LST" \
         " FROM COMPACT.DIM_NDC_CODE NDC " \
         " LEFT OUTER JOIN  COMPACT.DIM_AHFS_THER_CLASS_CODE ATCC ON NDC.AHFS_THER_CLASS_DIM_ID=ATCC.AHFS_THER_CLASS_DIM_ID" \
         " LEFT OUTER JOIN COMPACT.DIM_MEDCO_THER_CLASS_CODE MTCC ON NDC.MEDCO_THER_CLASS_DIM_ID=MTCC.MEDCO_THER_CLASS_DIM_ID" \
         " LEFT OUTER JOIN COMPACT.DIM_SPEC_THER_CLASS_CODE STCC ON NDC.SPEC_THER_CLASS_DIM_ID=STCC.SPEC_THER_CLASS_DIM_ID" \
         " LEFT OUTER JOIN COMPACT.DIM_STD_THER_CLASS_CODE STDTCC ON NDC.STD_THER_CLASS_DIM_ID=STDTCC.STD_THER_CLASS_DIM_ID" \
         " LEFT OUTER JOIN COMPACT.DIM_GNRC_THER_CLASS_CODE GTCC ON NDC.GNRC_THER_CLASS_DIM_ID=GTCC.GNRC_THER_CLASS_DIM_ID" \
         + " ORDER BY  NDC.NDC_CODE   FETCH FIRST " + rowcomparison + ' ROWS ONLY'

        q2 = "SELECT NDC_DRG_SYS_ID,PHRM_NDC_CD,NDC_ROW_EFF_DT,NDC_ROW_END_DT," \
         "AHFS_THRPTC_CLSS_CD,NDC_BRND_NM,NDC_DEA_DRG_ABUS_CD,NDC_DSTRB_ID," \
         "NDC_DSTRB_NM,NDC_DOSE_FM_DESC,NDC_ADMIN_RTE_TXT,NDC_AVL_CD," \
         "NDC_CATGY_CD,NDC_FM_CD,NDC_OBSLT_DT,NDC_PRC_TYP_CD,NDC_STRGTH_DESC," \
         "NDC_STRGTH_NBR,NDC_STRGTH_UNIT_DESC,NDC_STRGTH_VOL_NBR,NDC_STRGTH_VOL_UNIT_DESC," \
         "NDC_XPND_DRG_DESC,NDC_XPND_ORAG_BK_CD,NDC_GNRC_IND_CD,NDC_GNRC_NM," \
         "NDC_GNRC_NBR,NDC_GNRC_SEQ_NBR,NDC_GDR_SPEC_DRG_CD,NDC_GNRC_THRPTC_CLSS_CD," \
         "NDC_HCFA_APRV_DT,NDC_HIER_INGR_NBR,NDC_HIER_INGR_SEQ_NBR,NDC_INNOV_IND_CD," \
         "NDC_LBL_CD,NDC_PKG_CD,NDC_PRDCT_CD,NDC_PKG_DESC,NDC_PKG_SZ_EQ_NBR," \
         "NDC_PKG_SZ_NBR,NDC_PREG_PCAUT_NBR,NDC_REPKG_IND_CD,NDC_SIDE_EFF_NBR," \
         "NDC_SPEC_THRPTC_CLSS_CD,NDC_STD_PKG_IND_CD,NDC_STD_THRPTC_CLSS_CD," \
         "NDC_UNIT_DOSE_IND_CD,NDC_UNIT_OF_USE_IND_CD,NDC_COMPRS_BRND_NM," \
         "NDC_MEDCO_THRPTC_CLSS_CD,EXT_AHFS_THRPTC_CLSS_CD,SRC_REC_CD," \
         "REC_CRT_DT,REC_CRT_TM,REC_UPD_DT,REC_UPD_TM,NDC_PROC_CD," \
         "NDC_GPI_CD,OBH_SCR_RL_LST FROM OBH_DP.DIM_PHRM_NDC_DRG" \
         + " ORDER BY NDC_DRG_SYS_ID FETCH FIRST " + rowcomparison + " ROWS ONLY"

        execute_compare(q1, q2)


except Exception as e:
    print(e, e.args)
    pass

finally:
     sf_conn.close()

     