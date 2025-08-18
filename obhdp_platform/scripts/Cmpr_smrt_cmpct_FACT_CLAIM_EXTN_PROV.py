import pandas as pd
import argparse
import cx_Oracle
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
parser.add_argument("--oradsn", action="store", dest="ora_db", required=True, help="Oracle Connection")
parser.add_argument("--orauser", action="store", dest="ora_usr", required=True, help="Oracle user")
parser.add_argument("--orapwd", action="store", dest="ora_pwd", required=True, help="Oracle password")
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

ora_conn = cx_Oracle.connect(
    user=args.ora_usr,
    password=args.ora_pwd,
    dsn=args.ora_db)

diff_path = args.diffpath

def execute_compare(query1, query2, tbname, yearn, monthn, dayn):
    try:

        s1 = pd.read_sql(query1, con=sf_conn)
        s2 = pd.read_sql(query2, con=ora_conn)

        s1 = s1.replace('None','', regex=True)
        s2 = s2.replace('None','', regex=True)

        s1 = s1.fillna('')
        s2 = s2.fillna('')

        df_diff2 = pd.concat([s2, s1], keys=['Oracle', 'Snowflake']).drop_duplicates(keep=False)

        if(df_diff2.size>0):
            df_diff2.to_csv(diff_path + tbname + '_' + yearn + '_' + monthn + '_' + dayn + '_Diffs.csv')

    except Exception as e:
        print(e, e.args)
        raise


input_list = {
    'FACT_CLAIM_EXTN_PROV': ['COMPANY_DIM_ID','SEQ_CLAIM_ID','CLAIM_NUMBER','PRIMARY_SVC_DATE','CONVERTED_FROM_DIM_ID','MEMB_DIM_ID','BI_PROV_DIM_ID','BI_PROV_NPI','BI_PROVIDER_ID','BI_PROV_TAX_ID','BI_PROV_SSN','BI_PROV_TAXONOMY','BI_PROV_FIRST_NAME','BI_PROV_LAST_NAME','BI_PROV_ADDR1','BI_PROV_ADDR2','BI_PROV_CITY','BI_PROV_STATE','BI_PROV_ZIP','AT_PROV_NPI','AT_PROV_TAXONOMY','AT_PROV_FIRST_NAME','AT_PROV_LAST_NAME','RE_PROV_NPI','RE_PROV_TAXONOMY','RE_PROV_FIRST_NAME','RE_PROV_LAST_NAME','OP_PROV_NPI','OP_PROV_FIRST_NAME','OP_PROV_LAST_NAME','OT_OP_PROV_NPI','OT_OP_PROV_FIRST_NAME','OT_OP_PROV_LAST_NAME','RF_PROV_NPI','RF_PROV_FIRST_NAME','RF_PROV_LAST_NAME','SP_PROV_NPI','SP_PROV_FIRST_NAME','SP_PROV_LAST_NAME','SE_PROV_NPI','SE_PROV_FAC_ID','SE_PROV_FIRST_NAME','SE_PROV_LAST_NAME','SE_PROV_ADDR1','SE_PROV_ADDR2','SE_PROV_CITY','SE_PROV_STATE','SE_PROV_ZIP','SYS1','SYS2','SYS3','DW_INSERT_DATETIME','DW_UPDATE_DATETIME']
}
# update these 4 values to target specific dates by year, month and d, where "d" = Day of the month to start validation
# and endday = the day of the month to validate (up-to-this-day), example: validate every day from 8-11-2021 through 8-15-2021
yearNumber = 2022
month=4
day=1
endday=15
# you can increase the amount of records to validate per day with the next value, but I found 2000 is the sweet spot where
# performance is not greatly affected, yet the amount of records is high enough for accuracy
rowcomparison='2000'
try:
    for entity, attribute_list in input_list.items():
        listToStr = ','.join([str(elem) for elem in attribute_list])
        while day <= endday:
            try:
                print('generating report for ' + entity + ' ' + str(yearNumber) + ' ' + str(month) + ' ' + str(day))
                q1 = 'SELECT ' + listToStr + ' FROM COMPACT.' + entity + ' WHERE EXTRACT(YEAR FROM PRIMARY_SVC_DATE) = ' \
                     + str(yearNumber) + ' AND EXTRACT(MONTH FROM PRIMARY_SVC_DATE) = ' + str(month) \
                     + ' AND EXTRACT(DAY FROM PRIMARY_SVC_DATE) = ' + str(day) \
                     + ' ORDER BY CLAIM_NUMBER,COMPANY_DIM_ID,SEQ_CLAIM_ID LIMIT ' + rowcomparison
                q2 = 'SELECT ' + listToStr + ' FROM DW.' + entity + ' WHERE EXTRACT(YEAR FROM PRIMARY_SVC_DATE) = ' \
                     + str(yearNumber) + ' AND EXTRACT(MONTH FROM PRIMARY_SVC_DATE) = ' + str(month) \
                     + ' AND EXTRACT(DAY FROM PRIMARY_SVC_DATE) = ' + str(day) \
                     + ' ORDER BY CLAIM_NUMBER,COMPANY_DIM_ID,SEQ_CLAIM_ID FETCH FIRST ' + rowcomparison +' ROWS ONLY'
                execute_compare(q1, q2, entity, str(yearNumber),str(month), str(day))
            except Exception as e:
                print(e, e.args)
                raise
            day += 1
except Exception as e:
    print(e, e.args)
    raise
finally:
    sf_conn.close()
    ora_conn.close()