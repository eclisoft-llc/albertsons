# Author: Mary Jasline Vinodh
# Date: 04/27/2022
# Description: Compare a data set from Oracle against Snowflake, document the differences


import cx_Oracle
import pandas as pd
import snowflake.connector

sf_conn = snowflake.connector.connect(
    user='your-username',
    role='your-role',
    warehouse = 'your-warehouse',
    database = 'your-database',
    schema = 'COMPACT',
    account='your-account',
    authenticator='externalbrowser'
)

# Specify database connection details and establish connection.
ora_conn = cx_Oracle.connect(
    user="your-username",
    password="your-password",
    dsn="your-dsn")

diff_path = 'C:/Users/your-windows-user-folder/Documents/Data_Profiles/'

def execute_compare(query1, query2, tbname, yearn, monthn, dayn):
    try:

        s1 = pd.read_sql(query1, con=sf_conn)
        s2 = pd.read_sql(query2, con=ora_conn)

        s1 = s1.replace([None], [''], regex=True)
        s2 = s2.replace([None], [''], regex=True)

        s1 = s1.fillna('')
        s2 = s2.fillna('')

        df_diff2 = pd.concat([s2, s1], keys=['Oracle', 'Snowflake']).drop_duplicates(keep=False)

        if(df_diff2.size>0):
            df_diff2.to_csv(diff_path + tbname + '_' + yearn + '_' + monthn + '_' + dayn + '_Diffs.csv')

    except Exception as e:
        print(e, e.args)
        pass


input_list = {
    "FACT_CLAIM_EXTN_PROV": ["COMPANY_DIM_ID","TO_CHAR(SEQ_CLAIM_ID)","CLAIM_NUMBER","to_char(PRIMARY_SVC_DATE,'YYYY-MM-DD')","CONVERTED_FROM_DIM_ID","MEMB_DIM_ID","BI_PROV_DIM_ID","BI_PROV_NPI","BI_PROVIDER_ID","BI_PROV_TAX_ID","BI_PROV_SSN","BI_PROV_TAXONOMY","BI_PROV_FIRST_NAME","BI_PROV_LAST_NAME","BI_PROV_ADDR1","BI_PROV_ADDR2","BI_PROV_CITY","BI_PROV_STATE","BI_PROV_ZIP","AT_PROV_NPI","AT_PROV_TAXONOMY","AT_PROV_FIRST_NAME","AT_PROV_LAST_NAME","RE_PROV_NPI","RE_PROV_TAXONOMY","RE_PROV_FIRST_NAME","RE_PROV_LAST_NAME","OP_PROV_NPI","OP_PROV_FIRST_NAME","OP_PROV_LAST_NAME","OT_OP_PROV_NPI","OT_OP_PROV_FIRST_NAME","OT_OP_PROV_LAST_NAME","RF_PROV_NPI","RF_PROV_FIRST_NAME","RF_PROV_LAST_NAME","SP_PROV_NPI","SP_PROV_FIRST_NAME","SP_PROV_LAST_NAME","SE_PROV_NPI","SE_PROV_FAC_ID","SE_PROV_FIRST_NAME","SE_PROV_LAST_NAME","SE_PROV_ADDR1","SE_PROV_ADDR2","SE_PROV_CITY","SE_PROV_STATE","SE_PROV_ZIP","SYS1","SYS2","SYS3","DW_INSERT_DATETIME","DW_UPDATE_DATETIME"]
}
# update these 4 values to target specific dates by year, month and d, where "d" = Day of the month to start validation
# and endday = the day of the month to validate (up-to-this-day), example: validate every day from 8-11-2021 through 8-15-2021
yearNumber = 2022
month=1
day=11
endday=11
# you can increase the amount of records to validate per day with the next value, but I found 2000 is the sweet spot where
# performance is not greatly affected, yet the amount of records is high enough for accuracy
rowcomparison='2000'
try:
    for entity, attribute_list in input_list.items():
        listToStr = ','.join([str(elem) for elem in attribute_list])
        while day <= endday:
            try:
                print('generating report for ' + entity + ' ' + str(yearNumber) + ' ' + str(month) + ' ' + str(day))
                q1 = 'SELECT ' + listToStr + ' FROM ' + entity + ' WHERE EXTRACT(YEAR FROM PRIMARY_SVC_DATE) = ' \
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
                pass
            day += 1
except Exception as e:
    print(e, e.args)
    pass
finally:
    sf_conn.close()
    ora_conn.close()
