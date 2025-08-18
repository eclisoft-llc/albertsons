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

        df_diff2 = pd.concat([s2, s1], keys=['Oracle', 'Snowflake']).drop_duplicates(keep=False)

        if(df_diff2.size>0):
            df_diff2.to_csv(diff_path + tbname + '_' + yearn + '_' + monthn + '_' + dayn + '_Diffs.csv')

    except Exception as e:
        print(e, e.args)
        pass


input_list = {
    "FACT_MEMBER_REVENUE_DETAIL": ["MEMB_DIM_ID","COMPANY_DIM_ID","REVENUE_TYPE_DIM_ID","DATA_SOURCE","LINE_NUMBER","MEMBER_ID","MEMBER_SSN","MEMBER_LAST_NAME","MEMBER_FIRST_NAME","MEMBER_INIT","to_char(MEMBER_DOB,'YYYY-MM-DD')","MEMBER_COUNT","TRANSACTION_TYPE","PAYMENT_AMT","TO_CHAR(PAYMENT_DATE,'YYYY-MM-DD')","TO_CHAR(REMIT_MONTH,'YYYY-MM-DD')","TO_CHAR(SERVICE_START_DATE,'YYYY-MM-DD') ","TO_CHAR(SERVICE_END_DATE,'YYYY-MM-DD')","DW_INSERT_DATETIME ","DW_UPDATE_DATETIME","OTHER_MEMBER_ID_1","OTHER_MEMBER_ID_2","OTHER_MEMBER_ID_3","OTHER_AMT_1","OTHER_AMT_2","TO_CHAR(OTHER_DATE_1,'YYYY-MM-DD')","TO_CHAR(OTHER_DATE_2,'YYYY-MM-DD')","TO_CHAR(OTHER_DATE_3,'YYYY-MM-DD')","USER_DEFINED_ALPHA_1","USER_DEFINED_NUM_1","USER_DEFINED_KEY"]
}
# update these 4 values to target specific dates by year, month and d, where "d" = Day of the month to start validation
# and endday = the day of the month to validate (up-to-this-day), example: validate every day from 8-11-2021 through 8-15-2021
yearNumber = 9999
month=12
day=31
endday=31
# you can increase the amount of records to validate per day with the next value, but I found 2000 is the sweet spot where
# performance is not greatly affected, yet the amount of records is high enough for accuracy
rowcomparison='2000'
try:
    for entity, attribute_list in input_list.items():
        listToStr = ','.join([str(elem) for elem in attribute_list])
        while day <= endday:
            try:
                print('generating report for ' + entity + ' ' + str(yearNumber) + ' ' + str(month) + ' ' + str(day))
                q1 = 'SELECT ' + listToStr + ' FROM ' + entity + ' WHERE EXTRACT(YEAR FROM SERVICE_START_DATE) = ' \
                     + str(yearNumber) + ' AND EXTRACT(MONTH FROM SERVICE_START_DATE) = ' + str(month) \
                     + ' AND EXTRACT(DAY FROM SERVICE_START_DATE) = ' + str(day) \
                     + ' ORDER BY MEMB_DIM_ID,COMPANY_DIM_ID,TO_CHAR(PAYMENT_DATE),MEMBER_ID,MEMBER_LAST_NAME,MEMBER_FIRST_NAME LIMIT ' + rowcomparison
                q2 = 'SELECT ' + listToStr + ' FROM DW.' + entity + ' WHERE EXTRACT(YEAR FROM SERVICE_START_DATE) = ' \
                     + str(yearNumber) + ' AND EXTRACT(MONTH FROM SERVICE_START_DATE) = ' + str(month) \
                     + ' AND EXTRACT(DAY FROM SERVICE_START_DATE) = ' + str(day) \
                     + ' ORDER BY MEMB_DIM_ID,COMPANY_DIM_ID,TO_CHAR(PAYMENT_DATE),MEMBER_ID,MEMBER_LAST_NAME,MEMBER_FIRST_NAME FETCH FIRST ' + rowcomparison +' ROWS ONLY'
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
