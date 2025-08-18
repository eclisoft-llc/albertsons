# Author: Berenice Balboa
# Date: 08/13/2021
# Description: Compare a data set from Oracle against Snowflake, document the differences
# 08/23/2021 BBalboa - code clean-up

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
    'FACT_MEMBER_ELIG_HISTORY': ['MEMB_DIM_ID', 'COMPANY_DIM_ID', 'PLAN_DIM_ID', 'PROV_DIM_ID', 'ELIG_STATUS_DIM_ID', 'LINE_OF_BUSINESS_DIM_ID', 'GROUP_DIM_ID', 'TERM_REASON_DIM_ID', 'PCP_CHANGE_REASON_DIM_ID', 'ENROLL_REASON_DIM_ID', 'COA_DIM_ID', 'PROGRAM_STATUS_CODE_DIM_ID', 'EXTRACT(YEAR FROM EFFECTIVE_DATE) EFFECTIVE_DATE_Y', 'EXTRACT(MONTH FROM EFFECTIVE_DATE) EFFECTIVE_DATE_M', 'EXTRACT(DAY FROM EFFECTIVE_DATE) EFFECTIVE_DATE_D', 'EXTRACT(YEAR FROM TERM_DATE) TERM_DATE_Y', 'EXTRACT(MONTH FROM TERM_DATE) TERM_DATE_M', 'EXTRACT(DAY FROM TERM_DATE) TERM_DATE_D', 'EXTRACT(YEAR FROM INSERT_DATETIME) INSERT_DATETIME_Y', 'EXTRACT(MONTH FROM INSERT_DATETIME) INSERT_DATETIME_M', 'EXTRACT(DAY FROM INSERT_DATETIME) INSERT_DATETIME_D', 'EXTRACT(YEAR FROM UPDATE_DATETIME) UPDATE_DATETIME_Y', 'EXTRACT(MONTH FROM UPDATE_DATETIME) UPDATE_DATETIME_M', 'EXTRACT(DAY FROM UPDATE_DATETIME) UPDATE_DATETIME_D', 'CONVERTED_FROM_DIM_ID', 'COSMOS_CUST_SEG_DIM_ID', 'MEDICARE_TYPE_DIM_ID', 'LTC_IND_DIM_ID', 'UNISON_PRODUCT_DIM_ID', 'STATE_SPEC_COUNTY_DIM_ID', 'DISABLED_IND_DIM_ID', 'HOSPICE_IND_DIM_ID', 'PREGNANCY_IND_DIM_ID', 'COURT_CODE_DIM_ID', 'PLAN_LOB_DIM_ID', 'SYS1', 'SYS2', 'SYS3', 'SYS4', 'SOURCE_PRODUCT_DIM_ID', 'CLASS_DIM_ID', 'CLASS_PLAN_DIM_ID', 'PRODUCT_DIM_ID', 'SDA_DIM_ID']
}
# update these 4 values to target specific dates by year, month and d, where "d" = Day of the month to start validation
# and endday = the day of the month to validate (up-to-this-day), example: validate every day from 8-11-2021 through 8-15-2021
yearNumber = 2022
month=2
day=1
endday=15
# you can increase the amount of records to validate per day with the next value, but I found 4000 (for this FACT) is the sweet
# spot where performance is not greatly affected, yet the amount of records is high enough for accuracy
rowcomparison='4000'
try:
    for entity, attribute_list in input_list.items():
        listToStr = ','.join([str(elem) for elem in attribute_list])
        while day <= endday:
            try:
                print('generating report for ' + entity + ' ' + str(yearNumber) + ' ' + str(month) + ' ' + str(day))
                q1 = 'SELECT ' + listToStr + ' FROM ' + entity + ' WHERE EXTRACT(YEAR FROM EFFECTIVE_DATE) = ' \
                     + str(yearNumber) + ' AND EXTRACT(MONTH FROM EFFECTIVE_DATE) = ' + str(month) \
                     + ' AND EXTRACT(DAY FROM EFFECTIVE_DATE) = ' + str(day) \
                     + ' ORDER BY MEMB_DIM_ID,COMPANY_DIM_ID,EFFECTIVE_DATE,TERM_DATE,PLAN_DIM_ID LIMIT ' + rowcomparison
                q2 = 'SELECT ' + listToStr + ' FROM DW.' + entity + ' WHERE EXTRACT(YEAR FROM EFFECTIVE_DATE) = ' \
                     + str(yearNumber) + ' AND EXTRACT(MONTH FROM EFFECTIVE_DATE) = ' + str(month) \
                     + ' AND EXTRACT(DAY FROM EFFECTIVE_DATE) = ' + str(day) \
                     + ' ORDER BY MEMB_DIM_ID,COMPANY_DIM_ID,EFFECTIVE_DATE,TERM_DATE,PLAN_DIM_ID FETCH FIRST ' + rowcomparison +' ROWS ONLY'
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
