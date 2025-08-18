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
    'FACT_CLAIM_RX_ADDITION': [	'COMPANY_DIM_ID', 'SEQ_CLAIM_ID', 'LINE_NUMBER', 'SUB_LINE_CODE', 'APPROVED_NET_PATIENT_PAY_AMT', 'PLAN_COPAY_AMT', 'PRODUCT_SELECTION_DIFF_AMT', 'PROF_SVC_FEE_CONTRACT_AMT', 'INCENTIVE_BILLED_AMT', 'OTHER_ADJUSTMENT_AMT', 'OTHER_BILLED_AMT', 'PATIENT_DEDUCTIBLE_AMT', 'INGREDIENT_COST_CALCULATED_AMT', 'USUAL_BILLING_AMT', 'DRUG_UNIT_COST_AMT', 'OUT_OF_NETWORK_PENALTY_AMT', 'ALLOWED_SALES_TAX_AMT', 'REFILLS_AUTHORIZED', 'REFILL_CODE_DIM_ID', 'PAYMENT_TYPE_DIM_ID', 'RX_OTC_CODE_DIM_ID', 'DISPENSE_AS_WRITTEN_DIM_ID', 'OTHER_COVERAGE_CODE_DIM_ID', 'COMPOUND_CODE_DIM_ID', 'REIMBURSEMENT_BASIS_DIM_ID', 'COST_DETERMIN_BASIC_DIM_ID', 'APPROVED_COST_SOURCE_DIM_ID', 'PLAN_DRUG_STATUS_DIM_ID', 'PLAN_QUALIFIER_CODE_DIM_ID', 'SYS7', 'SYS8', 'SYS9', 'EXTRACT(YEAR FROM DETAIL_SVC_DATE) DETAIL_SVC_DATE_Y', 'EXTRACT(MONTH FROM DETAIL_SVC_DATE) DETAIL_SVC_DATE_M', 'EXTRACT(DAY FROM DETAIL_SVC_DATE) DETAIL_SVC_DATE_D', 'EXTRACT(YEAR FROM POST_DATE) POST_DATE_Y', 'EXTRACT(MONTH FROM POST_DATE) POST_DATE_M', 'EXTRACT(DAY FROM POST_DATE) POST_DATE_D', 'RX_ORIGINATION_FLAG_DIM_ID', 'SVC_PROV_REL_CODE', 'SVC_PROV_DISPENSER_CLASS', 'SVC_PROV_DISPENSER_TYPE', 'RX_PRODUCT_QUALIFIER_DIM_ID', 'DRUG_LABLE_NAME', 'DRUG_DOSAGE_FORM', 'DRUG_STRENGTH_OR_UNITS', 'PRODUCT_MANUFACTURER', 'DRUG_STRENGTH_IN_METRIC', 'ROUTE_OF_ADMINISTRATION', 'DOSAGE_FORM_MEDI_SPAN', 'RX_MEMB_CARDHOLDER_REL_DIM_ID', 'RX_PRIORAUTH_QUALIFIER_DIM_ID', 'PRIOR_AUTH_REASON_CODE', 'CARDHOLDER_ID', 'CARDHOLDER_INFO_1', 'CARDHOLDER_INFO_2', 'RX_THIRD_PARTY_RESTRIC_DIM_ID', 'UNIT_DOSE_IND', 'GENERIC_CODE_NUMBER', 'GENERIC_CODE_SEQ', 'TRANSACTION_CODE', 'CLAIM_MESSAGE', 'RX_BENEFIT_MAX_FLAG_DIM_ID', 'RX_CLIENT_TYPE_DIM_ID', 'BENEFIT_MAX_APPLIED_AMT', 'RX_THERAPEUTIC_GROUP_DIM_ID', 'RX_FORMULARY_CLAIM_FLAG_DIM_ID', 'RX_MEDICAL_CLAIM_FLAG_DIM_ID', 'RX_PRIORAUTH_CLAIM_FLAG_DIM_ID', 'RX_TRANSPLANT_FLAG_DIM_ID', 'RX_INJECTABLE_PROD_FLAG_DIM_ID', 'RXSOL_FORMULARY_FLAG_DIM_ID', 'RX_HALF_TAB_FLAG_DIM_ID', 'DRUG_PRICING_TIER', 'RX_ALT_INSURANCE_FLAG_DIM_ID', 'ADMIN_FEE', 'SYS15', 'SYS16', 'DRUG_LBL_NAM_DSG_FRM_STNGTH', 'CARDHOLDER_INFO_3', 'RX_ORIGIN', 'PATIENT_RESIDENCE_DIM_ID', 'RX_SOURCE_PLAN_CODE', 'RX_SUBMIT_TIME', 'OOP_MAX_AMT', 'REMITTANCE_TYPE_DIM_ID', 'REG_DISASTER_OVERRIDE', 'CLIENT_COPAY', 'RX_SOURCE_PLAN_TYPE', 'REJECT_1_REASON_DIM_ID', 'REJECT_2_REASON_DIM_ID', 'REJECT_3_REASON_DIM_ID', 'GROSS_AMT_DUE', 'REJECT_MESSAGE1', 'REJECT_MESSAGE2', 'REJECT_MESSAGE3', 'PROV_QUALIFIER_DIM_ID', 'PRESC_QUALIFIER_DIM_ID', 'PRESCRIPTION_SERIAL_NO', 'SUBMISSION_CLARIFICATION_CODE', 'MAIL_ORDER_RX_INDICATOR', 'SDA', 'RISKGROUP', 'HICN', 'RSN_FOR_SVC_CODE', 'PERSONAL_SVC_CODE', 'RESULT_OF_SVC_CODE', 'OTH_PAYER_REJ_COUNT', 'OTH_PAYER_REJ_CODE1', 'OTH_PAYER_REJ_CODE2', 'OTH_PAYER_REJ_CODE3', 'BENEFIT_STAGE_COUNT', 'BENEFIT_STAGE_QLFR1', 'BENEFIT_STAGE_AMT1', 'SPL_PACKAGING_IND', 'RX_SVC_TYPE', 'OTH_PAYER_SALES_TAX', 'OTH_PAYER_ID', 'FLAT_TAX_SUBMITTED', 'DISPENSING_FEE_SUBMITTED', 'OTH_PAYER1_BIN', 'OTH_PAYER1_PAID_AMT', 'OTH_PAYER1_SALES_TAX_AMT', 'OTH_PAYER1_COMP_PREP_AMT', 'OTH_PAYER2_BIN', 'OTH_PAYER2_PAID_AMT', 'OTH_PAYER2_SALES_TAX_AMT', 'OTH_PAYER2_COMP_PREP_AMT', 'QUANTITY_PRESCRIBED', 'SUBMISSION_CLARIFICATION_CD_1', 'SUBMISSION_CLARIFICATION_CD_2', 'SUBMISSION_CLARIFICATION_CD_3']
}
# update these 4 values to target specific dates by year, month and d, where "d" = Day of the month to start validation
# and endday = the day of the month to validate (up-to-this-day), example: validate every day from 8-11-2021 through 8-15-2021
yearNumber = 2021
month=8
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
                q1 = "SELECT " + listToStr + " FROM " + entity + " WHERE EXTRACT(YEAR FROM DETAIL_SVC_DATE) = " \
                     + str(yearNumber) + " AND EXTRACT(MONTH FROM DETAIL_SVC_DATE) = " + str(month) \
                     + " AND EXTRACT(DAY FROM DETAIL_SVC_DATE) = " + str(day) \
                     + " ORDER BY COMPANY_DIM_ID,SEQ_CLAIM_ID,LINE_NUMBER,SUB_LINE_CODE LIMIT " + rowcomparison
                q2 = "SELECT " + listToStr + " FROM DW." + entity + " WHERE EXTRACT(YEAR FROM DETAIL_SVC_DATE) = " \
                     + str(yearNumber) + " AND EXTRACT(MONTH FROM DETAIL_SVC_DATE) = " + str(month) \
                     + " AND EXTRACT(DAY FROM DETAIL_SVC_DATE) = " + str(day) \
                     + " AND DETAIL_SVC_DATE >= to_date('2019-01-01', 'yyyy-mm-dd') "\
                     + " ORDER BY COMPANY_DIM_ID,SEQ_CLAIM_ID,LINE_NUMBER,SUB_LINE_CODE FETCH FIRST " + rowcomparison +" ROWS ONLY"
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
