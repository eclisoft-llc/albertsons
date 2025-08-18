# -*- coding: utf-8 -*-
"""
Created on Mon Apr 12 18:39:13 2021

@author: valuwala
"""
import snowflake.connector as sf
import pandas as pd



# make changes as per your credentials
user='aluwala_varshini@optum.com'
role='AR_DEV_ALUWALA_VARSHINI_OPTUM_ROLE'
account = 'uhgdwaas.east-us-2.azure'
warehouse = 'ECT_DEV_OBH_DP_LOAD_WH'
database = 'ECT_DEV_OBH_DP_DB'
schema = 'ETL'


conn = sf.connect(user = user,
                  account = account,
                  authenticator='externalbrowser'
                  )

def run_query(connection,query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

def run_sql_query(connection,query):
    cursor = connection.cursor()
    cursor.execute(query)
    result=pd.DataFrame()
    for df in cursor.fetch_pandas_batches():
        result=result.append(df, ignore_index = True)
    cursor.close()
    return result

def create_dataProfilingReport():

    #inp=json.loads(argv)
    for entity,attribute_list in inp.items():
        for attribute in attribute_list:
            print('generating report for'+entity,attribute)
            sql='select '+attribute +' ,count('+attribute+') from ' +entity+ ' group by '+ attribute
            result=run_sql_query(conn, sql)
            try:
                result.to_excel(attribute+'.xlsx',sheet_name=entity)
            except Exception as e:
                print(e, e.args)
                pass


def configure():
    sql = 'use role {}'.format(role)
    run_query(conn, sql)

    sql = f'use warehouse {warehouse}'
    run_query(conn, sql)

    try:
        warehouse_sql = 'use warehouse {}'.format(warehouse)
        run_query(conn, warehouse_sql)

        try:
            sql = 'alter warehouse {} resume'.format(warehouse)
            run_query(conn, sql)
        except:
            pass


        sql = 'use database {}'.format(database)
        run_query(conn, sql)

        sql = f'use schema {schema}'
        run_query(conn, sql)

    except Exception as e:
        print(e)

configure()

#smartfactclaim'claim_number'

inp = {'Smart_Fact_claim_l': ['line_number','detail_svc_date','place_of_svc_1_dim_id','cpt_code_dim_id','revenue_code_dim_id','sub_line_code', 'paid_net_amt'], 'smart_dim_place_of_svc_l': ['place_of_svc_code','place_of_svc_dim_id'], 'smart_dim_procedure_code_l':['procedure_code','procedure_dim_id','Procedure_Code_Short_Desc'], 'smart_dim_procedure_modifier_l':['modifier_code','modifier_dim_id'],'smart_dim_ndc_code_l':['ndc_code','ndc_code_dim_id' ,'generic_name'],'smart_dim_diagnosis_l':['icd_ver_ind','diagnosis_dim_id','diagnosis_code'],'smart_Dim_member_l':['memb_dim_id'],'smart_dim_provider_l':['prov_first_name','national_provider_id','prov_last_name']}

create_dataProfilingReport()

