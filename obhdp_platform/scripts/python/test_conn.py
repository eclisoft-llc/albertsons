# -*- coding: utf-8 -*-
"""
Spyder Editor
This is a temporary script file.
"""
import snowflake.connector
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


# using datetime module
import datetime;

with open(r'/Users/vanbalag/tcoc_sf.yaml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

# ct stores current time
TableLoadStartTimeCT = datetime.datetime.now()
print("TableLoadStartTimeCT:-", TableLoadStartTimeCT)

snowFlakeInfo = config['snow-flake']
sandschema = snowFlakeInfo['sandschema']
sandinternalstage = snowFlakeInfo['sandinternalstage']
dbname = snowFlakeInfo['database']

print("snowflake:--", snowFlakeInfo)
print("Schema:--" , sandschema)
print("dbname:--" , dbname)
print("user name: --->", snowFlakeInfo['username'])
print("Key path:--->", snowFlakeInfo['private_key_path'])


with open(snowFlakeInfo['private_key_path'], "rb") as key:
    p_key = serialization.load_pem_private_key(
        key.read(),
        password=snowFlakeInfo['passwordphrase'].encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

con_SF = snowflake.connector.connect(
    user=snowFlakeInfo['username'],
    private_key=pkb,
    role=snowFlakeInfo['role'],
    warehouse=snowFlakeInfo['warehouse'],
    database=snowFlakeInfo['database'],
    schema=snowFlakeInfo['sandschema'],
    account=snowFlakeInfo['account'],
    session_parameters={
        'QUERY_TAG': 'Python_Connection_'
    }
)

cur_SF = con_SF.cursor()
#sql = 'select current_date;'
#sql= 'put file:///Volumes/vanbalag/BHEDW/claim_type_desc.csv  @TCOC.my_csv_stage overwrite=TRUE auto_compress=true PARALLEL = 50;'
sql= 'put file:///Users/vanbalag/DIM_VENDOR_cons/stage_files/*  @TCOC.dim_vendor_stage overwrite=TRUE auto_compress=true PARALLEL = 50;'
print("SQL-->",sql)
result = cur_SF.execute(sql)

print(result)
# Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
# df = cur_SF.fetch_pandas_all()
# print(df)
print("Successfully copied file to SF")
print("---------------------------------------------------------------------------------")

