#!/usr/bin/bash

set -x

#########################################################################################
# Script Name : obhdp_conf.sh
#
# Description : This configuration file will hold all the configuration paths used for mce.
#
#
#########################################################################################


Log_dir=/obhdp/tcoc/s_logs/
config=/obhdp/tcoc/s_conf
scripts=/obhdp/tcoc/s_scripts
conf_dir=/obhdp/tcoc/s_conf/
jars=/obhdp/tcoc/s_jars
DEL=/obhdp/tcoc/s_sql/ETL/Load/
ETL=/obhdp/tcoc/s_sql/ETL/Load/
FOUND=/obhdp/tcoc/s_sql/FOUNDATION/Load/
outbound=/obhdp/tcoc/s_outbound/
inbound=/obhdp/tcoc/s_inbound/
COMPACT=/obhdp/tcoc/s_sql/COMPACT/Load/
archive_fail=/obhdp/tcoc/s_archive/fail
archive_success=/obhdp/tcoc/s_archive/success
sql=/obhdp/tcoc/s_sql/

#unix environment
env=STG
#SF Environment
sf_env=optum_stg
wh_name=ECT_STG_OBH_DP_LOAD_WH
db_name=ECT_STG_OBH_DP_DB
r_name=AR_STG_OBHDPSTG_OPTUM_ROLE

email_from="obhdpstg@optum.com"
email_to="OBH_Elites@ds.uhc.com"

sfactname=uhgdwaas.east-us-2.azure
sfuser=obhdpstg@optum.com
keypath=/home/obhdpstg/new_key/rsa_key.p8
schemaname=COMPACT

orauser=SVC_OBHDPPRD
oradsn=EP21-SCAN01/aappr03svc.uhc.com