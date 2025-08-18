#!/usr/bin/bash

set -x

#########################################################################################
# Script Name : obhdp_conf.sh
#
# Description : This configuration file will hold all the configuration paths used for mce.
#
#
#########################################################################################


Log_dir=/obhdp/tcoc/p_logs/
config=/obhdp/tcoc/p_conf
scripts=/obhdp/tcoc/p_scripts
conf_dir=/obhdp/tcoc/p_conf/
jars=/obhdp/tcoc/p_jars
DEL=/obhdp/tcoc/p_sql/ETL/Load/
ETL=/obhdp/tcoc/p_sql/ETL/Load/
FOUND=/obhdp/tcoc/p_sql/FOUNDATION/Load/
COMPACT=/obhdp/tcoc/p_sql/COMPACT/Load/
archive_fail=/obhdp/tcoc/p_archive/fail
archive_success=/obhdp/tcoc/p_archive/success
sql=/obhdp/tcoc/p_sql/

outbound=/obhdp/tcoc/p_outbound/
inbound=/obhdp/tcoc/p_inbound/

#Unix environment
env=PRD

#SF Environment
sf_env=optum_prd
wh_name=ECT_PRD_OBH_DP_LOAD_WH
db_name=ECT_PRD_OBH_DP_DB
r_name=AR_PRD_OBHDPPRD_OPTUM_ROLE

email_from="obhdpprd@optum.com"
email_to="OBH_Elites@ds.uhc.com"

sfactname=uhgdwaas.east-us-2.azure
sfuser=obhdpprd@optum.com
keypath=/home/obhdpprd/new_key/rsa_key.p8
schemaname=COMPACT

orauser=SVC_OBHDPPRD
oradsn=EP21-SCAN01/aappr03svc.uhc.com