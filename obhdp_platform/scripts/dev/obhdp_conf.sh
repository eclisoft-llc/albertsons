#!/usr/bin/bash

set -x

#########################################################################################
# Script Name : obhdp_conf.sh
#
# Description : This configuration file will hold all the configuration paths used for mce.
#
#
#########################################################################################

Log_dir=/obhdp_dev/tcoc/d_logs/
config=/obhdp_dev/tcoc/d_conf
scripts=/obhdp_dev/tcoc/d_scripts
conf_dir=/obhdp_dev/tcoc/d_conf/
jars=/obhdp_dev/tcoc/d_jars/
DEL=/obhdp_dev/tcoc/d_sql/ETL/Load/
ETL=/obhdp_dev/tcoc/d_sql/ETL/Load/
FOUND=/obhdp_dev/tcoc/d_sql/FOUNDATION/Load/
outbound=/obhdp_dev/tcoc/d_outbound/
inbound=/obhdp_dev/tcoc/d_inbound/
COMPACT=/obhdp_dev/tcoc/d_sql/COMPACT/Load/
archive_fail=/obhdp_dev/tcoc/d_archive/fail
archive_success=/obhdp_dev/tcoc/d_archive/success
sql=/obhdp_dev/tcoc/d_sql/

#Unix environment
env=DEV
#SF Environment
sf_env=optum_dev
wh_name=ECT_DEV_OBH_DP_LOAD_WH
db_name=ECT_DEV_OBH_DP_DB
r_name=AR_DEV_OBHDPDEV_OPTUM_ROLE

email_from="obhdpdev@optum.com"
email_to="OBH_Elites@ds.uhc.com"

sfactname=uhgdwaas.east-us-2.azure
sfuser=obhdpdev@optum.com
keypath=/home/obhdpdev/new_key/rsa_key.p8
schemaname=COMPACT

orauser=SVC_OBHDPSTG
oradsn=ES06-SCAN01/aapst02.uhc.com


