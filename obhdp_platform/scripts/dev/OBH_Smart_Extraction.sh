#!/usr/bin/bash
set -x
#########################################################################################
# Script Name : OBH_Smart_Extraction.sh
# Parameters :  1. File Name
#				        2. File type
#
# Description : This Shell script will watch for a file in a specific location.
#
#
#########################################################################################


#######################################################################################################
#Function : msg
#Description : It will display and re-direct to the output file
#######################################################################################################
msg()
{
   echo "`date +%Y%m%d-%H:%M` $*"
   echo "`date +%Y%m%d-%H:%M` $*" >> $Log_Dir 2>&1
}

#######################################################################################################


#######################################################################################################
#Function : send_mail
#Description : Send the email with the R script information
#######################################################################################################
send_mail()
{
   subj=$1
   to_ls=$2
   attch=$3
   mailx -r $email_from -s "${subj}" -a ${attch} $to_ls << EOF
Team,
  Please find the subject line for details.

Note: Don't repsond to this email, it's automated email from Unix system.

EOF
}


#######################################################################################################
# 											Main Script
#######################################################################################################

#Source all the configurations
source /obhdp_dev/tcoc/d_conf/obhdp_conf.sh


log_name=`basename $0 | cut -f1 -d'.'`
Log_Dir=${Log_dir}/${log_name}_`date +%Y%m%d-%H:%M`.log
echo "${Log_dir}/${log_name}_`date +%Y%m%d-%H:%M`.log"

alias sql="/obhdp_dev/sqlcl/bin/sql"

if [ $# -eq 2 ]
then
  fname=$1
  fname_ext=`echo $2 |sed 's/_//g'`
  year_month=$2
  curr_date=`date +%Y%m%d`
  full_name=`echo ${fname}_${fname_ext}_${curr_date}.csv`
  msg "File Name is $full_name for export is started @ `date +%Y%m%d-%H:%M`"
  send_mail "File Name is $full_name for export is started" $email_to $Log_Dir
else
  msg "Parameter does not match"
   send_mail "parameter does not match" $email_to $Log_Dir
  exit 1
fi

sql_query="SELECT COMPANY_DIM_ID, CAST(SEQ_CLAIM_ID AS VARCHAR(40)) SEQ_CLAIM_ID, LINE_NUMBER, SUB_LINE_CODE, PRIMARY_SVC_DATE, PLAN_DIM_ID, CLAIM_NUMBER, CLAIM_TYPE_DIM_ID, IO_FLAG_DIM_ID, CLAIM_STATUS_DIM_ID, PROCESSING_STATUS_DIM_ID, MED_DEF_CODE_DIM_ID, POST_DATE, CHECK_DATE, CHECK_NUMBER, CHECK_NUMBER_2, CPT_CODE_DIM_ID, CPT_MODIFIER_DIM_ID, REVENUE_CODE_DIM_ID, DATE_RECEIVED, ADJUD_DATE, DETAIL_SVC_DATE, SVC_TO_DATE, PROV_DIM_ID, MEMB_DIM_ID, ALLOWED_REASON_DIM_ID, NOT_COVERED_REASON_DIM_ID, ADJUSTMENT_REASON_DIM_ID, CLAIM_THRU_DATE, MEMBER_AGE, PCP_DIM_ID, ATT_PROV_DIM_ID, PROV_SPEC_DIM_ID, PROV_TYPE_DIM_ID, PLACE_OF_SVC_1_DIM_ID, PLACE_OF_SVC_2_DIM_ID, PLACE_OF_SVC_3_DIM_ID, DIAGNOSIS_1_DIM_ID, DIAGNOSIS_2_DIM_ID, DIAGNOSIS_3_DIM_ID, DIAGNOSIS_4_DIM_ID, DIAGNOSIS_5_DIM_ID, DRG_CODE_DIM_ID, ICD_PRIM_DIAG, ICD_FLAG, ICD_PROC_1_DIM_ID, ICD_PROC_2_DIM_ID, ICD_PROC_3_DIM_ID, ICD_PROC_4_DIM_ID, ICD_PROC_5_DIM_ID, PAT_CONTROL_NO, ACTUAL_ADMISSION_DATE, ADMISSION_DATE, ADMIT_HOUR, ADMIT_SOURCE_DIM_ID, ADMIT_TYPE, ADMITTING_DIAGNOSIS_DIM_ID, DISCHARGE_HOUR, MEDICAL_RECORD_NO, EPSDT_IND, HCPCS_MODIFIER_1_DIM_ID, HCPCS_MODIFIER_2_DIM_ID, HCPCS_MODIFIER_3_DIM_ID, HCPCS_MODIFIER_4_DIM_ID, INVALID_DATA_IND, STATE_SUBMIT_DATE, EXTERNAL_CRN, BILL_TYPE_DIM_ID, LINE_OF_BUSINESS_DIM_ID, PATIENT_STATUS, DIAMOND_INSERT_DATE, VEND_DIM_ID, MEDIA_CODE_DIM_ID, AUTO_ADJUD_FLAG_DIM_ID, EDI_FLAG_DIM_ID, ER_FLAG_DIM_ID, UNCLEAN_FLAG_DIM_ID, QUANTITY, OTHER_CARRIER_AMT, BILLED_AMT, ALLOWED_AMT, NOT_COVERED_AMT, NET_AMT, PAID_NET_AMT, DW_INSERT_DATETIME, DW_UPDATE_DATETIME, INTEREST_AMT, WHOLE_CLAIM_STATUS_DIM_ID, PROVIDER_PAR_DIM_ID, COPAYMENT_1_AMT, COPAYMENT_2_AMT, DEDUCTIBLE_AMT, CALC_ALLO_PAID_NET_AMT, CALC_ALLO_LINE_NUMBER, COS_STL_4_DIM_ID, CONVERTED_FROM_DIM_ID, RX_GENERIC_BRAND_IND_DIM_ID, RX_SUPPLY_DAYS, CALC_ALLO_METHOD, CALC_VISITS, CALC_UNITS, CALC_PROCEDURES, CALC_DAYS, CALC_ADMITS, DTL_COS_STL_4_DIM_ID, GROUP_DIM_ID, PRODUCT_DIM_ID, DIAGNOSIS_6_DIM_ID, DIAGNOSIS_7_DIM_ID, DIAGNOSIS_8_DIM_ID, DIAGNOSIS_9_DIM_ID, TOTAL_BILLED_AMT, BATCH_NUMBER, CALC_QUANTITY, AUTH_NUMBER, DUPLICATE_CLAIM_FLAG_DIM_ID, DISCOUNT_AMT, WITHHOLD_AMT, ORIG_CHECK_DATE, ORIG_CHECK_NUMBER, RPS_FISCAL_DATE, COSMOS_CUST_SEG_DIM_ID, NDC_CODE_DIM_ID, RX_DISPENSING_FEE_AMT, RX_INGREDIENT_AMT, RX_FORMULARY_IND_DIM_ID, CARE_LEVEL_DIM_ID, DW_INSERT_PROCESS, DW_UPDATE_PROCESS, SYS1, SYS2, SYS3, SYS4, SYS5, SYS6, PLAN_LOB_DIM_ID, CONTRACT_DIM_ID, EOB_IND_DIM_ID, OC_ALLOWED_AMT, OC_PAID_REASON_DIM_ID, RX_DATE_PRESCRIPTION_WRITTEN, SEQ_AP_TRANS_ID, REF_PROV_DIM_ID, PCP_TYPE_DIM_ID, PCP_SPEC_DIM_ID, GL_TAG_DIM_ID, RX_PRESCRIPTION_NUMBER, RX_ORIG_BILLED_AMT, ORIG_BATCH_NUMBER, DIAGNOSIS_10_DIM_ID, DIAGNOSIS_11_DIM_ID, DIAGNOSIS_12_DIM_ID, DIAGNOSIS_13_DIM_ID, DIAGNOSIS_14_DIM_ID, DIAGNOSIS_15_DIM_ID, DIAGNOSIS_16_DIM_ID, DIAGNOSIS_17_DIM_ID, DIAGNOSIS_18_DIM_ID, CPT_MODIFIER_2_DIM_ID, CPT_MODIFIER_3_DIM_ID, CPT_MODIFIER_4_DIM_ID, COB_CARRIER_CODE_DIM_ID, EOB_ATTACHED, ORIG_DRG_DIM_ID, ICD_PROC_6_DIM_ID, ICD_PROC_1_DATE, ICD_PROC_2_DATE, ICD_PROC_3_DATE, ICD_PROC_4_DATE, ICD_PROC_5_DATE, ICD_PROC_6_DATE, SOI_DIM_ID, ROM_DIM_ID, CLAIM_IMAGE_ID, ORIG_SEQ_CLAIM_ID, DIAGNOSIS_1_POA, DIAGNOSIS_2_POA, DIAGNOSIS_3_POA, DIAGNOSIS_4_POA, DIAGNOSIS_5_POA, DIAGNOSIS_6_POA, DIAGNOSIS_7_POA, DIAGNOSIS_8_POA, DIAGNOSIS_9_POA, DIAGNOSIS_10_POA, DIAGNOSIS_11_POA, DIAGNOSIS_12_POA, DIAGNOSIS_13_POA, DIAGNOSIS_14_POA, DIAGNOSIS_15_POA, DIAGNOSIS_16_POA, DIAGNOSIS_17_POA, DIAGNOSIS_18_POA, FEE_SCHEDULE_DIM_ID, CLAIM_NUMBER_ADJ_FROM, CLAIM_NUMBER_ADJ_TO, BIRTH_WEIGHT, RX_GPI_DIM_ID, RX_BILLING_ACCT_DIM_ID, MEDICARE_TYPE_DIM_ID, CONDITION_CODE_1_DIM_ID, CONDITION_CODE_2_DIM_ID, CONDITION_CODE_3_DIM_ID, CONDITION_CODE_4_DIM_ID, CONDITION_CODE_5_DIM_ID, CONDITION_CODE_6_DIM_ID, CONDITION_CODE_7_DIM_ID, CONDITION_CODE_8_DIM_ID, CONDITION_CODE_9_DIM_ID, CONDITION_CODE_10_DIM_ID, CONDITION_CODE_11_DIM_ID, ADJ_SEQ_CLAIM_ID, PRICER_BASE_REIMB_AMT, PRICER_OUTLIER_PAYMENTS_AMT, PRICER_ALT_LEVEL_CARE_PYMT_AMT, PRICER_TOTAL_REIMB_AMT, PRICER_OUTLIER_TYPE_DIM_ID, PRICER_AVERAGE_LOS, ADJUDICATION_METHOD_DIM_ID, EFT_TRANS_NUMBER, ATT_PROV_NPI, PROVIDER_NPI, DIAGNOSIS_19_DIM_ID, DIAGNOSIS_20_DIM_ID, DIAGNOSIS_21_DIM_ID, DIAGNOSIS_22_DIM_ID, DIAGNOSIS_23_DIM_ID, DIAGNOSIS_24_DIM_ID, DIAGNOSIS_25_DIM_ID, DIAGNOSIS_19_POA, DIAGNOSIS_20_POA, DIAGNOSIS_21_POA, DIAGNOSIS_22_POA, DIAGNOSIS_23_POA, DIAGNOSIS_24_POA, DIAGNOSIS_25_POA, ICD_PROC_7_DIM_ID, ICD_PROC_8_DIM_ID, ICD_PROC_9_DIM_ID, ICD_PROC_10_DIM_ID, ICD_PROC_11_DIM_ID, ICD_PROC_12_DIM_ID, ICD_PROC_13_DIM_ID, ICD_PROC_14_DIM_ID, ICD_PROC_15_DIM_ID, ICD_PROC_16_DIM_ID, ICD_PROC_17_DIM_ID, ICD_PROC_18_DIM_ID, ICD_PROC_19_DIM_ID, ICD_PROC_20_DIM_ID, ICD_PROC_21_DIM_ID, ICD_PROC_22_DIM_ID, ICD_PROC_23_DIM_ID, ICD_PROC_24_DIM_ID, ICD_PROC_25_DIM_ID, ICD_PROC_7_DATE, ICD_PROC_8_DATE, ICD_PROC_9_DATE, ICD_PROC_10_DATE, ICD_PROC_11_DATE, ICD_PROC_12_DATE, ICD_PROC_13_DATE, ICD_PROC_14_DATE, ICD_PROC_15_DATE, ICD_PROC_16_DATE, ICD_PROC_17_DATE, ICD_PROC_18_DATE, ICD_PROC_19_DATE, ICD_PROC_20_DATE, ICD_PROC_21_DATE, ICD_PROC_22_DATE, ICD_PROC_23_DATE, ICD_PROC_24_DATE, ICD_PROC_25_DATE, RTCL_RATE_DIM_ID, ICD_VER_IND, SYS10, SYS11, SYS12, SYS13, SYS14, REF_PROVIDER_NPI, SOURCE_PRODUCT_DIM_ID, CLASS_DIM_ID, CLASS_PLAN_DIM_ID, BILLED_QUANTITY, EOB_DIM_ID, DTL_EOB_DIM_ID, PROV_AGREEMENT_DIM_ID, SDA_DIM_ID, SRC_SERVICE_NPI, SRC_BILLING_NPI, SRC_ADMITTING_NPI, SRC_OPERATING_NPI, DIAG_POINTER_1, DIAG_POINTER_2, DIAG_POINTER_3, DIAG_POINTER_4, DIAG_POINTER_5, DIAG_POINTER_6, DIAG_POINTER_7, DIAG_POINTER_8 FROM DW.FACT_CLAIM WHERE SEQ_CLAIM_ID <> 0 AND DETAIL_SVC_DATE = TO_DATE(@${year_month}@,'yyyy-mm-dd');"

sql_query1=`echo $sql_query | sed "s/@/'/g"`
msg $sql_query
echo $sql_query1 > /obhdp_dev/tcoc/d_logs/fileq_${fname_ext}.sql
temp=`echo $sql_query1 | sed 's/.$//g'`
echo "select count(1) from ($temp);" > /obhdp_dev/tcoc/d_logs/filec_${fname_ext}.sql

sql UHG_000866596@'ep05:1521/aappr03svc.uhc.com' <<EOF
<your password>
set trimspool on
set echo off
set feedback on
set termout off
SET SERVEROUTPUT OFF
alter session set nls_date_format='YYYY-MM-DD';
set sqlformat delimited |
spool /obhdp_dev/tcoc/d_outbound/${full_name}
`cat /obhdp_dev/tcoc/d_logs/fileq_${fname_ext}.sql`
spool off
spool /obhdp_dev/tcoc/d_outbound/${full_name}_c
`cat /obhdp_dev/tcoc/d_logs/filec_${fname_ext}.sql`
spool off
EOF

tblcount=`tail -2 /obhdp_dev/tcoc/d_outbound/${full_name}  | head -1 | awk -F ' ' '{ print $1 }'`
flcount=`cat /obhdp_dev/tcoc/d_outbound/${full_name}_c | head -2 | tail -1`

if [ $tblcount -eq  $flcount ]
then
  msg "File Name is $full_name for export is completed successfully @ `date +%Y%m%d-%H:%M`"
  send_mail "File Name is $full_name for export is completed successfully" $email_to $Log_Dir
else
  msg "File name is $full_name for export is failed  @ `date +%Y%m%d-%H:%M`"
  send_mail "File Name is $full_name for export is failed " $email_to $Log_Dir
fi
#######################################################################################################
# 											End Script
#######################################################################################################
