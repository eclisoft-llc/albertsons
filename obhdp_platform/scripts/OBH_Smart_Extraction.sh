#!/usr/bin/bash
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

alias sql="/obhdp_dev/Installtion_packages/sqlcl/bin/sql"

if [ $# -eq 1 ]
then
  tname=$1
  curr_date=`date +%Y%m%d`
  full_name=`echo ${tname}_${curr_date}.csv`

  msg "File Name is $full_name for export is started @ `date +%Y%m%d-%H:%M`"
  send_mail "File Name is $full_name for export is started" $email_to $Log_Dir
else
  msg "Parameter does not match"
   send_mail "parameter does not match" $email_to $Log_Dir
  exit 1
fi

#sql_query1=`cat ${conf_dir}/smart_select.json | grep -iw "${tname}" | cut -f2 -d':' | sed 's/"//g' | sed 's/,//g'`
sql_query1=`cat ${conf_dir}/smart_select.json | grep -iw "${tname}" | cut -f2 -d':' | sed 's/"//g' `
msg $sql_query
echo "${sql_query1}" | sed 's/$/;/g' > /obhdp_dev/tcoc/d_logs/fileq_${tname}.sql
echo "select count(1) from ($sql_query1);" > /obhdp_dev/tcoc/d_logs/filec_${tname}.sql

java -cp ${jars}/PBEEncryption_Optum.jar com.optum.peds.dl.common.AesDesCrypt decrypt `cat ${conf_dir}/smart_unix.conf` OBH_Smart1@ > ${outbound}/output.txt
pwd=`cat ${outbound}/output.txt`
#changing premission of the output file to avoid opening to read
chmod 400 ${outbound}/output.txt

#sql UHG_000866596@'ep05:1521/aappr03svc.uhc.com' <<EOF
sql SVC_OBHDPPRD@'EP21-SCAN01/aappr03svc.uhc.com' <<EOF
`echo $pwd`
set trimspool on
set echo off
set feedback on
set termout off
SET SERVEROUTPUT OFF
alter session set nls_date_format='YYYY-MM-DD';
set sqlformat delimited |
spool /obhdp_dev/tcoc/d_outbound/${full_name}
`cat /obhdp_dev/tcoc/d_logs/fileq_${tname}.sql`
spool off
spool /obhdp_dev/tcoc/d_outbound/${full_name}_c
`cat /obhdp_dev/tcoc/d_logs/filec_${tname}.sql`
spool off
EOF

tblcount=`tail -2 /obhdp_dev/tcoc/d_outbound/${full_name}  | head -1 | awk -F ' ' '{ print $1 }' | sed 's/,//g'`
flcount=`cat /obhdp_dev/tcoc/d_outbound/${full_name}_c | head -2 | tail -1 | sed 's/,//g'`

sed -i '/rows selected/d' /obhdp_dev/tcoc/d_outbound/${full_name}
sed -i '/row selected/d' /obhdp_dev/tcoc/d_outbound/${full_name}
sed  's/"//g' /obhdp_dev/tcoc/d_outbound/${full_name} | sed '/^$/d' > /obhdp_dev/tcoc/d_outbound/${full_name}_temp
mv /obhdp_dev/tcoc/d_outbound/${full_name}_temp /obhdp_dev/tcoc/d_outbound/${full_name}

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
