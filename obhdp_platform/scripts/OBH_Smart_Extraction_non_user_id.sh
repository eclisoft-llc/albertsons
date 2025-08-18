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

alias sql="/obhdp_dev/Installtion_packages/sqlcl/bin/sql"

#######################################################################################################
# 											Main Script
#######################################################################################################

# It required 5 parameter to initiate the script
if [ $# -eq 2 ]
then

    if [ $1 == "DEV" ]
    then
      #Source all the configurations
      source /obhdp_dev/tcoc/d_conf/obhdp_conf.sh
      echo "Application will be initiate in $1 environment"
    elif [ $1 == "STG" ]
    then
      source /obhdp/tcoc/s_conf/obhdp_conf.sh
      echo "Application will be initiate in $1 environment"
    elif [ $1 == "PRD" ]
    then
      source /obhdp/tcoc/p_conf/obhdp_conf.sh
      echo "Application will be initiate in $1 environment"

    else
       echo "Script initiate failed, environment is not set-up for $1"
       exit 1
    fi
else
   echo "Parameter does not match,it should environment environment name:${1} process: START/STOP/RESTART"
   exit 1
fi


log_name=`basename $0 | cut -f1 -d'.'`
Log_Dir=${Log_dir}/${log_name}_`date +%Y%m%d-%H:%M`.log
echo "${Log_dir}/${log_name}_`date +%Y%m%d-%H:%M`.log"

#Passing SQL for data Validation from SMART.
 SQL_Fname=$2
 curr_date=`date +%Y%m%d`
 full_name=`echo ${SQL_Fname}_${curr_date}.csv`

cat ${SQL_Fname} | sed 's/$/;/g' > ${Log_dir}/fileq_${full_name}.sql
echo "select count(1) from (`cat $SQL_Fname`);" > ${Log_dir}/filec_${full_name}.sql

#Decrypting non-user id credentail to execute SQL
java -cp ${jars}/PBEEncryption_Optum.jar com.optum.peds.dl.common.AesDesCrypt decrypt `cat ${config}/smart_unix.conf` OBH_Smart1@ > ${Log_dir}/output.txt
pwd=`cat ${Log_dir}/output.txt`


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
spool ${outbound}/${full_name}
`cat ${Log_dir}/fileq_${full_name}.sql`
spool off
spool ${outbound}/${full_name}_c
`cat ${Log_dir}/filec_${full_name}.sql`
spool off
EOF

tblcount=`tail -2 ${outbound}/${full_name}  | head -1 | awk -F ' ' '{ print $1 }' | sed 's/,//g'`
flcount=`cat ${outbound}/${full_name}_c | head -2 | tail -1 | sed 's/,//g'`

sed -i '/rows selected/d' ${outbound}/${full_name}
sed -i '/row selected/d' ${outbound}/${full_name}
sed  's/"//g' ${outbound}/${full_name} | sed '/^$/d' > ${outbound}/${full_name}_temp
mv ${outbound}/${full_name}_temp ${outbound}/${full_name}

if [ $tblcount -eq  $flcount ]
then
  msg "File Name is $full_name for export is completed successfully @ `date +%Y%m%d-%H:%M`"
  send_mail "File Name is $full_name for export is completed successfully" $email_to $Log_Dir
else
  msg "File name is $full_name for export is failed  @ `date +%Y%m%d-%H:%M`"
  send_mail "File Name is $full_name for export is failed " $email_to $Log_Dir
fi

rm -rf ${Log_dir}/output.txt
#######################################################################################################
# 											End Script
#######################################################################################################
