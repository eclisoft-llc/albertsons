#!/usr/bin/bash
set -x
#########################################################################################
# Script Name : OBH_File_Trnfr_SF_new.sh
# Parameters :  1. Database Name
#               2. Schema Name
#               3. Full path of folder to be transfer
#
# Description : This Shell script will transfer files from on-prem to Snowflake(SF)
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
   echo "`date +%Y%m%d-%H:%M` $*" >> $Log_Dir1 2>&1
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
Log_Dir1=${Log_dir}/${log_name}_`date +%Y%m%d-%H:%M`.log
echo "${Log_dir}/${log_name}_`date +%Y%m%d-%H:%M`.log"

if [ $# -eq 4 ]
then
  db_name=$1
  sc_name=$2
  fd_name=$3
  INTERNAL_STAGE=$4
  msg "Parameters passed db_name:${db_name} sc_name:${sc_name}  fd_name:${fd_name} INTERNAL_STAGE:${INTERNAL_STAGE}@ `date +%Y%m%d-%H:%M`"
else
  msg "Parameter does not match,it should db_name(database name) sc_name(schema name) fd_name(full path of folder) INTERNAL_STAGE(internal stage location)"
  send_mail "parameter does not match" $email_to $Log_Dir
  exit 1
fi

if [ -f "${fd_name}" ]; then
  # Take action if $DIR exists. #
  msg "${fd_name} exist and start transfer to snowflake"
else
  msg "${fd_name} does not exist and start transfer to snowflake"
  exit 1
fi

ls ${fd_name} | while read line
do
  msg "use ${db_name};"
  msg "file name : $line"
  msg "${Log_dir}/test.sql"
  echo "use database ${db_name};" > ${Log_dir}/test.sql
  echo "use SCHEMA ${sc_name};" >> ${Log_dir}/test.sql
  echo "put file:///${fd_name} @${sc_name}.${INTERNAL_STAGE} overwrite=TRUE auto_compress=true PARALLEL = 99;" >> ${Log_dir}/test.sql
  echo "!quit;" >> ${Log_dir}/test.sql
  msg "created transfer sql to initate snowsql"
  echo "snowsql -c optum_poc  -f ${Log_dir}/test.sql"
  snowsql -c optum_poc  -f ${Log_dir}/test.sql
  if [ $? -eq 0 ]
  then
    msg "${fd_name}/${line} completed successfully"
    #mv ${Log_dir}/${line}.sql ${Log_dir}/Success_${line}.sql
    #send_mail "File Transfer completed for $fd_name and file name:$line" $email_to $Log_Dir1
  else
    msg "${fd_name}/${line} failed to transfer"
    #mv ${Log_dir}/${line}.sql ${Log_dir}/failed_${line}.sql
    send_mail "File Transfer failed for $fd_name and file name:$line" $email_to $Log_Dir1
  fi
done
