#!/usr/bin/bash
set -x
#########################################################################################
# Script Name : OBH_File_Trnfr_SF.sh
# Parameters :  1. Database Name
#               2. Schema Name
#               3. Full path of folder to be transfer
#
# Description : This Shell script will transfer files from on-prem to Snowflake(SF)
#
#
#########################################################################################

#setting environment variable for Snowsql
source ~/.bash_profile


log_name=`basename $0 | cut -f1 -d'.'`
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
#Description : Send the email with logs
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
#Function : Transfer complete folder to Snowflake
#######################################################################################################
sf_trnsf_flder()
{
dir_name=$1
cd $dir_name
tot_file=`ls | wc -l`
if [ $tot_file -gt 0 ]
then
msg "Total # of files to be transfered : $tot_file"
else
msg "Total # of files to be transfered is 0 "
send_mail "Total # of files to be transfered is 0" $email_to $Log_Dir1
exit 1
fi

ls | while read line
do
  msg "use ${db_name};"
  msg "file name : $line"
  msg "${Log_dir}/${line}.sql"
  echo "use ${db_name};" > ${Log_dir}/${line}.sql
  echo "use SCHEMA ${sc_name};" >> ${Log_dir}/${line}.sql
  echo "put file:///${fd_name}/${line} @${sc_name}.${INTERNAL_STAGE} overwrite=TRUE auto_compress=true PARALLEL = 99;" >> ${Log_dir}/${line}.sql
  echo "!quit;" >> ${Log_dir}/${line}.sql
  msg "created transfer sql to initate snowsql"
  echo "snowsql -c ${sf_env}  -f ${Log_dir}/${line}.sql"
  snowsql -c ${sf_env}  -f ${Log_dir}/${line}.sql
  if [ $? -eq 0 ]
  then
    msg "${dir_name}/${line} completed successfully"
    #mv ${Log_dir}/${line}.sql ${Log_dir}/Success_${line}.sql
    #send_mail "File Transfer completed for $fd_name and file name:$line" $email_to $Log_Dir1
  else
    msg "${dir_name}/${line} failed to transfer"
    mv ${Log_dir}/${line}.sql ${Log_dir}/failed_${line}.sql
    send_mail "File Transfer failed for $dir_name and file name:$line" $email_to $Log_Dir1
  fi
done
}

#######################################################################################################
#Function : Transfer complete file to Snowflake
#######################################################################################################
sf_trnsf_file()
{
ls ${fd_name} | while read line
do
  msg "use ${db_name};"
  msg "file name : $line"
  msg "${Log_dir}/SF.sql"
  echo "use ${db_name};" > ${Log_dir}/SF.sql
  echo "use SCHEMA ${sc_name};" >> ${Log_dir}/SF.sql
  echo "put file:///${fd_name} @${sc_name}.${INTERNAL_STAGE} overwrite=TRUE auto_compress=true PARALLEL = 99;" >> ${Log_dir}/SF.sql
  echo "!quit;" >> ${Log_dir}/SF.sql
  msg "created transfer sql to initate snowsql"
  echo "snowsql -c ${sf_env}  -f ${Log_dir}/SF.sql"
  snowsql -c ${sf_env}  -f ${Log_dir}/SF.sql
  if [ $? -eq 0 ]
  then
    msg "File ${line} completed successfully"
    #mv ${Log_dir}/${line}.sql ${Log_dir}/Success_${line}.sql
    #send_mail "File Transfer completed for $fd_name and file name:$line" $email_to $Log_Dir1
  else
    msg "${line} failed to transfer"
    #mv ${Log_dir}/${line}.sql ${Log_dir}/failed_${line}.sql
    send_mail "File Transfer failed for $fd_name and file name:$line" $email_to $Log_Dir1
  fi
done

}

#######################################################################################################
# 											Main Script
#######################################################################################################

if [ $# -eq 5 ]
then

    if [ $1 == "DEV" ]
    then
      #Source all the configurations
      source /obhdp_dev/tcoc/d_conf/obhdp_conf.sh
      echo "File transfer initiate for DEV environment"
    elif [ $1 == "STG" ]
    then
      source /obhdp_dev/tcoc/s_conf/obhdp_conf.sh
      echo "File transfer initiate for STG environment"
    elif [ $1 == "PRD" ]
    then
      source /obhdp/tcoc/p_conf/obhdp_conf.sh
      echo "File transfer initiate for PRD environment"

    else
       echo "File transfer failed, enrivonment is not set-up for $1"
       exit 1
    fi
else
   echo "Parameter does not match,,it should envrionment db_name(database name) sc_name(schema name) fd_name(full path of folder) INTERNAL_STAGE(internal stage location)"
   exit 1
fi

Log_Dir1=${Log_dir}/${log_name}_`date +%Y%m%d-%H:%M`.log
echo "${Log_dir}/${log_name}_`date +%Y%m%d-%H:%M`.log"

##Setting other parameters
db_name=$2
sc_name=$3
fd_name=$4
INTERNAL_STAGE=$5




if [ -d "${fd_name}" ]; then
  # Take action if $DIR exists. #
  msg "${fd_name} exist and start transfer to snowflake"
  sf_trnsf_flder ${fd_name}
  if [ $? -eq 0 ]
  then
    msg "Folder ${fd_name} transfer completed successfully"
    send_mail "Folder ${fd_name} transfer completed successfully" $email_to $Log_Dir1
    exit 0
  else
    msg "Folder ${fd_name} transfer failed, please look into the logs"
    send_mail "Folder ${fd_name} transfer failed, please look into the logs" $email_to $Log_Dir1
  fi
elif [ -f "${fd_name}" ]; then
  msg "$fd_name is a file and will start transfer to snowflake"
  sf_trnsf_file ${fd_name}
  if [ $? -eq 0 ]
  then
    msg "File ${fd_name} transfer completed successfully"
    send_mail "File ${fd_name} transfer completed successfully" $email_to $Log_Dir1
    exit 0
  else
    msg "File ${fd_name} transfer failed, please look into the logs"
    send_mail "File ${fd_name} transfer failed, please look into the logs" $email_to $Log_Dir1
  fi
else
  msg "${fd_name} does not exist and start transfer to snowflake"
  exit 1
fi
