#!/usr/bin/bash
set -x
#########################################################################################
# Script Name : OBH_Smart_Sql_Exec.sh
# Parameters :  1. Database Name
#               2. Schema Name
#               3. Full path of script to be executed
# Description : This Shell script will execute the sql script from on-prem to Snowflake(SF)
# Note - This script is used both in Jenkins pipeline aswell as Loop script for data load, so any changes to the script should check for impact at both places
#########################################################################################

# Setting environment variable for Snowsql
source ~/.bash_profile
log_name=$(basename $0 | cut -f1 -d'.')

#######################################################################################################
#Function : msg
#Description : It will display and re-direct to the output file
#######################################################################################################
msg() {
  echo "$(date +%Y%m%d-%H:%M) $*"
  echo "$(date +%Y%m%d-%H:%M) $*" >>$log_directory 2>&1
}
#######################################################################################################

#######################################################################################################
#Function : send_mail
#Description : Send the email with logs
#######################################################################################################
send_mail() {
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
# Main Script
#######################################################################################################
# It required three parameter to initiate the script
if [ $# -eq 3 ]; then

  # Setting other parameters
  env_name=$1
  sc_name=$2
  file_name=$3

  if [ $1 == "DEV" ] || [ $1 == "dev" ]
  then
    #Source all the configurations
    source /obhdp_dev/tcoc/d_conf/obhdp_conf.sh
    echo "SQL script initiate for DEV environment"
  elif [ $1 == "STG" ] || [ $1 == "stg" ]
  then
    source /obhdp/tcoc/s_conf/obhdp_conf.sh
    echo "SQL script initiate for STG environment"
  elif [ $1 == "PRD" ] || [ $1 == "prd" ]
  then
    source /obhdp/tcoc/p_conf/obhdp_conf.sh
    echo "SQL script initiate for PRD environment"
  else
     echo "SQL script initialization failed, environment is not set-up for $1"
     exit 1
  fi
else
   echo "Parameter does not match, db_name:${db_name} sc_name:${sc_name} file_name:${fd_name}"
   exit 1
fi

log_directory=${Log_dir}/${log_name}_$(date +%Y%m%d_%H%M%S).log
echo "${Log_dir}/${log_name}_$(date +%Y%m%d_%H%M%S).log"
sql_file=${Log_dir}/${log_name}_$(date +%Y%m%d_%H%M%S).sql


if [ -f "${file_name}" ]; then
  # Take action if $DIR exists
  msg "${file_name} exist and will execute in snowflake"
else
  msg "${file_name} does not exist and failed to execute snowflake"
  exit 1
fi

msg "Sql to initiate into snowsql on Snowflake env : $1"
echo "use role ${r_name};" >>${sql_file}
echo "use database ${db_name};" >>${sql_file}
echo "use schema ${sc_name};" >>${sql_file}
#identify the execution required warehouse or not
wh=$(cat ${file_name} | grep -i 'create' | wc -l)
if [ $wh -eq 1 ]
then
  msg "it doesn't required warehouse"
else
  msg "It required warehouse to perform SQL operations"
  echo "use warehouse ${wh_name};" >>${sql_file}
fi
cat ${file_name} >>${sql_file}
echo "snowsql -c ${sf_env}  -f ${sql_file}"
msg "Snowsql initiate for ${sql_file}"
snowsql -c ${sf_env} -f ${sql_file} >>$log_directory 2>&1
sfs=$(echo $?)
error=$(cat $log_directory | egrep -i 'error:|failed|cannot' | wc -l)
if [[ $sfs -eq 0 && $error -eq 0 ]]; then
  msg "${file_name} executed successfully"
  #send_mail "SQL executed successfully for ${file_name} " $email_to $log_directory
else
  msg "${file_name} failed to execute, please look into the script"
  send_mail "SQL failed for ${file_name}" $email_to $log_directory
  exit 1
fi
