#!/usr/bin/bash
set -x
#########################################################################################
# Script Name : OBH_Smart_Trigger.sh
# Parameters :
# Description : This Shell script will trigger data load script based on .start files in d_outbound
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
# 											Main Script
#######################################################################################################
# It required 1 parameter to initiate the script
if [ $# -eq 1 ]
then

    if [ $1 == "DEV" ]
    then
      #Source all the configurations
      source /obhdp_dev/tcoc/d_conf/obhdp_conf.sh
      msg "SQL script initiate for DEV environment"
    elif [ $1 == "STG" ]
    then
      source /obhdp/tcoc/s_conf/obhdp_conf.sh
      msg "SQL script initiate for STG environment"
    elif [ $1 == "PRD" ]
    then
      source /obhdp/tcoc/p_conf/obhdp_conf.sh
      msg "SQL script initiate for PRD environment"

    else
       msg "SQL script initiate failed, enrivonment is not set-up for $1"
       exit 1
    fi
else
   msg "Parameter does not match,it should envrionment db_name:${db_name} sc_name:${sc_name}  Snowflake_Role_name file_name:${fd_name}"
   exit 1
fi

Log_Dir1=${Log_dir}/${log_name}_`date +%Y%m%d_%H%M%S`.log
echo "${Log_dir}/${log_name}_`date +%Y%m%d_%H%M%S`.log"
sql_file=${Log_dir}/${log_name}_`date +%Y%m%d_%H%M%S`.sql

#change to inbound folder to validate any .start file exit
cd $inbound
cnt_file=`ls *.start | wc -l`
if [ $cnt_file -ge 1 ]
then
  #create lock file
  touch $outbound/exec.loc
 msg "# of start files are $cnt_file and greater than 1 to trigger it"
 #send_mail "# of start files are $cnt_file and greater than 1 to trigger it" $email_to $Log_Dir1
 tb_name=`ls *.start | head -1 | cut -f1 -d'.'`
 msg "Identified table $tb_name to trigger it"
 sh $scripts/OBH_Smart_Data_Load.sh $env $tb_name
 if [ $? -eq 0 ]
 then
   msg "$tb_name triggered, please validate logs"
   rm -f ${tb_name}.start $outbound/exec.loc
    if [ $? -eq 0 ]
    then
      msg "Removed start & lock file successfully"
    else
       msg "Removed start & lock file failed , please validate logs"
       send_mail "$tb_name Removed start file failed"
    fi
 else
    msg "$tb_name triggered and filed,please validate logs"
    rm -f $outbound/exec.loc
    msg "Lock file will be removed to process another table"
    exit 1
  fi
 else
 msg "# of start files are $cnt_file and no process will trigger it"
 #send_mail "# of start files are $cnt_file and no process will trigger it" $email_to $Log_Dir1
fi
