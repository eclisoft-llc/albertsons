#!/usr/bin/bash
set -x
#########################################################################################
# Script Name : OBH_Smart_Data_Load.sh
# Parameters :
# Description : This Shell script will trigger sequence of SQL's to load ETL/FOUNDATION
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
# It required 2 parameter to initiate the script
if [ $# -eq 2 ]
then

    if [ $1 == "DEV" ]
    then
      #Source all the configurations
      source /obhdp_dev/tcoc/d_conf/obhdp_conf.sh
      msg "SQL script initiate for DEV environment"
    elif [ $1 == "STG" ]
    then
      source /obhdp_dev/tcoc/s_conf/obhdp_conf.sh
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
   msg "Parameter does not match,it should envrionment[DEV/STG/PRD] table_name"
   exit 1
fi

Log_Dir1=${Log_dir}/${log_name}_`date +%Y%m%d_%H%M%S`.log
echo "${Log_dir}/${log_name}_`date +%Y%m%d_%H%M%S`.log"
sql_file=${Log_dir}/${log_name}_`date +%Y%m%d_%H%M%S`.sql

##Setting other parameters
t_name=$2
msg "Table name to trigger load execution is : ${t_name}"
#Extract list of scripts to be executed to Truncate/ETL/LOAD
lst=`cat ${conf_dir}/smart_sf.config | grep -iw ${t_name}`
if [ `echo $lst | wc -l` -gt 0 ]
then
    msg "Table name: $2  exist in Config, it will continue the load"
echo $lst | cut -f2- -d'|' | tr -s '|' '\n' | while read line
do
  if [[ $line == "DEL" ]]
  then
    cd $DEL
    sql_fname=` ls | grep -i DEL_SMART_${t_name}_L.sql`
    msg "sh ${scripts}/OBH_Smart_Sql_Exec.sh $1 ETL ${DEL}/${sql_fname}"
    sh ${scripts}/OBH_Smart_Sql_Exec.sh $1 ETL ${DEL}/${sql_fname}
    if [ $? -eq 0 ]
    then
      msg "$line complete for ${t_name}"
      #send_mail "$line completed for ${t_name}" $email_to $Log_Dir1
   else
     msg "$line failed for ${t_name}"
     send_mail "$line failed for ${t_name}" $email_to $Log_Dir1
   fi
  elif [[ $line == "ETL" ]]
  then
    cd $ETL
    sql_fname=` ls | grep -iw SMART_${t_name}_L.sql`
    msg "sh ${scripts}/OBH_Smart_Sql_Exec.sh $1 ETL ${DEL}/${sql_fname}"
    sh ${scripts}/OBH_Smart_Sql_Exec.sh $1 ETL ${DEL}/${sql_fname}
    if [ $? -eq 0 ]
    then
      msg "$line complete for ${t_name}"
      #send_mail "$line completed for ${t_name}" $email_to $Log_Dir1
   else
     msg "$line failed for ${t_name}"
     send_mail "$line failed for ${t_name}" $email_to $Log_Dir1
   fi
  elif   [[ $line == "FOUND" ]]
  then
    cd $FOUND
    sql_fname=` ls | grep -iw FOUND_${t_name}_LD.sql`
      msg "sh ${scripts}/OBH_Smart_Sql_Exec.sh $1 FOUNDATION ${FOUND}/${sql_fname}"
      sh ${scripts}/OBH_Smart_Sql_Exec.sh $1 FOUNDATION ${FOUND}/${sql_fname}
      if [ $? -eq 0 ]
      then
        msg "$line complete for ${t_name}"
        #send_mail "$line completed for ${t_name}" $email_to $Log_Dir1
     else
       msg "$line failed for ${t_name}"
       send_mail "$line failed for ${t_name}" $email_to $Log_Dir1
     fi
  else
    msg "Tabe name does not exist in Smart configuration load, please look into it"
    send_mail "Tabe name does not exist in Smart configuration load, please look into it" $email_to $Log_Dir1
    exit 1
  fi
done
else
 msg "Table name:$2 does not exist in configure, please configure ${conf_dir}/smart_sf.config  and execute it"
 send_mail "Failed to execute for Table name:$2"  $email_to $Log_Dir1
 exit 1
 fi
