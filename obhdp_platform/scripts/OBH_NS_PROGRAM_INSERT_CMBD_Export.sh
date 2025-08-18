#!/usr/bin/bash
set -x &>/dev/null
# Setting environment variable for Snowsql
source ~/.bash_profile &>/dev/null

#########################################################################################
# Script Name : OBH_NS_PROGRAM_INSERT_CMBD_Export.sh
# Description : This Shell script will unload data from NORTHSTAR.NS_PROGRAM_INSERT_CMBD
#                table to a csv file and sftp csv file to ECG folder
#########################################################################################

log_name=`basename $0 | cut -f1 -d'.'`


#######################################################################################################
  # Main Script
#######################################################################################################
 #Argument validation
  if [ $# -eq 1 ]
    then
      if [ $1 == "DEV" ]
        then
          #Source all the configurations
          source /obhdp_dev/tcoc/d_conf/obhdp_conf.sh &>/dev/null
          echo "Data Load initiated for DEV environment"
      elif [ $1 == "STG" ]
        then
          source /obhdp/tcoc/s_conf/obhdp_conf.sh &>/dev/null
          echo "SData Load initiated for STG environment"
      elif [ $1 == "PRD" ]
        then
          source /obhdp/tcoc/p_conf/obhdp_conf.sh &>/dev/null
          echo "Data Load initiated for PRD environment"
      else
        echo "invalid Environment passed in the argument, please pass DEV/STG/PRD"
        exit 1
      fi
  else
    echo "Invalid Arguments, please pass one argument and should be DEV/STG/PRD"
    exit 1
  fi

log_file=${Log_dir}/${log_name}_`date +%Y%m%d_%H%M%S`.txt
env=$1

echo "Archiving any existing files before extraction" >> ${log_file}

if ls /obhdp/NorthStar/UH_AH_P_Program_Insert_NS* 1> /dev/null 2>&1
then
    mv -f /obhdp/NorthStar/UH_AH_P_Program_Insert_NS* /obhdp/NorthStar/Archive/
	echo "File archival completed"  >> ${log_file}
else
	echo "No files to archive" >> ${log_file}
fi

echo "Starting data export and file copy" >> ${log_file}
sh ${scripts}/OBH_Smart_Sql_Exec.sh $env ETL ${sql}/NORTHSTAR/NS_PROGRAM_INSERT_CMBD_Export.sql

if [ $? -eq 0 ]
  then
  echo "Data export successful and file copied"  >> ${log_file}
else
  echo "Data export/file copy failed"  >> ${log_file}
  exit 1
fi

echo "Renaming file to append date"
mv /obhdp/NorthStar/UH_AH_P_Program_Insert_NS.csv /obhdp/NorthStar/UH_AH_P_Program_Insert_NS_`date +%Y%m%d0000`.csv
if [ $? -eq 0 ]
  then
  echo "File renaming complete"  >> ${log_file}
else
  echo "File renaming failed"  >> ${log_file}
  exit 1
fi

echo "Sending file to ECG"
# sftp {user}@{host}:{remote-path} <<< $'put {local-path}'
sftp is10s9b@ecgpi.healthtechnologygroup.com:/northstar <<< $'put /obhdp/NorthStar/UH_AH_P_Program_Insert_NS*'

if [ $? -eq 0 ]
  then
  echo "File transfer to ECG complete"  >> ${log_file}
else
  echo "File transfer to ECG failed"  >> ${log_file}
  exit 1
fi

