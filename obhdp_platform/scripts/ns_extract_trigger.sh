set -x
# Setting environment variable for Snowsql
source ~/.bash_profile
export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_6:$LD_LIBRARY_PATH
log_name=`basename $0 | cut -f1 -d'.'`

#!/bin/bash -e
set -o pipefail



#######################################################################################################
  # Main Script
#######################################################################################################

  #Argument validation
  if [ $# -eq 1 ]
    then
      if [ $1 == "DEV" ]
        then
          #Source all the configurations
          source /obhdp_dev/tcoc/d_conf/obhdp_conf.sh
          echo "Validation initiated for DEV environment"
      elif [ $1 == "STG" ]
        then
          source /obhdp/tcoc/s_conf/obhdp_conf.sh
          echo "Validation initiated for STG environment"
      elif [ $1 == "PRD" ]
        then
          source /obhdp/tcoc/p_conf/obhdp_conf.sh
          echo "Validation initiated for PRD environment"
      else
        echo "invalid Environment passed in the argument, please pass DEV/STG/PRD"
        exit 1
      fi
  else
    echo "Invalid Arguments, please pass one argument and should be DEV/STG/PRD"
    exit 1
  fi

landing_file=${ns_landing_dir}/NS_EXTRACT_UPD.csv


DATA_DIR=/obhdp_dev/tcoc/d_northstar/d_landing

LOG=/obhdp_dev/tcoc/d_northstar/d_archive/ns_extract_log_`date +%Y%m%d_%H%M%S`.txt

ERR=/obhdp_dev/tcoc/d_northstar/d_archive/fail/ns_extract_fail.txt

ARCHIVE=/obhdp_dev/tcoc/d_northstar/d_archive/NS_EXTRACT_UPD_`date +%Y%m%d_%H%M%S`.csv

env=$1
#echo "$env"| tr "[:upper:]" "[:lower:]"


SNOWSQL_OPTIONS=(
  "-c" "optum_dev"
  "-o" "echo=ON"
  "-o" "exit_on_error=True"
  "-o" "friendly=False"
  "-o" "quiet=False"
  "-o" "timing=False"
  "-o" "output_file=$LOG"
  "-o" "variable_substitution=True"
)


while [ true ]; do
  sleep 10

if ! snowsql ${SNOWSQL_OPTIONS[@]} 2>$ERR<<SQL; then

rm @%NS_AE_DATA_EXTRACTION;

PUT file://$DATA_DIR/* @%NS_AE_DATA_EXTRACTION;

delete from NS_AE_DATA_EXTRACTION;

copy into NS_AE_DATA_EXTRACTION   from @%NS_AE_DATA_EXTRACTION   file_format = (type = csv field_optionally_enclosed_by='"')  pattern = '.*NS_EXTRACT_UPD*.csv.gz'  on_error = 'skip_file';

delete from COMPACT.NS_AE_DATA_EXTRACTION;


INSERT INTO COMPACT.NS_AE_DATA_EXTRACTION select * from ETL.NS_AE_DATA_EXTRACTION;

SQL

  echo "File not exists - Extraction can not be started"

  cat $ERR

  exit 1

else

  echo "File exists - Extraction succeeded and data loaded to ETL and COMPACT."

mv $landing_file $ARCHIVE

#delete from NS_EXTRACT_TST;
#mv /obhdp_dev/tcoc/d_northstar/d_landing/NS_EXTRACT_UPD.csv /obhdp_dev/tcoc/d_northstar/d_archive/NS_EXTRACT_UPD_`date +%Y%m%d_%H%M%S`.csv

fi
exit

done
