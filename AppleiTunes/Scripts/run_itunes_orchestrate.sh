#!/bin/bash

### Check to see if this script is already running and exit if it is. ###
source ~/scripts/highlander.sh
#sleep 5m   ### for testing only. comment out when done test.
#########################################################################

export LC_ALL=en_US.UTF-8
export LOADFILEPATH=~/scripts/datacollector/appleitunes/downloads
export JISQLPATH=~/apps/jisql-2.0.11
export LOGFILEPATH=~/scripts/logs
export ETL_ENV="prod"



l_s3_account="etl_prod"
### initialize all variables ###
#S3_SCRIPT_DIR="s3://sme.ca.dev.smanalytics.dbutils/scripts/datacollector/itunes/downloads/process-main"
#S3_SCRIPT_DIR="s3://sme-ca-dl-dev-process/scuba/scripts/itunes/process-main"

S3_SCRIPT_DIR="/home/etl-user/script_templates/datamove/dl_appleitunes/sme/downloads/process-main"


repl_lock_name=#lock_name#
repl_table_name=#table_name#
TABLE_NAME="itunes_job_locks"
ERR_COUNT=0

LOG_PREFIX="run_itunes_orchestrate_"`date +\%Y-\%m-\%d_\%H\%M`
LOG_PREFIX=`echo ${LOG_PREFIX} | cut -c1-38`
LOG_PREFIX+="*.log"
echo -e "LOG_PREFIX=${LOG_PREFIX}\n"

cd $JISQLPATH

LOCK_NAME="_success_datacollector_itunes_dl"
cp ${JISQLPATH}/sql/get_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
SUCCESS_DATACOLLECTOR_ITUNES=`./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql`
echo -e "SUCCESS_DATACOLLECTOR_ITUNES=${SUCCESS_DATACOLLECTOR_ITUNES}\n"

if [[ SUCCESS_DATACOLLECTOR_ITUNES -eq 0 ]]; then
  echo -e "No new itunes SME downloads data to process!\n"
  exit 1
fi

sleep 90

LOCK_NAME="_lock_datacollector_itunes_dl"
cp ${JISQLPATH}/sql/get_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
LOCK_DATACOLLECTOR_ITUNES_DL=`./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql`
echo -e "LOCK_DATACOLLECTOR_ITUNES_DL=${LOCK_DATACOLLECTOR_ITUNES_DL}\n"

if [[ LOCK_DATACOLLECTOR_ITUNES_DL -gt 0 ]]; then
  echo -e "itunes SME downloads DataCollector job is still running.\n"
  exit 1
fi

#sleep 5

## Check ca_admin.itunes_job_locks table to see if CA itunes job is still running ###
LOCK_NAME="_lock_ca_dl_itunes"
cp ${JISQLPATH}/sql/get_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
LOCK_CA_ITUNES=`./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql`
echo -e "LOCK_CA_ITUNES=${LOCK_CA_ITUNES}\n"

if [[ LOCK_CA_ITUNES -gt 0 ]]; then
  echo -e "itunes SME downloads orchestrated load into CA schema is still running.\n"
  exit 1
fi

sleep 5

aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
  --subject "${ETL_ENV}: Starting itunes SME downloads orchestrated load into CA schema..." \
  --message "Starting itunes SME downloads orchestrated load into CA schema..."

LOCK_NAME="_lock_ca_dl_itunes"
cp ${JISQLPATH}/sql/create_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql

echo -e "Starting DataCollector itunes SME downloads orchestrated load into CA schema...\n"
echo -e "Getting latest scripts from ${S3_SCRIPT_DIR}\n"

#--------------------------------------------------------------------------------------------------
## fact.consumer.sql == RUN for load_ids
## fact.consumer_singletime_load.sql == single RUN for all load_ids

SCRIPT_NAME="fact.consumer_singletime_load.sql"
cp ${S3_SCRIPT_DIR}/${SCRIPT_NAME} ${LOADFILEPATH}/
if [[ ! -s ${LOADFILEPATH}/${SCRIPT_NAME} ]]; then
  aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
          --subject "${ETL_ENV}: $SCRIPT_NAME script is missing" \
          --message "$SCRIPT_NAME was not download successfully from Script path."
  exit 1
fi

echo -e "\nRunning script: ${SCRIPT_NAME} ...\n"
./runit_ca-${ETL_ENV}-main.sh ${LOADFILEPATH}/${SCRIPT_NAME}
echo -e "\nScript ${SCRIPT_NAME} completed!\n"

ERR_COUNT=`grep "java.sql.SQLException" $LOGFILEPATH/$LOG_PREFIX | wc -l`
echo -e "\nERR_COUNT=${ERR_COUNT}\n"

if [[ ERR_COUNT -gt 0 ]]; then
  LOCK_NAME="_lock_ca_dl_itunes"
  cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
  sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
  sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
  ./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql

  aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
    --subject "${ETL_ENV}: $SCRIPT_NAME completed with error(s)" \
    --message "$SCRIPT_NAME completed with error(s). Job was terminated!"
  
  rm -f ${LOADFILEPATH}/${SCRIPT_NAME}
  exit 1
fi

rm -f ${LOADFILEPATH}/${SCRIPT_NAME}

#--------------------------------------------------------------------------------------------------

sleep 10

### create flag so that we can copy data over to reporting db ###
LOCK_NAME="_ready_reportdb_dl_itunes"
cp ${JISQLPATH}/sql/create_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql

sleep 5


LOCK_NAME="_success_datacollector_itunes_dl"
cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql

sleep 5

LOCK_NAME="_lock_ca_dl_itunes"
cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated.sql

aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
  --subject "${ETL_ENV}: itunes SME orchestrated load into CA schema completed!" \
  --message "itunes SME orchestrated load into CA schema completed!"

echo -e "\nPID=($$) - itunes SME downloads orchestrated load into CA schema completed.\n"
