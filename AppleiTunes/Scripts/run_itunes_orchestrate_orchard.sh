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

S3_SCRIPT_DIR="/home/etl-user/script_templates/datamove/dl_appleitunes/orchard/downloads/process-main"


repl_lock_name=#lock_name#
repl_table_name=#table_name#
TABLE_NAME="itunes_job_locks"
ERR_COUNT=0

LOG_PREFIX="run_itunes_orchestrate_orchard_"`date +\%Y-\%m-\%d_\%H\%M`
LOG_PREFIX=`echo ${LOG_PREFIX} | cut -c1-46`
LOG_PREFIX+="*.log"
echo -e "LOG_PREFIX=${LOG_PREFIX}\n"

cd $JISQLPATH

LOCK_NAME="_success_datacollector_itunes_dl_orchard"
cp ${JISQLPATH}/sql/get_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
SUCCESS_DATACOLLECTOR_ITUNES_ORCHARD=`./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql`
echo -e "SUCCESS_DATACOLLECTOR_ITUNES_ORCHARD=${SUCCESS_DATACOLLECTOR_ITUNES_ORCHARD}\n"

if [[ SUCCESS_DATACOLLECTOR_ITUNES_ORCHARD -eq 0 ]]; then
  echo -e "No new orchard itunes  downloads data to process!\n"
  exit 1
fi

sleep 90

LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
cp ${JISQLPATH}/sql/get_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
LOCK_DATACOLLECTOR_ITUNES_DL_ORCHARD=`./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql`
echo -e "LOCK_DATACOLLECTOR_ITUNES_DL_ORCHARD=${LOCK_DATACOLLECTOR_ITUNES_DL_ORCHARD}\n"

if [[ LOCK_DATACOLLECTOR_ITUNES_DL_ORCHARD -gt 0 ]]; then
  echo -e "Orchard itunes downloads DataCollector job is still running.\n"
  exit 1
fi

#sleep 5

## Check ca_admin.itunes_job_locks table to see if CA itunes job is still running ###
LOCK_NAME="_lock_ca_dl_itunes_orchard"
cp ${JISQLPATH}/sql/get_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
LOCK_CA_ITUNES_ORCHARD=`./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql`
echo -e "LOCK_CA_ITUNES_ORCHARD=${LOCK_CA_ITUNES_ORCHARD}\n"

if [[ LOCK_CA_ITUNES_ORCHARD -gt 0 ]]; then
  echo -e "Orchard itunes downloads orchestrated load into CA schema is still running.\n"
  exit 1
fi

sleep 5

aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
  --subject "${ETL_ENV}: Starting itunes downloads orchestrated load into CA schema..." \
  --message "Starting itunes downloads orchestrated load into CA schema..."

LOCK_NAME="_lock_ca_dl_itunes_orchard"
cp ${JISQLPATH}/sql/create_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql

echo -e "Starting DataCollector Orchard itunes downloads orchestrated load into CA schema...\n"
echo -e "Getting latest scripts from ${S3_SCRIPT_DIR}\n"

#--------------------------------------------------------------------------------------------------


SCRIPT_NAME="fact.consumer.orchard.sql"
cp ${S3_SCRIPT_DIR}/${SCRIPT_NAME} ${LOADFILEPATH}/
if [[ ! -s ${LOADFILEPATH}/${SCRIPT_NAME} ]]; then
  aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
          --subject "${ETL_ENV}: $SCRIPT_NAME script is missing" \
          --message "$SCRIPT_NAME was not download successfully from script path."
  exit 1
fi

echo -e "\nRunning script: ${SCRIPT_NAME} ...\n"
./runit_ca-${ETL_ENV}-main.sh ${LOADFILEPATH}/${SCRIPT_NAME}
echo -e "\nScript ${SCRIPT_NAME} completed!\n"

ERR_COUNT=`grep "java.sql.SQLException" $LOGFILEPATH/$LOG_PREFIX | wc -l`
echo -e "\nERR_COUNT=${ERR_COUNT}\n"

if [[ ERR_COUNT -gt 0 ]]; then
  LOCK_NAME="_lock_ca_dl_itunes_orchard"
  cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
  sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
  sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
  ./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql

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
LOCK_NAME="_ready_reportdb_dl_itunes_orchard"
cp ${JISQLPATH}/sql/create_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql

sleep 5


LOCK_NAME="_success_datacollector_itunes_dl_orchard"
cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql

sleep 5

LOCK_NAME="_lock_ca_dl_itunes_orchard"
cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchestrated_orchard.sql

aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
  --subject "${ETL_ENV}: itunes orchestrated load into CA schema completed!" \
  --message "itunes orchestrated load into CA schema completed!"

echo -e "\nPID=($$) - itunes downloads orchestrated load into CA schema completed.\n"
