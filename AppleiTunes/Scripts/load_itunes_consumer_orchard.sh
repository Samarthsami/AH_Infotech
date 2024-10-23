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

repl_date_key=#transaction_date_key#
repl_load_id=#load_id#
repl_lock_name=#lock_name#
repl_table_name=#table_name#
repl_file_name=#file_name#
#repl_aws_key=#key#
#repl_aws_secret=#secret#
#repl_s3_bucket=#s3_bucket#
#repl_etl_env=#etl_env#
repl_iam_role=#iamrole#

NOTIFY_NAME="Load Appleitunes(ORCHARD) Downloads to datacollector_db"

DAY_LOADED=0
NO_OF_LOADS=0

### S3 VARIABLES ###
S3_BUCKET_DL_PROCESS="s3://sme-ca-dl-${ETL_ENV}-process"
S3_PARTNER="/scuba/partner_data/orchard/P001/gras_enriched_files/"
S3_DONE_DIR="/scuba/load_status/orchard/done/"
S3_FILE_NAME=""
S3_PROFILE="default"

TABLE_NAME="itunes_job_locks"

LOG_PREFIX="load_itunes_consumer_orchard_"`date +\%Y-\%m-\%d_\%H\%M`
LOG_PREFIX=`echo ${LOG_PREFIX} | cut -c1-43`   ## check the length
LOG_PREFIX+="*.log"
echo -e "LOG_PREFIX=${LOG_PREFIX}\n"

cd $JISQLPATH

### Check ca_admin.itunes_job_locks table to see if CA Appleitunes(ORCHARD) job is still running ###
LOCK_NAME="_lock_ca_dl_itunes_orchard"
cp ${JISQLPATH}/sql/get_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
LOCK_CA_ITUNES_ORCHARD=`./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql`
echo -e "LOCK_CA_ITUNES_ORCHARD=${LOCK_CA_ITUNES_ORCHARD}\n"

if [[ LOCK_CA_ITUNES_ORCHARD -gt 0 ]]; then
  echo -e "Appleitunes(ORCHARD) Downloads orchestrated load into CA schema is still running.\n"
  exit 1
fi

## Check ca_admin.itunes_job_locks table to see if datacollector Appleitunes job is still running ###
LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
cp ${JISQLPATH}/sql/get_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
LOCK_DATACOLLECTOR_ITUNES_ORCHARD=`./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql`
echo -e "LOCK_DATACOLLECTOR_ITUNES_ORCHARD=${LOCK_DATACOLLECTOR_ITUNES_ORCHARD}\n"

if [[ LOCK_DATACOLLECTOR_ITUNES_ORCHARD -gt 0 ]]; then
  echo -e "Appleitunes(ORCHARD) Downloads data load into datacollector_db schema is still running.\n"
  exit 1
fi

### Create Lock in ca_admin.itunes_job_locks ###
LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
cp ${JISQLPATH}/sql/create_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql

cd ${LOADFILEPATH}

###running Vault (get_role_secretsrole)scripts###
source /home/etl-user/vault/bash/get_role_secrets.sh

for i in $( aws s3 ls ${S3_BUCKET_DL_PROCESS}${S3_PARTNER} --recursive | \
	grep -E '/P001_[[:digit:]]{1,}_gras_enriched_files_report_date=[[:xdigit:]]{4}-[[:xdigit:]]{2}-[[:xdigit:]]{2}\.manifest$'); do
  if [ ${i:0:5} == "scuba" ]; then
    STRING1=$i
    echo "STRING1=$STRING1"
    LOAD_ID=`echo $STRING1 | cut -d\_ -f6`
    echo "LOAD_ID=$LOAD_ID" 
    STRING2=${STRING1%/*}
    echo "STRING2=$STRING2"
    PROVIDER_KEY=`echo $STRING2 | cut -d\/ -f4`
    echo "PROVIDER_KEY=$PROVIDER_KEY"
    DATE_KEY=${STRING2##*/}
    echo "DATE_KEY=$DATE_KEY"
    DATE_KEY=`echo $DATE_KEY | cut -d\= -f2`
    DATE_KEY=`echo $DATE_KEY | tr -d -`
    echo "DATE_KEY=$DATE_KEY"
    S3_FILE_NAME="${S3_BUCKET_DL_PROCESS}/${STRING1}"
    echo -e "S3_FILE_NAME=${S3_FILE_NAME}\n"

    cd $JISQLPATH
    
    SCRIPT_NAME="count_transaction_date_key_orchard.sql"
    cp ${LOADFILEPATH}/$SCRIPT_NAME ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    sed -i -e "s/$repl_date_key/$DATE_KEY/g" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
          sed -i -e "s/$repl_load_id/$LOAD_ID/g" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    DUPES_FOUND=`./runit_ca-${ETL_ENV}-main.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql`
    echo -e "DUPES_FOUND=${DUPES_FOUND}\n"

    ERR_COUNT=`grep "java.sql.SQLException" $LOGFILEPATH/$LOG_PREFIX | wc -l`
    echo -e "ERR_COUNT=${ERR_COUNT}\n"
    
    if [[ ERR_COUNT -gt 0 ]]; then
      LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
      cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      ./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    
      aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
        --subject "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s)" \
        --message "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s). Job was terminated!"
        
      rm -f ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      exit 1
    fi

    if [[ DUPES_FOUND -gt 0 ]]; then
      echo -e "Duplicate Transaction Date found!\n"
      
      SCRIPT_NAME="delete_transaction_date_key_orchard.sql"
      cp ${LOADFILEPATH}/$SCRIPT_NAME ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_date_key/$DATE_KEY/g" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_load_id/$LOAD_ID/g" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      ./runit_ca-${ETL_ENV}-main.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      
      ERR_COUNT=`grep "java.sql.SQLException" $LOGFILEPATH/$LOG_PREFIX | wc -l`
      echo -e "ERR_COUNT=${ERR_COUNT}\n"
      
      if [[ ERR_COUNT -gt 0 ]]; then
        LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
        cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
        sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
        sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
        ./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      
        aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
          --subject "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s)" \
          --message "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s). Job was terminated!"
          
        rm -f ${LOADFILEPATH}/rstmp_itunes_orchard.sql
        exit 1
      fi
    fi

    SCRIPT_NAME="load_stg_itunes_downloads_orchard.sql"
    cp ${LOADFILEPATH}/$SCRIPT_NAME ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    sed -i -e "s|$repl_file_name|$S3_FILE_NAME|" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    #sed -i -e "s|$repl_aws_key|$AWS_KEY|" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    #sed -i -e "s|$repl_aws_secret|$AWS_SECRET|g" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    sed -i -e "s|$repl_iam_role|$IAM_ROLE|g" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    ./runit_ca-${ETL_ENV}-main.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    
    ### debug ###
    cp ${LOADFILEPATH}/rstmp_itunes_orchard.sql ${LOADFILEPATH}/repl_${SCRIPT_NAME}
    
    ERR_COUNT=`grep "java.sql.SQLException" $LOGFILEPATH/$LOG_PREFIX | wc -l`
    echo -e "\nERR_COUNT=${ERR_COUNT}\n"
    
    if [[ ERR_COUNT -gt 0 ]]; then
      LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
      cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      ./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    
      aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
        --subject "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s)" \
        --message "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s). Job was terminated!"
        
      rm -f ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      exit 1
    fi
    
    sleep 30
    
    echo -e "\nload_itunes_downloads_orchard.sql started...\n"
    SCRIPT_NAME="load_itunes_downloads_orchard.sql"
    cp ${LOADFILEPATH}/$SCRIPT_NAME ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    ./runit_ca-${ETL_ENV}-main.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    echo -e "\nload_itunes_downloads_orchard.sql completed!\n"
    
    ERR_COUNT=`grep "java.sql.SQLException" $LOGFILEPATH/$LOG_PREFIX | wc -l`
    echo -e "\nERR_COUNT=${ERR_COUNT}\n"
    
    if [[ ERR_COUNT -gt 0 ]]; then
      LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
      cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      ./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    
      aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
        --subject "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s)" \
        --message "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s). Job was terminated!"
        
      rm -f ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      exit 1
    fi

    ### Check to make sure data was actually loaded ###
    SCRIPT_NAME="count_transaction_date_key_orchard.sql"
    cp ${LOADFILEPATH}/$SCRIPT_NAME ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    sed -i -e "s/$repl_date_key/$DATE_KEY/g" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
          sed -i -e "s/$repl_load_id/$LOAD_ID/g" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    ROWS_FOUND=`./runit_ca-${ETL_ENV}-main.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql`
    echo -e "ROWS_FOUND=${ROWS_FOUND}\n"

    ERR_COUNT=`grep "java.sql.SQLException" $LOGFILEPATH/$LOG_PREFIX | wc -l`
    echo -e "ERR_COUNT=${ERR_COUNT}\n"
    
    if [[ ERR_COUNT -gt 0 ]]; then
      LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
      cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      ./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    
      aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
        --subject "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s)" \
        --message "${ETL_ENV}: ${NOTIFY_NAME}: $SCRIPT_NAME completed with error(s). Job was terminated!"
        
      rm -f ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      exit 1
    fi

    ### Wait for insert from staging table to datacollector table to be complete ###
    WAIT_COUNT=0
    while [ $ROWS_FOUND -eq 0 ] && [ $WAIT_COUNT -lt 10 ]
    do
      echo "${WAIT_COUNT} - insert from staging table to datacollector_db.appleitunes_downloads_orchard table is not done yet. Wait 30 seconds and check again..."
      sleep 30

      echo -e "\nload_itunes_downloads_orchard.sql started...\n"
      SCRIPT_NAME="load_itunes_downloads_orchard.sql"
      ./runit_ca-${ETL_ENV}-main.sh ${LOADFILEPATH}/${SCRIPT_NAME}
      echo -e "\nload_itunes_downloads_orchard.sql completed!\n"

      ROWS_FOUND=`./runit_ca-${ETL_ENV}-main.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql`
      echo -e "ROWS_FOUND=${ROWS_FOUND}\n"
      (( WAIT_COUNT++ ))
    done

    if [[ ROWS_FOUND -eq 0 ]]; then
      echo -e "Failed to load data from staging table etl_stage.stg_itunes_downloads_orchard!\n"
            
      LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
      cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      ./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
    
      aws sns publish --topic-arn arn:aws:sns:us-east-1:250735107403:sme-smanalytics-etl-batch-useast \
        --subject "${ETL_ENV}: ${NOTIFY_NAME}: Failed to load data from staging table" \
        --message "${ETL_ENV}: ${NOTIFY_NAME}: Uh...oh, failed to load data from etl_stage.stg_itunes_downloads_orchard. Job was terminated!"
        
      rm -f ${LOADFILEPATH}/rstmp_itunes_orchard.sql
      exit 1
    fi
            
    sleep 10

    ###running Vault (get_role_secretsrole)scripts###
    source /home/etl-user/vault/bash/get_role_secrets.sh
    
    aws s3 mv ${S3_FILE_NAME} ${S3_BUCKET_DL_PROCESS}${S3_DONE_DIR}${PROVIDER_KEY}/
    ((NO_OF_LOADS+=1))
    echo -e "\nNO_OF_LOADS=${NO_OF_LOADS}\n"
    ((DAY_LOADED+=1))
    echo -e "\nDAY_LOADED=${DAY_LOADED}\n"
  fi
done

cd $JISQLPATH

if [[ NO_OF_LOADS -gt 0 ]]; then
  echo -e "${NO_OF_LOADS} new load_id(s) loaded to datacollector_db.itunes_downloads_orchard\n"

  LOCK_NAME="_success_datacollector_itunes_dl_orchard"
  cp ${JISQLPATH}/sql/create_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
  sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
  sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
  ./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql
else
  echo -e "No new Appleitunes(ORCHARD) Downloads data available to load!\n"
fi 

sleep 5

LOCK_NAME="_lock_datacollector_itunes_dl_orchard"
cp ${JISQLPATH}/sql/remove_lock.sql ${LOADFILEPATH}/rstmp_itunes_orchard.sql
sed -i -e "s/$repl_lock_name/$LOCK_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
sed -i -e "s/$repl_table_name/$TABLE_NAME/" ${LOADFILEPATH}/rstmp_itunes_orchard.sql
./runit_api-db-${ETL_ENV}_rs_logs.sh ${LOADFILEPATH}/rstmp_itunes_orchard.sql

echo -e "\nPID=($$) - ${ETL_ENV}: ${NOTIFY_NAME} completed.\n"