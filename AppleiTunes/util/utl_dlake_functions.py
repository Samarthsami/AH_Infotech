from __future__ import print_function
#-----------------  IMPORT MODULES ----------------
from builtins import str
import sys,time,boto3,codecs
from datetime import datetime
import multiprocessing
import subprocess

#from env import init
#retcode=init()
from utl_mysql_interface import *
import config
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext, SQLContext
#------------------- END IMPORT -------------------


#--------------- FUNCTION --------------------------------------
# NAME:             COPY_FROM_TEMP
# Args:             Dimension Name, S3 Bucket Object, Temp and Final S3 Folder Prefixes
# Return:           0 on success
# Purpose:          Delete and Write contents from Temp S3 folder to final folder location
# Author:           Anand Vasudevan
# Modified Date     30.10.2019
# Comments          Initial Version
#
#
#---------------------------------------------------------------

def copy_from_temp(dim_name, obj_dl_bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix):

    ## DELETE CONTENTS IN FINAL DEST FOLDER
    obj_dl_bucket.objects.filter(Prefix=dl_s3_dim_prefix+dim_name+'/').delete()

    for obj in obj_dl_bucket.objects.filter(Prefix=temp_dl_s3_dim_prefix+dim_name+'/'):
        src=obj_dl_bucket.name+'/'+obj.key
        file_name=obj.key.split('/')[len(obj.key.split('/'))-1]
        dest=dl_s3_dim_prefix+dim_name+'/'+file_name

        ## COPY TO FINAL DEST FOLDER
        obj_dl_bucket.Object(dest).copy_from(CopySource=src)

    return 0
#--------------- FUNCTION --------------------------------------
# NAME:             WRITE_DLAKE_DIM
# Args:             S3 Temp folder path, Dimension Name, S3 Bucket Object, Temp and Final S3 Folder Prefixes
# Return:           0 on success, -1 on failure
# Purpose:          Write DF object to a Temp S3 folder and call for copy to final folder location
# Author:           Anand Vasudevan
# Modified Date     30.10.2019
# Comments          Initial Version
#
#
#---------------------------------------------------------------

def write_dlake_dim(temp_dl_dim_path, dim_name, dim_df, obj_dl_bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix):

    #### 1. WRITE IT TO A TEMP FOLDER FIRST
    dim_df.write.mode('overwrite').parquet(temp_dl_dim_path+dim_name+"/")
    time.sleep(10)
    #### 2. COPY FROM TEMP TO FINAL DEST FOLDER
    result=copy_from_temp(dim_name, obj_dl_bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix)
    #dim_df=sqlContext.read.parquet(temp_dl_dim_path+dim_name+"/")
    #dim_df.write.mode('overwrite').parquet(temp_dl_dim_path.replace('temp/','')+dim_name+'/')
    #dim_df=sqlContext.read.parquet(temp_dl_dim_path.replace('temp/','')+dim_name+'/')

    #return 0

    if result!=0:
        return -1
    else:
        return 0

#--------------- FUNCTION --------------------------------------
# NAME:             WRITE_DLAKE_DIM_V2
# Args:             S3 Temp folder path, Dimension Name, S3 Bucket Object, Temp and Final S3 Folder Prefixes
# Return:           0 on success, -1 on failure
# Purpose:          Write DF object to a Temp S3 folder, read from Temp S3 folder and write back to final dest
# Author:           Anand Vasudevan
# Modified Date     08.05.2020
# Comments          Initial Version
#
#
#---------------------------------------------------------------

def write_dlake_dim_v2(temp_dl_dim_path, dim_name, dim_df, obj_dl_bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix):

    #### 1. WRITE IT TO A TEMP FOLDER FIRST
    dim_df.write.mode('overwrite').parquet(temp_dl_dim_path+dim_name+"/")
    time.sleep(10)
    #### 2. COPY FROM TEMP TO FINAL DEST FOLDER
    #result=copy_from_temp(dim_name, obj_dl_bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix)
    dim_df=sqlContext.read.parquet(temp_dl_dim_path+dim_name+"/")
    dim_df.write.mode('overwrite').parquet(temp_dl_dim_path.replace('temp/','')+dim_name+'/')
    dim_df=sqlContext.read.parquet(temp_dl_dim_path.replace('temp/','')+dim_name+'/')

    return dim_df


    #if result!=0:
    #    return -1
    #else:
    #    return 0

def write_dlake_dim_v3(temp_dl_dim_path, dim_name, dim_df, obj_dl_bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix,final_dl_dim_path):

    #### 1. WRITE IT TO A TEMP FOLDER FIRST
    dim_df.write.mode('overwrite').parquet(temp_dl_dim_path+dim_name+"/")
    time.sleep(10)
    #### 2. COPY FROM TEMP TO FINAL DEST FOLDER
    #result=copy_from_temp(dim_name, obj_dl_bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix)
    dim_df=sqlContext.read.parquet(temp_dl_dim_path+dim_name+"/")
    ##3.copy directly into the final datalake bucket.
    dim_df.write.mode('overwrite').parquet(final_dl_dim_path+dim_name+'/')
    dim_df=sqlContext.read.parquet(final_dl_dim_path+dim_name+'/')

    return dim_df

#--------------- FUNCTION --------------------------------------
# NAME:             RUN_CRAWLER
# Args:             AWS Profile Name, Crawler Name
# Return:           0 on success, -1 on failure
# Purpose:          Trigger Crawler Run in AWS Glue
# Author:           Anand Vasudevan
# Modified Date     29.09.2020
# Comments          Initial Version
#
#
#---------------------------------------------------------------


def run_crawler(v_aws_profile_name,v_crawler_name):
    b3s=boto3.session.Session(profile_name=v_aws_profile_name)
    glueClient=b3s.client("glue")
    resp=glueClient.get_crawler(Name=v_crawler_name)
    if resp["Crawler"]["State"]=="READY": # can start
        resp=glueClient.start_crawler(Name=v_crawler_name)
        time.sleep(10)
        resp=glueClient.get_crawler(Name=v_crawler_name)
        while resp["Crawler"]["State"] != "READY":
            time.sleep(10)
            resp=glueClient.get_crawler(Name=v_crawler_name)

        resp=glueClient.get_crawler(Name=v_crawler_name)
        if resp["Crawler"]["LastCrawl"]["Status"]!="SUCCEEDED": # mmail to admin, but proceed
            mailSubject="Apple Music Reports transfer failed at stage run crawler for load id"+str(load_id)
            mailBody='<html><head></head><body>Dear Admin, <br><br>the Spotify Daily Box reports transfer failed at stage RUN_CRAWLER '+v_crawler_name+'.<br><br>Best regards,<br>SCUBA Hotline<br><br>In case of any questions, please email <a href="mailto:ca.admin.help@sonymusic.com">SCUBA Hotline</a></body></html>'
            send_mail(mailSubject,mailBody,mailType='html',mailGroup='dcadmin')
            return -1
        else:
            print("Crawler ", v_crawler_name, " successfuly completed.")
            return 0
    else: # crawler not available for starting, ignore
        print("Crawler ", v_crawler_name, " could not be run, due to previous state failure")
        return -1


#--------------- FUNCTION -------------------------------------------------------------
# NAME:             LOAD_FROM_GLUE_TO_DB
# Args:             DBConn, Provider Key, SQL File Name, Report Date in YYYYMMDD format
# Return:           0 on success, -1 on failure
# Purpose:          Load data in Glue to Database
# Author:           Anand Vasudevan
# Modified Date     01.10.2020
# Comments          Initial Version
#
#
#----------------------------------------------------------------------------------------

def load_from_glue_to_db(dbConn,etlProfile,codeBucket,codePath,v_sql_file,v_report_date,v_client='sme'):
    print("Passed "+str(v_client))
    b3s=boto3.session.Session(profile_name=etlProfile)
    s3Client = b3s.client('s3')
    print("SQL Source: ",codePath+v_sql_file)
    source = s3Client.get_object(Bucket=codeBucket, Key=codePath+v_sql_file)
    sqlText = source.get('Body').read().decode('utf8')
    cur = dbConn.cursor()

    if len(v_report_date) != 0:
        sqlText=sqlText.replace("#report_date#",v_report_date[0:4]+"-"+v_report_date[4:6]+"-"+v_report_date[6:])
    if len(v_client) !=0:
         sqlText=sqlText.replace("#licensor_code#",v_client)

    print(sqlText)

    cur.execute(sqlText)
    retVal=cur.statusmessage
    print(retVal)
    cur.close()
    dbConn.commit()

def load_from_glue_to_db_loadid(dbConn,etlProfile,codeBucket,codePath,v_sql_file,v_loadid,v_client='sme'):
    print("Passed "+str(v_client))
    b3s=boto3.session.Session(profile_name=etlProfile)
    s3Client = b3s.client('s3')
    print("SQL Source: ",codePath+v_sql_file)
    source = s3Client.get_object(Bucket=codeBucket, Key=codePath+v_sql_file)
    sqlText = source.get('Body').read().decode('utf8')
    cur = dbConn.cursor()

    #if len(v_report_date) != 0:
    #    sqlText=sqlText.replace("#report_date#",v_report_date[0:4]+"-"+v_report_date[4:6]+"-"+v_report_date[6:])
    if len(v_client) !=0:
         sqlText=sqlText.replace("#licensor_code#",v_client)
    if len(v_loadid) !=0:
         sqlText=sqlText.replace("#load_id#",v_loadid)

    print(sqlText)

    cur.execute(sqlText)
    retVal=cur.statusmessage
    print(retVal)
    cur.close()
    dbConn.commit()

def load_from_glue_to_db_country(dbConn,etlProfile,codeBucket,codePath,v_sql_file,v_loadid,v_report_date,v_country,v_provider,v_client='sme'):
    print("Passed "+str(v_client))
    b3s=boto3.session.Session(profile_name=etlProfile)
    s3Client = b3s.client('s3')
    print("SQL Source: ",codePath+v_sql_file)
    source = s3Client.get_object(Bucket=codeBucket, Key=codePath+v_sql_file)
    sqlText = source.get('Body').read().decode('utf8')
    cur = dbConn.cursor()

    #if len(v_report_date) != 0:
    #    sqlText=sqlText.replace("#report_date#",v_report_date[0:4]+"-"+v_report_date[4:6]+"-"+v_report_date[6:])
    if len(v_client) !=0:
         sqlText=sqlText.replace("#licensor_code#",v_client)
    if len(v_country) !=0:
         sqlText=sqlText.replace("#country_code#",v_country)
    if len(v_provider) !=0:
         sqlText=sqlText.replace("#provider_key#",v_provider)
    if len(v_report_date) != 0:
        sqlText=sqlText.replace("#report_date#",v_report_date[0:4]+"-"+v_report_date[4:6]+"-"+v_report_date[6:])
    print(sqlText)

    cur.execute(sqlText)
    retVal=cur.statusmessage
    print(retVal)
    cur.close()
    dbConn.commit()

def load_from_glue_to_db_country_test(dbConn,etlProfile,codeBucket,codePath,v_sql_file, v_loadid, v_report_date,v_country,v_provider, v_partner, v_client='sme'):
    print("Passed "+str(v_client))
    b3s=boto3.session.Session(profile_name=etlProfile)
    s3Client = b3s.client('s3')
    print("SQL Source: ",codePath+v_sql_file)
    source = s3Client.get_object(Bucket=codeBucket, Key=codePath+v_sql_file)
    sqlText = source.get('Body').read().decode('utf8')
    cur = dbConn.cursor()

    #if len(v_report_date) != 0:
    #    sqlText=sqlText.replace("#report_date#",v_report_date[0:4]+"-"+v_report_date[4:6]+"-"+v_report_date[6:])
    if len(v_client) !=0:
        sqlText=sqlText.replace("#client_key#", v_client)
    if len(v_country) !=0:
         sqlText=sqlText.replace("#country_code#",v_country)
    if len(v_provider) !=0:
         sqlText=sqlText.replace("#provider_key#",v_provider)
    if len(v_partner) !=0:
         sqlText=sqlText.replace("#partner_key#",v_partner)
    if len(v_report_date) != 0:
        sqlText=sqlText.replace("#report_date#",v_report_date[0:4]+"-"+v_report_date[4:6]+"-"+v_report_date[6:])
    print(sqlText)

    cur.execute(sqlText)
    retVal=cur.statusmessage
    print(retVal)
    cur.close()
    dbConn.commit()


def load_from_glue_to_db_platform(dbConn,etlProfile,codeBucket,codePath,v_sql_file,v_loadid,v_report_date,v_client='sme',v_platform=None):
    print("Passed "+str(v_client))
    b3s=boto3.session.Session(profile_name=etlProfile)
    s3Client = b3s.client('s3')
    print("SQL Source: ",codePath+v_sql_file)
    source = s3Client.get_object(Bucket=codeBucket, Key=codePath+v_sql_file)
    sqlText = source.get('Body').read().decode('utf8')
    cur = dbConn.cursor()

    #if len(v_report_date) != 0:
    #    sqlText=sqlText.replace("#report_date#",v_report_date[0:4]+"-"+v_report_date[4:6]+"-"+v_report_date[6:])
    if len(v_client) !=0:
         sqlText=sqlText.replace("#licensor_code#",v_client)
    if len(v_platform) !=0:
         sqlText=sqlText.replace("#platform_name#",v_platform)
    if len(v_report_date) != 0:
        sqlText=sqlText.replace("#report_date#",v_report_date[0:4]+"-"+v_report_date[4:6]+"-"+v_report_date[6:])
    print(sqlText)

    cur.execute(sqlText)
    retVal=cur.statusmessage
    print(retVal)
    cur.close()
    dbConn.commit()


if __name__ == "__main__":
    config.metadata=read_config(section="table")
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    bugs_schema = StructType([
                        StructField('track_key',StringType(),True)
                        ,StructField('vendor_key',StringType(),True)
                        ,StructField('customer_key',StringType(),True)
                        ,StructField('stream_source_key',StringType(),True)
                        ,StructField('country_key',StringType(),True)
                        ,StructField('activity_key',StringType(),True)
                        ,StructField('activity_type',StringType(),True)
                        ,StructField('location_of_track_in_playlist',StringType(),True)
                        ,StructField('shuffle_mode',StringType(),True)
                        ,StructField('device_type',StringType(),True)
                        ,StructField('os_type',StringType(),True)
                        ,StructField('referral_source_type',StringType(),True)
                        ,StructField('referral_source_detail',StringType(),True)
                        ,StructField('royalty_bearing_play',StringType(),True)
                        ,StructField('time_and_date_stamp_of_activity',StringType(),True)
                        ,StructField('length_of_stream',StringType(),True)
                        ,StructField('artist_fan_',StringType(),True)
                        ,StructField('promoted_playlist_play',StringType(),True)
                        ,StructField('cached_play',StringType(),True)
                        ,StructField('end_reason_type',StringType(),True)
                        ,StructField('previous_track_isrc',StringType(),True)
                        ,StructField('no_of_streaming',StringType(),True)
                        ,StructField('report_day',StringType(),True)
                        ,StructField('provider_key',StringType(),True)
                        ,StructField('sales_type_key',StringType(),True)
                        ,StructField('partner_id',StringType(),True)
                        ,StructField('partner_name',StringType(),True)
                        ,StructField('transaction_date_key',StringType(),True)
                        ,StructField('distribution_key',StringType(),True)
                        ,StructField('transaction_type_key',StringType(),True)
                        ,StructField('service_type_key',StringType(),True)
                        ,StructField('product_type_key',StringType(),True)
                        ,StructField('artist_name',StringType(),True)
                        ,StructField('artist_key',StringType(),True)
                        ,StructField('official_sme_product_no',StringType(),True)
                        ,StructField('provider_genre',StringType(),True)
                        ,StructField('participant_full_name',StringType(),True)
                        ,StructField('track_title',StringType(),True)
                        ,StructField('isrc',StringType(),True)
                        ,StructField('upc',StringType(),True)
                        ,StructField('title',StringType(),True)
                        ,StructField('album_name',StringType(),True)
                        ,StructField('prod_no_dig',StringType(),True)
                        ,StructField('label_key',StringType(),True)
                        ,StructField('client_key',StringType(),True)
                        ,StructField('product_conf_key',StringType(),True)
                        ,StructField('load_id',StringType(),True)
                        ,StructField('trans_id',StringType(),True)
                        ,StructField('product_key',StringType(),True)
                        ,StructField('sales_division_key',StringType(),True)
                        ,StructField('consumer_key',StringType(),True)
                        ,StructField('distribution_channel_key',StringType(),True)
                        ,StructField('sales_category_key',StringType(),True)
                        ,StructField('spnl_key',StringType(),True)
                        ,StructField('product_types_key',StringType(),True)
                        ])

    prov_key='P654'
    load_id=2751
    params=get_provider_params(prov_key)
    session = boto3.session.Session(profile_name=params['DATALAKE_S3_PROFILE_NAME'])
    s3 = session.resource('s3')
    bucket = s3.Bucket(params['DATALAKE_BUCKET_NAME'])

    sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'datalake_dir'"
    datalake_file_dir = query_all(connect(),sql_query)
    if len(datalake_file_dir) == 0:
        exit()

    datalake_file_dir=datalake_file_dir[0][0].lower()
    dl_dim_path=datalake_file_dir+'partner/bugs/streaming/v1/dimensions/'
    temp_dl_dim_path=datalake_file_dir+'temp/partner/bugs/streaming/v1/dimensions/'
    dl_s3_dim_prefix=params['DATALAKE_S3_FOLDER']+'/partner/bugs/streaming/v1/dimensions/'
    temp_dl_s3_dim_prefix=params['DATALAKE_S3_FOLDER']+'/temp/partner/bugs/streaming/v1/dimensions/'
    dim_name = "dim_consumers"

    sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'enriched_files_dir'"
    cons_enriched_file_dir = query_all(connect(),sql_query)

    if len(cons_enriched_file_dir) == 0:
        exit()

    cons_enriched_file_dir = cons_enriched_file_dir[0][0].lower()

    sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and spec_id like '%P654-STREAM%' and file_ready_consumer is not null"
    fileExpectation = query_all(connect(),sql_query)
    if len(fileExpectation) == 0:
        exit()

    filename = fileExpectation[0][12]
    report_day = filename.split('_')[2]
    provider_key = fileExpectation[0][2]
    cons_file = fileExpectation[0][28]
    cons_file_path = cons_enriched_file_dir+provider_key+"/"+report_day+cons_file

    cdf_new=sqlContext.read.csv(cons_file_path,sep="\07",header="false",schema=bugs_schema)

    current_date = datetime.strptime(report_day,'%Y%m%d').strftime('%Y-%m-%d')
    cons_new=cdf_new.select("consumer_key","customer_key").distinct()
    a_cons_new = cons_new.alias("a_cons_new")

    objs = list(bucket.objects.filter(Prefix=dl_s3_dim_prefix+'dim_consumers/'))
    if len(objs) > 1:
        cons_old=sqlContext.read.parquet(dl_dim_path+"dim_consumers/").cache()
        cons_old.count()
        cons_join=cons_new.union(cons_old).distinct()
        cons_df = cons_join
    else:
        print('dim_consumers does not exist in datalake. Create the dimension')

    cons_df=a_cons_new.withColumnRenamed("customer_key","consumer_id").alias("cons_df")
    cons_df=cons_df.select(cons_df["consumer_key"].cast(LongType()), cons_df["consumer_id"].cast(LongType()))

    write_dlake_dim(temp_dl_dim_path, dim_name, cons_df, bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix)
    #cons_df.drop("report_day").write.mode('overwrite').parquet(dl_dim_path+"dim_consumers/")

    cons_df.unpersist()
    a_cons_new.unpersist()

    if len(objs) > 1:
        cons_old.unpersist()
