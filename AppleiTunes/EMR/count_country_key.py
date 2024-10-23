#from pyspark.sql import SparkSession
from __future__ import print_function
#-----------------  IMPORT MODULES ----------------
from builtins import str
import sys,time,boto3,codecs,psycopg2
from datetime import datetime,timedelta

from env import init
retcode=init()

from utl_dlake_functions import *
from dlake_dim_new_keygen import *
from utl_mysql_interface import *
from utl_functions import *
import config
import pyspark
from pyspark import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.window import *
import re
import boto3
from pyspark.sql import SparkSession

config.metadata=read_config(section="table")
v_procname = 'appleitunes_dl_load_dlake'
prov_key='P001'
params=get_provider_params(prov_key)
sec_to_wait=10

#### GET INPUT DIR ##########
sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'enriched_files_dir_v2'"
cons_enriched_file_dir = query_all(connect(),sql_query)

if len(cons_enriched_file_dir) == 0:
    write_log_info(v_procname,'Input path not available',str(load_id))
    exit()

cons_enriched_file_dir = cons_enriched_file_dir[0][0].lower()
print(cons_enriched_file_dir.split())

sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'datalake_dir'"
datalake_file_dir = query_all(connect(),sql_query)
if len(datalake_file_dir) == 0:
    write_log_info(v_procname,'Datalake output path not available',str(load_id))
    exit()

datalake_file_dir=datalake_file_dir[0][0].lower()

sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'datalake_main'"

main_bucket = query_all(connect(),sql_query)
if len(main_bucket) == 0:
    print("Main Output path not available")
    exit()
datalake_main_dir  = main_bucket[0][0].lower()

dl_dim_path=datalake_file_dir+'partner_data/apple/downloads/v1/dimensions/'
temp_dl_dim_path=datalake_file_dir+'temp/partner_data/apple/downloads/v1/dimensions/'
dl_s3_dim_prefix=params['DATALAKE_S3_FOLDER']+'/partner_data/apple/downloads/v1/dimensions/'
temp_dl_s3_dim_prefix=params['DATALAKE_S3_FOLDER']+'/temp/partner_data/apple/downloads/v1/dimensions/'
dest=datalake_main_dir+'apple/partner_data/main/v1/fact_downloads'
dl_fact_path=datalake_file_dir+'partner_data/apple/downloads/v1/fact/fact_downloads/'
v_dest=datalake_main_dir+'apple/partner_data/main/v1/'
consumer_gras = 'partner_data/sme/P001/gras_enriched_files/'
prod_consumer_gras = 'partner_data/sme/P001/Prod_Gras_Enriched_files/'

if params['DATALAKE_S3_PROFILE_NAME'] not in boto3.session.Session().available_profiles:
    write_log_info(v_procname,'Could not find Datalake S3 AWS profile. Unable to proceed with loading dimensions',str(load_id),None,prov_key)
    exit()


codeBucket=params["CODE_BUCKET"]
codePath=params["CODE_PATH"]
etlProfile=params["ETL_PROFILE"]
crawlerName=params["CRAWLER_NAME"]
#["DATALAKE_BUCKET_NAME"]
rsconn=read_config(section="redshift_reportdb")
prodmainconn=read_config(section="redshift_etl")



##### GET S3 SESSION ##########
session = boto3.session.Session(region_name='s3-external-1', profile_name=params['DATALAKE_S3_PROFILE_NAME'])
s3 = session.resource('s3')
bucket = s3.Bucket(params['OUTPUT_BUCKET_NAME'])
print("bucket >>",'s3://'+bucket.name)


objs = list(bucket.objects.filter(Prefix=prod_consumer_gras+'report_date=2024-02-01'))
#print("objs >>>> ",objs)
list_files = []
for i in objs:
    #if not i.key.endswith("_SUCCESS") and not i.key.endswith(".manifest"):
    if not i.key.endswith("_SUCCESS") and not i.key.endswith(".manifest") and not i.key.endswith("enriched.csv.gz/") and not i.key.endswith("2024-02-01/"): #PROD
        list_files.append('s3://'+bucket.name+'/'+i.key)


print("list_files --->>>",list_files)

for i in list_files:
    fl = i.split('/')[:-1]
    fl = '/'.join(fl)+'/'
    df = sqlContext.read.csv(fl,header=True,sep="\x07")
    df = df.select(sum('quantity')).collect()[0][0]
    print(df)
    #df.createOrReplaceTempView('cdf')
    #df = sqlContext.sql("select sum(quantity) as sum from cdf").show(5,False)
