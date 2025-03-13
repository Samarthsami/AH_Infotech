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
#------------------- END IMPORT -------------------
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




if params['DATALAKE_S3_PROFILE_NAME'] not in boto3.session.Session().available_profiles:
    write_log_info(v_procname,'Could not find Datalake S3 AWS profile. Unable to proceed with loading dimensions',str(load_id),None,prov_key)
    exit()


codeBucket=params["CODE_BUCKET"]
codePath=params["CODE_PATH"]
etlProfile=params["ETL_PROFILE"]
crawlerName=params["CRAWLER_NAME"]

rsconn=read_config(section="redshift_reportdb")
prodmainconn=read_config(section="redshift_etl")





##### GET S3 SESSION ##########
session = boto3.session.Session(region_name='s3-external-1', profile_name=params['DATALAKE_S3_PROFILE_NAME'])
s3 = session.resource('s3')
bucket = s3.Bucket(params['DATALAKE_BUCKET_NAME'])

print("consumer enriched directory")
print(cons_enriched_file_dir)
print("datalake file dir")
print(datalake_file_dir)
print("dl_dim_path")
print(dl_dim_path)
print("temp_dl_dim_path")
print(temp_dl_dim_path)
print("dl_s3_dim_prefix")
print(dl_s3_dim_prefix)
print(temp_dl_s3_dim_prefix)



appleitunes_schema = StructType([                               StructField('load_id',StringType(),True)
                                                                ,StructField('trans_id',StringType(),True)
                                                                ,StructField('vendor_key',StringType(),True)
                                                                ,StructField('client_key',StringType(),True)
                                                                ,StructField('provider_key',StringType(),True)
                                                                ,StructField('provider',StringType(),True)
                                                                ,StructField('provider_country',StringType(),True)
                                                                ,StructField('upc',StringType(),True)
                                                                ,StructField('isrc',StringType(),True)
                                                                ,StructField('product_identifier',StringType(),True)
                                                                ,StructField('report_day',StringType(),True)
                                                                ,StructField('sale_return',StringType(),True)
                                                                ,StructField('currency_key',StringType(),True)
                                                                ,StructField('country_key',StringType(),True)
                                                                ,StructField('royalty_currency',StringType(),True)
                                                                ,StructField('preorder',StringType(),True)
                                                                ,StructField('isan',StringType(),True)
                                                                ,StructField('cma',StringType(),True)
                                                                ,StructField('asset_content',StringType(),True)
                                                                ,StructField('grid_no',StringType(),True)
                                                                ,StructField('promo_code',StringType(),True)
                                                                ,StructField('parent_id',StringType(),True)
                                                                ,StructField('parent_type_id',StringType(),True)
                                                                ,StructField('attributable_purchase',StringType(),True)
                                                                ,StructField('primary_genre',StringType(),True)
                                                                ,StructField('prod_no_dig',StringType(),True)
                                                                ,StructField('product_type_key',StringType(),True)
                                                                ,StructField('distribution_key',StringType(),True)
                                                                ,StructField('sales_type_key',StringType(),True)
                                                                ,StructField('service_type_key',StringType(),True)
                                                                ,StructField('transaction_type_key',StringType(),True)
                                                                ,StructField('distribution_channel_key',StringType(),True)
                                                                ,StructField('partner_id',StringType(),True)
                                                                ,StructField('label_key',StringType(),True)
                                                                ,StructField('product_key',StringType(),True)
                                                                ,StructField('artist_key',StringType(),True)
                                                                ,StructField('spnl_key',StringType(),True)
                                                                ,StructField('sales_division_key',StringType(),True)
                                                                ,StructField('product_conf_key',StringType(),True)
                                                                ,StructField('product_types_key',StringType(),True)
                                                                ,StructField('sales_category_key',StringType(),True)
                                                                ,StructField('zip_code',StringType(),True)
                                                                ,StructField('transaction_date_key',StringType(),True)
                                                                ,StructField('consumer_key',StringType(),True)
                                                                ,StructField('vendor_identifier',StringType(),True)
                                                                ,StructField('artist',StringType(),True)
                                                                ,StructField('title',StringType(),True)
                                                                ,StructField('label',StringType(),True)
                                                                ,StructField('quantity',StringType(),True)
                                                                ,StructField('quantity_returned',StringType(),True)
                                                                ,StructField('download_date',StringType(),True)
                                                                ,StructField('order_id',StringType(),True)
                                                                ,StructField('customer_id',StringType(),True)
                                                                ,StructField('apple_id',StringType(),True)
                                                                ,StructField('vendor_offer_code',StringType(),True)
                                                                ,StructField('retail_value',StringType(),True)
                                                                ,StructField('wholesale_value',StringType(),True)
                                                                ,StructField('rpu',StringType(),True)
                                                                ,StructField('wpu',StringType(),True)
                                                                ,StructField('media_key',StringType(),True)
                                                        ])



###################### Apple.dim_consumers ####################

def itunes_load_consumers(load_id,trans_id,lic):

    write_log_info(v_procname,'Begin loading dim_consumers ',str(load_id),None,prov_key)
    sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and trans_id = "+ str(trans_id) +" \
                                                             and file_available is not null and file_complete is null order by spec_id,trans_id \
             and file_ready_consumer is not null"
    fileExpectation = query_all(connect(),sql_query)
    if len(fileExpectation) == 0:
        write_log_info(v_procname,' No conusumer files to load ',str(load_id))
        exit()


    filename = fileExpectation[0][12]
    report_day = filename.split('_')[3]
    provider_key = fileExpectation[0][2]
    cons_file = fileExpectation[0][28]
    cons_file_path = cons_enriched_file_dir+lic+'/'+provider_key+cons_file
    #temp_dl_dim_cons_path=temp_dl_dim_path+'/report_licensor='+lic+'/'
    print("filename" +str(filename))
    print("report_day " +str(report_day))
    print("provider_key "+ str(provider_key))
    print("consumer file" +str(cons_file))
    print("consumer file path " +str(cons_file_path))
   # print("temp consumer file path "+str(temp_dl_dim_cons_path))
    write_log_info(v_procname,'Reading processed AppleMusic Consumer data from '+cons_file_path,str(load_id),None,provider_key)
    cdf_new=sqlContext.read.csv(cons_file_path,sep="\07",header="false",schema=appleitunes_schema)

    current_date = datetime.datetime.strptime(report_day,'%Y%m%d').strftime('%Y-%m-%d')
    cons_new=cdf_new.select("consumer_key","customer_id").distinct()


    objs = list(bucket.objects.filter(Prefix=dl_s3_dim_prefix+'dim_consumers/report_licensor='+lic+'/'))
    if len(objs) > 1:
        write_log_info(v_procname,'dim_consumers exists in datalake and reading from '+dl_dim_path+'dim_consumers/report_licensor='+lic+'/',str(load_id),None,provider_key)
        cons_old=sqlContext.read.parquet(dl_dim_path+'dim_consumers/report_licensor='+lic+'/*').cache()
        cons_old.count()
        cons_join=cons_new.union(cons_old).distinct()
        cons_df = cons_join.alias('cons_df')

    else:
        write_log_info(v_procname,'dim_consumers does not exist in datalake. Create the dimension',str(load_id),None,provider_key)
        cons_df=cdf_new.alias('cons_df')

    cons_df=cons_df.withColumnRenamed("customer_id","consumer_id").alias("cons_df")
    cons_df=cons_df.select(cons_df["consumer_key"].cast(LongType()), cons_df["consumer_id"].cast(StringType()))

    write_log_info(v_procname,'Writing dim_consumers to '+dl_dim_path+"dim_consumers/report_licensor="+lic+'/',str(load_id),None,provider_key)
    cons_df=write_dlake_dim_v2(temp_dl_dim_path, 'dim_consumers/report_licensor='+lic+'/', cons_df.drop("report_day"), bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix)

    cons_df.unpersist()

    if len(objs) > 1:
        cons_old.unpersist()

    write_log_info(v_procname,'Finished loading dim_consumers ',str(load_id),None,provider_key)
    return 0



################# appleitunes.dim_partner_info ################
def itunes_load_partner_info(load_id,trans_id,lic):

    write_log_info(v_procname,'Begin loading dim_partner_info ',str(load_id),None,prov_key)
    sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and trans_id = "+ str(trans_id) +" and  file_ready_consumer is not null and file_complete is null"
    fileExpectation = query_all(connect(),sql_query)
    if len(fileExpectation) == 0:
        write_log_info(v_procname,' No dim_partner_info files to load ',str(load_id))
        exit()

    flag='itunes_partner_info_key_gen'
    key_name='itunes_partner_info_key'
    filename = fileExpectation[0][12]
    report_day = filename.split('/')[8]
    current_date = datetime.datetime.strptime(report_day,'%Y%m%d').strftime('%d/%m/%Y')
    provider_key = fileExpectation[0][2]
    cons_file = fileExpectation[0][28]
    licensor_name=lic
    cons_file_path = cons_enriched_file_dir+licensor_name+'/'+provider_key+'/'+cons_file
    print(cons_file_path)
    write_log_info(v_procname,'Reading processed data from '+cons_file_path,str(load_id),None,provider_key)
    tdf_new=sqlContext.read.csv(cons_file_path,sep="\07",header="false",schema=appleitunes_schema).na.fill({'isrc':'UNKNOWN','upc':'UNKNOWN','grid_no':'UNKNOWN'}).filter(col("isrc") != "UNKNOWN").filter(col("isrc") != "null")
    tdf_new=tdf_new.withColumn('isrc',when(tdf_new.isrc == 'null','UNKNOWN').otherwise(tdf_new.isrc))\
                   .withColumn('upc',when(tdf_new.upc == 'null','UNKNOWN').otherwise(tdf_new.upc))\
                   .withColumn('grid_no',when(tdf_new.grid_no == 'null','UNKNOWN').otherwise(tdf_new.grid_no))
    cols = tdf_new.columns
    for colm in cols:
        tdf_new = tdf_new.withColumn(colm, when(tdf_new[colm]==0,None).otherwise(tdf_new[colm]))
        #tdf_new = tdf_new.withColumn(colm, when(length(trim(tdf_new[colm]))==0,None).otherwise(tdf_new[colm]))
        tdf_new = tdf_new.withColumn(colm, when(length(trim(tdf_new[colm]))==0,None).otherwise(tdf_new[colm]))


    tdf_new=tdf_new.withColumn("report_day",lit(report_day)).alias('tdf_new')
    tdf_new=tdf_new.selectExpr("apple_id as apple_id","upc as upc","isrc as isrc", "artist as artist_name", "title as track_title", "label as label_name",\
                                                                                                                "product_identifier as product_identifier","isan as isan", "asset_content as asset_content",\
                                   "grid_no as grid_no", "primary_genre as primary_genre", "prod_no_dig as prod_no_dig","report_day").distinct().alias('tdf_new')

    #tdf_new.createOrReplaceTempView('c')

    tdf_new=tdf_new.groupBy("apple_id").agg(max("upc"),max("isrc"),max("artist_name"),max("track_title"),max("label_name"),max("product_identifier"),\
                                        max("isan"),max("asset_content"),max("grid_no"),max("primary_genre"), max("prod_no_dig"), max("report_day"))\
                              .withColumnRenamed("max(upc)","upc")\
                              .withColumnRenamed("max(isrc)","isrc")\
                              .withColumnRenamed("max(artist_name)","artist_name")\
                              .withColumnRenamed("max(track_title)","track_title")\
                              .withColumnRenamed("max(label_name)","label_name")\
                              .withColumnRenamed("max(product_identifier)","product_identifier")\
                              .withColumnRenamed("max(isan)","isan")\
                              .withColumnRenamed("max(asset_content)","asset_content")\
                              .withColumnRenamed("max(grid_no)","grid_no")\
                              .withColumnRenamed("max(primary_genre)","primary_genre")\
                              .withColumnRenamed("max(prod_no_dig)","prod_no_dig")\
                              .withColumnRenamed("max(report_day)","report_day")\
                              .select("apple_id","upc","isrc","artist_name","track_title","label_name","product_identifier","isan","asset_content","grid_no","primary_genre","prod_no_dig","report_day")\
                              .alias('tdf_new')
    #print("tdf new")
    #tdf_new.show()
    #tdf_new.createOrReplaceTempView('c')
    #sqlContext.sql("select * from c where apple_id =423549784 ").show()
    while True:
        objs = list(bucket.objects.filter(Prefix=dl_s3_dim_prefix+'dim_partner_info_with_report_date/'))
        if len(objs) > 1:
            write_log_info(v_procname,'dim_partner_info exists in datalake and reading from '+dl_dim_path+'dim_partner_info_with_report_date/',str(load_id),None,provider_key)
            tdf_old=sqlContext.read.parquet(dl_dim_path+"dim_partner_info_with_report_date/")
            tdf_old.cache()##### persist(StorageLevel.MEMORY_AND_DISK).Doing this since SPARK complains about simultaneous read and write operation from same S3 location
            tdf_old.count()##### Explicit call due to laziness of SPARK DFs
            tid=tdf_old.withColumnRenamed("partner_info_key","itunes_partner_info_key")
     #       print('tid here print')
     #       tid.show()

            #dataWithKey=tdf_new.join(tid,(lower(tdf_new.isrc)==(lower(tid.isrc))),how='left').select("tdf_new.*","itunes_partner_info_key").distinct()

            dataWithKey = tdf_new.join(tid,(lower(tdf_new.apple_id)==(lower(tid.apple_id))),how='left').select("tdf_new.*","itunes_partner_info_key").distinct()
            dataWithKey=dataWithKey.alias('dataWithKey')
            #print('datawithKey print here')
            #dataWithKey.show()
            print()
            #new_count=dataWithKey.filter(col("itunes_partner_info_key").isNull()).count()
            new_count=dataWithKey.filter(col("itunes_partner_info_key").isNull()).count()
            dataWithKey.filter(col("itunes_partner_info_key").isNull()).show()
            print(new_count)

            if new_count > 0:
                write_log_info(v_procname,str(new_count)+' New ISRCs in file. Need to generate partner info keys',str(load_id),None,provider_key)
                print('provider_key', str(provider_key))
                print('flag', str(flag))
                print('key_name', str(key_name))
                print('reading/...')
                tdf_new=enrich_with_new_keys(dataWithKey,new_count,provider_key,flag,key_name)
                print('read to dataframe 1')
                tdf_new=tdf_new.select("itunes_partner_info_key","apple_id","upc","isrc","artist_name","track_title","label_name","product_identifier","isan","asset_content","grid_no","primary_genre",\
                                                               "prod_no_dig","report_day")
#                print('tdf_new after passing thru enrich_with_new_keys')
#                tdf_new.show()

                tdf=tdf_new.union(tid) ## COMBINE NEW ISRCs AND OLD ISRCs
                w = Window.partitionBy(tdf['itunes_partner_info_key']).orderBy(tdf['report_day'].desc())
                tdf=tdf.withColumn("rn", row_number().over(w))
                tdf=tdf.where(tdf.rn==1).drop("rn").distinct()
            else:
                write_log_info(v_procname,'No new TRACK KEYs in file. Not generating any keys',str(load_id),None,provider_key)
                print('No new TRACK KEYs in file. Not generating any keys')
                tdf=tdf_old
                break
        else:
            write_log_info(v_procname,'dim_partner_info_with_report_date does not exist in datalake. Generate partner info keys and create the dimension',str(load_id),None,provider_key)
            tdf1=enrich_with_new_keys(tdf_new.withColumn("itunes_partner_info_key", lit(None)),tdf_new.count(),provider_key,flag,key_name)
            exec("undf=sc.parallelize([Row('UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKOWN','Unknown','Unknown','Unknown','Unknown','UNKNOWN','UNKNOWN','"+report_day+"',-1)]).toDF()")
            tdf=tdf1.union(undf).distinct()
      #  print("tdf after no new tracks")
      #  tdf.show(2)
        #exit()
        tdf=tdf.withColumnRenamed("itunes_partner_info_key","partner_info_key")
        tdf.cache()
        tdf.count()
        tdf = tdf.repartition(10)
        write_log_info(v_procname,'Writing dim_partner_info_with_report_date to '+dl_dim_path+"dim_partner_info_with_report_day/",\
str(load_id),None,provider_key)
        tdf=tdf.select(tdf["partner_info_key"].cast(LongType()),tdf["apple_id"].cast(LongType()),"upc","isrc","artist_name","track_title","label_name",\
"product_identifier","isan","asset_content","grid_no","primary_genre",\
                                         "prod_no_dig","report_day")
        tdf.createOrReplaceTempView('a')
        #sqlContext.sql("select * from a ").show(4)
        #exit()
        tdf.write.mode('overwrite').parquet(dl_dim_path+"dim_partner_info_with_report_date/")
        write_log_info(v_procname,'Writing dim_partner_info to '+dl_dim_path+"dim_partner_info/",str(load_id),None,provider_key)
        tdf=tdf.select(tdf["partner_info_key"].cast(LongType()),tdf["apple_id"].cast(LongType()),"upc","isrc","artist_name","track_title","label_name","product_identifier","isan","asset_content","grid_no","primary_genre",\
                                  "prod_no_dig")
        tdf.write.mode('overwrite').parquet(dl_dim_path+"dim_partner_info/")
        break

    tdf.unpersist()
    tdf_new.unpersist()
    if len(objs) > 1:
        tdf_old.unpersist()
        dataWithKey.unpersist()
    write_log_info(v_procname,'Finished loading dim_partner_info',str(load_id),None,provider_key)
    return 0





#######################appleitunes.dim_promo_info ################
def itunes_load_promo_info(load_id,trans_id,lic):

    write_log_info(v_procname,'Begin loading dim_promo_info ',str(load_id),None,prov_key)
    sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and trans_id = "+ str(trans_id) +" and  file_ready_consumer is not null and file_complete is null"
    fileExpectation = query_all(connect(),sql_query)
    if len(fileExpectation) == 0:
        write_log_info(v_procname,' No dim_promo_info files to load ',str(load_id))
        exit()

    flag='itunes_promo_info_key_gen'
    key_name='itunes_promo_info_key'
    filename = fileExpectation[0][12]
    print(filename)
    report_day = filename.split('/')[8]
    provider_key = fileExpectation[0][2]
    cons_file = fileExpectation[0][28]
    licensor_name=lic
    cons_file_path = cons_enriched_file_dir+licensor_name+'/'+provider_key+cons_file
    print(cons_file_path)
    print(report_day)
    write_log_info(v_procname,'Reading processed data from '+cons_file_path,str(load_id),None,provider_key)
    print("promo code dim started ")

    tdf_new=sqlContext.read.csv(cons_file_path,sep="\07",header="true",schema=appleitunes_schema)


    current_date = datetime.datetime.strptime(report_day,'%Y%m%d').strftime('%Y-%m-%d')
    report_day = report_day[0:4]+"-"+report_day[4:6]+"-"+report_day[6:]
    tdf_new=tdf_new.select("promo_code", "cma").distinct()

    cols = tdf_new.columns
    for colm in cols:
           tdf_new = tdf_new.withColumn(colm, when (length(trim(tdf_new[colm]))==0, None).otherwise(tdf_new[colm]))
    tdf_new=tdf_new.withColumn('n_promo_code',lit('UNK'))\
                   .withColumn('n_cma',lit('UNK'))




    tdf_new=tdf_new.withColumn('n_promo_code',coalesce(tdf_new.promo_code,tdf_new.n_promo_code)) \
                   .withColumn('n_cma',coalesce(tdf_new.cma,tdf_new.n_cma))\
                   .withColumnRenamed('promo_code','old_promo_code')\
                   .withColumnRenamed('n_promo_code','promo_code')\
                   .withColumnRenamed('cma','old_cma')\
                   .withColumnRenamed('n_cma','cma')



    tdf_new=tdf_new.withColumn("report_day",lit(report_day))\
                   .withColumn('etl_hash',md5(concat_ws('',tdf_new.promo_code,tdf_new.cma)))\
                   .alias('tdf_new')




   # tdf_new.createOrReplaceTempView('a')
    #sqlContext.sql('select promo_code, old_promo_code, cma, old_cma,etl_hash from a group by 1,2,3,4,5').show()
    tdf_new=tdf_new.selectExpr("promo_code", "cma as cma_code","etl_hash","report_day").distinct().alias('tdf_new')


    while True:

          objs = list(bucket.objects.filter(Prefix=dl_s3_dim_prefix+'dim_promo_info_with_report_date/'))
          if len(objs) > 1:
                    write_log_info(v_procname,'dim_promo_info exists in datalake and reading from '+dl_dim_path+'dim_promo_info_with_report_date/',str(load_id),None,provider_key)
                    tdf_old=sqlContext.read.parquet(dl_dim_path+"dim_promo_info_with_report_date/").cache()  #### persist(StorageLevel.MEMORY_AND_DISK).Doing this since SPARK complains about simultaneous read and write operation from same S3 location
                    tdf_old.count() #### Explicit call due to laziness of SPARK DFs

                    tid=tdf_old.withColumnRenamed("promo_info_key","itunes_promo_info_key")
                    dataWithKey =tdf_new.join(tid,tid.etl_hash==tdf_new.etl_hash, how ='left')\
                                        .select("tdf_new.etl_hash","itunes_promo_info_key").distinct()

                    new_count = dataWithKey.filter(col("itunes_promo_info_key").isNull()).count()
                    if new_count>0:
                        write_log_info(v_procname,str(new_count)+' New Promo code and cma  info in file. Need to generate promo info keys',str(load_id),None,provider_key)
                        pdf=enrich_with_new_keys(dataWithKey,new_count,provider_key,flag,key_name)
                        pdf=pdf.alias('pdf')
                        tdf_new=tdf_new.join(pdf, tdf_new.etl_hash==pdf.etl_hash,how='left')\
                                       .select('pdf.itunes_promo_info_key','tdf_new.promo_code','tdf_new.cma_code','tdf_new.etl_hash','tdf_new.report_day').alias('tdf_new')


                    ##Combine new promo kets and old promo keys
                        tdf=tdf_new.union(tdf_old).alias('tdf')
                        tdf=tdf.groupBy('etl_hash','itunes_promo_info_key')\
                               .agg(max('promo_code'), max('cma_code'), max('report_day'))\
                               .withColumnRenamed('max(promo_code)','promo_code')\
                               .withColumnRenamed('max(cma_code)','cma_code')\
                               .withColumnRenamed('max(report_day)','report_day')\
                               .select("itunes_promo_info_key","promo_code","cma_code","etl_hash","report_day")\
                               .alias('tdf')
                    else:
                        write_log_info(v_procname,'No new promo cma codes in file. Not generating any keys',str(load_id),None,provider_key)
                        print('No new promo cma codes in file. Not generating any keys')
                        tdf=tdf_old
                        break


          else:
              write_log_info(v_procname,'dim_promo_info_with_report_date does not exist in datalake. Generate partner info keys and create the dimension',str(load_id),None,provider_key)
              hdf=tdf_new.select('etl_hash').distinct().alias('hdf')
              pdf=enrich_with_new_keys(hdf.withColumn("itunes_promo_info__key", lit(None)),hdf.count(),provider_key,flag,key_name)
              pdf=pdf.alias('pdf')
              tdf1=tdf_new.join(pdf, tdf_new.hash_val==pdf.hash_val,how='left')\
                          .select('itunes_promo_info_key', 'tdf_new.promo_code','cma_code', 'tdf_new.etl_hash', \
                                    'tdf_new.report_day').alias('tdf1')
#              tdf1.show()
              tdf1=tdf1.groupBy('etl_hash','itunes_promo_info_key')\
                       .agg(max('promo_code'), max('cma_code'), max('report_day'))\
                       .withColumnRenamed('max(promo_code)','promo_code')\
                       .withColumnRenamed('max(cma_code)','cma_code')\
                       .withColumnRenamed('max(report_day)','report_day')\
                       .select("itunes_promo_info_key","promo_code","cma_code","etl_hash","report_day")\
                               .alias('tdf1')

              exec("undf=sc.parallelize([Row(-1,'UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','"+report_day+"')]).toDF()")
              tdf=tdf1.union(undf).distinct()

          tdf=tdf.withColumnRenamed("itunes_promo_info_key","promo_info_key")
          tdf.cache()
          tdf.count()
          write_log_info(v_procname,'Writing dim_promo_info_with_report_date to '+dl_dim_path+"dim_promo_info_with_report_day/",str(load_id),None,provider_key)
          tdf=tdf.select(tdf["promo_info_key"].cast(LongType()),"promo_code","cma_code","etl_hash","report_day")\
                 .na.fill({'promo_code':'UNK','cma_code':'UNK'})
          write_dlake_dim(temp_dl_dim_path, 'dim_promo_info_with_report_date', tdf, bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix)

          write_log_info(v_procname,'Writing dim_promo_info to '+dl_dim_path+"dim_promo_info/",str(load_id),None,provider_key)
          tdf=tdf.select(tdf["promo_info_key"].cast(LongType()),"promo_code","cma_code")
          write_dlake_dim(temp_dl_dim_path, 'dim_promo_info', tdf, bucket,dl_s3_dim_prefix,temp_dl_s3_dim_prefix)
          break

    tdf.unpersist()
    tdf_new.unpersist()
    if len(objs) > 1:
        tdf_old.unpersist()
        dataWithKey.unpersist()

    write_log_info(v_procname,'Finished loading dim_promo_info',str(load_id),None,provider_key)
    return 0





def itunes_load_partner_regions(load_id,trans_id,lic):
    write_log_info(v_procname,'Begin loading dim_partner_regions ',str(load_id),None,prov_key)
    sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and trans_id = "+ str(trans_id) +" and  file_ready_consumer is not null and file_complete is null"
    fileExpectation = query_all(connect(),sql_query)
    if len(fileExpectation) == 0:
        write_log_info(v_procname,' No dim_partner_regions files to load ',str(load_id))
        exit()

    flag='itunes_partner_region_key_gen'
    key_name='itunes_partner_region_key'
    filename = fileExpectation[0][12]
    print(filename)
    report_day = filename.split('/')[8]
    provider_key = fileExpectation[0][2]
    cons_file = fileExpectation[0][28]
    licensor_name=lic
    cons_file_path = cons_enriched_file_dir+licensor_name+'/'+provider_key+cons_file
    print(cons_file_path)
    print(report_day)
    write_log_info(v_procname,'Reading processed data from '+cons_file_path,str(load_id),None,provider_key)
    print("partner regions dim started ")

    tdf_new=sqlContext.read.csv(cons_file_path,sep="\07",header="true",schema=appleitunes_schema)

    current_date = datetime.datetime.strptime(report_day,'%Y%m%d').strftime('%Y-%m-%d')
    report_day = report_day[0:4]+"-"+report_day[4:6]+"-"+report_day[6:]
    tdf_new=tdf_new.select("country_key", "zip_code").distinct()

    cols = tdf_new.columns
    for colm in cols:
           tdf_new = tdf_new.withColumn(colm, when (length(trim(tdf_new[colm]))==0, None).otherwise(tdf_new[colm]))

    tdf_new=tdf_new.withColumn('n_country_key',lit(-1))\
                   .withColumn('n_zip_code',lit(-1))

    tdf_new=tdf_new.withColumn('n_country_key',coalesce(tdf_new.country_key,tdf_new.n_country_key)) \
               .withColumn('n_zip_code',coalesce(tdf_new.zip_code,tdf_new.n_zip_code))\
               .withColumnRenamed('country_key','old_country_key')\
               .withColumnRenamed('n_country_key','country_key')\
               .withColumnRenamed('zip_code','old_zip_code')\
               .withColumnRenamed('n_zip_code','zip_code')
    tdf_new=tdf_new.withColumn("t_country_key", when(col('country_key') =='ZZ',-1).otherwise(col('country_key')))\
               .withColumn("t_zip_code",when(length(col('zip_code')) == 0, -1).otherwise(col('zip_code')))

    tdf_new=tdf_new.withColumn("report_day",lit(report_day))\
                   .withColumn('etl_hash',md5(concat_ws('',tdf_new.country_key,tdf_new.zip_code)))\
                   .alias('tdf_new')

#    tdf_new.createOrReplaceTempView('a')
#    sqlContext.sql('select country_key, zip_code,etl_hash from a group by 1,2,3').show()
    tdf_new=tdf_new.selectExpr("country_key as country_code", "zip_code","etl_hash","report_day").distinct().alias('tdf_new')
 #   print("tdf_new")

#   tdf_new.show(3)
    print("tid")

    while True:
          objs = list(bucket.objects.filter(Prefix=dl_s3_dim_prefix+'dim_partner_regions_with_report_date/'))
          if len(objs) > 1:
                    write_log_info(v_procname,'dim_partner_regions exists in datalake and reading from '+dl_dim_path+'dim_partner_regions_with_report_date/',str(load_id),None,provider_key)
                    tdf_old=sqlContext.read.parquet(dl_dim_path+"dim_partner_regions_with_report_date/").cache()  #### persist(StorageLevel.MEMORY_AND_DISK).Doing this since SPARK complains about simultaneous read and write operation from same S3 location
                    tdf_old.count() #### Explicit call due to laziness of SPARK DFs
                    tid=tdf_old.withColumnRenamed("partner_region_key","itunes_partner_region_key")
                    #tid.show()


                    dataWithKey =tdf_new.join(tid,tid.etl_hash==tdf_new.etl_hash, how ='left')\
                                        .select("tdf_new.etl_hash","itunes_partner_region_key").distinct()
                    print("datawithKey")
                    #dataWithKey.show(3)
                    #exit()
                    new_count = dataWithKey.filter(col("itunes_partner_region_key").isNull()).count()
                    print("new zip codes")
                    dataWithKey.filter(col("itunes_partner_region_key").isNull()).show()
                    if new_count>0:
                        write_log_info(v_procname,str(new_count)+' New Partner Regions and zipcodes exists in file. Need to generate partner region keys',str(load_id),None,provider_key)
                        pdf=enrich_with_new_keys(dataWithKey,new_count,provider_key,flag,key_name)
                        pdf=pdf.alias('pdf')
                        tdf_new=tdf_new.join(pdf, tdf_new.etl_hash==pdf.etl_hash,how='left')\
                                       .select('itunes_partner_region_key','tdf_new.country_code','tdf_new.zip_code','tdf_new.etl_hash','tdf_new.report_day').alias('tdf_new')
                        print("tdf new enr with key")
                     #   tdf_new.show(3)
                        #exit()
                     ##Combine new promo kets and old promo keys
                        tdf=tdf_new.union(tdf_old).alias('tdf')
                        tdf=tdf.groupBy('etl_hash','itunes_partner_region_key')\
                               .agg(max('country_code'), max('zip_code'), max('report_day'))\
                               .withColumnRenamed('max(country_code)','country_code')\
                               .withColumnRenamed('max(zip_code)','zip_code')\
                               .withColumnRenamed('max(report_day)','report_day')\
                               .select("itunes_partner_region_key","country_code","zip_code","etl_hash","report_day")\
                               .alias('tdf')
                      #  print("tdf after Union")
                      #  tdf.show(3)
                        #exit()
                    else:
                        write_log_info(v_procname,'No new country and zipcodes in file. Not generating any keys',str(load_id),None,provider_key)
                        print('No new country and zip codes in file. Not generating any keys')
                        tdf=tdf_old
                        break


          else:
                    write_log_info(v_procname,'dim_partner_regions_with_report_date does not exist in datalake. Generate partner region keys and create the dimension',str(load_id),None,provider_key)
                    tdf1=enrich_with_new_keys(tdf_new.withColumn("itunes_partner_region_key", lit(None)),tdf_new.count(),provider_key,flag,key_name)
                    exec("undf=sc.parallelize([Row('UNKNOWN','UNKNOWN','Unknown','"+report_day+"',-1)]).toDF()")
                    tdf=tdf1.union(undf).distinct()


          tdf=tdf.withColumnRenamed("itunes_partner_region_key","partner_region_key")
          tdf.cache()
          tdf.count()
          tdf = tdf.repartition(10)
          write_log_info(v_procname,'Writing dim_partner_regions_with_report_date to '+dl_dim_path+"dim_partner_regions_with_report_day/",str(load_id),None,provider_key)
          tdf=tdf.select(tdf["partner_region_key"].cast(LongType()),"country_code","zip_code","etl_hash","report_day")
          tdf.write.mode('overwrite').parquet(dl_dim_path+"dim_partner_regions_with_report_date/")
          write_log_info(v_procname,'Writing dim_partner_regions to '+dl_dim_path+"dim_partner_regions/",str(load_id),None,provider_key)
          tdf=tdf.select(tdf["partner_region_key"].cast(LongType()),"country_code","zip_code","etl_hash")
          tdf.write.mode('overwrite').parquet(dl_dim_path+"dim_partner_regions/")
          break
    #print("after Union")
    #exit()
    tdf.unpersist()
    tdf_new.unpersist()
    if len(objs) > 1:
        tdf_old.unpersist()
        dataWithKey.unpersist()
    write_log_info(v_procname,'Finished loading dim_partner_regions',str(load_id),None,provider_key)
    return 0





def itunes_dl_load_fact(load_id,trans_id,lic):
    write_log_info(v_procname,'Begin loading itunes download fact started ',str(load_id),None,prov_key)
    sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and trans_id = "+ str(trans_id) +" and  file_ready_consumer is not null and file_complete is null"
    fileExpectation = query_all(connect(),sql_query)
    if len(fileExpectation) == 0:
        write_log_info(v_procname,' No files to load ',str(load_id))
        exit()


    userFile = fileExpectation[0][12]
    load_id = load_id
    transid = fileExpectation[0][1]
    clientKey = fileExpectation[0][27]
    provider_key = fileExpectation[0][2]
    report_day = userFile.split('_')[3]
    spec_id = userFile.split('_')[2]
    dl_file = fileExpectation[0][28]

    sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'enriched_files_dir_v2'"
    activity_enriched_file_dir = query_all(connect(),sql_query)
    if len(activity_enriched_file_dir) == 0:
        write_log_info(v_procname,'Input path not available',str(load_id),None,provider_key)
        exit()

    activity_enriched_file_dir = activity_enriched_file_dir[0][0].lower()
    dl_file_path = activity_enriched_file_dir+lic+'/'+provider_key+"/"+dl_file

    sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'datalake_dir'"

    datalake_file_dir = query_all(connect(),sql_query)
    if len(datalake_file_dir) == 0:
        write_log_info(v_procname,'Datalake output path not available',str(load_id),None,provider_key)
        exit()
    print(spec_id)

    datalake_file_dir=datalake_file_dir[0][0].lower()
    datalake_file_path=datalake_file_dir+"partner_data/apple/downloads/v1/fact/fact_downloads/report_date="+datetime.datetime.strptime(report_day, "%Y%m%d").strftime('%Y-%m-%d')+'/report_licensor='+lic+'/'+load_id+'_'+spec_id

    dl_dim_path=datalake_file_dir+'partner_data/apple/downloads/v1/dimensions/'
 #   print(userFile)
 #   print(transid)
 #   print(clientKey)
 #   print(provider_key)
 #   print(report_day)
 #   print(dl_file_path)
 #   print(datalake_file_path)
 #   print(lic)

    write_log_info(v_procname,'Reading processd iTunes Downloads data from '+dl_file_path,str(load_id),None,provider_key)
    strdf=sqlContext.read.csv(dl_file_path,sep="\07",header="true",schema=appleitunes_schema).alias('strdf')
    print(strdf.count())

    #partner_info
    write_log_info(v_procname,'Reading dim_partner_info data from '+dl_dim_path+'dim_partner_info/',str(load_id),None,provider_key)
    pidf=sqlContext.read.parquet(dl_dim_path+"dim_partner_info/").alias('pidf')
    pidf.createOrReplaceTempView('a')

    write_log_info(v_procname,'Join dim_partner_info and downloads data ',str(load_id),None,provider_key)
    strdf=strdf.join(pidf,strdf.apple_id.eqNullSafe(pidf.apple_id),how='left').select("strdf.*","pidf.partner_info_key").alias('strdf')

    ##promo_code
    write_log_info(v_procname,'Reading dim_promo_info data  from '+dl_dim_path+'dim_promo_info/',str(load_id),None,provider_key)
    prdf=sqlContext.read.parquet(dl_dim_path+"dim_promo_info/").alias('prdf')

    write_log_info(v_procname,'Join promo code and downloads data ',str(load_id),None,provider_key)
    strdf=strdf.withColumn('n_promo_code',lit('UNK'))\
               .withColumn('n_cma',lit('UNK'))

    strdf=strdf.withColumn('n_promo_code',coalesce(strdf.promo_code,strdf.n_promo_code)) \
                   .withColumn('n_cma',coalesce(strdf.cma,strdf.n_cma))\
                   .withColumnRenamed('promo_code','old_promo_code')\
                   .withColumnRenamed('n_promo_code','promo_code')\
                   .withColumnRenamed('cma','old_cma')\
                   .withColumnRenamed('n_cma','cma')

    strdf= strdf.join(prdf,(lower(strdf.promo_code)==lower(prdf.promo_code)) & (lower(strdf.cma)==lower(prdf.cma_code)),how='left')\
                .select("strdf.*","prdf.promo_info_key").alias("strdf")
    #print("Dim_promo_code count -->",strdf.count())

#    strdf=strdf.withColumn('n_country_key',lit(-1))\
#               .withColumn('n_zip_code',lit(-1))
#
#    strdf=strdf.withColumn('n_country_key',coalesce(strdf.country_key,strdf.n_country_key)) \
#               .withColumn('n_zip_code',coalesce(strdf.zip_code,strdf.n_zip_code))\
#               .withColumnRenamed('country_key','old_country_key')\
#               .withColumnRenamed('n_country_key','country_key')\
#               .withColumnRenamed('zip_code','old_zip_code')\
#               .withColumnRenamed('n_zip_code','zip_code')
#    strdf=strdf.withColumn("t_country_key", when(col('country_key') =='ZZ',-1).otherwise(col('country_key')))\
#               .withColumn("t_zip_code",when(length(col('zip_code')) == 0, -1).otherwise(col('zip_code')))



    ##partner regions
    write_log_info(v_procname,'Reading dim_partner_regions data  from '+dl_dim_path+'dim_partner_regions/',str(load_id),None,provider_key)
    pgdf=sqlContext.read.parquet(dl_dim_path+"dim_partner_regions/").alias('pgdf')

    write_log_info(v_procname,'Join dim_partner_regions and downloads data ',str(load_id),None,provider_key)
    strdf=strdf.join(pgdf,(lower(strdf.country_key)==lower(pgdf.country_code)) & (lower(strdf.zip_code)==lower(pgdf.zip_code)),how='left').select("strdf.*","pgdf.partner_region_key").alias('strdf')

    prov_df = get_df_from_db_table("dim_provider").alias('prov_df')
    vend_df = get_df_from_db_table("dim_vendor").alias('vend_df')
    spnl_df = get_df_from_db_table("dim_spnl").alias('spnl_df')
    salt_df = get_df_from_db_table("dim_sales_types").alias('salt_df')
    cli_df = get_df_from_db_table("dim_client").alias('cli_df')

    strdf=strdf.withColumnRenamed("provider_key", "provider_key_orig")\
                .withColumnRenamed("vendor_key","vendor_key_orig")\
                .withColumnRenamed("spnl_key", "spnl_key_orig")\
                .withColumnRenamed("sales_type_key","sales_type_key_orig")\
                .withColumnRenamed("client_key","client_key_orig")\
                .alias('strdf')

    strdf=strdf.join(prov_df,strdf.provider_key_orig==prov_df.provider_cd, how='left')\
                .select("strdf.*","prov_df.provider_key").alias("strdf")\
                .join(vend_df,strdf.vendor_key_orig==vend_df.vendor_cd,how='left')\
                .select("strdf.*","vend_df.vendor_key").alias("strdf")\
                .join(broadcast(spnl_df),strdf.spnl_key_orig==spnl_df.spnl_id,'left')\
                .select("strdf.*","spnl_df.spnl_key").alias("strdf")\
                .join(broadcast(salt_df),strdf.sales_type_key_orig==salt_df.sales_type_code,'left')\
                .select("strdf.*","salt_df.sales_type_key").alias("strdf")\
                .join(broadcast(cli_df),strdf.client_key_orig==cli_df.client_id,'left')\
                .select("strdf.*","cli_df.client_key").alias("strdf")





    ##build fact df
    strdf=strdf.withColumn("transaction_amount",when(trim(col("sale_return"))=='R',col("quantity_returned")*col("wpu")).otherwise(col("quantity"))*col("wpu"))
    #           .withColumnRenamed("sale_return","transaction_code")

    strdf=strdf.withColumn('download_date1',to_date(from_unixtime(unix_timestamp('download_date','MM/dd/yyy'))))
    strdf=strdf.selectExpr("consumer_key","country_key","artist_key","product_key","label_key","client_key","distribution_key","distribution_channel_key","partner_id as partner_key"\
                           ,"service_type_key","transaction_type_key","product_type_key",\
                          "provider_key","vendor_key","spnl_key","sales_type_key","sales_category_key","sales_division_key","partner_info_key","partner_region_key","promo_info_key",\
                           "royalty_currency","currency_key","rpu as retail_price","order_id","preorder","transaction_amount","sale_return as transaction_code","quantity","download_date1 as download_date","report_day")
    strdf.printSchema()


    strdf=strdf.select(strdf["consumer_key"].cast(LongType()),"country_key",strdf["artist_key"].cast(LongType()),\
                        strdf["product_key"].cast(LongType()),strdf["label_key"].cast(LongType()),strdf["client_key"].cast(LongType()),\
                        strdf["distribution_key"],strdf["distribution_channel_key"],\
                        strdf["service_type_key"],strdf["transaction_type_key"],\
                        strdf["partner_key"].cast(LongType()),strdf["provider_key"].cast(LongType()),strdf["vendor_key"].cast(LongType()),\
                        strdf["spnl_key"].cast(LongType()),strdf["sales_type_key"].cast(LongType()),strdf["sales_category_key"],\
                        strdf["product_type_key"],\
                        strdf["sales_division_key"].cast(LongType()),strdf["partner_info_key"].cast(LongType()),\
                        strdf["partner_region_key"].cast(LongType()),strdf["promo_info_key"].cast(LongType()),\
                        strdf["royalty_currency"],strdf["currency_key"],strdf["retail_price"], \
                        strdf["order_id"].cast(LongType()),strdf["preorder"], strdf["transaction_amount"].cast(DoubleType()),strdf["transaction_code"],\
                        strdf["quantity"],strdf["download_date"],strdf["report_day"])
#datetime.datetime.strptime(report_day, "%Y%m%d").strftime('%Y-%m-%d')

    print(strdf.count())

    report_day=datetime.datetime.strptime(report_day, "%Y%m%d").strftime('%Y-%m-%d')
    datalake_file_path=datalake_file_dir+"partner_data/apple/downloads/v1/fact/fact_downloads/report_date="+report_day+"/report_licensor="+lic+'/'+load_id+'_'+spec_id
    print(datalake_file_path)

    write_log_info(v_procname,'Writing processd iTunes Download  data to '+datalake_file_path,str(load_id),None,provider_key)
    strdf.repartition(1).write.mode('overwrite').parquet(datalake_file_path)

    stmt="update "+config.metadata["trans_control"]+" set file_complete=sysdate() where trans_id="+str(transid)
    success=upd_ins_data(connect(),stmt,str(load_id))

    return report_day


def itunes_sync_main(report_day,load_id,lic,src_dim,src_fact,dest):
    print("itunes DL file count check"+datetime.datetime.now().strftime("%a, %d %B %Y %H:%M:%S"))
    write_log_info(v_procname,'iTunes DL file count check for datatype like SA%',str(load_id),None,prov_key)
    print(report_day)
   # delivery_time_expected='2020-09-28'
    #print(delivery_time_expected)
    print(lic)
    print(load_id)
    v_src_dim = dl_dim_path
    stmt="select client_key from "+config.metadata["licensor_map"]+" where licensor_id='"+str(lic)+"'"
    client_key=query_all(connect(),stmt)[0][0]
    print(client_key)

    sql_query = "select date(delivery_time_expected) from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and  provider_key= 'P001'"
    delivery_expected = query_all(connect(),sql_query)[0][0]
    print(delivery_expected)
    #exit()


    #received_file_count = query_all(connect(),sql_query)[0][0]

   # sql_query ="select count(*) from "+config.metadata["trans_control"]+"t ,"+config.metadata["dlake_control"]  +"d where t.load_id = d.load_id and t.provider_key=d.provider_key and  provider_key = 'P001' and date(delivery_time_expected)="+report_day+1+"and client_key="+client_key+\
    #            "and data_type like 'SA%' and file_available is not null and file_complete is not null and d.fact_loaded is not null "

    #processed_file_count=query_all(connect(),sql_query)[0][0]

    #if received_file_count==processed_file_count:
    #   write_log_info(v_procname,'All SA files received and processed ',str(load_id),None,prov_key)
    #   write_log_info(v_procname,'Ready to crawl download data',str(load_id),None,prov_key)




    sql_query = "select * from "+ config.metadata["trans_control"] +" where provider_key = 'P001' and load_id="+str(load_id)+" and client_key="+str(client_key)+" and data_type like 'P001_IT%' and file_available is not null"
    fileExpectation = query_all(connect(),sql_query)
    if len(fileExpectation) == 0:
           write_log_info(v_procname,' No files to load ',str(load_id))
           exit()

    userFile = fileExpectation[0][12]

    v_main_file_path="/report_date="+report_day+"/report_licensor="+lic

    print("Syncing Datalake structures with Main")
    print("aws s3 sync "+v_src_dim+"dim_partner_info/ "+v_dest+"dim_partner_info/ --delete")
    subprocess.check_output("aws s3 sync "+v_src_dim+"dim_partner_info/ "+v_dest+"dim_partner_info/ --delete --profile emrmgmt", shell=True)
    subprocess.check_output("aws s3 sync "+v_src_dim+"dim_consumers/ "+v_dest+"dim_consumers/ --delete --profile emrmgmt", shell=True)
    subprocess.check_output("aws s3 sync "+v_src_dim+"dim_partner_regions/ "+v_dest+"dim_partner_regions/ --delete --profile emrmgmt", shell=True)
    subprocess.check_output("aws s3 sync "+v_src_dim+"dim_promo_info/ "+v_dest+"dim_promo_info/ --delete --profile emrmgmt", shell=True)
    subprocess.check_output("aws s3 sync "+src_fact+" "+dest+v_main_file_path+"/ --delete --profile emrmgmt", shell=True)
    print("Done -Syncing Datalake structures with Main")

    #else:
    #   write_log_info(v_procname,'Still waiting for some more files to process',str(load_id),None,prov_key)
    #   write_log_info(v_procname,' Load to redshift can wait',str(load_id),None,prov_key)
    #   exit()



def itunes_load_to_redshift(load_id):
    sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and spec_id like '%P001_SALE%' and file_ready_consumer is not null"
    fileExpectation = query_all(connect(),sql_query)
    if len(fileExpectation) == 0:
        write_log_info(v_procname,' No files to load ',str(load_id))
        exit()

    userFile = fileExpectation[0][12]
    v_report_date = userFile.split('_')[1]
    print("report_day"+str(v_report_date))

    ## LOAD INTO REDSHIFT
    print("Loading to Redshift at "+datetime.datetime.now().strftime("%a, %d %B %Y %H:%M:%S"))

    rsConn=psycopg2.connect(dbname= rsconn["database"], host=rsconn["host"], port= rsconn["port"], user= rsconn["user"], password= rsconn["password"])
    load_from_glue_to_db(rsConn,etlProfile,codeBucket,codePath,"load_dimensions.sql",v_report_date)
    load_from_glue_to_db(rsConn,etlProfile,codeBucket,codePath,"load_fact.sql",v_report_date)
    rsConn.close()
    print("  Loaded to Reporting DB at "+datetime.datetime.now().strftime("%a, %d %B %Y %H:%M:%S"))
    """rsConn=psycopg2.connect(dbname= prodmainconn["database"], host=prodmainconn["host"], port= prodmainconn["port"], user= prodmainconn["user"], password= prodmainconn["password"])
    load_from_glue_to_db(rsConn,etlProfile,codeBucket,codePath,"load_facebook_data.sql",v_report_date)
    rsConn.close()
    print("  Loaded to Prod Main DB")"""

def itunes_load_dlake_main(load_id,trans_id,lic):

    print("itunes DL started at "+datetime.datetime.now().strftime("%a, %d %B %Y %H:%M:%S"))
    write_log_info(v_procname,'Dimension load started.....',str(load_id),None,prov_key)
    flag='appleitunes_dlake_dim_refresh'
    is_running = get_flag_value(flag)
    sec_to_wait=10

    while is_running == 1:
        write_log_info(v_procname,'=== Waiting for running process to finish flag '+flag+' value is '+str(is_running),str(load_id),-1,prov_key)
        time.sleep(sec_to_wait)
        is_running = get_flag_value(flag)
    write_log_info(v_procname,'=== Out of wait loop flag '+flag+' value is '+str(is_running),str(load_id),-1,prov_key)
  #  upd_result = upd_flag_value(1,flag)
    #write_log_info(v_procname,'== flag '+flag+' value updated to 1(no new process start until this finishes) output value is '+str(upd_result),str(load_id),-
#1,prov_key)
  #  if upd_result != 1:
  #      write_log_info(v_procname,'failed to update the flag '+flag+' to 1 ',str(load_id),-1,prov_key)
  #      exit()


    sql_query = "select count(*) from "+ config.metadata["dlake_control"] +" where load_id  = "+str(load_id)
    cnt = query_all(connect(),sql_query)[0][0]
    if cnt == 0:
        write_log_info(v_procname,'No record found for '+str(load_id)+' creating one...',str(load_id),None,prov_key)
        sql_query = "select file_name from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and spec_id like '%P001_SALE_%'"
        file_name = query_all(connect(),sql_query)
        report_day = file_name[0][0].split('_')[3]

        ins_query = ("insert into "+config.metadata["dlake_control"] +"(load_id,report_date,provider_key,dims_started) values(%s,%s,%s,sysdate())")
        ins_data = str(load_id)+","+str(report_day)+","+prov_key
        ins_data=ins_data.split(',')
        output=upd_ins_data(connect(),ins_query,ins_data)
    else:
        write_log_info(v_procname,'Record found for load id '+str(load_id)+'. Updating dim flags',str(load_id),None,prov_key)
        stmt="update "+config.metadata["dlake_control"]+" set dims_started=sysdate(),dims_loaded=null,fact_started=null,fact_loaded=null where load_id="+str(load_id)
        success=upd_ins_data(connect(),stmt,str(load_id))

    print("Start Dims")
    itunes_load_consumers(load_id,trans_id,lic)
    itunes_load_partner_info(load_id,trans_id,lic)
    itunes_load_promo_info(load_id,trans_id,lic)
    itunes_load_partner_regions(load_id,trans_id,lic)

#    stmt="update "+config.metadata["dlake_control"]+" set dims_loaded=sysdate() where load_id="+str(load_id)
#    success=upd_ins_data(connect(),stmt,str(load_id))
#    write_log_info(v_procname,'Dimension load ended.....',str(load_id),None,prov_key)




 #   write_log_info(v_procname,'iTunes Download Fact load started.....',str(load_id),None,prov_key)
 #   report_day=-1

    report_day=itunes_dl_load_fact(load_id,trans_id,lic)

    write_log_info(v_procname,'iTunes Download Fact load ended.....',str(load_id),None,prov_key)

   # upd_result =  upd_flag_value(0,flag)
   # write_log_info(v_procname,'== flag value updated to 0 so new process can start output of update is '+str(upd_result),str(load_id),-1,prov_key)


    #if upd_result != 1:
    #    write_log_info(v_procname,'failed to update the flag '+flag+' to 0 ',str(load_id),-1,prov_key)
    #    exit()

    #if report_day==-1:
    #    write_log_info(v_procname,'Fact Stream routine failed. Cannot create partition information. Exiting..',str(load_id),None,prov_key)
    #    exit()

    #else:
    #    sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and  file_ready_consumer is not null"
    #    fileExpectation = query_all(connect(),sql_query)
    #    if len(fileExpectation) == 0:
    #         write_log_info(v_procname,' No files to load ',str(load_id))
    #         exit()

    #    userFile = fileExpectation[0][12]
    #    spec_id = userFile.split('_')[2]
    #    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true")
    #    df=sqlContext.createDataFrame([report_day], "string").toDF("i_date")
    #    df.repartition(1).write.mode("overwrite").parquet(datalake_file_dir+"partner_data/apple/downloads/v1/partitions/report_date="+report_day+"/report_licensor="+lic+'/'+load_id+'_'+spec_id)
     #   stmt="update "+config.metadata["dlake_control"]+" set fact_loaded=sysdate() where load_id="+str(load_id)
     #   success=upd_ins_data(connect(),stmt,str(load_id))

   # print("iTunes Download  Datalake ended at "+datetime.datetime.now().strftime("%a, %d %B %Y %H:%M:%S"))

    #stmt="update "+config.metadata["trans_control"]+" set file_complete=sysdate() where load_id="+str(transid)
    #success=upd_ins_data(connect(),stmt,str(load_id))


    #time.sleep(30)
    v_src_dim = dl_dim_path
    print(v_src_dim)
    #report_day='2020-09-27'
    print(report_day)
    print(lic)
    print(dl_fact_path)
    src_fact = dl_fact_path+"report_date="+report_day+"/report_licensor="+lic+"/"
    print(src_fact)
    print("START SYNC")
    itunes_sync_main(report_day,str(load_id), lic,v_src_dim,src_fact,dest)
    print("SYNC DONE")

