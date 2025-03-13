from __future__ import division
from __future__ import print_function
#-----------------  IMPORT MODULES ----------------
from builtins import str
from past.utils import old_div
import sys
from env import init
retcode=init()

from utl_mysql_interface import *
import config

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

from utl_functions import *

from threading import Thread
from multiprocessing.dummy import Pool as ThreadPool
from gras_matching_for_str_data_v2 import *
from utl_upd_process_control_v2 import *
from create_manifest_v2 import *

from datetime import *

import time as t
import datetime
import _strptime

import re
import codecs

#------------------- END IMPORT -------------------

config.metadata=read_config(section="table")
#v_main_proc = 'appleitunes_process.py'
v_main_proc = 'appleitunes_process.py'
sec_to_wait = 10
num_files = 42
#sys.stdout = codecs.getwriter('utf_8')(sys.stdout)

def proc_appleitunes(procargs):
   #licensor='sme'

   print('============> START ', str(datetime.datetime.now()))
   v_procname = v_main_proc + '.proc_appleitunes'
   #config.metadata=read_config(section="table")

   tid = procargs[1]
   loadid = procargs[0]

   trackFile = procargs[12].lower()
   #transid = procargs[1]
   clientKey = procargs[27]
   provider_key = procargs[2]
   report_day = trackFile.split("/")[8]
   vendor_id = trackFile.split("_")[2]
   coun_key = procargs[3]
   report_day_col = datetime.datetime.strptime(report_day,'%Y%m%d').strftime('%Y-%m-%d')
   licensor=get_licensor_from_client_key(clientKey)  
   print(tid)
   print(loadid)
   print(trackFile)
   print(clientKey)
   print(provider_key)
   print(report_day)
   print(vendor_id)
   print(coun_key)
   print(report_day_col)


   write_log_info(v_procname,' ============= SUBPROCESS STARTED ======== ',str(loadid),str(tid),provider_key)

   sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'enriched_files_dir_v2'"

   content_enriched_file_dir = query_all(connect(),sql_query)
   if len(content_enriched_file_dir) == 0:
      print("Output path not available")
      exit()

   content_enriched_file_dir  = content_enriched_file_dir[0][0].lower()


   upd_result = upd_trans_control_flag(loadid,tid,'file_started')
   if upd_result != 1:
     write_log_info(v_procname,'!!!! Failed to update the flag(file_started) in trans control ',str(loadid),str(tid),provider_key)
     exit()


   partprovmapDf = get_df_from_db_table("part_prov_mapping")
   prodtypeDf = get_df_from_db_table("product_types")
   salecatmapDf = get_df_from_db_table("sale_cat_map")
   cutoff=sqlContext.read.format("parquet").load(content_enriched_file_dir.replace("partner_data","lookup")+ "/stgrp_cutoff_dates/*")
   cutoff=cutoff.withColumn("country_key", trim(cutoff.country_key)).withColumn("isrc_country_code", trim(cutoff.isrc_country_code)).withColumn("prod_no",trim(cutoff.prod_no)).withColumn("cutoff_date",trim(cutoff.cutoff_date))
   gras_data_df = get_df_from_db_table("product_master")
   artist_data_df = get_df_from_db_table("artists")
   labels_df = get_df_from_db_table("labels")
   dts_df = get_df_from_db_table("general_dts_map")
   prodtypeDf = get_df_from_db_table("product_types")
   

   a_partprovmapDf = partprovmapDf.alias('a_partprovmapDf')
   a_prodtypeDf = prodtypeDf.alias('a_prodtypeDf')
   a_salecatmapDf = salecatmapDf.alias('a_salecatmapDf')
   a_cutoff=cutoff.alias('a_cutoff')
   a_gras_data_df = gras_data_df.alias('a_gras_data_df')
   a_artist_data_df = artist_data_df.alias('a_artist_data_df')
   a_labels_df = labels_df.alias('a_labels_df')
   a_dts_df = dts_df.alias('a_dts_df')
   a_prodtypeDf = prodtypeDf.alias('a_prodtypeDf')
   
   part_recs_cnt = 5000


   partner_details = query_all(connect(), "select distinct partner_key,partner_name from "+config.metadata["part_prov_mapping"]+" where provider_key='"+provider_key+"'")
   partner_id = partner_details[0][0]
   partner_name = partner_details[0][1]   
   
   #Read user file
   write_log_info(v_procname,' Reading user file ',str(loadid),str(tid),provider_key)
   #BY IK exec('userDf = sqlContext.read.parquet("'+content_enriched_file_dir+provider_key+'/'+report_day+'/tmp_intermediate/'+'user_enriched_file_'+str(loadid)+'.parquet")')
   #BY IK  userDf = sqlContext.read.parquet(content_enriched_file_dir+licensor+'/'+provider_key+'/'+report_day+'/tmp_intermediate/'+'user_enriched_file_'+str(loadid)+'.parquet')
   userDf = sqlContext.read.parquet(content_enriched_file_dir+licensor+'/'+provider_key+'/'+'user_enriched_files/report_day='+report_day_col+'/user_enriched_file_'+str(loadid)+'.parquet')
   a_userDf = userDf.alias('a_userDf')


  #BY IK  cmd = 'fileDf = sqlContext.read.format("csv").option("header","true").option("delimiter","\t").option("samplingRatio", "0.1").load("'+trackFile+'")'
  #BY IK   exec(cmd)
   fileDf = sqlContext.read.format("csv").option("header","true").option("delimiter","\t").option("samplingRatio", "0.1").load(trackFile)
   fileDf = remove_column_spaces(fileDf)
   fileDf = fileDf.withColumn("wholesale_value",col("Units")*col("Royalty_Price"))\
                  .withColumn("retail_value",when(col("Customer_Price") < 0,col("Units")*-1).otherwise(col("Units"))*col("Customer_Price"))\
                  .withColumn("quantity",when(trim(col("Sale_Return")) != 'R',col("Units").cast('integer')).otherwise(lit(0)).cast('integer'))\
                  .withColumn("quantity_returned",when(trim(col("Sale_Return")) == 'R',col("Units").cast('integer')).otherwise(lit(0)).cast('integer'))\
                  .withColumn("Customer_Price",col("Customer_Price").cast('double'))\
                  .withColumn("Royalty_Price",col("Royalty_Price").cast('double'))\
                  .withColumn("Parent_Type_Id",trim(col("Parent_Type_Id")).cast('integer'))\
                  .withColumn("Product_Type_Identifier",trim(col("Product_Type_Identifier")))\
                  .drop("Units")
                  #
   fileDf.printSchema()
   split_parent_id = split(fileDf['Parent_Identifier'], '_')
   fileDf1 = fileDf.withColumn("parent_id_clean",trim(split_parent_id.getItem(0)))
   a_fileDf1 = fileDf1.alias('a_fileDf1')
   a_fileDf1=a_fileDf1.drop("Parent_Identifier")
   file_Df = a_fileDf1.withColumnRenamed("parent_id_clean","Parent_Identifier")

   a_fileDf = file_Df.alias('a_fileDf')
   a_fileDf.show(7,False)

   upd_result = upd_trans_control_flag(loadid,tid,'file_loaded')
   if upd_result != 1:
     write_log_info(v_procname,'!!!! Failed to update the flag(file_loaded) in trans control ',str(loadid),str(tid),provider_key)
     exit()

   loaded_recs = a_fileDf.count()
   print(loaded_recs)
   a_fileDf = a_fileDf.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   
   write_log_info(v_procname,' File loaded loaded_recs count '+str(loaded_recs),str(loadid),str(tid),provider_key)
   
   #Apply special rules, parent rollup etc

   tmp_intermediate_fname = apply_special_rules(a_fileDf,a_gras_data_df,procargs)
   
  #BY IK  exec('spcial_gras_Df = sqlContext.read.parquet("' + tmp_intermediate_fname +'/*")')
   spcial_gras_Df = sqlContext.read.parquet(tmp_intermediate_fname +'/*')
   write_log_info(v_procname,' Special rules file read '+str(loaded_recs),str(loadid),str(tid),provider_key)
   
   #special_gras_Df = sqlContext.read.csv("s3://sme-ca-dl-dev-output/partner/P001/20190521/tmp_intermediate/Special_GRAS_DONE_AT_enriched.csv/*",sep='\x07',header=True)
   
   write_log_info(v_procname,' special gras rules completed ',str(loadid),str(tid),provider_key)
   
   #General GRAS matching
   generic_gras_Df = apply_generic_gras_match(spcial_gras_Df,a_gras_data_df,a_artist_data_df,a_labels_df,procargs)
   
   #generic_gras_Df = sqlContext.read.parquet("s3://sme-ca-dl-dev-output/partner/P001/20190521/tmp_track_enriched_files/d_d_80031738_20190521_v1_2.txt.gz.parquet/*")
   
   write_log_info(v_procname,' generic gras rules completed ',str(loadid),str(tid),provider_key)
   
   generic_gras_Df = generic_gras_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   
   #Normal processing 
   with_various_key_Df = generic_gras_Df.withColumn("distribution_key",when(col("Product_Type_Identifier") == 'R','25')\
                                                                      .when(col("Product_Type_Identifier") == 'PR','10')\
                                                                      .when(col("Product_Type_Identifier") == 'RX','10')\
                                                                      .otherwise("05"))\
                                        .withColumn("sales_type_key",when(col("Product_Type_Identifier").isin({'U','W','X'}),'UG')\
                                                                    .when((col("Customer_Price") == 0) & (col("Royalty_Price") == 0) ,'PR')\
                                                                    .when((col("Customer_Price") == 0) & (col("Royalty_Price") != 0) ,'FG')\
                                                                    .otherwise('SA'))\
                                        .withColumn("service_type_key",lit("10"))\
                                        .withColumn("transaction_type_key",when((col("Product_Type_Identifier") == 'F'),'10').otherwise('20'))\
                                        .withColumn("partner_id",lit(partner_id))\
                                        .withColumn("partner_name",lit(partner_name))\
                                        .withColumn("client_key",lit(clientKey))\
                                        .withColumn("vendor_key",lit("?"))\
                                        .withColumn("transaction_date_key",lit(report_day).cast('string'))\
                                        .withColumn("Report_Date_Local",date_format(to_date(col("Report_Date_Local"),'MM/dd/yyyy'),'yyyy-MM-dd'))\
                                        .withColumnRenamed("Report_Date_Local","report_day")
                                        
   with_various_key_Df = with_various_key_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   
   a_with_various_key_Df = with_various_key_Df.alias('a_with_various_key_Df')
   write_log_info(v_procname,' with_various_key_Df loaded ',str(loadid),str(tid),provider_key)
   """
   with_sdiv_key_Df = a_with_various_key_Df.join(a_gras_data_df,a_with_various_key_Df.product_no == a_gras_data_df.product_no,how='left')\
                                           .join(a_labels_df,a_gras_data_df.label_key == a_labels_df.label_key,how='left')\
                                           .select("a_with_various_key_Df.*",when(col("finlabel_parent_cd") == 'C338',lit(3)).otherwise(lit(5)))\
                                           .withColumnRenamed("CASE WHEN (finlabel_parent_cd = C338) THEN 3 ELSE 5 END","sales_division_key")
                                           
   a_with_sdiv_key_Df = with_sdiv_key_Df.alias('a_with_sdiv_key_Df')
   write_log_info(v_procname,' with_sdiv_key_Df loaded ',str(loadid),str(tid),provider_key)
   """
   tmp = a_dts_df.join(a_salecatmapDf,a_dts_df.dist_channel_key == a_salecatmapDf.dist_channel_key,how='left')\
                                 .select("a_dts_df.*",a_salecatmapDf.sale_cat_id,a_salecatmapDf.product_type_key).withColumnRenamed("sale_cat_id","sales_cat_key")
   a_tmp = tmp.alias('a_tmp')                                 
   with_scat_key_Df = a_with_various_key_Df.join(a_tmp,((a_with_various_key_Df.transaction_type_key == a_tmp.transaction_type_key))\
                                   & (a_with_various_key_Df.service_type_key == a_tmp.service_type_key)\
                                   & (a_with_various_key_Df.distribution_key == a_tmp.distribution_type_key)\
                                   & (a_with_various_key_Df.product_type_key == a_tmp.product_type_key),how='left')\
                                   .select("a_with_various_key_Df.*",a_tmp.sales_cat_key,a_tmp.dist_channel_key)\
                                   .na.fill({'dist_channel_key':'999'})
                                   
   with_scat_key_Df = with_scat_key_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   
   a_with_scat_key_Df = with_scat_key_Df.alias('a_with_scat_key_Df')
   write_log_info(v_procname,' with_scat_key_Df loaded ',str(loadid),str(tid),provider_key)
   
   


   df_with_ptypes = a_with_scat_key_Df.join(broadcast(a_prodtypeDf),(a_with_scat_key_Df.product_type_key == a_prodtypeDf.samis_prod_type_key) & \
                                  (a_with_scat_key_Df.dist_channel_key == a_prodtypeDf.samis_distrib_ch_key),how='left')\
                                  .drop(a_prodtypeDf.samis_prod_type_key)\
                                  .drop(a_prodtypeDf.samis_distrib_ch_key)\
                                  .withColumnRenamed("prod_type_lkup_key","prod_types_key")
   write_log_info(v_procname,' df_with_ptypes loaded ',str(loadid),str(tid),provider_key)
   df_with_ptypes = df_with_ptypes.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   
   a_df_with_ptypes = df_with_ptypes.alias('a_df_with_ptypes')
   
   
   df_with_consumer_key = a_df_with_ptypes.join(a_userDf,a_df_with_ptypes.Customer_Identifier == a_userDf.consumer_id).select("a_df_with_ptypes.*","a_userDf.consumer_key")
   write_log_info(v_procname,' df_with_consumer_key loaded ',str(loadid),str(tid),provider_key)
   
   df_with_consumer_key = df_with_consumer_key.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   
   a_df_with_consumer_key = df_with_consumer_key.alias('a_df_with_consumer_key')
   
   
   df_with_stgrp_key = a_df_with_consumer_key.join(a_cutoff,(trim(a_df_with_consumer_key.Country_Code) == a_cutoff.isrc_country_code) & (a_df_with_consumer_key.product_no == a_cutoff.prod_no),how='left')\
                                            .select("a_df_with_consumer_key.*",a_cutoff.cutoff_date)
   
   df_with_stgrp_key = df_with_stgrp_key.withColumn('spnl_key',when(to_date(col('transaction_date_key'),"yyyyMMdd")<=to_date(col('cutoff_date'),"yyyy-MM-dd"),1)\
                                                              .when(to_date(col('transaction_date_key'),"yyyyMMdd")>to_date(col('cutoff_date'),"yyyy-MM-dd"),2)\
                                                              .otherwise(-1))
   
   df_with_stgrp_key = df_with_stgrp_key.drop('cutoff_date').drop('rec_no').persist()
   write_log_info(v_procname,' Persist done ',str(loadid),str(tid),provider_key)
   
   df_with_stgrp_key = df_with_stgrp_key.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   #.withColumn("transaction_date_key",lit(None).cast('string'))
   
   write_log_info(v_procname,' df_with_stgrp_key loaded ',str(loadid),str(tid),provider_key)
   

   #IK 
   #df_with_stgrp_key.write.mode("overwrite").parquet("s3://sme-ca-dl-dev-output/partner_data/sme/P001/temp/c/")
   tmp1 = df_with_stgrp_key.filter((col("Parent_Type_Id") == 1) & (col("Parent_Identifier").isNotNull()))\
                           .groupBy("load_id","trans_id","vendor_key","client_key","provider_key","provider","provider_country"\
                                    ,"upc",col("Parent_Identifier").alias("isrc"),col("Product_Type_Identifier").alias("product_identifier")\
                                    ,"report_day","sale_return",col("customer_currency").alias("currency_key"),col("country_code").alias("country_key"),"royalty_currency"\
                                    ,"preorder","isan","cma",col("Asset_Content_Flavor").alias("asset_content"),col("Parent_Identifier").alias("grid_no")\
                                    ,"promo_code",col("Parent_Identifier").alias("parent_id"),"parent_type_id","attributable_purchase"\
                                    ,"primary_genre",col("product_no").alias("prod_no_dig"),"product_type_key","distribution_key"\
                                    ,"sales_type_key","service_type_key","transaction_type_key",col("dist_channel_key").alias("distribution_channel_key"),"partner_id","label_key","product_key"\
                                    ,"artist_key","spnl_key","sales_division_key","product_conf_key","prod_types_key",col("sales_cat_key").alias("sales_category_key")\
                                    ,"state_province","city","transaction_date_key","consumer_key")\
                           .agg(max("Vendor_Identifier").alias("vendor_identifier"),max("Artist___Show").alias("artist"),max("Title").alias("title"),min("Label_Studio_Network").alias("label")\
                           ,round(avg("Quantity")).alias("quantity"),round(avg("quantity_returned")).alias("quantity_returned")\
                           ,round(avg(col("Quantity")-col("quantity_returned"))).alias("qty_minus_qtyrtr")\
                           ,max("Download_Date_PST").alias("download_date"),max("Order_Id").alias("order_id")\
                           ,max("Customer_Identifier").alias("customer_id"),max("Apple_Identifier").alias("apple_id")\
                           ,max("Vendor_Offer_Code").alias("vendor_offer_code"),sum("retail_value").alias("retail_value")\
                           ,sum("wholesale_value").alias("wholesale_value"))\
                           .withColumn("rpu",when(col("qty_minus_qtyrtr") == 0,0).otherwise(old_div(col("retail_value"),col("qty_minus_qtyrtr"))))\
                           .withColumn("wpu",when(col("qty_minus_qtyrtr") == 0,0).otherwise(old_div(col("wholesale_value"),col("qty_minus_qtyrtr"))))\
                           .drop("qty_minus_qtyrtr")
   tmp1 = tmp1.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   #tmp2 = df_with_stgrp_key.filter(((col("Parent_Type_Id") == '17') | (col("Parent_Type_Id").isNull())) | ((col("Parent_Type_Id") =='1') & (col("Parent_Type_Id").isNull())))\
   tmp2 = df_with_stgrp_key.filter(((col("Parent_Type_Id") == 17) | (col("Parent_Type_Id").isNull())) | ((col("Parent_Type_Id") == 1) & (col("Parent_Identifier").isNull())))\
                           .groupBy("load_id","trans_id","vendor_key","client_key","provider_key","provider","provider_country"\
                                    ,"upc","isrc",col("Product_Type_Identifier").alias("product_identifier")\
                                    ,"report_day","sale_return",col("customer_currency").alias("currency_key"),col("country_code").alias("country_key"),"royalty_currency"\
                                    ,"preorder","isan","cma",col("Asset_Content_Flavor").alias("asset_content"),col("Parent_Identifier").alias("grid_no")\
                                    ,"promo_code",col("Parent_Identifier").alias("parent_id"),"parent_type_id","attributable_purchase"\
                                    ,"primary_genre",col("product_no").alias("prod_no_dig"),"product_type_key","distribution_key"\
                                    ,"sales_type_key","service_type_key","transaction_type_key",col("dist_channel_key").alias("distribution_channel_key"),"partner_id","label_key","product_key"\
                                    ,"artist_key","spnl_key","sales_division_key","product_conf_key","prod_types_key",col("sales_cat_key").alias("sales_category_key")\
                                    ,"state_province","city","vendor_identifier",col("Artist___Show").alias("artist"),"title",col("Label_Studio_Network").alias("label")\
                                    ,"order_id",col("Download_Date_PST").alias("download_date"),col("Customer_Identifier").alias("customer_id"),col("Apple_Identifier").alias("apple_id")\
                                    ,"vendor_offer_code","transaction_date_key","consumer_key")\
                           .agg(sum("Quantity").alias("quantity"),sum("quantity_returned").alias("quantity_returned")\
                           ,sum("retail_value").alias("retail_value")\
                           ,sum(col("Quantity")-col("quantity_returned")).alias("qty_minus_qtyrtr")\
                           ,sum("wholesale_value").alias("wholesale_value"))\
                           .withColumn("rpu",when(col("qty_minus_qtyrtr") == 0,0).otherwise(old_div(col("retail_value"),col("qty_minus_qtyrtr"))))\
                           .withColumn("wpu",when(col("qty_minus_qtyrtr") == 0,0).otherwise(old_div(col("wholesale_value"),col("qty_minus_qtyrtr"))))\
                           .select("load_id","trans_id","vendor_key","client_key","provider_key","provider","provider_country","upc","isrc","product_identifier","report_day","sale_return","currency_key","country_key","royalty_currency","preorder","isan","cma","asset_content","grid_no","promo_code","parent_id","parent_type_id","attributable_purchase","primary_genre","prod_no_dig","product_type_key","distribution_key","sales_type_key","service_type_key","transaction_type_key","distribution_channel_key","partner_id","label_key","product_key","artist_key","spnl_key","sales_division_key","product_conf_key","prod_types_key","sales_category_key","state_province","city","transaction_date_key","consumer_key","vendor_identifier","artist","title","label","quantity","quantity_returned","qty_minus_qtyrtr","download_date","order_id","customer_id","apple_id","vendor_offer_code","retail_value","wholesale_value","rpu","wpu")\
                           .drop("qty_minus_qtyrtr")
   #IK 
   print("tmp1 &temp 2")
   #df_with_stgrp_key.write.mode("overwrite").parquet("s3://sme-ca-dl-dev-output/partner_data/sme/P001/temp/c/")
   #tmp1.write.mode("overwrite").parquet("s3://sme-ca-dl-dev-output/partner_data/sme/P001/temp/a/")
   #tmp2.write.mode("overwrite").parquet("s3://sme-ca-dl-dev-output/partner_data/sme/P001/temp/b/")
   tmp2 = tmp2.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   
   final_df = tmp1.unionAll(tmp2).withColumnRenamed("prod_types_key","product_types_key")\
                                 .withColumn("media_key", when(col("product_identifier").isin({'U','W','X'}),'AP3').when(col("product_identifier").isin({'H','I','J'}),'MP3').otherwise('AAC'))
   final_df = final_df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   
   processed_recs = final_df.count()
   
   write_log_info(v_procname,' tmp1 and tmp2 loaded ',str(loadid),str(tid),provider_key)

   final_df = final_df.repartition((old_div(loaded_recs,30000))+1)
   
   print("final frame")
   final_df.printSchema()
   
   
   write_log_info(v_procname,' Processed records final count '+str(processed_recs),str(loadid),str(tid),provider_key)
   
   if loaded_recs == processed_recs:
       write_log_info(v_procname,' Yayy Records count matches',str(loadid),str(tid),provider_key)
   else:
       write_log_info(v_procname,'ERROR Some records are lost in wires!! please check ',str(loadid),str(tid),provider_key)
       #exit()
       
   upd_result = upd_trans_control_flag(loadid,tid,'file_enriched')
   
   cmd_string = 'final_df.write.mode("overwrite").format("csv").option("delimiter", "\x07").option("header","true").option("quote","").option("nullValue","").option("compression", "gzip")\
                                         .option("emptyValue",None).save("'+content_enriched_file_dir+licensor+'/'+provider_key+'/gras_enriched_files/report_date='+str(report_day_col)+'/DL_'+coun_key+'_'+str(vendor_id)+'_enriched.csv.gz")'
   exec(cmd_string)
   write_log_info(v_procname,' Enriched file written to S3 ',str(loadid),str(tid),provider_key)
   upd_trans_stat(tid,'sum_units_db_consumer',processed_recs)
   write_log_info(v_procname,' trans stat updated ',str(loadid),str(tid),provider_key)
   
   success=upd_ins_data(connect(),'update '+config.metadata["trans_control"] +' set consumer_enriched_file_name = "'+'/gras_enriched_files/report_date='+str(report_day_col)+'/DL_'+coun_key+'_'+str(vendor_id)+'_enriched.csv.gz"\
                                                                                 where trans_id ='+str(tid),'/gras_enriched_files/report_date='+str(report_day_col)+'/DL_'+coun_key+'_'+str(vendor_id)+'_enriched.csv.gz"')
   
   write_manifest_file(loadid,'gras_enriched_files/DL_'+coun_key+'_'+str(vendor_id)+'_enriched.csv.gz','enriched_files_dir_v2',clientKey)

   write_log_info(v_procname,' Consumer Manifest generated ',str(loadid),str(tid),provider_key)
   
   upd_result = upd_trans_control_flag(loadid,tid,'file_ready_consumer')
   

    ##### check no of files received vs processed and generate success file


    #delivery_expected= report_day_col
   delivery_expected = datetime.datetime.strptime(report_day_col, "%Y-%m-%d")
   delivery_expected = delivery_expected + datetime.timedelta(days=1)
   print(delivery_expected)

   sql_query="select count(*) from "+ config.metadata["trans_control"] +" where  provider_key = 'P001' and \
      date(delivery_time_expected)='"+str(delivery_expected)+ "' and client_key="+str(clientKey)+" and data_type like 'SA%' and file_downloaded is not null"


   files_received=query_all(connect(),sql_query)
   print("files_received")
   print(files_received[0][0])

   sql_query ="select count(*) from "+ config.metadata["trans_control"] +" where  provider_key = 'P001' and \
   date(delivery_time_expected)='"+str(delivery_expected)+"'  and client_key="+str(clientKey)+ " and data_type like 'SA%' and file_downloaded is not null \
   and consumer_enriched_file_name is not null"


   file_processed=query_all(connect(),sql_query)
   print("file_processed")
   print(file_processed[0][0])
   write_log_info(v_procname,'No of consumer Files processed: '+str(file_processed),str(loadid),str(tid),provider_key)
   files_processed = file_processed[0][0]
   if(int(files_processed)>=num_files):
       write_log_info(v_procname,' Yayy All files recevied and processed ..writing success file',str(loadid),str(tid),provider_key)

       eschema=StructType([StructField('col1',StringType(),True)])
       df2 = sc.parallelize([]).toDF(eschema)
       df2.write.mode('append').csv(content_enriched_file_dir+'/'+licensor+'/'+provider_key+'/gras_enriched_files/report_date='+str(report_day_col))





   write_log_info(v_procname,' ============= SUBPROCESS COMPLETED ======== ',str(loadid),str(tid),provider_key)
   

def apply_generic_gras_match(in_df,a_gras_data_df,a_artist_data_df,a_labels_df,procargs):


   tid = procargs[1]
   trackFile = procargs[12].lower()
   loadid = procargs[0]
   provider_key = procargs[2]
   clientKey = procargs[27]
   report_day = trackFile.split("/")[7]
   file_name = trackFile.split('/')[9]
   v_procname = v_main_proc + '.apply_generic_gras_match'

   write_log_info(v_procname,' ============= GENERIC GRAS MATCH STARTED ======== ',str(loadid),str(tid),provider_key)
   #gras_data_df = get_df_from_db_table("product_master")
   #a_gras_data_df = gras_data_df.alias('a_gras_data_df')

   #artist_data_df = get_df_from_db_table("artists")
   #a_artist_data_df = artist_data_df.alias('a_artist_data_df')

   #labels_df = get_df_from_db_table("labels")
   #a_labels_df = labels_df.alias('a_labels_df')

   sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'enriched_files_dir'"

   out_dir_path = query_all(connect(),sql_query)
   if len(out_dir_path) == 0:
      print("Output path not available")
      exit()

   out_dir  = out_dir_path[0][0].lower()
   
   
   a_track_file_df = in_df.alias('a_track_file_df')
   a_track_file_df.printSchema()
   
   gras_match_cols = {'artist':'Artist___Show', 'upc_cd':'UPC', 'isrc_cd':'ISRC','product_name':'Title'}
   isrc_all_matched_df = gras_match_str_file(a_track_file_df, a_gras_data_df, a_artist_data_df, gras_match_cols, provider_key, loadid,'DL')
   enrichedDataFinal  = enrich_str_file(isrc_all_matched_df, a_gras_data_df, a_labels_df, loadid, tid, clientKey, provider_key, report_day, out_dir, file_name)
   write_log_info(v_procname,' Gras match data enriched ',str(loadid),str(tid),provider_key)
   
   write_enrich_str_file(enrichedDataFinal, loadid, tid, provider_key, report_day, out_dir, file_name)

   #write_log_info(v_procname,' Enriched file written ',str(loadid),str(tid),provider_key) 
   
   write_log_info(v_procname,' ============= GENERIC GRAS MATCH COMPLETED ======== ',str(loadid),str(tid),provider_key)
   
   return enrichedDataFinal
   
   

def apply_special_rules(in_df,a_product_master,procargs):
   print('============> START ', str(datetime.datetime.now()))
   #config.metadata=read_config(section="table")
   v_procname = v_main_proc + '.apply_special_rules'

   tid = procargs[1]
   loadid = procargs[0]

   trackFile = procargs[12].lower()
   #transid = procargs[1]
   clientKey = procargs[27]
   provider_key = procargs[2]
   report_day = trackFile.split("/")[8]
   vendor_id = trackFile.split("_")[2]
   coun_key = procargs[3]
   report_day_col=datetime.datetime.strptime(report_day,'%Y%m%d').strftime('%Y-%m-%d')
   file_name = trackFile.split('/')[9]

   write_log_info(v_procname,' ============= SPECIAL RULES STARTED ======== ',str(loadid),str(tid),provider_key)
   
   sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'enriched_files_dir_v2'"
   
   content_enriched_file_dir = query_all(connect(),sql_query)
   if len(content_enriched_file_dir) == 0:
      print("Output path not available")
      exit()

   content_enriched_file_dir  = content_enriched_file_dir[0][0].lower()
   licensor=get_licensor_from_client_key(clientKey)
   write_log_info(v_procname,' processing file '+file_name,str(loadid),str(tid),provider_key)
   
   prod_conf_dim = get_df_from_db_table("prod_conf_dim")
   configuration = get_df_from_db_table("config")
   #product_master = get_df_from_db_table("product_master")
   gpp_prod_map = get_df_from_db_table("gpp_prod_map")
   
   a_prod_conf_dim = prod_conf_dim.alias('a_prod_conf_dim')
   a_configuration = configuration.alias('a_configuration')
   a_product_master = a_product_master.alias('a_product_master')
   a_gpp_prod_map = gpp_prod_map.alias('a_gpp_prod_map')
   
   column_drop = ''

   print("Track file "+trackFile)
   print("load id "+str(loadid))
   print("trans id "+str(tid))
   print("client key "+str(clientKey))
   print("Provider key "+str(provider_key))
   print("Report Day "+str(report_day))
   print("file_name "+file_name)
   
   a_fileDf = in_df
   loaded_recs = a_fileDf.count()
   sum_qty = a_fileDf.groupBy().agg(sum("Quantity")).collect()
   print("loaded_recs"+str(loaded_recs))
   print("sum-qty" +str(sum_qty))

   part_recs_cnt = 5000
   
   #STEP 1: update the invalid parent id reported by apple to null as its not available in GRAS
   step1_upd_recs = a_fileDf.filter((col("Parent_Type_Id") == 1) \
                              & (col("Parent_Identifier").isNotNull())\
                              & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-A')) \
                              & ((col("Product_Type_Identifier") == 'S') | (col("Product_Type_Identifier")== 'H'))).count()
   write_log_info(v_procname,'Step 1: Update rec max count '+str(step1_upd_recs),str(loadid),str(tid),provider_key)                              
   if step1_upd_recs > 0:
      write_log_info(v_procname,'Step 1: Update running',str(loadid),str(tid),provider_key)
      barcodeDf = a_product_master.filter(col("barcode").isNotNull()).filter(col("is_complete") == 'Y')\
                                  .join(a_prod_conf_dim,(a_product_master.product_conf_key == a_prod_conf_dim.product_conf_key),how='left')\
                                  .join(a_configuration,(a_configuration.config_key == a_prod_conf_dim.product_conf_cd) & (a_configuration.is_single_track_config == 'N'))\
                                  .select("a_product_master.barcode").distinct()
                                  
      local_barcodeDf = a_product_master.filter(col("local_barcode").isNotNull()).filter(col("is_complete") == 'Y')\
                                  .join(a_prod_conf_dim,(a_product_master.product_conf_key == a_prod_conf_dim.product_conf_key),how='left')\
                                  .join(a_configuration,(a_configuration.config_key == a_prod_conf_dim.product_conf_cd) & (a_configuration.is_single_track_config == 'N'))\
                                  .select("a_product_master.local_barcode").distinct()
                                  
      all_brcdDf = barcodeDf.union(local_barcodeDf).filter(col("barcode").isNotNull()).distinct()
      
      a_all_brcdDf = all_brcdDf.alias('a_all_brcdDf')
          
      step1_Df = a_fileDf.join(a_all_brcdDf,a_fileDf.Parent_Identifier == a_all_brcdDf.barcode,how='left')
      actual_upd_rec_cnt = step1_Df.filter(\
                                 (col("Parent_Type_Id") == 1) \
                              & (col("Parent_Identifier").isNotNull())\
                              & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                              & (col("Product_Type_Identifier").isin({'S','H'}) )\
                              & (col("barcode").isNull())).count()
                              
      write_log_info(v_procname,'Step 1: Actual updated rec count'+str(actual_upd_rec_cnt),str(loadid),str(tid),provider_key)
      actual_upd_rec_cnt = 0
      
      step1_Df = step1_Df.withColumn("Parent_Identifier",when(\
                                                               (col("Parent_Type_Id") == 1) \
                                                               & (col("Parent_Identifier").isNotNull())\
                                                               & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                                                               & (col("Product_Type_Identifier").isin({'S','H'}))\
                                                               & (col("barcode").isNull()),lit(None))\
                                                               .otherwise(lit(col("Parent_Identifier"))))
      column_drop = column_drop + '.drop("barcode")'
                         
      
   else:
      step1_Df = a_fileDf.alias('a_step1_Df')
      write_log_info(v_procname,'Step 1: Update skipped',str(loadid),str(tid),provider_key)                              
   #a_step1_Df = a_step1_Df.repartition(1)
   #a_step1_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_step1.csv")   
   step1_Df = step1_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step1_Df = step1_Df.alias('a_step1_Df')
   
   #STEP 1.2: update the valid parent id's with Album UPC and title as it was reported as tracks based on prod no
   step2_upd_recs = a_step1_Df.filter((col("Parent_Type_Id") == 1) \
                              & (col("Parent_Identifier").isNotNull())\
                              & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-A')) \
                              & ((col("Product_Type_Identifier") == 'S') | (col("Product_Type_Identifier")== 'H'))).count()
   
   #Using MAX logic samne as in PreProc and GRAS matching
   write_log_info(v_procname,'Step 2: Update rec count '+str(step2_upd_recs),str(loadid),str(tid),provider_key)                              
   if step2_upd_recs > 0 or 1 == 1:
      write_log_info(v_procname,'Step 2: Update running',str(loadid),str(tid),provider_key)                              
      max_prod = a_product_master.withColumn("grp_col",when(col("barcode").isNull(),lit(col("digital_barcode"))).otherwise(lit(col("barcode"))))\
                                 .groupBy(col("grp_col")).agg(max("product_no"))\
                                 .withColumnRenamed("max(product_no)","product_no")

      a_max_prod = max_prod.alias('a_max_prod')
      prod_tmp = a_max_prod.join(a_product_master.filter(col("is_complete") == 'Y'),a_max_prod.product_no == a_product_master.product_no).select("grp_col","a_product_master.product_no","product_name")
      a_prod_tmp = prod_tmp.alias('a_prod_tmp')
      
      step2_Df = a_step1_Df.join(a_prod_tmp,(a_step1_Df.Parent_Identifier == a_prod_tmp.grp_col),how='left')
      #upd_stmt = udf(lambda a,b,c,d: )
      
      step2_Df = step2_Df.withColumn("UPC",when(\
                                             (col("Parent_Type_Id") == 1)\
                                             & (col("Parent_Identifier").isNotNull())\
                                             & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                                             & (col("Product_Type_Identifier").isin({'S','H'}) ),lit(col("Parent_Identifier")))\
                                             .otherwise(lit(col("UPC"))))\
                         .withColumn("ISRC",when(\
                                             (col("Parent_Type_Id") == 1)\
                                             & (col("Parent_Identifier").isNotNull())\
                                             & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                                             & (col("Product_Type_Identifier").isin({'S','H'}) ),lit(None))\
                                             .otherwise(lit(col("ISRC"))))\
                         .withColumn("Title",when(\
                                             (col("Parent_Type_Id") == 1)\
                                             & (col("Parent_Identifier").isNotNull())\
                                             & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                                             & (col("Product_Type_Identifier").isin({'S','H'}) ),lit(col("product_name")))\
                                             .otherwise(lit(col("Title"))))
      
      step2_Df = step2_Df.drop("grp_col").drop("product_no").drop("product_name")
      
   else:
      step2_Df = a_step1_Df.alias('a_step2_Df') 
      write_log_info(v_procname,'Step 2: Update skipped',str(loadid),str(tid),provider_key)                                    
   #a_step2_Df = a_step2_Df.repartition(1)
   #a_step2_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_step2.csv")
   
   step2_Df = step2_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step2_Df = step2_Df.alias('a_step2_Df')

   #STEP 2.2: update the valid parent id's with Album UPC and title as it was reported as tracks based on local prod no                        
   step3_upd_recs = 0
   write_log_info(v_procname,'Step 3: Update rec count '+str(step3_upd_recs),str(loadid),str(tid),provider_key)                              
   if step3_upd_recs > 0 or 1 == 1:
      #step3_Df_tmp = step2_Df.filter((col("Parent_Identifier").isNotNull()) & (col("parent_type_id") == 1) & (col("UPC").isNull()) & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-A')))\
      #                     .select("Parent_Identifier").distinct()
      write_log_info(v_procname,'Step 3: Update running',str(loadid),str(tid),provider_key)                                    
      max_prod = a_product_master.select("local_barcode","product_no")\
                                 .groupBy(col("local_barcode")).agg(max("product_no"))\
                                 .withColumnRenamed("max(product_no)","product_no")\
                                 .withColumnRenamed("local_barcode","loc_bcode")

      a_max_prod = max_prod.alias('a_max_prod')
      prod_tmp = a_max_prod.join(a_product_master,a_max_prod.product_no == a_product_master.product_no).select("a_product_master.local_barcode","a_product_master.product_no","a_product_master.product_name")
      a_prod_tmp = prod_tmp.alias('a_prod_tmp')
      step3_upd_recs = a_step2_Df.filter(col("UPC").isNull()).join(a_prod_tmp,(a_step1_Df.Parent_Identifier == a_prod_tmp.local_barcode),how='left').count()
      
      step3_Df = a_step2_Df.join(a_prod_tmp,(a_step1_Df.Parent_Identifier == a_prod_tmp.local_barcode),how='left')
      #upd_stmt = udf(lambda a,b,c,d: )
      
      step3_Df = step3_Df.withColumn("UPC",when(\
                                             (col("Parent_Type_Id") == 1)\
                                             & (col("Parent_Identifier").isNotNull())\
                                             & (col("UPC").isNull())\
                                             & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                                             & (col("UPC").isNull())\
                                             & (col("Product_Type_Identifier").isin({'S','H'}) ),col("Parent_Identifier"))\
                                             .otherwise(col("UPC")))\
                         .withColumn("ISRC",when(\
                                             (col("Parent_Type_Id") == 1)\
                                             & (col("Parent_Identifier").isNotNull())\
                                             & (col("UPC").isNull())\
                                             & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                                             & (col("UPC").isNull())\
                                             & (col("Product_Type_Identifier").isin({'S','H'}) ),None)\
                                             .otherwise(col("ISRC")))\
                         .withColumn("Title",when(\
                                             (col("Parent_Type_Id") == 1)\
                                             & (col("Parent_Identifier").isNotNull())\
                                             & (col("UPC").isNull())\
                                             & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                                             & (col("UPC").isNull())\
                                             & (col("Product_Type_Identifier").isin({'S','H'}) ),col("product_name"))\
                                             .otherwise(col("Title")))   


      step3_Df = step3_Df.drop("local_barcode").drop("product_no").drop("product_name")
      #print(step3_Df.count())
      #print(step3_Df.filter(col("Title").isNotNull()).count())
      #print(step3_Df.filter(col("Parent_Identifier").isNotNull()).count())
      #print(step3_Df.filter(col("UPC").isNotNull()).count())
      #print(step3_Df.filter(col("ISRC").isNotNull()).count())
      
      
   else:
      step3_Df = a_step2_Df.alias('a_step3_Df')
      write_log_info(v_procname,'Step 3: Update skipped',str(loadid),str(tid),provider_key)                                    
   #a_step3_Df = a_step3_Df.repartition(1)
   #a_step3_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_step3.csv")

   step3_Df = step3_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step3_Df = step3_Df.alias('a_step3_Df')
   
   #STEP 1.4: update product_type based on parent_type id
   step4_upd_recs = 0
   if step4_upd_recs > 0 or 1 == 1:
      write_log_info(v_procname,'Step 4: Update running',str(loadid),str(tid),provider_key)
      product_identifier_map = udf(lambda x: 'P' if x == 'S' else 'I' if x == 'H' else 'W' if x == 'U' else 'I' if x == 'J' else 'W' if x == 'X' else x,StringType())
      step4_Df = a_step3_Df.withColumn("Product_Type_Identifier",when(\
                                                (col("Parent_Type_Id") == 1)\
                                                & (col("Parent_Identifier").isNotNull())\
                                                & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                                                & (col("Product_Type_Identifier").isin({'S','H'}) ),product_identifier_map(col("Product_Type_Identifier")))\
                                                .otherwise(col("Product_Type_Identifier"))) 
      write_log_info(v_procname,'Step 4: Update rec count '+str(step4_upd_recs),str(loadid),str(tid),provider_key)                              
      
   else:
      step4_Df = a_step3_Df.alias('a_step4_Df') 
      write_log_info(v_procname,'Step 4: Update skipped',str(loadid),str(tid),provider_key)                                       
   #a_step4_Df = a_step4_Df.repartition(1)   
   #a_step4_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_step4.csv")
   #STEP 2: special case for "Matt Cardle" used from RAW logic
   step4_Df = step4_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step4_Df = step4_Df.alias('a_step4_Df')
   write_log_info(v_procname,'Step 5: Update running',str(loadid),str(tid),provider_key)
   product_identifier_map_spz = udf(lambda x: 'S' if x == 'P' else 'H' if x == 'I' else x,StringType())

   step5_Df = a_step4_Df.withColumn("Product_Type_Identifier",when(\
                                                (col("UPC") == '884977795691')\
                                                & (col("Product_Type_Identifier").isin({'P','S','I','U'}))\
                                                & ((trim(col("CMA")) == '') | (trim(col("CMA")) != 'CMA-C') | (col("CMA").isNull()))\
                                                & (col("Product_Type_Identifier").isin({'S','H'}) ),product_identifier_map_spz(col("Product_Type_Identifier")))\
                                                .otherwise(col("Product_Type_Identifier"))) 
   
   step5_Df = step5_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step5_Df = step5_Df.alias('a_step5_Df')
   
   #a_step5_Df = a_step5_Df.repartition(1)
   #a_step5_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_step5.csv")
   write_log_info(v_procname,'Step 5: Update completed',str(loadid),str(tid),provider_key)
   
   
   
   #STEP 3: special case for "Matt Cardle" used from RAW logic
   write_log_info(v_procname,'Step 6: Update running',str(loadid),str(tid),provider_key)
   step6_Df = a_step5_Df.withColumn("ISRC",when(\
                                          (col("Product_Type_Identifier") == 'E'),col("UPC"))\
                                          .otherwise(col("ISRC"))) 
   
   #print(step6_Df.count())
   step6_Df = step6_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step6_Df = step6_Df.alias('a_step6_Df')
   #a_step6_Df = a_step6_Df.repartition((loaded_recs/100000)+1)
   #a_step6_Df.write.mode("overwrite").parquet("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_"+str(tid)+"_enriched.parquet");
   #a_step5_Df = a_step5_Df.repartition(1)
   #a_step6_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_step6.csv")
   
   #exit()
   
   #a_step6_Df = sqlContext.read.parquet("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_"+str(tid)+"_enriched.parquet/*")
   #a_step6_Df = a_step6_Df.alias('a_step6_Df')
   write_log_info(v_procname,'Step 6: Update completed',str(loadid),str(tid),provider_key)
   


   #STEP 4: update participants from Various to something else for BMG for Track,Video, S, H, U, V, J, X, R
   write_log_info(v_procname,'Step 7: Update running',str(loadid),str(tid),provider_key)
   step7_Df_tmp = a_step6_Df.filter(trim(col("Product_Type_Identifier")).isin({'Track', 'Video', 'S', 'H', 'U', 'V', 'J', 'X', 'R', 'PR', 'T', 'TE', 'TS', 'SP', 'RX', 'S3', 'H3', 'V3', 'J3'}))\
                            .groupBy(trim(col("ISRC"))).agg(max("Artist___Show"))\
                            .withColumnRenamed("max(Artist___Show)","particip_name")\
                            .withColumnRenamed("trim(ISRC)","ISRC7")\
                            .where("upper(particip_name) not like '%VARIOUS%'")
   step7_Df_tmp = step7_Df_tmp.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step7_Df_tmp = step7_Df_tmp.alias('a_step7_Df_tmp')
   
   #a_step7_Df_tmp.printSchema()
   
   
   
   step7_Df_tmp1 = a_step6_Df.filter(trim(col("Product_Type_Identifier")).isin({'Track', 'Video', 'S', 'H', 'U', 'V', 'J', 'X', 'R', 'PR', 'T', 'TE', 'TS', 'SP', 'RX', 'S3', 'H3', 'V3', 'J3'}))\
                             .where("upper(Artist___Show) like '%VARIOUS%'")\
                             .join(a_step7_Df_tmp,trim(a_step6_Df.ISRC) == trim(a_step7_Df_tmp.ISRC7)).select("a_step7_Df_tmp.ISRC7","a_step7_Df_tmp.particip_name")
   step7_Df_tmp1 = step7_Df_tmp1.repartition((old_div(loaded_recs,part_recs_cnt))+1)                            
   a_step7_Df_tmp1 = step7_Df_tmp1.alias('a_step7_Df_tmp1')                              
   
   step7_Df = a_step6_Df.join(a_step7_Df_tmp1,trim(a_step6_Df.ISRC) == trim(a_step7_Df_tmp1.ISRC7),how='left').select("a_Step6_Df.*","a_step7_Df_tmp1.particip_name")
   step7_Df = step7_Df.withColumn("Artist___Show",when(col("particip_name").isNull(),col("Artist___Show")).otherwise(col("particip_name"))).drop("particip_name")
   
   step7_Df = step7_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)                            
   a_step7_Df = step7_Df.alias('a_step7_Df')
   #a_step7_Df = a_step7_Df.repartition(1)
   #a_step7_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_step7.csv")
   
   #a_step7_Df = step7_Df.alias('a_step7_Df')
   write_log_info(v_procname,'Step 7: Update completed',str(loadid),str(tid),provider_key)


   
   #STEP 5: update Sony BMG Global Digital Business
   write_log_info(v_procname,'Step 8: Update running',str(loadid),str(tid),provider_key)
   step8_Df = a_step7_Df.withColumn("UPC",when((col("Product_Type_Identifier") == 'G'),'888880265623').otherwise(col("UPC")))\
                        .withColumn("Artist___Show",when((col("Product_Type_Identifier") == 'G'),'Sony BMG Global Digital Business').otherwise(col("Artist___Show")))\
                        .withColumn("Title",when((col("Product_Type_Identifier") == 'G'),'Musika').otherwise(col("Title")))
   
   #print(step8_Df.count())
   step8_Df = step8_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step8_Df = step8_Df.alias('a_step8_Df')
   
   #a_step8_Df = a_step8_Df.repartition(1)
   #a_step8_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_step8.csv")
   write_log_info(v_procname,'Step 8: Update completed',str(loadid),str(tid),provider_key)


   #STEP 5: update Sony BMG Global Digital Business
   write_log_info(v_procname,'Step 9: Update running',str(loadid),str(tid),provider_key)
   step9_Df = a_step8_Df.withColumn("CMA",trim(col("CMA")))
   
   #print(step9_Df.count())
   step9_Df = step9_Df.withColumn("mod_user",lit('gaur04').cast("string"))\
                      .withColumn("mod_stamp",lit('28/05/2019').cast("string"))\
                      .withColumn("load_id",lit(loadid))\
                      .withColumn("trans_id",lit(tid))\
                      .withColumn("rec_no",lit(None).cast('integer'))\
                      .withColumn("provider_key",lit('P001'))
   step9_Df = step9_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step9_Df = step9_Df.alias('a_step9_Df')
   
   #a_step9_Df = a_step9_Df.repartition(1)
   #a_step9_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/"+coun_key+"_step9.csv")
   write_log_info(v_procname,'Step 9: Update completed',str(loadid),str(tid),provider_key)
   
   
   
   step10_Df = a_step9_Df.join(a_gpp_prod_map.filter(col("mapping_type") == 'APPLE_SAMIS'),a_step9_Df.Product_Type_Identifier == a_gpp_prod_map.source_product_type_key,how='left')\
                         .select("a_step9_Df.*","a_gpp_prod_map.dest_product_type_key")\
                         .withColumnRenamed("dest_product_type_key","product_type_key")\
                         .withColumnRenamed("Download_Date_(PST)","Download_Date_PST")\
                         .withColumnRenamed("Report_Date_(Local)","Report_Date_Local")\
                         .na.fill({'product_type_key':99})
                         
   step10_Df.printSchema()
   step10_Df = step10_Df.select("load_id","trans_id","rec_no","provider_key","Provider",
                                                "Provider_Country",
                                                "Vendor_Identifier",
                                                "UPC",
                                                "ISRC",
                                                "Artist___Show",
                                                "Title",
                                                "Label_Studio_Network",
                                                "Product_Type_Identifier",
                                                "quantity",
                                                "quantity_returned",
                                                "wholesale_value",
                                                "retail_value",
                                                "Royalty_Price",
                                                "Download_Date_PST",
                                                "Order_Id",
                                                "state_province",
                                                "city",
                                                "Customer_Identifier",
                                                "Report_Date_Local",
                                                "Sale_Return",
                                                "Customer_Currency",
                                                "Country_Code",
                                                "Royalty_Currency",
                                                "PreOrder",
                                                "ISAN",
                                                "Customer_Price",
                                                "Apple_Identifier",
                                                "CMA",
                                                "Asset_Content_Flavor",
                                                "Vendor_Offer_Code",
                                                "Grid",
                                                "Promo_Code",
                                                "Parent_Identifier",
                                                "Parent_Type_Id",
                                                "Attributable_Purchase",
                                                "Primary_Genre","mod_user","mod_stamp","product_type_key")
   #step10_Df = step10_Df.repartition((loaded_recs/500000)+1)
   step10_Df = step10_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   #step10_Df.write.mode("overwrite").parquet("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/"+coun_key+"_enriched.parquet");
   
   #step10_Df.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header","true").save("s3://sme-ca-dl-dev-output/partner/P001/"+str(report_day)+"/tmp_intermediate/Special_GRAS_DONE_"+coun_key+"_enriched.csv")
   #exec('step10_Df.write.mode("overwrite").parquet("'+content_enriched_file_dir+'/P001/'+str(report_day)+'/tmp_intermediate/Special_GRAS_DONE_'+coun_key+'_'+str(tid)+'_enriched.parquet")')
   step10_Df.write.mode("overwrite").parquet(content_enriched_file_dir+licensor+'/P001/tmp_intermediate/report_day='+str(report_day_col)+'/Special_GRAS_DONE_'+coun_key+'_'+str(tid)+'_enriched.parquet')
   
   #a_step10_Df = step10_Df.alias('a_step10_Df')
   tmp_fname = content_enriched_file_dir+licensor+'/P001/tmp_intermediate/report_day='+str(report_day_col)+'/Special_GRAS_DONE_'+coun_key+'_'+str(tid)+'_enriched.parquet'

   write_log_info(v_procname,' ============= SPECIAL RULES COMPLETED ======== ',str(loadid),str(tid),provider_key)
   print("END TIME: " , datetime.datetime.now())
   
   return tmp_fname

   

def proc_nc_appleitunes(procargs):

   tid = procargs[1]
   loadid = procargs[0]
   trackFile = procargs[12].lower()
   clientKey = procargs[27]
   provider_key = procargs[2]
   report_day = trackFile.split("/")[8]
   vendor_id = trackFile.split("_")[2]
   coun_key = procargs[3]
   v_procname = v_main_proc + '.proc_nc_appleitunes'
   report_day = datetime.datetime.strptime(report_day,'%Y%m%d').strftime('%Y-%m-%d')
   write_log_info(v_procname,' ============= APPLEITUNES NC STARTED ======== ',str(loadid),str(tid),provider_key)
   sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'enriched_files_dir_v2'"
   licensor=get_licensor_from_client_key(clientKey) 
   content_enriched_file_dir = query_all(connect(),sql_query)
   if len(content_enriched_file_dir) == 0:
      print("Output path not available")
      exit()

   content_enriched_file_dir  = content_enriched_file_dir[0][0].lower()
   
   cal_df = get_df_from_db_table("calendar_ftot")
   a_cal_df = cal_df.alias('a_cal_df')
   
   mdb_df = get_df_from_db_table("mdb_vendor_cntry")
   mdb = mdb_df.alias('mdb')
   
   cmp_df = get_df_from_db_table("company")
   comp = cmp_df.alias('comp')
   
   dig_ptype = get_df_from_db_table("dig_prod_type")
   digptype = dig_ptype.alias('digptype')

   mini_cntry_df = get_df_from_db_table("mini_country").filter(col('current_mapping')=="Y")
   mc = mini_cntry_df.alias('mc')   
   
   part_recs_cnt = 5000
   
   consumer_Df = sqlContext.read.csv(content_enriched_file_dir+licensor+'/'+provider_key+'/gras_enriched_files/report_date='+str(report_day)+'/DL_'+coun_key+'_'+str(vendor_id)+'_enriched.csv.gz',sep="\x07",header=True)
   #print cmd
   #exec(cmd)
   loaded_recs = consumer_Df.count()
   
   consumer_Df.printSchema()
   
   write_log_info(v_procname,' consumer enriched file loaded total count loaded_recs '+str(loaded_recs),str(loadid),str(tid),provider_key)
   
   a_consumer_Df = consumer_Df.alias('a_consumer_Df')
   
   
   step1 = a_consumer_Df.join(mdb, (trim(a_consumer_Df.country_key) == trim(mdb.isrc_country_code)) & (trim(a_consumer_Df.provider_key) == trim(mdb.provider_key))\
                              &(a_consumer_Df.vendor_key == trim(mdb.vendor_key)) ,how='left')\
                    .select("a_consumer_Df.*", "mdb.mdb_provider_key")\
                    .withColumn("rec_type",when(isnull(col('mdb_provider_key')), 'P').when(isnull(col('prod_no_dig')),'P').when(isnull(col('distribution_channel_key')),'P').otherwise('N'))
                    
   write_log_info(v_procname,' step 1 completed ',str(loadid),str(tid),provider_key)
   step1 = step1.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step1 = step1.alias('a_step1')
   
   step2 = a_step1.join(comp, a_step1.vendor_key == comp.company_key  ,how='left')\
                 .select("a_step1.*", "comp.company_name").withColumnRenamed("company_name", "vendor_name")\
                 .na.fill({'vendor_name':'unknown'})
   step2 = step2.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   write_log_info(v_procname,' step 2 completed ',str(loadid),str(tid),provider_key)
   
   a_step2 = step2.alias('a_step2')
   
   step3 = a_step2.join(digptype,a_step2.product_type_key == digptype.product_type_key,how='left')\
                     .select("a_step2.*","product_type_cat_key")\
                     .withColumn("copyright_indicator",when(trim(col("country_key")).isin({'US','MX'}),'N')\
                                                .when((trim(col("country_key")) == 'CA') & (col("product_type_cat_key").isin({'V','T'})),'N')\
                                                .otherwise('Y'))
   step3 = step3.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   write_log_info(v_procname,' step 3 completed ',str(loadid),str(tid),provider_key)
   a_step3 = step3.alias('a_step3')
   
   step4 = a_step3.join(mc,a_step3.country_key == mc.isrc_country_code,how='left').select("a_step3.*",mc.country_key).drop(a_step3.country_key)
   
   step4 = step4.repartition((old_div(loaded_recs,part_recs_cnt))+1)
   a_step4 = step4.alias('a_step4')
   write_log_info(v_procname,' step 4 completed ',str(loadid),str(tid),provider_key)
   #.withColumnRenamed("Provider","vendor_name")
   #.withColumn("copyright_indicator", lit("Y").cast(StringType()))
   #.withColumn("mdb_provider_key", lit(None).cast(StringType()))
   #.withColumn("rec_type", lit(None).cast(StringType()))
   #.withColumn("label", lit(None).cast(StringType()))\
   with_rep_week_Df = a_step4.join(a_cal_df,a_step4.transaction_date_key == a_cal_df.day_id,how='left')\
                                 .withColumn("barcode",lit(None).cast('string'))\
                                 .withColumn("prod_no",lit(None).cast('string'))\
                                 .withColumn("prod_no_orig",lit(None).cast('string'))\
                                 .withColumn("origin", lit("I").cast(StringType()))\
                                 .withColumn("external_key", lit(None).cast(StringType()))\
                                 .withColumn("transaction_qty",col("quantity")-col("quantity_returned"))\
                                 .withColumn("retail_value",(col("quantity")+col("quantity_returned"))*col("rpu"))\
                                 .withColumn("charity_amount", lit(0).cast(StringType()))\
                                 .withColumn("vat_tax", lit(0).cast(StringType()))\
                                 .withColumn("vat_tax_charity_amount", lit(0).cast(StringType()))\
                                 .withColumn("wholesale_currency", lit(None).cast(StringType()))\
                                 .withColumn("wholesale_value",(col("quantity")+col("quantity_returned"))*col("wpu"))\
                                 .withColumn("partner_name",lit(col("vendor_name")))\
                                 .select("load_id","trans_id","provider_key","partner_id","client_key","rec_type","report_day","country_key","vendor_key","isrc","upc","grid_no","barcode","external_key",
                                 "prod_no","prod_no_dig","prod_no_orig","distribution_key","transaction_type_key","service_type_key","distribution_channel_key","product_type_key","sales_category_key",
                                 "sales_type_key","mdb_provider_key","quantity","quantity_returned","wholesale_value","currency_key","wholesale_currency",
                                 "vat_tax","vat_tax_charity_amount","charity_amount","retail_value","copyright_indicator",
                                 "media_key","origin",col("week_ftot").alias("reporting_week"),col("wholesale_value").alias("net_invoice_value"),"vendor_name","partner_name","label","label_key","artist_key","spnl_key",
                                 "sales_division_key","product_conf_key","product_types_key","product_key","transaction_date_key","transaction_qty")\
                                 .groupBy("load_id","trans_id","provider_key","partner_id","client_key","rec_type","report_day","country_key","vendor_key","isrc","upc","grid_no","barcode","external_key",
                                 "prod_no","prod_no_dig","prod_no_orig","distribution_key","transaction_type_key","service_type_key","distribution_channel_key","product_type_key","sales_category_key",
                                 "sales_type_key","mdb_provider_key","currency_key","wholesale_currency","copyright_indicator",
                                 "media_key","origin","reporting_week","vendor_name","partner_name","label","label_key","artist_key","spnl_key",
                                 "sales_division_key","product_conf_key","product_types_key","product_key","transaction_date_key")\
                                 .agg(sum("quantity").alias("quantity"),sum("quantity_returned").alias("quantity_returned"),sum("transaction_qty").alias("transaction_qty")\
                                 ,sum("vat_tax").alias("vat_tax"),sum("vat_tax_charity_amount").alias("vat_tax_charity_amount")\
                                 ,sum("charity_amount").alias("charity_amount"),sum("retail_value").alias("retail_value"),sum("wholesale_value").alias("wholesale_value")\
                                 ,sum("net_invoice_value").alias("net_invoice_value"))\
                                 .withColumn("retail_price",when(col("quantity")-col("quantity_returned") != 0,old_div(col("retail_value"),(col("quantity")-col("quantity_returned")))).otherwise(0))\
                                 .withColumn("arpu",lit(None).cast('string'))\
                                 .withColumn("net_invoice_price",when(col("quantity")-col("quantity_returned") != 0,old_div(col("wholesale_value"),(col("quantity")-col("quantity_returned")))).otherwise(0))\
                                 .withColumn("wpu",when(col("quantity")-col("quantity_returned") != 0,old_div(col("wholesale_value"),(col("quantity")-col("quantity_returned")))).otherwise(0))\
                                 .withColumn("ppd",when(col("quantity")-col("quantity_returned") != 0,old_div(col("wholesale_value"),(col("quantity")-col("quantity_returned")))).otherwise(0))\
                                 .select("load_id","trans_id","provider_key","partner_id","client_key","rec_type","report_day","country_key",
                                   "vendor_key","isrc","upc","grid_no","barcode","external_key","prod_no","prod_no_dig","prod_no_orig",
                                   "distribution_key","transaction_type_key","service_type_key","distribution_channel_key","product_type_key",
                                   "sales_category_key","sales_type_key","mdb_provider_key","quantity","quantity_returned","wholesale_value",
                                   "currency_key","wholesale_currency","ppd","wpu","arpu","vat_tax","vat_tax_charity_amount","charity_amount",
                                   "net_invoice_price","net_invoice_value","retail_price","retail_value","copyright_indicator","media_key",
                                   "origin","reporting_week","vendor_name","partner_name","label","label_key","artist_key","spnl_key",
                                   "sales_division_key","product_conf_key","product_types_key","product_key","transaction_date_key",
                                   "transaction_qty")
   
   with_rep_week_Df = with_rep_week_Df.repartition((old_div(loaded_recs,part_recs_cnt))+1)                             
   
   write_log_info(v_procname,' with_rep_week_Df created going to write it now ',str(loadid),str(tid),provider_key) 
   upd_result = upd_trans_control_flag(loadid,tid,'file_aggregated_daily')
   
   with_rep_week_Df = with_rep_week_Df.repartition((old_div(loaded_recs,30000))+1)
   
   with_rep_week_Df.write.mode("overwrite").format("csv").option("delimiter", "\x07").option("header", "true").option("compression", "gzip").option("quote","").option("nullValue","") \
           .option("emptyValue",None).save(content_enriched_file_dir+"/"+licensor+"/"+provider_key+"/non_consumer_files/report_date="+report_day+"/DL_nc_"+coun_key+"_"+str(loadid)+"_"+str(tid)+"_enriched.bsv.gz")


   write_log_info(v_procname,' NC file written to S3 ',str(loadid),str(tid),provider_key)
   
   #write_nc_manifest_file(loadid,'non_consumer_files','enriched_files_dir_v2',clientKey)
   write_manifest_file(loadid,'non_consumer_files/DL_nc_'+coun_key+"_"+str(loadid)+"_"+str(tid)+"_enriched.bsv.gz",'enriched_files_dir_v2',clientKey)

   write_log_info(v_procname,' NC Manifest generated ',str(loadid),str(tid),provider_key)
   
   upd_result = upd_trans_control_flag(loadid,tid,'file_ready_non_consumer')
   

   upd_trans_stat(tid,'sum_units_db_nc','DL_nc_'+coun_key+'_'+str(loadid)+'_'+str(tid)+'_enriched.bsv.gz')

   write_log_info(v_procname,' trans stat updated ',str(loadid),str(tid),provider_key)
   

    ##### check no of files received vs processed and generate success file


    #delivery_expected= report_day_col
   delivery_expected = datetime.datetime.strptime(report_day, "%Y-%m-%d")
   delivery_expected = delivery_expected + datetime.timedelta(days=1)
   print(delivery_expected)

   sql_query="select count(*) from "+ config.metadata["trans_control"] +" where  provider_key = 'P001' and \
   date(delivery_time_expected)='"+str(delivery_expected)+ "' and client_key="+str(clientKey)+" and data_type like 'SA%' and file_downloaded is not null"



   files_received=query_all(connect(),sql_query)
   print("files_received")
   print(files_received[0][0])

   sql_query ="select count(*) from "+ config.metadata["trans_control"] +" where  provider_key = 'P001' and \
   date(delivery_time_expected)='"+str(delivery_expected)+"'  and client_key="+str(clientKey)+ " and data_type like 'SA%' and file_downloaded is not null \
   and consumer_enriched_file_name is not null and file_ready_non_consumer is not null"


   file_processed=query_all(connect(),sql_query)
   print("file_processed")
   print(file_processed[0][0])
   write_log_info(v_procname,'No of non consumer Files processed: '+str(file_processed),str(loadid),str(tid),provider_key)
   files_processed=file_processed[0][0]
   if(int(files_processed)>=num_files):
       write_log_info(v_procname,' Yayy All files recevied and processed ..writing success file',str(loadid),str(tid),provider_key)

       eschema=StructType([StructField('col1',StringType(),True)])
       df2 = sc.parallelize([]).toDF(eschema)
       df2.write.mode('append').csv(content_enriched_file_dir+'/'+licensor+'/'+provider_key+'/non_consumer_files/report_date='+str(report_day))



   write_log_info(v_procname,' ============= APPLEITUNES NC COMPLETED ======== ',str(loadid),str(tid),provider_key)


def appleitunes_gras_main(load_id):

   print("START TIME: " , datetime.datetime.now())
   #licensor='sme'
   v_procname = 'appleitunes_gras_match.py'
   write_log_info(v_procname,' ============= START ======== ',str(load_id),None)
   #write_log_info(v_procname,'==  STARTED ? ===== ',str(load_id),None, None)

   sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and (file_ready_consumer is null or file_ready_consumer = '')\
                and (file_available is not null or file_available != '')"
   fileExpectation = query_all(connect(),sql_query)
   if len(fileExpectation) == 0:
      print("Nothing to load")
      write_log_info(v_procname,' Nothing to load for normal processing ',str(load_id))
   else:
      provider_key = fileExpectation[0][2]
      print(provider_key)
      filename = fileExpectation[0][12]
      clientKey=fileExpectation[0][27]
      licensor=get_licensor_from_client_key(clientKey)
      write_log_info(v_procname,' ============= STARTED Normal processing ======== ',str(load_id),None,provider_key)

      # Here comes the code to check if parquet files are being udpated
      # check if parquet files are being updated
      is_running = get_flag_value('parquet_process')
      while is_running == 1000:
          write_log_info(v_procname,'=== Waiting for running process to finish flag value(parquet udpate) '+str(is_running),str(load_id),None,provider_key)
          t.sleep(sec_to_wait)
          is_running = get_flag_value('parquet_process')

      write_log_info(v_procname,'=== Out of wait loop flag value '+str(is_running),str(load_id),None,provider_key)
      upd_result = increment_flag_value('parquet_process')
      write_log_info(v_procname,'parquet process flag value increment by 1',str(load_id),None,provider_key)
      

      pool = ThreadPool(1)
      results = pool.map(proc_appleitunes, fileExpectation)
      pool.close()
      pool.join()
      
      upd_result = decrement_flag_value('parquet_process')
      write_log_info(v_procname,'parquet process flag value decrement by 1',str(load_id),None,provider_key)
      
      write_log_info(v_procname,' ============= COMPLETED Normal processing ======== ',str(load_id),None,provider_key)



   sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " and file_ready_non_consumer is null\
                              and file_ready_consumer is not null"
   
   fileExpectation = query_all(connect(),sql_query)
   if len(fileExpectation) == 0:
      print("Nothing to load for nc")
      write_log_info(v_procname,' Nothing to load for nc processing ',str(load_id))
   else:
      provider_key = fileExpectation[0][2]
      print (fileExpectation)
      
      write_log_info(v_procname,' ============= STARTED nc processing ======== ',str(load_id),None,provider_key)

      pool = ThreadPool(1)
      results = pool.map(proc_nc_appleitunes, fileExpectation)
      pool.close()
      pool.join()

      
      write_log_info(v_procname,' ============= COMPLETED nc processing ======== ',str(load_id),None,provider_key)    

   write_log_info(v_procname,' ============= FINISHED?? ======== ',str(load_id))

