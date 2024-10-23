from __future__ import division
from __future__ import print_function
#-----------------  IMPORT MODULES ----------------
from builtins import str
from past.utils import old_div
#import sys
#reload(sys)
#sys.setdefaultencoding('utf8')

from env import init
retcode=init()

from utl_mysql_interface import *
import config

from pyspark.sql.functions import *
from pyspark.sql.window import *
from datetime import *
import time as t

from utl_functions import *
from random import randint
import utl_awstools
#------------------- END IMPORT -------------------
config.metadata=read_config(section="table")
v_procname = 'appleitunes_user_process.py'
sec_to_wait = 5


def proc_user_files(loadid,in_bucket,provider_key,report_day,stream_fname,licname,dataType):
   print(("START TIME USER ENRICHMENT: " , datetime.datetime.now()))
   report_day_col = datetime.datetime.strptime(report_day, '%Y%m%d').strftime('%Y-%m-%d')
   print("file and bucket details")
   print(loadid)
   print(in_bucket)
   print(provider_key)
   print(report_day)
   print(stream_fname)
   print(licname)
   write_log_info(v_procname,' ============= START USER PROC ======== ',str(loadid),None,provider_key)

   #cmd = 'streamDf = sqlContext.read.format("csv").option("header","true").option("delimiter","\t").option("samplingRatio", "0.1").load("'+stream_fname+'")'
   streamDf = sqlContext.read.format("csv").option("header","true").option("delimiter","\t").option("samplingRatio", "0.1").load(stream_fname)
   #exec(cmd)
   write_log_info(v_procname,' Read all stream files ',str(loadid),None,provider_key)
   streamDf.printSchema()
 
   #allStreamDf = streamDf.unionAll(nonRoyaltystreamDf)
   allStreamDf = remove_column_spaces(streamDf)
   allStreamDf = allStreamDf.withColumnRenamed("Customer_Identifier","consumer_id")
   print("dataframe")
   allStreamDf.printSchema()
   
   allStreamDf = allStreamDf.select(allStreamDf.consumer_id).na.fill({'consumer_id':'-1'}).distinct()

   total_consumer_cnt = allStreamDf.count()
   write_log_info(v_procname,' Union Done : Total distinct consumer count '+str(total_consumer_cnt),str(loadid),None,provider_key)

   a_allStreamDf = allStreamDf.alias('a_allStreamDf')


   is_running = get_flag_value('applem_consumer_key_gen')
   while is_running == 1:
      write_log_info(v_procname,'=== Waiting for running process to finish flag applem_consumer_key_gen value '+str(is_running),str(loadid),None,provider_key)
      t.sleep(randint(1,15))
      is_running = get_flag_value('applem_consumer_key_gen')

   write_log_info(v_procname,'=== Out of wait loop flag applem_consumer_key_gen value '+str(is_running),str(loadid),None,provider_key)

   upd_result = upd_flag_value(1,'applem_consumer_key_gen')

   write_log_info(v_procname,'== flag applem_consumer_key_gen value updated to 1(no new process start until this finishes) output value is '+str(upd_result),str(loadid),None,provider_key)
   if upd_result != 1:
      write_log_info(v_procname,'failed to update the flag applem_consumer_key_gen to 1 ',str(loadid),None,provider_key)
      exit()

   sql_query = "select parameter_value from " + config.metadata["job_parameter"] + " where parameter_name = 'enriched_files_dir_v2'"

   content_enriched_file_dir = query_all(connect(),sql_query)
   if len(content_enriched_file_dir) == 0:
      write_log_info(v_procname,' ERROR!!!! enriched file directory empty ',str(loadid),None,provider_key)
      print("Output path not available")
      exit()
   content_enriched_file_dir  = content_enriched_file_dir[0][0].lower()

   try:
      #exec('consumerKeyDf = sqlContext.read.parquet("'+ content_enriched_file_dir.replace('partner_data','lookup') +'consumer_lookup/appleitunes/*").select("consumer_key","consumer_id")')
      #BY IK exec('consumerKeyDf = sqlContext.read.parquet("' + content_enriched_file_dir.replace('partner_data','lookup') + 'consumer_lookup/applemusic/*").select("consumer_key","consumer_id")') 
      consumerKeyDf = sqlContext.read.parquet(content_enriched_file_dir.replace('partner_data','lookup') + 'consumer_lookup/applemusic/*').select("consumer_key","consumer_id")
      a_consumerKeyDf = consumerKeyDf.alias('a_consumerKeyDf')
      current_total_consumer_count = a_consumerKeyDf.count()
      write_log_info(v_procname,'consumerKeyDf loaded total count current_total_consumer_count '+str(current_total_consumer_count),str(loadid),None,provider_key)
      dataWithConsumerkey = a_allStreamDf.join(a_consumerKeyDf,a_consumerKeyDf.consumer_id == a_allStreamDf.consumer_id,how='left').select("a_allStreamDf.consumer_id","a_consumerKeyDf.consumer_key")

   except Exception as e:
      if type(e).__name__ == 'AnalysisException':
         print('folder not found')
         write_log_info(v_procname,'This is first time consumer keys would be generated',str(loadid),None,provider_key)
         dataWithConsumerkey = a_allStreamDf.withColumn("consumer_key",lit(None))
         current_total_consumer_count = 0
      else:
         print("error during processing")
         write_log_info(v_procname,'=== some exception!! EXITING from processing',str(loadid),None,provider_key)
         upd_result = upd_flag_value(0,'applem_consumer_key_gen')
         exit()


   new_consumer_count = dataWithConsumerkey.filter(col("consumer_key").isNull()).count()

   write_log_info(v_procname,'New consumers not having consumer keys '+str(new_consumer_count),str(loadid),None,provider_key)

   if new_consumer_count > 0:
      write_log_info(v_procname,'Get latest sequence value ',str(loadid),None,provider_key)

      latest_seq_val = query_all(connect(),"select cur_value from "+config.metadata["sequence"]+" where name = 'applem_consumer_key'")[0][0]

      write_log_info(v_procname,'Latest sequence applemusic_consumer_key value is '+str(latest_seq_val),str(loadid),None,provider_key)

      dataWithConsumerkey1 = dataWithConsumerkey.filter(col("consumer_key").isNotNull())
      dataWithoutConsumerkey = dataWithConsumerkey.filter(col("consumer_key").isNull())

      dataWithoutConsumerkey = dataWithoutConsumerkey.withColumn("consumer_key",row_number().over(Window.partitionBy(lit(None)).orderBy(lit(None)))+latest_seq_val)

      write_log_info(v_procname,'Updated the new consumer keys ',str(loadid),None,provider_key)

      new_consumers_for_parquet = dataWithoutConsumerkey.select(col("consumer_key").cast('string'),col("consumer_id"))
      new_consumers_for_parquet = new_consumers_for_parquet.repartition((old_div(new_consumer_count,50000))+1)

      write_log_info(v_procname,'Created DF for new consumer data only consumer key and consumer id ',str(loadid),None,provider_key)

      seq_to_update_on_mysql = latest_seq_val + new_consumer_count + 1

      print("seq_to_update_on_mysql "+str(seq_to_update_on_mysql))

      ins_query = ("update "+config.metadata["sequence"]+" set cur_value = "+str(seq_to_update_on_mysql)+" where name = 'applem_consumer_key'")
      ins_data = str(seq_to_update_on_mysql)
      upd_result = upd_ins_data(connect(),ins_query,ins_data)

      if upd_result != 1:
        write_log_info(v_procname,'update to the sequence applemusic_consumer_key failed new value = '+str(seq_to_update_on_mysql),str(loadid),None,provider_key)
        exit()

      print(" update result value " + str(upd_result))

      write_log_info(v_procname,'new value for the sequence applemusic_consumer_key =  '+str(seq_to_update_on_mysql),str(loadid),None,provider_key)

      cmd_string = 'new_consumers_for_parquet.write.mode("append").parquet("'+content_enriched_file_dir.replace("partner_data","lookup")+'consumer_lookup/applemusic/'+str(loadid)+'.parquet")'
      exec(cmd_string)

      write_log_info(v_procname,'New consumer keys parquet file appended ',str(loadid),None,provider_key)

      #dataWithConsumerkey = dataWithConsumerkey1.unionAll(dataWithoutConsumerkey)
      #Read the consumer file again and see if all consumer keys are generated

      #BY IK exec('consumerKeyDf = sqlContext.read.parquet("'+content_enriched_file_dir.replace("partner_data","lookup")+'consumer_lookup/applemusic/*").select("consumer_key","consumer_id")')
      consumerKeyDf = sqlContext.read.parquet(content_enriched_file_dir.replace("partner_data","lookup")+'consumer_lookup/applemusic/*').select("consumer_key","consumer_id")
      new_total_consumer_count = consumerKeyDf.count()
      write_log_info(v_procname,'consumerKeyDf loaded loaded again new_total_consumer_count '+str(new_total_consumer_count),str(loadid),None,provider_key)
      upd_result = upd_flag_value(0,'applem_consumer_key_gen')
      write_log_info(v_procname,'== flag applem_consumer_key_gen value updated to 0 so new process can start output of update is '+str(upd_result),str(loadid),None,provider_key)
      if new_total_consumer_count - current_total_consumer_count == new_consumer_count:
        write_log_info(v_procname,'New consumer count matches correctly',str(loadid),None,provider_key)
      else:
        write_log_info(v_procname,'ERROR!!New consumer count dont match',str(loadid),None,provider_key)
        exit()

      a_consumerKeyDf = consumerKeyDf.alias('a_consumerKeyDf')

      #dataWithConsumerkey = a_userDf.join(a_consumerKeyDf,a_consumerKeyDf.consumer_id == a_userDf.user_id,how='left').select("a_userDf.*","a_consumerKeyDf.consumer_key")

      dataWithConsumerkey = a_allStreamDf.join(a_consumerKeyDf,a_consumerKeyDf.consumer_id == a_allStreamDf.consumer_id,how='left').select("a_consumerKeyDf.consumer_key","a_allStreamDf.consumer_id")
      dataWithConsumerkey.createOrReplaceTempView('ck')
      sqlContext.sql("select * from ck where consumer_key is null").show(10,False)
      dataWithConsumerkey.filter(col("consumer_key").isNull()).show(10,False)
      new_consumer_count = dataWithConsumerkey.filter(col("consumer_key").isNull()).count()

      new_consumer_count = dataWithConsumerkey.filter(col("consumer_key").isNull()).count()
      write_log_info(v_procname,'Check if new consumer count is zero now new_consumer_count '+str(new_consumer_count),str(loadid),None,provider_key)
      if new_consumer_count != 0:
        write_log_info(v_procname,'ERROR!!New consumer count is not zero check this!!',str(loadid),None,provider_key)
        exit()
      dataWithConsumerkey = dataWithConsumerkey.repartition((old_div(total_consumer_cnt,100000))+1)
      #cmd_string = 'dataWithConsumerkey.write.mode("overwrite").parquet("'+content_enriched_file_dir+provider_key+'/'+report_day+'/tmp_intermediate/'+'user_enriched_file_'+str(loadid)+'.parquet")'
      cmd_string = 'dataWithConsumerkey.write.mode("overwrite").parquet("' + content_enriched_file_dir + licname + '/' + provider_key + '/user_enriched_files/report_day=' + report_day_col + '/' + 'user_enriched_file_' + str(loadid) + '.parquet")'
      exec(cmd_string)

      write_log_info(v_procname,' Consumer file written(under temp folder) for particular load id',str(loadid),None,provider_key)

   else:
      write_log_info(v_procname,'No new consumer keys are created ',str(loadid),None,provider_key)
      upd_result = upd_flag_value(0,'applem_consumer_key_gen')
      write_log_info(v_procname,'== flag applem_consumer_key_gen value updated to 0 so new process can start output of update is '+str(upd_result),str(loadid),None,provider_key)
      cmd_string = 'dataWithConsumerkey.write.mode("overwrite").parquet("'+content_enriched_file_dir+licname+'/'+provider_key+'/user_enriched_files/report_day='+report_day_col+'/'+'user_enriched_file_'+str(loadid)+'.parquet")'
      exec(cmd_string)
      write_log_info(v_procname,'Consumer enriched file written to handle this loadid ',str(loadid),None,provider_key)
   

   write_log_info(v_procname,' ============= COMPLETED USER PROC ======== ',str(loadid),None,provider_key)


   #################New Step for reorganizing consumer lookup data#####################################################
   if is_first_sat() and dataType=='SA80026921':
       is_running = get_flag_value('applem_consumer_key_gen')
       while is_running == 1:
           write_log_info(v_procname,'=== Waiting for running process to finish flag applem_consumer_key_gen value ' + str(is_running), str(loadid), "0", provider_key)
           t.sleep(sec_to_wait)
       is_running = get_flag_value('applem_consumer_key_gen')
       write_log_info(v_procname,'=== Out of wait loop flag applem_consumer_key_gen for lookup reorg  ' + str(is_running), str(loadid), "0", provider_key)
       upd_result = upd_flag_value(1, 'applem_consumer_key_gen')
       utl_awstools.compactParquet("s3://sme-ca-dl-prod-output/lookup/consumer_lookup/applemusic",500)
       utl_awstools.compactParquet("s3://sme-ca-dl-prod-output/lookup/consumer_lookup/appleitunes",30)
       upd_result = upd_flag_value(0, 'applem_consumer_key_gen')
   ####################################################################################################################

   print(("END TIME USER ENRICHMENT: " , datetime.datetime.now()))
      


def appleitunes_user_main(load_id):

   print("START TIME: " , datetime.datetime.now())





   sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + " \
                                                             and file_available is not null order by spec_id,trans_id"
   fileExpectation = query_all(connect(),sql_query)
   if len(fileExpectation) == 0:
      print("No files to load=> exiting")
      exit()
   pkey=fileExpectation[0][2]
   trackFile = fileExpectation[0][12].lower()
   report_day = trackFile.split("/")[8]
   clientKey = fileExpectation[0][27]
   dataType=fileExpectation[0][7]
   licensor_name = get_licensor_from_client_key(clientKey)


   write_log_info(v_procname,' ============= START ======== ',str(load_id),None,pkey)


   sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'enriched_files_dir_v2'"

   content_enriched_file_dir = query_all(connect(),sql_query)
   if len(content_enriched_file_dir) == 0:
      print("Output path not available")
      exit()

   input_file_bucket  = content_enriched_file_dir[0][0].split('/')[2].replace('output','input')

   proc_user_files(load_id,input_file_bucket,pkey,report_day,trackFile,licensor_name,dataType)

   write_log_info(v_procname,' ============= COMPLETED SUCCESSFULLY ======== ',str(load_id),None,pkey)



