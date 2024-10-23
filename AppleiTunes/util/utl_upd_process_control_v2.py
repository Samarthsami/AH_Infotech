from __future__ import print_function
#-----------------  IMPORT MODULES ----------------
from builtins import str
import sys,boto3,os
from env import init
retcode=init()

from utl_mysql_interface import *
import config
from botocore.exceptions import *

from datetime import *
from utl_functions import *
from importlib import util
if util.find_spec("pyspark"):
    from pyspark.sql.functions import *
#------------------- END IMPORT -------------------

config.metadata=read_config(section="table")
v_main_proc = 'utl_upd_process_control'

#--------------- FUNCTION --------------------------------------
# Name:             get_read_command
# Purpose:          Build the command string to read dataframe
# Return:           command string to read the particular file type
# Author:
# Arguments:        file_type(CSV,PARQUET), file delimiter(if no delimiter then null)
# Modified date     Comments
# 28.02.2019        Initial version
#
#
#---------------------------------------------------------------
def get_read_command(file_type,file_delimiter):
   #set the correct delimiter
   #if file_delimiter == 'tab':
   #    f_del = '\t'
   #elif file_delimiter == 'bell':
   #    f_del = '\x07'
   #f_del = file_delimiter

   #set correct file type
   print("file_delimiter "+file_delimiter)

   if file_type == 'JSON':
      x = 'sqlContext.read.format("json").option("samplingRatio","0.1").load("##srcf##")'
   elif file_type == 'CSV':
      x = 'sqlContext.read.format("csv").option("header","false").option("delimiter","'+file_delimiter+'").option("samplingRatio", "0.1").load("##srcf##")'
   elif file_type == 'PARQUET':
      x = 'sqlContext.read.parquet("##srcf##")'
   else:
      x = 'sqlContext.read.format("csv").option("header","false").option("delimiter","'+file_delimiter+'").option("samplingRatio", "0.1").load("##srcf##")'
   return x

#--------------- FUNCTION --------------------------------------
# Name:             check_file_s3
# Purpose:          Check if given file is available on S3
# Return:           True or False
# Author:
# Arguments:        bucket name, rest of the file name along with path after bucket name (e.g. bucket_name, default/folder1/folder2)
# Modified date     Comments
# 28.02.2019        Initial version
#
#
#---------------------------------------------------------------
def check_file_s3(fname):
   v_procname = v_main_proc+'.check_file_s3'
   print(fname)
   if fname.find('s3:///') != 0:
      bucket_name = fname.split('/')[2]
      filename = fname.replace(bucket_name+'/','').replace('s3://','')
   else:
      print("incorrect filename")
      write_log_info(v_procname,' File name is incorrect or not exists '+str(source_file_cnt),0,None,None)
      return False
   print(bucket_name)
   print(filename)
   s3client = boto3.resource('s3')
   try:
      s3client = boto3.resource('s3')
      s3client.Object(bucket_name, filename).load()
      return True
   except ClientError as err:
      return False

#--------------- FUNCTION --------------------------------------
# Name:             f_no_recs_file
# Purpose:          Update the number of lines in the downloaded file into t_trans_stat column no_recs_file
# Return:           Nothing
# Author:
# Arguments:        trans id,loadid,provider key,filename,header_rows,trail_rows(for substraction from total lines)
# Modified date     Comments
# 28.02.2019        Initial version
#
#
#---------------------------------------------------------------
def f_no_recs_file(transid,load_id,provider_key,specid,fname,c_cat_cmd):
   print("trans_id  : "+str(transid))
   v_procname = v_main_proc+'.f_no_recs_file'

   proc_files=query_all(connect(), "select * from "+config.metadata["trans_control"]+" where trans_id="+str(transid))
   print(len(proc_files))
   print("x")
   if len(proc_files) == 0:
     head_rows = 0
     trail_rows = 0
   else:
     head_rows = int(proc_files[0][29] or 0)
     tail_rows = int(proc_files[0][30] or 0)

   print(type(head_rows))
   print(type(tail_rows))
   c_cat_cmd = c_cat_cmd.replace('#filename#',fname)

   #final_cmd = "source_file_cnt = int(os.popen('"+c_cat_cmd+" "+fname+" | wc -l').read().replace('\\n',''))"
   final_cmd = "source_file_cnt = int(os.popen('"+c_cat_cmd+"').read().replace('\\n',''))"
   print(final_cmd)
   #exit()
   #exec("source_file_cnt = int(os.popen('"+c_cat_cmd+" "+fname+" | wc -l').read().replace('\\n',''))")
   source_file_cnt=None
   ldic=locals()
   exec("source_file_cnt = int(os.popen('"+c_cat_cmd+"').read().replace('\\n',''))",globals(),ldic)
   source_file_cnt=ldic["source_file_cnt"]

   source_file_cnt = source_file_cnt - head_rows - tail_rows

   sql_query = "update "+config.metadata["trans_stat"] +" set no_recs_file =" +str(source_file_cnt)+" where trans_id = "+str(transid)
   print(sql_query)
   ins_data = 0
   upd_result = upd_ins_data(connect(),sql_query,ins_data)
   write_log_info(v_procname,' == updated no_recs_file to '+str(source_file_cnt),load_id,transid,provider_key)

#--------------- FUNCTION --------------------------------------
# Name:             f_sum_units_db_consumer
# Purpose:          Update column sum_units_db_consumer in t_trans_stat table
# Return:           Nothing
# Author:
# Arguments:        trans id,load id,provider key, filename(complete path) file type and delimiter
# Modified date     Comments
# 28.02.2019        Initial version
#
#
#---------------------------------------------------------------

def f_sum_units_db_consumer(transid,load_id,provider_key,filename,ftype,fdel):
   try:
      v_procname = v_main_proc+'.f_sum_units_db_consumer'
      #if check_file_s3(filename):
      cmd = get_read_command(ftype,fdel)
      print(cmd)
      tgtDf=None
      ldic=locals()
      exec('tgtDf = '+cmd.replace('##srcf##',filename),globals(),ldic)
      tgtDf=ldic["tgtDf"]
      #subtracting 1 for header
      target_file_cnt = tgtDf.count()
      sql_query = "update "+config.metadata["trans_stat"] +" set sum_units_db_consumer =" +str(target_file_cnt)+" where trans_id = "+str(transid)+""
      ins_data = 0
      upd_result = upd_ins_data(connect(),sql_query,ins_data)
      write_log_info(v_procname,' == updated sum_units_db_consumer to '+str(target_file_cnt),load_id,transid,provider_key)
      #else:
      #write_log_info(v_procname,' file doesnt exists '+str(filename),load_id,transid,provider_key)
   except Exception as e:
      if type(e).__name__ == 'AnalysisException':
         write_log_info(v_procname,'ERROR!! file doesnt exists '+str(filename),load_id,transid,provider_key)
      else:
         write_log_info(v_procname,'ERROR!! other exception '+str(filename),load_id,transid,provider_key)

#--------------- FUNCTION --------------------------------------
# Name:             f_sum_units_db_consumer_new
# Purpose:          Update column sum_units_db_consumer in t_trans_stat table
# Return:           Nothing
# Author:
# Arguments:        trans id,load id,provider key, filename(complete path) file type and delimiter
# Modified date     Comments
# 28.02.2019        Initial version
#
#
#---------------------------------------------------------------

def f_sum_units_db_consumer_new(transid,load_id,provider_key,cnt_to_upd):
   try:
      v_procname = v_main_proc+'.f_sum_units_db_consumer_new'
      write_log_info(v_procname,' updating value for sum_units_db_consumer = '+str(cnt_to_upd),load_id,transid,provider_key)
      sql_query = "update "+config.metadata["trans_stat"] +" set sum_units_db_consumer =" +str(cnt_to_upd)+" where trans_id = "+str(transid)+""
      ins_data = 0
      upd_result = upd_ins_data(connect(),sql_query,ins_data)
      write_log_info(v_procname,' updated sum_units_db_consumer to '+str(cnt_to_upd),load_id,transid,provider_key)

   except:
      write_log_info(v_procname,'ERROR!! during the update for sum_units_db_consumer ',load_id,transid,provider_key)


#--------------- FUNCTION --------------------------------------
# Name:             f_sum_units_db_nc
# Purpose:          Update column sum_units_db_nc in t_trans_stat table
# Return:           Nothing
# Author:
# Arguments:        trans id,load id,provider key, filename(complete path) file type and delimiter
# Modified date     Comments
# 28.02.2019        Initial version
#
#
#---------------------------------------------------------------

def f_sum_units_db_nc(transid,load_id,provider_key,filename,ftype,fdel):

   v_procname = v_main_proc+'.f_sum_units_db_nc'
   #if check_file_s3(filename):
   cmd = get_read_command(ftype,fdel)
   print(cmd)
   try:
      tgtDf=None
      ldic=locals()
      exec('tgtDf = '+cmd.replace('##srcf##',filename).replace('"header","false"','"header","true"'),globals(),ldic)
      tgtDf=ldic["tgtDf"]

      print(tgtDf.count())
      target_file_cnt = tgtDf.agg(sum(col("quantity").cast('int'))).collect()[0][0]
      sql_query = "update "+config.metadata["trans_stat"] +" set sum_units_db_nc =" +str(target_file_cnt)+" where trans_id = "+str(transid)+""
      ins_data = 0
      upd_result = upd_ins_data(connect(),sql_query,ins_data)
      write_log_info(v_procname,' == updated sum_units_db_nc to '+str(target_file_cnt),load_id,transid,provider_key)
      #else:
      #write_log_info(v_procname,' file doesnt exists '+str(filename),load_id,transid,provider_key)
   except Exception as e:
      if type(e).__name__ == 'AnalysisException':
         print(e)
         write_log_info(v_procname,'ERROR!! file doesnt exists '+str(filename),load_id,transid,provider_key)
      else:
         print(e)
         write_log_info(v_procname,'ERROR!! other exception '+str(filename),load_id,transid,provider_key)
#--------------- FUNCTION --------------------------------------
# Name:             upd_trans_stat
# Purpose:          Updates the trans stat table for particular trans id and the column name from table
# Return:           Nothing
# Author:
# Arguments:        transid, column name from t_trans_stat table
# Modified date     Comments
# 28.02.2019        Initial version
#
#
#---------------------------------------------------------------

def upd_trans_stat(transid,field_name,fname=None):
   v_procname = v_main_proc+'.upd_trans_stat'
   if fname == None and field_name == 'no_recs_file':
     write_log_info(v_procname,' File name not passed!!Exiting ',None,transid,None)
     exit()

   sql_query = "select * from "+ config.metadata["trans_control"] +" where trans_id = "+ str(transid)
   trans_record = query_all(connect(),sql_query)
   if len(trans_record) == 0:
     print("Trans id not found => exiting")
     write_log_info(v_procname,' Trans id not found!!Exiting ',None,transid,None)
     exit()

   print(trans_record)

   provider_key = trans_record[0][2]
   load_id = trans_record[0][0]
   src_filename = trans_record[0][12]
   spec_id = trans_record[0][4]
   client_key = trans_record[0][27]
   country_key = trans_record[0][3]
   dtype = trans_record[0][7]
   v_client=get_licensor_from_client_key(client_key)

   if int(load_id)>26087:
     report_day = src_filename.split("/")[9]
   else:
     report_day = src_filename.split("/")[8]
   report_day_part=report_day[0:4]+"-"+report_day[4:6]+"-"+report_day[6:]
   ### FOR MONTHLY FILES
   if len(report_day)==6:
       report_day=report_day+'01'

   report_date = datetime.datetime.strptime(report_day,'%Y%m%d').strftime('%Y-%m-%d')
   tdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
   tgt_fname = trans_record[0][28]
   print(report_day)
   print(report_date)

   write_log_info(v_procname,' === START ==== ',load_id,transid,provider_key)

   #check if trans stat has record for this trans id

   sql_query = "select count(*) from "+config.metadata["trans_stat"] + " where trans_id = "+str(transid)
   rec_exists = query_all(connect(),sql_query)[0][0]
   print(rec_exists)
   if rec_exists == 1:
     sql_query = "update "+config.metadata["trans_stat"] +" set trans_date = '"+tdate+"',report_date = '"+report_date+"',provider_key ='"+provider_key+"',client_key='"+client_key+"',country_key='"+country_key+"'\
                                             ,data_type='"+dtype+"' where trans_id = "+str(transid)+""
     print(sql_query)
     ins_data = 0
     upd_result = upd_ins_data(connect(),sql_query,ins_data)
     write_log_info(v_procname,' == updated existing record in trans_stat == ',load_id,transid,provider_key)
   elif rec_exists == 0:
     sql_query = "insert into "+config.metadata["trans_stat"] +"(trans_id,trans_date,report_date,provider_key,client_key,data_type,country_key) values(%s,%s,%s,%s,%s,%s,%s)"
     ins_data = str(transid) + "," + tdate + "," + report_date + "," + provider_key + "," + client_key + "," + dtype + "," + country_key
     ins_data = ins_data.split(',')
     upd_result = upd_ins_data(connect(),sql_query,ins_data)
     write_log_info(v_procname,' == inserted new record into trans_stat == ',load_id,transid,provider_key)
   else:
     write_log_info(v_procname,'Error t_trans_stat table has multile records for given transid ' ,str(load_id),str(transid),provider_key)
     exit()



   if field_name == 'sum_units_db_consumer':
      f_sum_units_db_consumer_new(transid,load_id,provider_key,fname)
      #write_log_info(v_procname,'==== COMPLETE ==== ' ,str(load_id),str(transid),provider_key)
   else:

      sql_query = "select src_file_type,src_file_delimiter,cons_file_type,cons_file_delimiter,is_intermediate,src_file_unzip_cmd from "+ config.metadata["control_proc_param"] +\
                        " where client_key = '"+client_key+"' and provider_key = '"+provider_key+"' and spec_id = '"+spec_id+"' and data_type = '"+dtype+"'"
      print(sql_query)
      control_params = query_all(connect(),sql_query)
      if len(control_params) == 0:
        print("t_control_process_param has no entry for this provider/spec id")
        exit()
      else:
         tgt_file_type = control_params[0][2]
         tgt_file_del = control_params[0][3]
         intermediate_file = control_params[0][4]
         cat_cmd = control_params[0][5]

         #Check if enriched directory exists
         sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = 'enriched_files_dir_v2'"

         content_enriched_file_dir = query_all(connect(),sql_query)
         if len(content_enriched_file_dir) == 0:
           print("Output path not available")
           #exit()
         else:
            content_enriched_file_dir  = content_enriched_file_dir[0][0].lower()
            print(field_name)
            print(intermediate_file)


            if field_name == 'no_recs_file' and fname != None:
              write_log_info(v_procname,' updating no_recs_file ',load_id,transid,provider_key)
              f_no_recs_file(transid,load_id,provider_key,spec_id,fname,cat_cmd)
            elif field_name == 'sum_units_db_nc' and intermediate_file == 'N':
              write_log_info(v_procname,' updating sum_units_db_nc ',load_id,transid,provider_key)
              if fname == None:
                tgt_filename = content_enriched_file_dir+''+v_client+'/'+provider_key+'/non_consumer_files/report_date='+str(report_day_part)+'/nc_'+str(load_id)+'_'+str(transid)+'_enriched.bsv.gz'
              else:
                tgt_filename = content_enriched_file_dir+''+v_client+'/'+provider_key+'/non_consumer_files/report_date='+str(report_day_part)+'/'+fname

              print(tgt_filename)

              f_sum_units_db_nc(transid,load_id,provider_key,tgt_filename,tgt_file_type,'\x07')
            else:
              write_log_info(v_procname,' ERROR! Field doesnt exists in t_trans_stat table or its intermediate file ',load_id,transid,provider_key)

   #Read the target file and update the t_trans_stat with the record count

   write_log_info(v_procname,'==== COMPLETE ==== ' ,str(load_id),str(transid),provider_key)

#--------------- FUNCTION --------------------------------------
# Name:             check_line_count
# Purpose:          check line count for a particular trans id against a threshold value
# Return:           True/False
# Author:
# Arguments:        transid
# Modified date     Comments
# 28.02.2019        Initial version
#
#
#---------------------------------------------------------------


def check_line_count(transid):
   print(" checking if record count is within the expected limit")
   trans_record=query_all(conn, "select * from "+config.metadata["trans_control"]+" where trans_id="+str(transid))

   if len(trans_record) == 0:
     write_log_info(v_procname,' ERROR! Incorrect trans id ',0,None,None)
     exit()

   provider_key = trans_record[0][2]
   load_id = trans_record[0][0]
   src_filename = trans_record[0][12]
   spec_id = trans_record[0][4]
   client_key = trans_record[0][27]
   dtype = trans_record[0][7]

   sql_query = "select threshold_line_cnt,threshold_avg_days from "+ config.metadata["control_proc_param"] +\
                     " where provider_key = '"+provider_key+"' and spec_id = '"+spec_id+"' and data_type = '"+dtype+"'"
   control_params = query_all(connect(),sql_query)
   if len(control_params) == 0:
     print("t_control_process_param has no entry for this provider/spec id")
     exit()
   threshold_line_count = control_params[0][0]
   avg_days = control_params[0][1]




if __name__ == '__main__':
   transid = sys.argv[1]
   field = sys.argv[2]
   v_procname = 'main'
   if len(transid) == 0 or len(field) == 0:
     print("No trans Id passed. I don't know how to proceed")
     exit()

   sql_query = "select * from "+ config.metadata["trans_control"] +" where trans_id = "+ str(transid)
   fileExpectation = query_all(connect(),sql_query)
   if len(fileExpectation) == 0:
     print("Trans id not found => exiting")
     write_log_info(v_procname,' Trans id not found!!Exiting ',None,transid,None)
     exit()

   provider_key = fileExpectation[0][2]
   load_id = fileExpectation[0][0]
   write_log_info(v_main_proc+'.'+v_procname,' ============= START UPDATE ======== ',str(load_id),str(transid),provider_key)
   upd_trans_stat(transid,field)
   write_log_info(v_procname,' ============= END UPDATE ======== ',str(load_id),str(transid),provider_key)
