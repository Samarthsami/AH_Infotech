from __future__ import print_function
#-----------------  IMPORT MODULES ----------------
from builtins import str
import sys,datetime,time,boto3
from env import init
retcode=init()

from utl_mysql_interface import *
import config
from pyspark.sql.functions import *
from pyspark.sql.window import *
from utl_functions import *
#------------------- END IMPORT -------------------

config.metadata=read_config(section="table")
v_procname = 'write_nc_manifest_file'
#--------------- FUNCTION --------------------------------------
# Name:             write_nc_manifest_file
# Purpose:          genrate the manifest file and save it to S3
# Return:           return path to the manifest file
# Author:           Brijesh Gaur
# Arguments:        load_id
# Modified date     Comments
# 10.06.2020        Non Consumer for downlaods
#
#
#---------------------------------------------------------------

def write_nc_manifest_file(p_load_id,p_folder,p_param_name,p_client="2222"):
    try:
      """
      if p_load_id == None:
          load_id = 0
      else:
          load_id = p_load_id

      if p_param_name is None:
        param_name = 'enriched_files_dir'
      else:
        param_name = p_param_name

      if p_folder is None:
        write_log_info(v_procname,'No folder name specified',str(load_id))
        exit()
      else:
        source_folder_name = p_folder
      """
      load_id = p_load_id
      param_name = p_param_name

      #Added this clause so that loads like amazon can create the files by considering the enriched files within each load folder.
      if p_folder.find('/') == -1:
        source_folder_name = p_folder
        subfolder_name = ''
      else:
        source_folder_name = p_folder.split('/')[0]
        subfolder_name = p_folder.replace(p_folder.split('/')[0],'')

      v_client=get_licensor_from_client_key(p_client)

      write_log_info(v_procname,' ==== START ==== ',str(load_id))

      write_log_info(v_procname,'enriched file directory name is '+param_name,str(load_id))

      write_log_info(v_procname,'Folder name passed is '+source_folder_name,str(load_id))

      sql_query = "select parameter_value from "+config.metadata["job_parameter"] +" where upper(parameter_name)= upper('"+ param_name +"')"
      print(sql_query)

      s3_info = query_all(connect(),sql_query)[0][0].split('/',3)
      print(s3_info)
      s3_bucket = s3_info[2]  #'aws-glue-temporary-594705885108-us-east-1'
      s3_folder = s3_info[3][0:len(s3_info[3])-1]  #'dataload'

      s3 = boto3.resource('s3')
      bucket = s3.Bucket(name=s3_bucket)

      manifest_file_str = '{ "entries": ['

      #load_id = 139

      sql_query = "select * from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + ""

      write_log_info('spt_processing.py','sql query constructed')

      fileExpectation = list(query_all(connect(),sql_query)[0])
      if len(fileExpectation) == 0:
          print("Nothing to load")
          write_log_info(v_procname,'Loadid didnt return anything ',str(load_id))
          exit()

      write_log_info(v_procname,' fileExpectation not empty ',str(load_id))

      userFile = fileExpectation[12]

      provider = fileExpectation[2]
      #if provider in ('PCO3','Q174','PNE5','PMN9','PRU0') and int(load_id)<=26087:
      #    day_folder = "report_date="+userFile.split("/")[7][0:4]+"-"+userFile.split("/")[7][4:6]+"-"+userFile.split("/")[7][6:]
      #else:
      day_folder = "report_date="+userFile.split("/")[8][0:4]+"-"+userFile.split("/")[8][4:6]+"-"+userFile.split("/")[8][6:]
      provider = fileExpectation[2]



      provider_data_type=userFile.split("/")[6]


      s3_folder_manifest = s3_folder + '/' + v_client+"/" + provider + '/' + source_folder_name+"/"+ day_folder+"/" + provider_data_type+"/"
      s3_folder = s3_folder + '/'+v_client+"/"+ provider + '/' + source_folder_name+ '/' + day_folder +'/' +provider_data_type + '/' +subfolder_name
      for obj in bucket.objects.filter(Prefix=s3_folder):
          filename = '{0}/{1}'.format(bucket.name, obj.key)
          #print filename
          if filename.find('_SUCCESS') == -1 and filename.find('manifest') == -1:
              manifest_file_str = manifest_file_str + '{"url":"s3://' + filename + '"},'
     # print('filename')
      #print(filename)
      manifest_file_str = manifest_file_str[0:len(manifest_file_str)-1]
      manifest_file_str = manifest_file_str + ']}'
      #print manifest_file_str
      print("manifest")
      print(s3_folder_manifest)
      print("s3_folder")
      print(s3_folder)

      manifest_file_name = s3_folder_manifest + provider + "_"  + source_folder_name + '_' + day_folder   +'.manifest'

      object = s3.Object(s3_bucket, manifest_file_name)
      object.put(Body=manifest_file_str)
      return manifest_file_name

    except Error:
      print("Error")
      return -1


if __name__ == '__main__':
    if sys.argv[1] == None:
        load_id = 0
    else:
        load_id = sys.argv[1]

    if sys.argv[2] is None:
      source_folder_name = 'tmp_non_consumer_files'
    else:
      source_folder_name = sys.argv[2]

    #if sys.argv[3] is None:
    #  write_log_info(v_procname,'No folder name specified',str(load_id))
    #  exit()
    #else:

    sql_query = "select distinct client_key from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id) + ""
    v_client = query_all(connect(),sql_query)[0][0]
    param_name = 'enriched_files_dir_v2'

    print(write_nc_manifest_file(load_id,source_folder_name,param_name,v_client))
