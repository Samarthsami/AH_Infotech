from __future__ import print_function
#-----------------  IMPORT MODULES ----------------
from builtins import str
from mysql.connector import MySQLConnection, Error
from utl_configreader import read_config
import config
from pyspark import SparkContext
from pyspark.sql import SQLContext
#------------------- END IMPORT -------------------

config.metadata=read_config(section="table")
config.db=read_config(section="mysql")
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

#--------------- FUNCTION --------------------------------------
# Name:             connect
# Purpose:          Establishes connection with mysql DB
# Return:           string
# Author:
# Arguments:        None
# Modified date     Comments
# 07.08.2018        Initial version
#
#
#---------------------------------------------------------------

def connect(section="mysql"):
    """ Connect to MySQL database """

    db_config = read_config(section=section)

    try:
        print('Connecting to MySQL database...')
        conn = MySQLConnection(**db_config)

        if conn.is_connected():
            print('connection established.')
        else:
            print('connection failed.')

    except Error as error:
        print(error)

    finally:
        return conn


#--------------- FUNCTION --------------------------------------
# Name:             query_all
# Purpose:          Runs the query passed
# Return:           result rows of the query
# Author:
# Arguments:        output from connect function and query
# Modified date     Comments
# 07.08.2018        Initial version
#
#
#---------------------------------------------------------------

def query_all(open_con,query):
    rows={}
    try:
        cursor = open_con.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

    except Error as e:
        print(e)

    finally:
        cursor.close()
        return rows

#--------------- FUNCTION --------------------------------------
# Name:             upd_ins_data
# Purpose:          Insert new rows into the table(query passed as argument)
# Return:           result 0 for failure, 1 for success
# Author:
# Arguments:        output from connect function, insert query and rows to be inserted
# Modified date     Comments
# 07.08.2018        Initial version
#
#
#---------------------------------------------------------------


def upd_ins_data(open_con,stmt,data):
    success=0
    #vals=(**data)
    try:
        cursor = open_con.cursor()
        cursor.execute(stmt,data)
        open_con.commit()
        success=1
    except Error as error:
        print(error)
        success=0
    finally:
        cursor.close()
        return success

#--------------- FUNCTION --------------------------------------
# Name:             get_provider_params
# Purpose:          Fetches various parameters for a given provider
# Return:           param_name and param_value
# Author:
# Arguments:        provider_key
# Modified date     Comments
# 07.08.2018        Initial version
#
#
#---------------------------------------------------------------

def get_provider_params(provider_key):
    conn=connect()
    v_id_query="select param_name,param_value from  "+ config.metadata["provider_parameter"]+" where provider_key='"+provider_key+"'"
    resultset=query_all(conn,v_id_query)
    params = {}
    for row in resultset:
        params.update({row[0]:row[1]})
    return params

#--------------- FUNCTION --------------------------------------
# Name:             get_load_file_list
# Purpose:          Fetches the filename and other information for a particular load id
# Return:           result rows for a particular load id
# Author:
# Arguments:        load id
# Modified date     Comments
# 07.08.2018        Initial version
#
#
#---------------------------------------------------------------

def get_load_file_list(load_id):
    filelist = {}
    conn=connect()
    v_id_query="select load_id,trans_id,provider_key,country_key,spec_id,header_spec_id,trailer_spec_id,data_type,process_name,ftp_setup,market_share_spec_id,delivery_time_expected,file_name,priority,client_key from "+config.metadata["trans_control"]+" where file_downloaded is null and load_id="+str(load_id)
    resultset=query_all(conn,v_id_query)
    filelist=[list(i) for i in resultset]
    return filelist


#--------------- FUNCTION --------------------------------------
# Name:             get_data_to_load_list
# Purpose:          Fetches the files which needs to be loaded/available
# Return:           result rows from t_trans_control
# Author:
# Arguments:        None
# Modified date     Comments
# 07.08.2018        Initial version
#
#
#---------------------------------------------------------------

def get_data_to_load_list():
    loadlist = {}
    conn=connect()
    v_id_query="select distinct provider_key,client_key, load_id, priority from "+config.metadata["trans_control"]+" where file_available is null and delivery_time_expected<sysdate()"
    resultset=query_all(conn,v_id_query)
    loadlist = [list(i) for i in resultset]
    return loadlist

#--------------- FUNCTION --------------------------------------
# Name:             get_ftp_setup
# Purpose:          Fetches the ftp setup required to upload/download file
# Return:           ftpsetup string
# Author:
# Arguments:        provider_key,datatype and ftp_Setup_name
# Modified date     Comments
# 07.08.2018        Initial version
#
#
#---------------------------------------------------------------

def get_ftp_setup(provider_key,data_type,ftp_setup_name):
    ftpsetup={}
    conn=connect()
    v_id_query="select provider_key,data_type,delivery_format,ftp_setup_name,host,port,user_name,pwd,ftp_mode,ftp_path,local_dest_path,local_arch_path,source_path from "+config.metadata["provider_file_info"]+" where provider_key='"+provider_key+"' and data_type='"+data_type+"' and ftp_setup_name='"+ftp_setup_name+"'"
    resultset=query_all(conn,v_id_query)
    ftpsetup = [list(i) for i in resultset]
    return ftpsetup



#--------------- FUNCTION --------------------------------------
# Name:             write_log_info
# Purpose:          write log info into DB table master_data.t_process_log
# Return:           -1 for ERROR, 1 for Success
# Author:           Brijesh Gaur
# Arguments:        process name,log message,load id,trans id,provider key,log user
# Modified date     Comments
# 09.08.2018        Initial version
#
#
#---------------------------------------------------------------


def write_log_info(p_proc_name,p_log_message,p_load_id=None,p_trans_id=None,p_provider_key=None,p_log_user=None):

 try:
    if p_load_id is None:
        p_load_id = 0
    if p_trans_id is None:
        p_trans_id = 0
    if p_provider_key is None:
        p_provider_key = '0000'
    if p_log_user is None:
        p_log_user = 'system'

    db_conn=connect()

    #ins_query = ("insert into "+config.metadata["log"] +"(load_id,trans_id,provider_key,proc_name,log_message,log_date,log_user) values(%s,%s,%s,%s,%s,sysdate(),%s)")
    ins_query = ("insert into "+config.metadata["log"] +"(load_id,trans_id,provider_key,proc_name,log_message,log_date,log_user) values(%s,%s,%s,%s,%s,now(3),%s)")
    ins_data = str(p_load_id)+","+str(p_trans_id)+","+p_provider_key+","+p_proc_name+","+p_log_message+","+p_log_user
    ins_data=ins_data.split(',')
    #print ins_query
    #print ins_data
    output=upd_ins_data(db_conn,ins_query,ins_data)
    return output

 except Error:
        print("Error during insert")
        return -1


#--------------- FUNCTION --------------------------------------
# Name:             get_df_from_db_table
# Purpose:          Convert RDB Table to a Spark Dataframe Object
# Return:           -1 for ERROR, Dataframe Object
# Author:           Anand Vasudevan
# Arguments:        Name of RDB Table
# Modified date     Comments
# 11.09.2018        Initial version
#
#
#---------------------------------------------------------------


def get_df_from_db_table(p_table_name):

  try:
    table_df_cmd = "sqlContext.read.format(\"jdbc\").options(url=\"jdbc:mysql://"+config.db['host']+":"+config.db['port']+"/"+config.db['database']+"?zeroDateTimeBehavior=CONVERT_TO_NULL\"\
    ,driver=\"com.mysql.cj.jdbc.Driver\",dbtable=\""+config.metadata[p_table_name]+"\",user=\""+config.db['user']+"\",password=\""+config.db['password']+"\",useSSL='false').load()"

    table_df = eval(table_df_cmd)
    return table_df

  except Error:
    print("Error during Table to DataFrame conversion")
    return -1


def get_df_from_sql(p_query):

  try:
    table_df_cmd = "sqlContext.read.format(\"jdbc\").options(url=\"jdbc:mysql://"+config.db['host']+":"+config.db['port']+"/"+config.db['database']+"?zeroDateTimeBehavior=CONVERT_TO_NULL\"\
    ,driver=\"com.mysql.cj.jdbc.Driver\",query=\""+p_query+"\",user=\""+config.db['user']+"\",password=\""+config.db['password']+"\",useSSL='false').load()"

    table_df = eval(table_df_cmd)
    return table_df

  except Error:
    print("Error during Table to DataFrame conversion")
    return -1


#--------------- FUNCTION --------------------------------------
# Name:             get_flag_value
# Purpose:          This function return the value of the flag in job_parameter table
# Return:           Return 1 if something running else return 0, returns -1 for ERROR
# Author:           Brijesh Gaur
# Arguments:        flag name checked against parameter table master_data.t_job_parameter
# Modified date     Comments
# 06.10.2018        Initial version
#
#
#---------------------------------------------------------------


def get_flag_value(p_name):
    try:
        sql_query = "select parameter_value from "+ config.metadata["job_parameter"] +" where parameter_name = '" + p_name + "'"
        is_running = int(query_all(connect(),sql_query)[0][0])
        return is_running
    except Error:
        print("Error during sql read")
        return -1


#--------------- FUNCTION --------------------------------------
# Name:             upd_flag_value
# Purpose:          This function updates the flag value in parameter table
# Return:           Return 1 for success, -1 for ERROR
# Author:           Brijesh Gaur
# Arguments:        new value to be updated, flag name, it updates flag to 0 or 1 in table  master_data.t_job_parameter
# Modified date     Comments
# 06.10.2018        Initial version
#
#
#---------------------------------------------------------------


def upd_flag_value(p_value,p_name):
    try:
        sql_query = ("update "+config.metadata["job_parameter"] + " set parameter_value = "+str(p_value)+" where parameter_name = '" + p_name +"'")
        upd_result = upd_ins_data(connect(),sql_query,p_value)
        return upd_result
    except Error:
        print("Error during sql read")
        return -1


#--------------- FUNCTION --------------------------------------
# Name:             increment_flag_value
# Purpose:          This function increment the flag value by 1
# Return:           Return 1 for success, -1 for ERROR
# Author:           Brijesh Gaur
# Arguments:        flag name, it updates flag to flag_value + 1in master_data.t_job_parameter
# Modified date     Comments
# 06.10.2018        Initial version
#
#
#---------------------------------------------------------------


def increment_flag_value(p_name):
    try:
        curr_val = get_flag_value(p_name)
        sql_query = ("update "+config.metadata["job_parameter"] + " set parameter_value = "+str(curr_val+1)+" where parameter_name = '" + p_name +"'")
        upd_result = upd_ins_data(connect(),sql_query,curr_val+1)
        return upd_result
    except Error:
        print("Error during sql read")
        return -1


#--------------- FUNCTION --------------------------------------
# Name:             decrement_flag_value
# Purpose:          This function decrement the flag value by 1
# Return:           Return 1 for success, -1 for ERROR
# Author:           Brijesh Gaur
# Arguments:        flag name, it updates flag to flag_value + 1in master_data.t_job_parameter
# Modified date     Comments
# 06.10.2018        Initial version
#
#
#---------------------------------------------------------------


def decrement_flag_value(p_name):
    try:
        curr_val = get_flag_value(p_name)
        if curr_val > 0:
          sql_query = ("update "+config.metadata["job_parameter"] + " set parameter_value = "+str(curr_val-1)+" where parameter_name = '" + p_name +"'")
          upd_result = upd_ins_data(connect(),sql_query,curr_val-1)
          return upd_result
        else:
          print("flag value is less than 0")
    except Error:
        print("Error during sql read")
        return -1


#--------------- FUNCTION --------------------------------------
# Name:             upd_trans_control_flag
# Purpose:          This function updates the passed column to sysdate
# Return:           Return 1 for success, -1 for ERROR
# Author:           Brijesh Gaur
# Arguments:        loadid,transid and column name to update
# Modified date     Comments
# 24.10.2018        Initial version
#
#
#---------------------------------------------------------------


def upd_trans_control_flag(p_load_id,p_trans_id,p_column_name):
    try:
        sql_query = ("update "+config.metadata["trans_control"] + " set "+p_column_name+" = sysdate()  where load_id = " + str(p_load_id) +" and trans_id = "+str(p_trans_id))
        print(sql_query)
        p_value = 'xx'
        upd_result = upd_ins_data(connect(),sql_query,p_value)
        return upd_result
    except Error:
        print("Error during sql update")
        return -1
#--------------- FUNCTION --------------------------------------
# Name:             write_chart_gras_log
# Purpose:          This function update the gras log info in mysql
# Return:           Return 1 for success, -1 for ERROR
# Author:           Brijesh Gaur
# Arguments:        loadid,transid and column name to update
# Modified date     Comments
# 22.03.2024        Initial version
#
#
#---------------------------------------------------------------
def write_chart_gras_log(chartKey,countryCode,reportDate,emrId,emrName,startTime=None,endTime=None):
    try:
        db_conn = connect()
        sql_query = """select count(*) from chart_meta.t_charts_gras_log """ + \
                        """where chart_key = """ + str(chartKey) + \
                        """ and country_code =  '""" + countryCode + """'""" + \
                        """ and report_date = '""" + str(reportDate) + """'""" + \
                        """ and emr_name = '""" + emrName + """'"""

        res = query_all(db_conn,sql_query)[0][0]
        if res == 0:
            ins_query = ("insert into chart_meta.t_charts_gras_log(chart_key,country_code,report_date,emr_id,emr_name) values(%s,%s,%s,%s,%s)")
            ins_data = str(chartKey) + "," + countryCode + "," + str(reportDate) + "," + emrId + "," + emrName
            ins_data = ins_data.split(',')
            upd_ins_data(db_conn, ins_query, ins_data)
            print(ins_query)
        else:
            if startTime != None:
                upd_query = "update chart_meta.t_charts_gras_log set load_started = '" + str(startTime) + "'" + \
                              " where chart_key = " + str(chartKey) + \
                            " and country_code = '" + countryCode + "'" \
                            " and report_date = '" + str(reportDate) + "'" \
                            " and emr_name = '" + emrName + "'"
                print(upd_query)
                upd_ins_data(db_conn, upd_query, 'x')

            if endTime != None:
                upd_query = "update chart_meta.t_charts_gras_log set load_complete = '" + str(endTime) + "'" + \
                            " where chart_key = " + str(chartKey) + \
                            " and country_code = '" + countryCode + "'" \
                            " and report_date = '" + str(reportDate) + "'" \
                            " and emr_name = '" + emrName + "'"
                print(upd_query)
                upd_ins_data(db_conn, upd_query, 'x')

    except Error:
        print("Error during insert")
        return -1

#--------------- FUNCTION --------------------------------------
# Name:             upd_gras_log
# Purpose:          This function update the gras log info in mysql
# Return:           Return 1 for success, -1 for ERROR
# Author:           Brijesh Gaur
# Arguments:        loadid,transid and column name to update
# Modified date     Comments
# 22.03.2024        Initial version
#
#
#---------------------------------------------------------------

def upd_gras_log(chartKey,emrName,startTime=None,endTime=None):
    try:
        db_conn = connect()
        if startTime != None:
            upd_query = "update chart_meta.t_charts_gras_log set load_started = '" + str(startTime) + "'" + \
                          " where chart_key = " + str(chartKey) + \
                        " and emr_name = '" + emrName + "'"
            print(upd_query)
            upd_ins_data(db_conn, upd_query, 'x')

        if endTime != None:
            upd_query = "update chart_meta.t_charts_gras_log set load_complete = '" + str(endTime) + "'" + \
                        " where chart_key = " + str(chartKey) + \
                        " and emr_name = '" + emrName + "'"
            print(upd_query)
            upd_ins_data(db_conn, upd_query, 'x')

    except Error:
        print("Error during insert")
        return -1

if __name__ == '__main__':
    params = {}
    newvar=connect()
    if newvar.is_connected():
        print('connection still open')
        resultset=query_all(newvar,"select param_name,param_value from master_data.t_provider_parameter where provider_key='PEU1'")
        for row in resultset:
              params.update({row[0]:row[1]})
        print(params['SPOTIFY_GRANT_TYPE'])
        newvar.close()
