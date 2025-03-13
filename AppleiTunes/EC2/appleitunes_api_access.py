from __future__ import print_function
#-----------------  IMPORT MODULES ----------------
from builtins import str
import sys,boto3,datetime,os,time,botocore
from env import *
retcode=init()
from utl_mysql_interface import *
from utl_configreader import *
import config
from utl_upd_process_control_v2 import *
#------------------- END IMPORT -------------------

config.metadata=read_config(section="table")
config.dbfunc=read_config(section="db_function")

def appleit_file_upload(filename,specid,bucket_name,dayid,upload_filename,licname):
    session = boto3.session.Session(profile_name="emrmgmt")
    s3client=session.client("s3")
    dir(session)
    source_file = '/home/cadlsys/datastore/PKE4/'+ licname + '/'+filename
    s3client.upload_file(source_file,bucket_name,upload_filename)

def appleit_file_dl(url):
    return os.popen(url).read()

def get_appleit_url_param(p_specid):
    if p_specid == 'P001_SALE_IT':
        #return 'APPLE_IT_URL'
        return ['APPLE_IT_URL','APPLE_IT_FOLDER']
    elif p_specid == 'P001_SALE_PO':
        #return 'APPLE_PO_URL'
        return ['APPLE_PO_URL','APPLE_PO_FOLDER']
    else:
        return 'None'

def appleit_process_dl_file(p_loadid):

    v_procname = 'appleitunes_process_dl_file'
    write_log_info(v_procname,'======= STARTED  === ',str(p_loadid),None,'PKE4')
    conn=connect()
    params=get_provider_params('P001')
    print("select data_type from "+ config.metadata["trans_control"] + " where load_id = " + str(p_loadid))
    dtype = query_all(connect(),"select data_type from "+ config.metadata["trans_control"] + " where load_id = " + str(p_loadid))[0][0]
    print("dtype--",dtype)
    bucket_name = get_ftp_setup('P001',str(dtype),'FTP_WORK')[0][12].split('/')[2]
    filestoprocess = get_load_file_list(p_loadid)
    if len(filestoprocess) != 0:
        licensor_name = get_licensor_from_client_key(filestoprocess[0][14])
    else:
        licensor_name = ''

    for onefile in filestoprocess:
        specid=onefile[4]
        trans_id=onefile[1]
        providerKey = onefile[2]
        write_log_info(v_procname,'======= Started Download process === ',str(p_loadid),str(trans_id),providerKey)
        filename = onefile[12].split('/')[10]
        url_val = get_appleit_url_param(specid)
        print("url_val")
        print(url_val)
        folder = url_val[1]
        url_param = url_val[0]
    #        url_param = get_appleit_url_param(specid)
        dateval = onefile[12].split('/')[8]
        client_key = onefile[14]
        if specid == 'P001_SALE_PO':
            vendorid = onefile[12].split('_')[3]
        elif specid == 'P001_SALE_IT':
            vendorid = onefile[12].split('_')[2]
        else:
            print("No valid vendor passed!! exiting")
            exit()
        print('url_param')
        print(url_param)
        url = params[url_param].replace('#vendornum#',vendorid)\
                               .replace('#dayid#',dateval)\
                               .replace('#properties#',params['REPORTER_'+client_key])\
                               .replace('#lic#',licensor_name)


        print (url)

        upload_filename= 'default/appleitunes/' + licensor_name + '/'+params[folder]+'/v1/' + dateval + '/' + filename.lower()
        output = appleit_file_dl(url)

        if output.find('Successfully downloaded') != -1:
            write_log_info(v_procname,' == file downloaded == ',str(p_loadid),str(trans_id),providerKey)
            upd_trans_control_flag(p_loadid,trans_id,'file_downloaded')
            appleit_file_upload(filename,specid,bucket_name,dateval,upload_filename,licensor_name)
            write_log_info(v_procname,' == file uploaded to S3 == ',str(p_loadid),str(trans_id),providerKey)
            success=upd_ins_data(conn,'update '+config.metadata["trans_control"] +' set file_name = "'+'s3://'+bucket_name+'/'+upload_filename+'" where trans_id ='+str(trans_id),filename)
            write_log_info(v_procname,' == file_name updated == ',str(p_loadid),str(trans_id),providerKey)
            upd_trans_stat(trans_id,'no_recs_file','/home/cadlsys/datastore/PKE4/sme/'+filename)
            write_log_info(v_procname,' == t_trans_stat updated == ',str(p_loadid),str(trans_id),providerKey)
            output = os.system('rm -f /home/cadlsys/datastore/PKE4/sme/'+filename)
        elif output.find('<Code>220</Code>') != -1 or output.find('<Code>213</Code>') != -1 or output.find('<Code>221</Code>') != -1:
            if specid == 'P001_SALE_PO':
                write_log_info(v_procname,' == file not exists this will be removed == ',str(p_loadid),str(trans_id),providerKey)
                insdata=trans_id
                #stmt="delete from "+config.metadata["trans_control"]+" where trans_id="+str(trans_id)
                #success=upd_ins_data(conn,stmt,insdata)
        else:
            write_log_info(v_procname,' == file not available this time... will be checked later == ',str(p_loadid),str(trans_id),providerKey)

    stmt="update "+config.metadata["trans_control"]+" set file_available=sysdate() where load_id="+str(p_loadid)
    selstmt="select count(*) from "+config.metadata["trans_control"]+" where load_id="+str(p_loadid)+" and file_downloaded is null"
    chkresult=query_all(conn,selstmt)
    if chkresult[0][0]==0:
        insdata=p_loadid
        success=upd_ins_data(conn,stmt,insdata)
        write_log_info(v_procname,' == All files available.. file_available flag updated == ',str(p_loadid),None,None)
    #output = os.system('rm -f /home/cadlsys/datastore/P001/'+filename)
    #output = os.system('rm -f /home/cadlsys/datastore/P001/*'+dateval+'/*.gz*')
    write_log_info(v_procname,' == Deleting file expectations which did not download  == ',str(p_loadid),str(trans_id),providerKey)
    insdata = p_loadid
    stmt="delete from "+config.metadata["trans_control"]+" where provider_key = 'P001' and spec_id = 'P001_SALE_PO' and delivery_time_expected between NOW() - INTERVAL 7 DAY and NOW() - INTERVAL 2 DAY and file_downloaded is null"
    success=upd_ins_data(conn,stmt,insdata)

    write_log_info(v_procname,' == Finished for complete loadid == ',str(p_loadid),None,None)


if __name__ == '__main__':

    p_loadid = sys.argv[1]
    if len(p_loadid) == 0:
        print("No Load Id passed. I don't know how to proceed")
        exit()
    appleit_process_dl_file(p_loadid)

