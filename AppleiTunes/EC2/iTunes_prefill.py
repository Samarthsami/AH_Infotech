from __future__ import print_function
import env
import sys
import config
#import utl_configreader
abc=env.init()
#config.metadata=utl_configreader.read_config(section="table")
from utl_configreader import *

from time import gmtime, strftime
import utl_prep_tables
import utl_mysql_interface

if len(sys.argv[1]) != 8:
    print("Invalid date..exitting..")
    exit()
datekey=sys.argv[1]

my_conn=utl_mysql_interface.connect()

expected_qry=utl_mysql_interface.query_all(my_conn,"select distinct provider_key,client_key,data_type,is_active from "+config.metadata["provider_delivery"]+" where is_active=1 and frequency='D' and provider_key = 'P001' and client_key = '2222' and spec_id='P001_SALE_IT' order by provider_priority")
act_day=strftime('%Y%m%d',gmtime())
act_day=datekey
print(act_day)

for one_type in expected_qry:
    utl_prep_tables.prep_t_trans_control(p_input_date=datekey,p_data_type=one_type[2],p_provider_key=one_type[0],p_client_key=one_type[1])
