## IMPORTS FROM ALL SCRIPTS
from appleitunes_gras_match_v4 import *
from appleitunes_user_process_v2 import *

from create_manifest_v2 import *

from pyspark import SparkContext
from pyspark.sql import SQLContext

sc=SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
sqlContext.sql("SET spark.sql.analyzer.failAmbiguousSelfJoin=False")

## LOAD ID
load_id = sys.argv[1]
if len(load_id) == 0:
    print("No Load Id passed. I don't know how to proceed")
    exit()
param_name = 'enriched_files_dir_v2'
## MAX RETRIES FOR STREAM AND NON CONS PROCESSING
max_retry=5

sql_query = "select distinct client_key from "+ config.metadata["trans_control"] +" where load_id = "+ str(load_id)
v_client = query_all(connect(),sql_query)[0][0]
lic = get_licensor_from_client_key(v_client)

##try:
## STEP-1 USER PROC

appleitunes_user_main(load_id)

## STEP-2 GRAS and NonConsumer PROC

appleitunes_gras_main(load_id)

## STEP-3 DATALAKE
#itunes_load_dlake_main(load_id,lic)


## STEP-6 LOAD COMPLETE
stmt="update "+config.metadata["load_stat"] + " set load_complete=sysdate() where load_id="+str(load_id)
conn=connect()
success=upd_ins_data(conn,stmt,load_id)

##except Exception as e:
##    print("Error for load id "+str(load_id))
##    print("Sending email")

