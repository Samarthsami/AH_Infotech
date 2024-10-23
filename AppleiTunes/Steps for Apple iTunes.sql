Token:
cd /home/cadlsys/datastore/PKE4/sme; java -jar /home/cadlsys/script/util/apple/Reporter.jar p=Reporter.properties m=Robot.XML Sales.getReport 85741872,Sales,Detailed,Daily,20231004,1_2


INSERT INTO t_provider_file_info (provider_key, data_type, delivery_format, ftp_setup_name, frequency, host, port, user_name, pwd, ftp_mode, ftp_path, local_dest_path, local_arch_path, source_path, client_key) VALUES ('P001', 'P001_IT', 'API', 'FTP_WORK', 'D', null, null, null, null, null, null, 's3://sme-ca-dl-dev-temp/dataload/appleitunes/sales/', null, 's3://sme-ca-dl-dev-input/default/apple/sme/downloads/v1/', '2222')
INSERT INTO t_control_process_param (provider_key, spec_id, client_key, data_type, is_intermediate, src_file_type, src_file_delimiter, src_file_ziptype, src_file_unzip_cmd, cons_file_type, cons_file_delimiter, threshold_line_cnt, threshold_quantity, threshold_avg_days) VALUES ('P001', 'P001_SALE_IT', '2222', 'P001_IT', 'N', '', '', '', 'zcat #filename# | wc -l', 'CSV', '', 20, 20, 14);

INSERT into t_provider_parameter (provider_key,param_name,param_order,param_value,param_desc) 
values 
('P001','DC_APPLE_BUCKET_NAME',Null,'sme-ca-dev-partners','AppleiTunes files delivery S3 location'),
('P001','DC_APPLE_S3_PROFILE_NAME',Null,'emrmgmt','profile to access S3 bucket'),
('P001','APPLE_LOCAL_FOLDER',Null,'/home/cadlsys/datastore/P001','AppleiTunes local temp directory to download the files');

-------- Check Count (SPARK) ------------------
df = sqlContext.read.format("csv").option("header","true").option("delimiter","\t").option("samplingRatio", "0.1").load('s3://sme-ca-dl-dev-input/default/appleitunes/sme/downloads/v1/20240515/*')
df = sqlContext.read.csv("s3://sme-ca-dl-dev-input/default/appleitunes/sme/downloads/v1/20240401/*",header=False,sep="\x07")
df.count()
df.show(4,False)

df = sqlContext.read.parquet("s3://sme-ca-dl-dev-output/partner_data/sme/P001/user_enriched_files/report_day=2024-04-01/user_enriched_file_16712.parquet/",header=True,sep="\x07")
df = sqlContext.read.csv("s3://sme-ca-dl-dev-output/partner_data/sme/P001/gras_enriched_files/report_date=2024-05-15/*",header=True,sep="\x07")
df = sqlContext.read.csv("s3://sme-ca-dl-dev-output/partner_data/sme/P001/gras_enriched_files/report_date=2024-05-15/DL_US_80026921_enriched.csv.gz/*",header=True,sep="\x07")
df = sqlContext.read.csv("s3://sme-ca-dl-dev-output/partner_data/sme/P001/temp/20240105/gras_enriched_file/report_date=2024-01-05/*",header=True,sep="\x07")
df = sqlContext.read.csv("s3://sme-ca-dl-dev-output/partner_data/sme/P001/non_consumer_files/report_date=2024-01-05/*",header=True,sep="\x07")

df = sqlContext.read.parquet("s3://sme-ca-dev-datalake-core/apple/partner_data/main/v1/dim_consumers/report_licensor=sme/",header=False,sep="\x07")
df = sqlContext.read.parquet("s3://sme-ca-dev-datalake-core/apple/partner_data/main/v1/dim_dl_partner_info/",header=False,sep="\x07")
df = sqlContext.read.parquet("s3://sme-ca-dev-datalake-core/apple/partner_data/main/v1/dim_dl_partner_regions/",header=False,sep="\x07")
df = sqlContext.read.parquet("s3://sme-ca-dev-datalake-core/apple/partner_data/main/v1/fact_downloads/report_date=2024-05-15/report_licensor=sme/",header=False,sep="\x07")

df.createOrReplaceTempView('cdf')
sqlContext.sql("select sum(quantity) from cdf").show(10,False)

import pyspark.sql.functions as F
df.agg(F.sum("quantity")).collect()[0][0]
------------------------------------------------

------------- Count -------------------------
2023-12-01 > Input-88856, Gras-87698, Non-30117, Fact-87697,
2023-12-02 > I-99109, G-10347, N-6105, F-10076,
2023-12-03 > I-149635, G-208, F-165 
2023-12-04 > I-120506, F-195    Sum-144080

2024-01-01 > I-135692, G-131903, F-131903
---------------------------------------------

----- Time Taking/load_id -----
User_Enrich > less than 2 mins
Gras_Matching > around 2.5 hrs
Non_Consumer > less than 3 mins
Dimensions > less than 2 mins
Fact_files > less than 1 mins
-------------------------------

2023-12-04 > 122319  --- 122
			 184592
			 
			 
Initial Reading     - SUM(125472), COUNT(128619)
After joining all promo_code - 125472, 128619
After Joining dim_partner_regions and downloads data - 125472, 128619
After standard join - 125472, 128619
After Triming       - 122319, 128619

Load_id : 16839 (2024-04-01)
Sum('quantity')
		 Dev       Prod
gras  - 131042    131032
Non   - 131042    131032
count - 61981	  61971

~ 10 difference  from Prod
---------------------------------------------------
If we do Changes in temp1:

Sum  - 500 difference
avg(round) - 10 difference (current code)
round - 10 difference
Sum(round) - 500 difference
avg -  10 difference
----------------------------------------------------
For report_day: 2024-04-04
Sum (Prod) - 133745, US-79777
Sum (Dev)  - 133765 (~increased by 20), US-79777

For report_day: 2024-04-05
Sum (Prod) - 149109, US-85983
Sum (Dev)  - 149118 (~increased by 9), US-85983

--------------------------------------------------
tmp2.createOrReplaceTempView('cdf')
tmp2_sum = sqlContext.sql("select sum(quantity) as quantity from cdf where country_key='US'")

sum_quantity = tmp2_sum.select('quantity').collect()[0][0]
write_log_info(v_procname,'For tmp2 '+str(sum_quantity),str(loadid),str(tid),provider_key)
--------------------------------------------------


1.File loaded loaded_recs count 79842/145971 (initial sum/count)
2.Special rules file read       79842/145971
3.generic gras rules completed  79843/145972
4.with_various_key_Df loaded    79843/145972  
5.with_scat_key_Df loaded       79843/145972
6.df_with_ptypes loaded         79843/145972
7.df_with_stgrp_key loaded      79843/145972

8.test_df.filter((col("Parent_Type_Id") == 1) & (col("Parent_Identifier").isNotNull()))
  test_df  88
  
9. tmp1 88
10.tmp2 79755
-------------------------------------------------------

For 10-05-2024:
Dev:  Gras - 138739, Non_con - 138739, Dlake - 138739 (~37 difference)
Prod: Gras - 138702, Non_con - 138702, Dlake - 133978

For 11-05-2024:
Dev:  Gras - 139714, Non_con - 139714, Dlake - 139714 (~8 difference)
Prod: Gras - 139706, Non_con - 139706, Dlake - 136075

For 12-05-2024:
Dev:  Gras - 125115, Non_con - 125115, Dlake - 125115 (~19 difference)
Prod: Gras - 125096, Non_con - 125096, Dlake - 122028

For 13-05-2024:
Dev:  Gras - 113684, Non_con - 113684, Dlake - 113684 (~1 difference)
Prod: Gras - 113683, Non_con - 113683, Dlake - 110538

For 14-05-2024:
Dev:  Gras - 108627, Non_con - 108627, Dlake - 108627 (~3 difference)
Prod: Gras - 108623, Non_con - 108623, Dlake - 105981

For 15-05-2024:
Dev:  Gras - 114433,count(117177), Non_con - 114433, Dlake - 114433, 111682 (New count)(~188 difference)
Prod: Gras - 114429,count(117357), Non_con - 114429, Dlake - 111494 
                  
---------------------------------------------------------
Dlake part: 10-05-2024

Initial Reading: 									  sum-138739, 143199
Reading all dim_promo_info data:                      sum-138739, 143199
After joining all promo_code:                         sum-138739, 143199
After Joining dim_partner_regions and downloads data: sum-138739, 143199
After standard join:                                  sum-138739, 143199
After Triming:                                        sum-134276, 143199 (~ Sum Mismatching in this step)
Fact output:                                          sum-134276, 143199



+-------+--------+----------+----------+------------+--------+----------------+-------------+------------+------------------+----------+-----------+------------+-----------+----------------+--------+----+-------------+-------------+-------------+--------------+---------------------+-------------+--------------+----------------+----------------+--------------+----------------+--------------------+------------------------+----------+---------+-----------+----------+--------+------------------+----------------+-----------------+------------------+--------------+---------+--------------------+---------------+-----------------+-----------------+-------------+-------------------------------+--------+-----------------+-------------+---------------+----------------------------------------------------------------+----------+-----------------+------------+---------------+-----+-----+----------------+--------------+
|load_id|trans_id|vendor_key|client_key|provider_key|provider|provider_country|upc          |isrc        |product_identifier|report_day|sale_return|currency_key|country_key|royalty_currency|preorder|isan|asset_content|grid_no      |parent_id    |parent_type_id|attributable_purchase|primary_genre|prod_no_dig   |product_type_key|distribution_key|sales_type_key|service_type_key|transaction_type_key|distribution_channel_key|partner_id|label_key|product_key|artist_key|spnl_key|sales_division_key|product_conf_key|product_types_key|sales_category_key|state_province|city     |transaction_date_key|consumer_key   |vendor_identifier|artist           |title        |label                          |quantity|quantity_returned|download_date|order_id       |customer_id                                                     |apple_id  |vendor_offer_code|retail_value|wholesale_value|rpu  |wpu  |partner_info_key|promo_info_key|
+-------+--------+----------+----------+------------+--------+----------------+-------------+------------+------------------+----------+-----------+------------+-----------+----------------+--------+----+-------------+-------------+-------------+--------------+---------------------+-------------+--------------+----------------+----------------+--------------+----------------+--------------------+------------------------+----------+---------+-----------+----------+--------+------------------+----------------+-----------------+------------------+--------------+---------+--------------------+---------------+-----------------+-----------------+-------------+-------------------------------+--------+-----------------+-------------+---------------+----------------------------------------------------------------+----------+-----------------+------------+---------------+-----+-----+----------------+--------------+
|16947  |572921  |?         |2222      |P001        |APPLE   |US              |196871853729 |null        |I                 |2024-05-15|S          |GBP         |GB         |GBP             |null    |null|null         |null         |null         |null          |null                 |Country      |null          |20              |05              |SA            |10              |20                  |100                     |10094     |-1       |-1         |-1        |-1      |null              |-1              |1                |DLA               |England       |Cambridge|20240515            |100000604357338|196871853729     |Beyoncé          |COWBOY CARTER|Parkwood Entertainment/Columbia|1.0     |0.0              |05/14/2024   |253000056304736|84d5b84ce862f127e2d303ea992db30480a20560ba5867402e7d4464030e6fa7|1738363766|196871853729     |16.99       |8.96           |16.99|8.96 |null            |154           |
|16947  |572921  |?         |2222      |P001        |APPLE   |US              |4547366668513|JPU902400069|H                 |2024-05-15|S          |JPY         |JP         |JPY             |null    |null|null         |4547366668513|4547366668513|17            |null                 |J-Pop        |EJXX00167B00Z |11              |05              |SA            |10              |20                  |100                     |10094     |2611     |5603857    |1199275   |1       |5                 |38              |7                |DLT               |Tokyo         |Tokyo    |20240515            |100000604338532|JPU902400069     |NiziU            |Memories     |Sony Music Labels Inc.         |1.0     |0.0              |05/14/2024   |257000049576471|3993c2e8b285c250985172e86fc12f2fa1caf5b0d6221cdc9d26b934527c5ca7|1727093423|EJXX00167B00Z    |255.0       |148.0          |255.0|148.0|5156864         |154           |
|16947  |572921  |?         |2222      |P001        |APPLE   |US              |074643254021 |USSM17301126|H                 |2024-05-15|S          |USD         |US         |USD             |null    |null|null         |074643254021 |074643254021 |17            |null                 |Soft Rock    |G010000604433G|11              |05              |SA            |10              |20                  |100                     |10094     |69       |1180170    |145608    |2       |5                 |38              |7                |DLT               |Illinois      |Chicago  |20240515            |100000604304234|USSM17301126     |Loggins & Messina|My Music     |Columbia                       |1.0     |0.0              |05/15/2024   |63070411100287 |c6ea2d0461bfd18af2ea4eba6b02dd751ae7a66510244d6c76777f3bc35eec4d|158407876 |G0100017009779   |1.29        |0.91           |1.29 |0.91 |5161464         |154           |
+-------+--------+----------+----------+------------+--------+----------------+-------------+------------+------------------+----------+-----------+------------+-----------+----------------+--------+----+-------------+-------------+-------------+--------------+---------------------+-------------+--------------+----------------+----------------+--------------+----------------+--------------------+------------------------+----------+---------+-----------+----------+--------+------------------+----------------+-----------------+------------------+--------------+---------+--------------------+---------------+-----------------+-----------------+-------------+-------------------------------+--------+-----------------+-------------+---------------+----------------------------------------------------------------+----------+-----------------+------------+---------------+-----+-----+----------------+--------------+
only showing top 3 rows


+------------------+------------+--------+--------------------------------+--------------+-------+
|partner_region_key|country_code|zip_code|etl_hash                        |state_province|city   |
+------------------+------------+--------+--------------------------------+--------------+-------+
|856               |CO          |5       |c686839f11c9236cefbb5aa126f4a77f|UNKNOWN       |UNKNOWN|
|952               |CO          |2006    |c886ef2c48f423fdd059a44bedf1ac78|UNKNOWN       |UNKNOWN|
|8093              |CO          |52      |dd378d2541b52e52d7e70c53cc337fcd|UNKNOWN       |UNKNOWN|
|10179             |CO          |00000   |06480b24b7e2c75fa944b7b9af86ef6b|UNKNOWN       |UNKNOWN|
+------------------+------------+--------+--------------------------------+--------------+-------+
only showing top 4 rows
