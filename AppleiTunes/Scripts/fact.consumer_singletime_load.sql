--------------------------------------------------- INSTRUCTIONS BEGIN ---------------------------------
-- Run this after Datacollector load is completed
-- This will load semantic tables for AppleiTunes Downloads
-- In prod-main DB
-- Step 1 of 1
--------------------------------------------------- INSTRUCTIONS END -----------------------------------
insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_dl','loading started');

insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_dl','cleanup raw tables'); 
BEGIN;



update datacollector_db.itunes_downloads
set updated_at=sysdate
	,partner_id=decode(length(partner_id),0,10094,nvl(partner_id,10094))
	,provider_key=decode(length(provider_key),0,'P001',nvl(trim(upper(provider_key)),'P001'))
	,vendor_key=decode(length(vendor_key),0,'?',nvl(trim(upper(vendor_key)),'?'))
	,distribution_channel_key=decode(length(distribution_channel_key),0,'-1',nvl(distribution_channel_key,'-1'))
    ,distribution_key=decode(length(distribution_key),0,'-1',nvl(distribution_key,'-1'))
    ,service_type_key=decode(length(service_type_key),0,'-1',nvl(service_type_key,'-1'))
	,sales_division_key=decode(length(sales_division_key),0,'-1',nvl(sales_division_key,'-1'))
    ,transaction_type_key=decode(length(transaction_type_key),0,'-1',nvl(transaction_type_key,'-1'))
	,country_key=decode(length(trim(country_key)),0,'-1',decode(country_key,'ZZ','-1',nvl(country_key,'-1')))
	,zip_code=decode(length(zip_code),0,'-1',lower(zip_code))
	,promo_code=decode(length(promo_code),0,'UNK',decode(promo_code,'','UNK',nvl(promo_code,'UNK')))
	,cma=decode(length(cma),0,'UNK',decode(cma,'','UNK',nvl(cma,'UNK')))
	,spnl_key=decode(length(spnl_key),0,'-1',nvl(spnl_key,'-1'))
	,customer_currency=decode(length(customer_currency),0,'USD',nvl(customer_currency,'USD'))
	,royalty_currency=decode(length(royalty_currency),0,'USD',nvl(royalty_currency,'USD'))
	,state_province=decode(length(state_province),0,'-1',initcap(trim(state_province)))
	,city_name=decode(length(city_name),0,'-1',initcap(trim(city_name)))
	,rs_updated=1
where rs_updated=0
;
COMMIT;
analyze datacollector_db.itunes_downloads;

-- ca.dim_itunes_partner_info
insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_dl','1:load dim_itunes_partner_info');
BEGIN;
insert into ca.dim_itunes_partner_info
(apple_id,upc,isrc,artist_name,track_title,label_name,product_identifier,isan,asset_content,grid,primary_genre,prod_no_dig)
select 
raw_apple_id 
,max(upc) as upc 
,max(isrc) as isrc  
,max(artist_name) as artist_name 
,max(track_title)::varchar(450) as track_title 
,max(label_name) as label_name  
,max(product_identifier) as product_identifier  
,max(isan) as isan  
,max(asset_content) as asset_content  
,max(grid) as grid  
,max(primary_genre) as primary_genre  
,max(prod_no_dig) as prod_no_dig 
from etl_stage.vw_itunes_load_dc
where raw_apple_id not in (select apple_id from ca.dim_itunes_partner_info group by 1)
group by 1
;
COMMIT;
analyze ca.dim_itunes_partner_info;

-- ca.dim_itunes_promo_info
insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_dl','1:load dim_itunes_promo_info');
BEGIN;
insert into ca.dim_itunes_promo_info 
(promo_code,cma_code,etl_hash)
select A.promo_code,A.cma_code,A.hash_promo_key 
from etl_stage.vw_itunes_load_dc  A
where A.hash_promo_key not in (select etl_hash from ca.dim_itunes_promo_info group by 1)
group by 1,2,3
;
COMMIT;
analyze ca.dim_itunes_promo_info;

-- ca.dim_itunes_partner_regions
insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_dl','1:load dim_itunes_partner_regions');
BEGIN;
insert into ca.dim_itunes_partner_regions
(country_code,zip_code,etl_hash,state_province,city_name)
select A.country_code,A.zip_code,A.hash_partner_region_key,A.state_province,A.city_name
from  etl_stage.vw_itunes_load_dc A
where A.hash_partner_region_key not in (select etl_hash from ca.dim_itunes_partner_regions group by 1)
group by 1,2,3,4,5
;
COMMIT;
analyze ca.dim_itunes_partner_regions;

------------------ TEMP Fix ------------------
-- Once we have real fix
-- 1. remove this code
-- 2. drop column rs_consumer_key from consolidated_db.t_f_dl_apple_itunes_d_s
-- 
insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_dl','1:load dc_dim_apple_consumers');

BEGIN;
-- 1. create new consumers into mapping table
insert into etl_stage.dc_dim_apple_consumers 
(consumer_id)
select consumer_id
from datacollector_db.itunes_downloads A
group by 1
minus
(select A.consumer_id
from ca.dim_amusic_consumers A
group by 1 
union all
select A.consumer_id
from etl_stage.dc_dim_apple_consumers  A
group by 1
)
;
analyze etl_stage.dc_dim_apple_consumers;
COMMIT;

BEGIN;
-- 2. load into temp table
truncate table etl_stage.stg_dc_dim_apple_consumers_temp;
insert into etl_stage.stg_dc_dim_apple_consumers_temp
(consumer_id,consumer_key)
select A.consumer_id,joined.consumer_key as consumer_key
	from datacollector_db.itunes_downloads A,
	(
		select consumer_id, consumer_key
		from ca.dim_amusic_consumers AA 
		group by 1,2
		union
		select consumer_id, consumer_key
		from etl_stage.dc_dim_apple_consumers BB 
		group by 1,2
	) joined
where joined.consumer_id=A.consumer_id
and A.rs_consumer_key is null
and A.consumer_id is not null
group by 1,2
;

-- 3. Now update the rs_consumer_key
update datacollector_db.itunes_downloads
set rs_consumer_key=DD.consumer_key2,
    updated_at=sysdate
from
(
	select A.consumer_id as consumer_id2,A.consumer_key as consumer_key2
	from etl_stage.stg_dc_dim_apple_consumers_temp A
) DD 
where consumer_id=DD.consumer_id2
and rs_consumer_key is null
;
COMMIT;
analyze  datacollector_db.itunes_downloads;
------------------ TEMP Fix ------------------
                                                      
insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_dl','1:loading dim_amusic_consumers');

-- load consumers downloads
insert into ca.dim_amusic_consumers
(consumer_key,consumer_id)
select consumer_key,consumer_id
from etl_stage.dc_dim_apple_consumers
where consumer_id not in (select consumer_id from ca.dim_amusic_consumers)
;
--select report_date_key from etl_stage.stg_itunes_dl_main group by 1

truncate table etl_stage.stg_itunes_dl_main;
insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_dl','1:load etl_stage.stg_itunes_dl_main');
BEGIN;
insert into etl_stage.stg_itunes_dl_main
(	
report_date_key
,download_date_key
,consumer_key
,raw_consumer_id
,country_code
,artist_key
,product_key
,label_key
,client_key
,dst_key
,ppv_key
,spnl_key
,sales_type_key
,sales_cat_key
,sales_division_key
,partner_info_key
,partner_region_key
,promo_info_key
,royalty_currency_key
,consumer_currency_key
,retail_price
,order_id
,preorder_code
,transaction_amount
,transaction_code
,transaction_qty
,raw_client_key
,raw_spnl_key
,raw_sales_type_key
,raw_sales_division_key
,raw_apple_id
,hash_dst_key
,hash_ppv_key
,hash_sales_cat_key
,hash_partner_region_key
,hash_promo_key
)
select 
report_date_key
,download_date_key
,rs_consumer_key
,consumer_id
,country_code
,artist_key
,product_key
,label_key
,null as client_key
,null as dst_key
,null as ppv_key
,null as spnl_key
,null as sales_type_key
,null as sales_cat_key
,null as sales_division_key
,null as partner_info_key
,null as partner_region_key
,null as promo_info_key
,royalty_currency_key
,consumer_currency_key
,retail_price
,order_id
,preorder_code
,transaction_amount
,transaction_code
,transaction_qty
,raw_client_key
,raw_spnl_key
,raw_sales_type_key
,raw_sales_division_key
,raw_apple_id
,hash_dst_key
,hash_ppv_key
,hash_sales_cat_key
,hash_partner_region_key
,hash_promo_key
from etl_stage.vw_itunes_load_dc A 
where  (A.report_date_key,country_code) not in (select report_date_key,country_code from ca.fact_itunes_downloads group by 1,2)
;
COMMIT;

BEGIN;
--  client_key
update etl_stage.stg_itunes_dl_main 
set client_key=filter_key2
from
(select
 XX.client_key as filter_key2,
  A.raw_client_key as raw_client_key2
from etl_stage.stg_itunes_dl_main A,
 ca.vw_all_client_keys  XX
where A.raw_client_key = XX.client_id
and A.client_key is null
group by 1,2) DD
where DD.raw_client_key2=raw_client_key
;

--dst_key
update etl_stage.stg_itunes_dl_main  
set dst_key=filter_key2
from
(select XX.dst_key as filter_key2, A.hash_dst_key as hash_dst_key2
from etl_stage.stg_itunes_dl_main  A,ca.vw_all_dst_keys XX
where A.hash_dst_key=XX.etl_code_hash
and A.dst_key is null
group by 1,2) DD
where DD.hash_dst_key2=hash_dst_key
;

-- ppv_key
update etl_stage.stg_itunes_dl_main 
set ppv_key=filter_key2
from
(select XX.ppv_key as filter_key2, hash_ppv_key as hash_key2
from etl_stage.stg_itunes_dl_main A, ca.vw_all_ppv_keys  XX
where A.hash_ppv_key = XX.etl_code_hash
and A.ppv_key is null
group by 1,2) DD
where DD.hash_key2=hash_ppv_key 
;

-- spnl keys
update etl_stage.stg_itunes_dl_main 
set spnl_key=filter_key2
from
(select XX.spnl_key as filter_key2, A.raw_spnl_key as raw_spnl_key2
from etl_stage.stg_itunes_dl_main A, ca.vw_all_spnl_keys XX
where A.raw_spnl_key = XX.spnl_id
and A.spnl_key is null
group by 1,2) DD
where DD.raw_spnl_key2=raw_spnl_key 
;

-- sales_type_key
update etl_stage.stg_itunes_dl_main 
set sales_type_key=filter_key2
from
(select XX.sales_key as filter_key2, A.raw_sales_type_key as raw_sales_type_key2
from etl_stage.stg_itunes_dl_main A, ca.vw_all_sales_types XX
where A.raw_sales_type_key = XX.sales_code
and A.sales_type_key is null
group by 1,2) DD
where DD.raw_sales_type_key2=raw_sales_type_key 
;

-- sales_division_key
update etl_stage.stg_itunes_dl_main 
set sales_division_key=filter_key2
from
(select XX.sales_division_key as filter_key2, A.raw_sales_division_key as raw_sales_division_key2
from etl_stage.stg_itunes_dl_main A, ca.vw_all_sales_divisions XX
where A.raw_sales_division_key = XX.sales_division_id
and A.sales_division_key is null
group by 1,2) DD
where DD.raw_sales_division_key2=raw_sales_division_key 
;

-- sales_cat_key
update etl_stage.stg_itunes_dl_main 
set sales_cat_key=filter_key2
from
(select XX.sales_cat_key as filter_key2, A.hash_sales_cat_key as hash_sales_cat_key2
from etl_stage.stg_itunes_dl_main A, ca.vw_all_sales_categories XX
where A.hash_sales_cat_key = XX.etl_code_hash
and A.sales_cat_key is null
group by 1,2) DD
where DD.hash_sales_cat_key2=hash_sales_cat_key 
;

-- partner_info_key
update etl_stage.stg_itunes_dl_main 
set partner_info_key=filter_key2
from
(select XX.partner_info_key as filter_key2, A.raw_apple_id as raw_apple_id2
from etl_stage.stg_itunes_dl_main A, ca.dim_itunes_partner_info XX
where A.raw_apple_id = XX.apple_id
and A.partner_info_key is null
group by 1,2) DD
where DD.raw_apple_id2=raw_apple_id  
;
update etl_stage.stg_itunes_dl_main
set partner_info_key = -1 
where partner_info_key is null 
;

-- promo_info_key
update etl_stage.stg_itunes_dl_main 
set promo_info_key=filter_key2 
from
(select XX.promo_info_key as filter_key2, A.hash_promo_key as hash_promo_key2
from etl_stage.stg_itunes_dl_main A, ca.dim_itunes_promo_info XX
where A.hash_promo_key = XX.etl_hash
and A.promo_info_key is null
group by 1,2) DD
where DD.hash_promo_key2=hash_promo_key  
;

-- partner_region_key
update etl_stage.stg_itunes_dl_main 
set partner_region_key=filter_key2
from
(select XX.partner_region_key as filter_key2, A.hash_partner_region_key as hash_partner_region_key2
from etl_stage.stg_itunes_dl_main A, ca.dim_itunes_partner_regions XX
where A.hash_partner_region_key = XX.etl_hash
and A.partner_region_key is null
group by 1,2) DD
where DD.hash_partner_region_key2=hash_partner_region_key  
;
COMMIT;




insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_adhoc','1:load ca.fact_itunes_download');
BEGIN;

insert into ca.fact_itunes_downloads
(
report_date_key
,download_date_key
,consumer_key
,country_code
,artist_key
,product_key
,label_key
,client_key
,dst_key
,ppv_key
,spnl_key
,sales_type_key
,sales_cat_key
,sales_division_key
,partner_info_key
,partner_region_key
,promo_info_key
,royalty_currency_key
,consumer_currency_key
,retail_price
,order_id
,preorder_code
,transaction_amount
,transaction_code
,transaction_qty 
)
select 
report_date_key
,download_date_key
,consumer_key
,country_code
,artist_key
,product_key
,label_key
,client_key
,dst_key
,ppv_key
,spnl_key
,sales_type_key
,sales_cat_key
,sales_division_key
,partner_info_key
,partner_region_key
,promo_info_key
,royalty_currency_key
,consumer_currency_key
,retail_price
,order_id
,preorder_code
,transaction_amount
,transaction_code
,transaction_qty  
from etl_stage.stg_itunes_dl_main A 
where 
(A.report_date_key,country_code) not in (select report_date_key,country_code from ca.fact_itunes_downloads group by 1,2)
;
COMMIT;
analyze ca.fact_itunes_downloads;

insert into ca_admin.itune_job_logs (job_name,step_name) values ('itunes_dl','1:loading completed');


