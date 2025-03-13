--------------------------------------------------- INSTRUCTIONS BEGIN ---------------------------------
-- DB: PROD-MAIN & PROD-REPORT
--------------------------------------------------- INSTRUCTIONS END -----------------------------------

------------------
--- Dimensions ---
------------------

--dim_partner_info
--truncate table apple.dim_partner_info;

insert into apple.dim_dl_partner_info 
(
  partner_info_key, 
  apple_id,
  upc,
  isrc,
  artist_name,  
  track_title, 
  label_name, 
  product_identifier, 
  isan, 
  asset_content, 
  grid,
  primary_genre,
  prod_no_dig
)
select 
  partner_info_key, 
  apple_id,
  upc,
  isrc,
  artist_name,  
  track_title, 
  label_name, 
  product_identifier, 
  isan, 
  asset_content, 
  grid_no as grid, 
  primary_genre,
  prod_no_dig
  
from  dl_apple_downloads_db.dim_dl_partner_info A 
where 1=1
and A.partner_info_key not in (select partner_info_key from apple.dim_dl_partner_info group by 1);


--dim_consumers
--truncate table apple.dim_consumers;

insert into apple.dim_consumers 
(
  consumer_key,
  consumer_id
)
select 
  consumer_key, 
  consumer_id
from  dl_apple_downloads_db.dim_consumers A 
where 1=1
and A.consumer_key not in (select consumer_key from apple.dim_consumers group by 1)
and A.consumer_id is not null
and A.consumer_key is not null;


--dim_dl_partner_regions
--truncate table apple.dim_dl_partner_regions;

insert into apple.dim_dl_partner_regions 
(
  partner_region_key
 ,country_code
 ,zip_code
 ,state_province
 ,city_name
)
select 
  partner_region_key
 ,country_code
 ,zip_code
 ,state_province
 ,city as city_name
from  dl_apple_downloads_db.dim_dl_partner_regions A;


--dim_dl_promo_info
--truncate table apple.dim_dl_promo_info;

insert into apple.dim_dl_promo_info
(
  promo_info_key, 
  promo_code, 
  cma_code
)
SELECT 
  promo_info_key, 
  promo_code, 
  cma_code
FROM 
    dl_apple_downloads_db.dim_dl_promo_info;
	
