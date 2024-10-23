--------------------------------------------------- INSTRUCTIONS BEGIN ---------------------------------
-- DB: PROD-MAIN & PROD-REPORT
--------------------------------------------------- INSTRUCTIONS END -----------------------------------

------------
--- Fact ---
------------

delete from apple.fact_downloads where report_date='#report_date#'::date;

insert into apple.fact_downloads
(
  fact_key,
  report_date,
  download_date,
  consumer_key,
  country_code,
  artist_key,
  product_key,
  label_key,
  dst_key,
  ppv_key,
  spnl_key,
  sales_type_key,
  sales_cat_key,
  sales_division_key,
  partner_info_key,
  partner_region_key,
  promo_info_key,
  royalty_currency_key,
  consumer_currency_key,
  retail_price,
  order_id,
  preorder_code,
  transaction_amount,
  transaction_code,
  transaction_qty
 )
select
  RANK() OVER(ORDER BY report_date DESC) AS fact_key,
  report_date::date,
  download_date,
  consumer_key,
  nvl(country_key,'-1') as country_code,
  artist_key,
  product_key,
  label_key,
  C.dst_key,
  B.ppv_key,
  spnl_key,
  sales_type_key,
  D.sales_cat_key,
  nvl(sales_division_key,-1) as sales_division_key,
  nvl(partner_info_key,-1) as partner_info_key, 
  nvl(partner_region_key,-1) as partner_region_key,
  promo_info_key,
  royalty_currency as royalty_currency_key,
  currency_key as consumer_currency_key,
  CAST(retail_price as FLOAT) as retail_price,
  order_id,
  preorder as preorder_code,
  transaction_amount,
  transaction_code,
  quantity as transaction_qty
from dl_apple_downloads_db.fact_downloads A
Left join common.dim_ppv as B
ON A.vendor_key_orig=B.vendor_key and A.provider_key_orig=B.provider_key
Left join common.dim_dst_keys as C
ON A.distribution_channel_key=C.distribution_channel_code and A.transaction_type_key=C.transaction_type_code and A.service_type_key=C.service_type_code
Left join common.dim_sales_category_types as D
ON A.product_type_key=D.product_type_code and A.distribution_channel_key=D.distribution_channel_code
where 1=1
and A.report_licensor='sme'
and A.report_date = '#report_date#'::date;
  
  