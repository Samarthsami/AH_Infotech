insert into datacollector_db.itunes_downloads_orchard
( load_id, transaction_date_key, trans_id, partner_id, vendor_key, client_key, provider_key, provider_name, provider_country, vendor_id, upc, isrc, particip_full_name, track_title, label, product_identifier, quantity, quantity_returned, rpu, download_date, order_id, zip_code, report_date, sale_or_return, customer_currency, country_key, royalty_currency, preorder, isan, wpu, apple_id, asset_content, cma, vendor_offer_code, grid, promo_code, parent_id, parent_type_id, atrributable_purchase, primary_genre, prod_no_dig, product_type_key, distribution_channel_key, distribution_key, sales_type_key, service_type_key, transaction_type_key, sales_category_key, label_key, artist_key, product_conf_key, sales_division_key, spnl_key, product_types_key, product_key,      consumer_id, consumer_key, state_province, city_name
)
select
load_id, transaction_date_key, trans_id,cast(partner_id as int), vendor_key, client_key, provider_key, provider, provider_country, vendor_identifier, upc, isrc, artist::varchar(300), title::varchar(300), label, product_identifier, quantity, quantity_returned, rpu, cast(download_date as date)
,cast(order_id as bigint), 
zip_code, 
to_date(transaction_date_key::text,'YYYYMMDD') as report_date,
sale_or_return, currency_key, country_key, royalty_currency ,preorder, isan, wpu, apple_id, asset_content, cma, vendor_offer_code, grid_no, promo_code, cast(parent_id as varchar), 
parent_type_id, attributable_purchase, primary_genre, prod_no_dig, product_type_key, distribution_channel_key, distribution_key, sales_type_key, service_type_key, transaction_type_key, sales_category_key, label_key, artist_key, product_conf_key, sales_division_key, spnl_key, product_types_key, product_key,      customer_id, consumer_key, state_province, city_name
from
etl_stage.stg_itunes_downloads_orchard 
where
(transaction_date_key,load_id) not in (select transaction_date_key,load_id from datacollector_db.itunes_downloads_orchard group by 1,2)
;

