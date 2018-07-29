# ShopifyETL

## Data Description

id: unique identifier for the order that's used by the shop owner and customer

order_number: A unique identifier for the order, used by the shop owner and customer. This is different from the id property, which is the unique identifier used for API purposes.

number: For internal use only. An identifier unique to the shop.

total_price: The sum of all line item prices, discounts, shipping, taxes, and tips.

subtotal_price: The price of the order after discounts but before shipping, taxes and tips.

name: The order name as represented by a number

app_id: The ID of the app that created the order

Since there are several columns with all NULL values, I decided to drop these columns: closed_at, note, source_url, landing_site_ref.

Also, I droped currency column since we can use column total_price_usd.


## Schema

There are three tables in total. 



### user table:

user_id (Primary Key)

email

customer_locale

buyer_accepts_marketing



### order table:

id (Primary Key)

user_id (Foreign Key)

order_number 

number 

token  

contact_email 

created_at

updated_at

processed_at

gateway  (shopify_payments 1, cash 2)

test  (TRUE, FALSE)

total_price

subtotal_price

total_weight 

total_tax  

taxes_included  (TRUE, FALSE)

financial_status  (pending 1, authorized 2, partially_paid 3, paid 4, partially_refunded 5, refunded 6, voided 7)

confirmed  (TRUE, FALSE)

total_discounts 

total_line_items_price

cart_token 

name 

total_price_usd

checkout_token

reference

source_identifier

device_id

app_id

browser_ip 

processing_method  (checkout 1, direct 2, manual 3, offsite 4, express 5)

checkout_id

source_name   (web 1, pos 2, iphone 3, android 4)

order_status_url  



### orderline table:

line_id (Primary Key)

order_id (Foreign Key)

variant_id 

product_id

quantity


## Data Preprocessing:

I converted timestamp with offset like "2017-12-27T17:00:30-05:00" to UTC time


