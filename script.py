import psycopg2
import json
import os
import time
import re
from datetime import datetime
from datetime import timedelta

hostname = 'datacandidatehomework.czwbfb7cwdaf.us-east-1.rds.amazonaws.com'
username = 'anqi_chen'
password = 'waWitd7A3TpUmqRE'
database = 'balm_cleanser_makeup'

conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur = conn.cursor()

# Create Users table
def create_user_table():
    cur.execute('DROP TABLE IF EXISTS Users;')
    cur.execute('CREATE TABLE Users(user_id BIGINT PRIMARY KEY, email CHAR(255), customer_locale CHAR(255), buyer_accepts_marketing BOOLEAN);')

# Create Orderlines table
def create_ordervariants_table():
    cur.execute('DROP TABLE IF EXISTS Orderlines;')
    cur.execute('CREATE TABLE Orderlines(line_id BIGINT NOT NULL, order_id BIGINT NOT NULL, variant_id BIGINT, product_id BIGINT, quantity INTEGER, PRIMARY KEY (line_id));')

# Create Orders table
def create_order_table():
    cur.execute('DROP TABLE IF EXISTS Orders;')
    cur.execute('CREATE TABLE Orders(id BIGINT NOT NULL, user_id BIGINT NOT NULL, order_number BIGINT, number BIGINT, token CHAR(255), created_at TIMESTAMP, updated_at TIMESTAMP, processed_at TIMESTAMP, gateway CHAR(255), test BOOLEAN, total_price_usd FLOAT, subtotal_price FLOAT, total_weight FLOAT, total_tax FLOAT, taxes_included BOOLEAN, financial_status CHAR(255), confirmed BOOLEAN, total_discounts FLOAT, total_line_items_price FLOAT, cart_token CHAR(255), name CHAR(255), checkout_token CHAR(255), reference VARCHAR(255), source_identifier CHAR(255), contact_email CHAR(255), device_id BIGINT, app_id BIGINT, browser_ip CHAR(255), processing_method CHAR(255), checkout_id BIGINT, source_name CHAR(255), order_status_url CHAR(255), PRIMARY KEY(id), FOREIGN KEY(user_id) REFERENCES Users (user_id));')

# Insert records into Users table
def get_user_query(directory):
    l = set()
    for filename in os.listdir(directory):
        with open(directory+"/"+filename) as f:
            data = json.load(f)
            for dic in data["orders"]:
                if dic["user_id"] in l:
                    continue
                else: 
                    l.add(dic["user_id"])
                    query = "INSERT INTO Users VALUES (%s, %s, %s, %s);"
                    cur.execute(query, (dic["user_id"],dic["email"],dic["customer_locale"],dic["buyer_accepts_marketing"]))


# Insert records into Orderlines table
def get_orderline_query(directory):
    l = set()
    for filename in os.listdir(directory):
        with open(directory+"/"+filename) as f:
            data = json.load(f)
            for dic in data["orders"]:
                for line in dic["line_items"]:
                    if line["id"] in l:
                        continue
                    else:
                        l.add(line["id"])
                        query = "INSERT INTO Orderlines VALUES (%s, %s, %s, %s, %s);"
                        cur.execute(query, (line["id"],dic["id"],line["variant_id"],line["product_id"],line["quantity"]))


# convert timestamp with offset "2017-12-27T17:00:30-05:00" to UTC time
def process_timestamp(ts):
    conformed_timestamp = re.sub(r"[:]|([-](?!((\d{2}[:]\d{2})|(\d{4}))$))", '', ts)

    # split on the offset to remove it
    split_timestamp = re.split(r"[+|-]",conformed_timestamp)
    output_datetime = datetime.strptime(split_timestamp[0] +"Z", "%Y%m%dT%H%M%SZ" )
    # add the time offset if syntax is '-', subtract the time offset if syntax is '+'
    if conformed_timestamp[-5]=='-':
        offset_delta = timedelta(hours=int(split_timestamp[1][:-2]), minutes=int(split_timestamp[1][-2:]))
        output_datetime = output_datetime + offset_delta
    else:
        offset_delta = timedelta(hours=-int(split_timestamp[1][:-2]), minutes=int(split_timestamp[1][-2:]))
        output_datetime = output_datetime + offset_delta
    return output_datetime


# Insert records into Orders
def get_order_query(directory):
    l = set()
    for filename in os.listdir(directory):
        with open(directory+"/"+filename) as f:
            data = json.load(f)
            for dic in data["orders"]:
                if dic["id"] in l:
                    continue
                else:
                    l.add(dic["id"])
                    query = "INSERT INTO Orders VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s,%s, %s,%s, %s);"
                    created_at = process_timestamp(dic["created_at"])
                    updated_at = process_timestamp(dic["updated_at"])
                    processed_at = process_timestamp(dic["processed_at"])
                    cur.execute(query, (dic["id"],dic["user_id"],dic["order_number"],dic["number"],dic["token"], created_at, updated_at, processed_at ,dic["gateway"],dic["test"], float(dic["total_price_usd"]),float(dic["subtotal_price"]),float(dic["total_weight"]),float(dic["total_tax"]),dic["taxes_included"],dic["financial_status"],dic["confirmed"],float(dic["total_discounts"]),float(dic["total_line_items_price"]),dic["cart_token"],dic["name"],dic["checkout_token"],dic["reference"],dic["source_identifier"],dic["contact_email"],dic["device_id"],dic["app_id"],dic["browser_ip"],dic["processing_method"],dic["checkout_id"],dic["source_name"],dic["order_status_url"]))
                    cur.execute("ALTER TABLE Orderlines ADD CONSTRAINT fk_orderline FOREIGN KEY (order_id) REFERENCES Orders (id)")

if __name__ == '__main__':
    create_user_table()
    create_ordervariants_table()
    create_order_table()
    get_user_query('../data')
    get_orderline_query('../data')
    get_order_query('../data')







