import pandas as pd
import pika
import json
import os

tenants = {
    'customer_1': {
        'file_path': os.getcwd() + '/../data/client-staging-input-directory/customer_1/listings/listings_test.csv',
        'table_metadata': {
            "table_name": "listings",
            "primary_key": ["id", "host_id"],
            "schema": [
                {"field": "id", "type": "BIGINT"},
                {"field": "listing_url", "type": "TEXT"},
                {"field": "scrape_id", "type": "BIGINT"},
                {"field": "last_scraped", "type": "DATE"},
                {"field": "description", "type": "TEXT"},
                {"field": "name", "type": "TEXT"},
                {"field": "picture_url", "type": "TEXT"},
                {"field": "host_id", "type": "BIGINT"},
                {"field": "host_url", "type": "TEXT"},
                {"field": "latitude", "type": "FLOAT"},
                {"field": "longitude", "type": "FLOAT"},
                {"field": "property_type", "type": "TEXT"},
                {"field": "room_type", "type": "TEXT"},
                {"field": "accommodates", "type": "INT"},
                {"field": "bathrooms_text", "type": "TEXT"},
                {"field": "bedrooms", "type": "FLOAT"},
                {"field": "amenities", "type": "TEXT"},
                {"field": "price", "type": "TEXT"}
            ]
        }
    },
    'customer_2': {
        'file_path': os.getcwd() + '/../data/client-staging-input-directory/customer_2/reviews/reviews_test.csv',
        'table_metadata': {
            "table_name": "reviews",
            "primary_key": ["id", "listing_id", "reviewer_id"],
            "schema": [
                {"field": "listing_id", "type": "BIGINT"},
                {"field": "id", "type": "BIGINT"},
                {"field": "date", "type": "DATE"},
                {"field": "reviewer_id", "type": "BIGINT"},
                {"field": "reviewer_name", "type": "TEXT"},
                {"field": "comments", "type": "TEXT"}
            ]
        }
    }
}

# connect to rabbitmq
CRENDENTIALS = pika.PlainCredentials('mysimbdp', 'mysimbdp')
connection = pika.BlockingConnection(pika.ConnectionParameters(host="35.228.109.23", virtual_host='customer_2', credentials=CRENDENTIALS))
channel = connection.channel()

# declare exchange
channel.exchange_declare(exchange='default', exchange_type='direct')

# read csv and publish to RabbitMQ

reader = pd.read_csv(tenants['customer_2']['file_path'], chunksize=30)
for chunk_df in reader:
    for row in chunk_df.itertuples():
        data = {
            item['field']: getattr(row, item['field']) if not pd.isna(getattr(row, item['field'])) else None for item in tenants['customer_2']['table_metadata']['schema']
        }
        # produce to mq
        channel.basic_publish(exchange='default', routing_key=tenants['customer_2']['table_metadata']['table_name'], body=json.dumps(data))

