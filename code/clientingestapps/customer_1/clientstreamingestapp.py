import os
import sys
import json
from datetime import datetime
UTILS_PATH = os.getcwd()+'/clientingestapps/utils'
sys.path.append(UTILS_PATH)
from mysimbdp import MySimBdpClient  # provided by mysimbdp

# 1. write customer config
my_config = {
    "tenant_id": "customer_1",
    "tables": [
        {
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
    ]
}


def stream_ingest(table_name, data):

    # 1. compose json ingest task
    ingest_task = {
        "table_name": table_name,
        "data": json.loads(data)
    }

    # 2. new a MySimBdpClient instance
    client = MySimBdpClient()

    # 3. use mysimbdp client to execute the ingest job
    result = client.start_stream_ingest_job(my_config, ingest_task)
    print(result)