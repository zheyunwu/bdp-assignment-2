import os
import sys
import json
from datetime import datetime
UTILS_PATH = os.getcwd()+'/clientingestapps/utils'
sys.path.append(UTILS_PATH)
from mysimbdp import MySimBdpClient  # provided by mysimbdp

# 1. write customer config
my_config = {
    "tenant_id": "customer_2",
    "tables": [
        {
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