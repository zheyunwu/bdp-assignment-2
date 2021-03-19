import os
from datetime import datetime
import sys
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


if __name__ == '__main__':

    if len(sys.argv) >= 3:
        table_name = sys.argv[1]  # get table name
        file_name = sys.argv[2]  # get file name

    # 2. compose json ingest task
    ingest_task = {
        "table_name": table_name,
        "file_name": file_name
    }

    # 3. new a MySimBdpClient instance
    client = MySimBdpClient()

    # 4. use mysimbdp client to execute the ingest job
    result = client.start_batch_ingest_job(my_config, ingest_task)
    print(result)
