import os
import sys
import csv
import requests
import logging
import pandas as pd
from datetime import datetime
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel


class MySimBdpClient():
    
    def start_batch_ingest_job(self, tenant_config, ingest_task):
        # examine tenant_id
        if 'tenant_id' not in tenant_config or not tenant_config['tenant_id']:
            return {
                'status': 'error',
                'msg': 'Parameter tenant_id is missing'
            }

        # examine ingest task
        required_paras = ['file_name', 'table_name']
        for para in required_paras:
            if para not in ingest_task or not ingest_task[para]:
                return {
                    'status': 'error',
                    'msg': 'Parameter {} is missing'.format(para)
                }

        # examine if table metadata exists
        table_metadata = None
        for table in tenant_config['tables']:
            if table['table_name'] == ingest_task['table_name']:
                table_metadata = table

        if not table_metadata:
            return {
                'status': 'error',
                'msg': 'Table metadata is missing'
            }

        # ----------------- Start Ingestion--------------------
        # setting logging config
        logging.basicConfig(filename=os.getcwd() + '/../logs/batch_ingest_log.log', encoding='utf-8', level=logging.INFO)
        # start time
        start_time = datetime.now()
        logging.info("[Ingestion started] start_time={}, tenant_id={}, file_name={}, table_name={}".format(start_time.strftime("%Y/%m/%d %H:%M:%S"), tenant_config['tenant_id'], ingest_task['file_name'], ingest_task['table_name']))
        
        # 1. Ensure table exist
        res = requests.post("http://localhost:5000/{}/create_table_if_not_exist".format(tenant_config['tenant_id']), json={"table": table_metadata})
        if res.status_code != 200:
            return {
                'status': 'error',
                'msg': 'Error occur when creating table'
            }
   
        # 2. Execute batch ingestion
        FILE_PATH = os.getcwd() + '/../data/client-staging-input-directory/{}/{}/{}'.format(tenant_config['tenant_id'], ingest_task['table_name'], ingest_task['file_name'])
        file_size = os.stat(FILE_PATH).st_size
        count = 0  # count successful insertions
        rows = []  # used as json payload when sending API request

        # use pandas to read csv by batches
        reader = pd.read_csv(FILE_PATH, chunksize=15)

        for chunk_df in reader:
            for row in chunk_df.itertuples():
                rows.append({
                    item['field']: getattr(row, item['field']) if not pd.isna(getattr(row, item['field'])) else None for item in table_metadata['schema']
                })
            # send this batch to daas API
            res = requests.post("http://localhost:5000/{}/batch_ingest".format(tenant_config['tenant_id']), json={'table_name': ingest_task['table_name'], 'rows': rows})
            if res.status_code == 200:
                count += res.json()['rows']
            rows = []  # clear array after every insertion

        end_time = datetime.now()

        # return ingestion result to clientbatchingestapp
        logging.info("[Ingestion finished] status={}, tenant_id={}, file_name={}, file_size_bytes={}, table_name={}, ingestion_rows={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
            'success',
            tenant_config['tenant_id'], 
            ingest_task['file_name'],
            file_size,
            ingest_task['table_name'],
            count,
            start_time.strftime('%Y/%m/%d %H:%M:%S'),
            end_time.strftime('%Y/%m/%d %H:%M:%S'),
            (end_time-start_time).seconds
            )
        )
        return {
            'status': 'success',
            'tenant_id': tenant_config['tenant_id'],
            'file_name': ingest_task['file_name'],
            'file_size_bytes': file_size,
            'table_name': ingest_task['table_name'],
            'ingestion_rows': count,
            'start_time': start_time.strftime('%Y/%m/%d %H:%m:%s'),
            'end_time': end_time.strftime('%Y/%m/%d %H:%m:%s'),
            'total_time_cost_seconds': (end_time-start_time).seconds
        }

    def start_stream_ingest_job(self, tenant_config, ingest_task):

        # examine tenant_id
        if 'tenant_id' not in tenant_config or not tenant_config['tenant_id']:
            return {
                'status': 'error',
                'msg': 'Parameter tenant_id is missing'
            }

        # examine ingest task
        required_paras = ['data', 'table_name']
        for para in required_paras:
            if para not in ingest_task or not ingest_task[para]:
                return {
                    'status': 'error',
                    'msg': 'Parameter {} is missing'.format(para)
                }

        # examine if table metadata exists
        table_metadata = None
        for table in tenant_config['tables']:
            if table['table_name'] == ingest_task['table_name']:
                table_metadata = table

        if not table_metadata:
            return {
                'status': 'error',
                'msg': 'Table metadata is missing'
            }

        # ----------------- Start Ingestion--------------------

        # setting logging config
        logging.basicConfig(filename=os.getcwd() + '/../logs/stream_ingest_log.log', encoding='utf-8', level=logging.INFO)
        
        start_time = datetime.now()
        # Insert data
        res = requests.post("http://localhost:5000/{}/stream_ingest".format(tenant_config['tenant_id']), json={'table_name': ingest_task['table_name'], 'data': ingest_task['data']})
        end_time = datetime.now()

        # log and return ingestion result
        if res.status_code == 200:
            logging.info("[Stream ingestion] status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
                'success',
                tenant_config['tenant_id'],
                ingest_task['table_name'],
                start_time.strftime('%Y/%m/%d %H:%M:%S'),
                end_time.strftime('%Y/%m/%d %H:%M:%S'),
                (end_time-start_time).seconds
                )
            )
            return {
                'status': 'success',
                'tenant_id': tenant_config['tenant_id'],
                'table_name': ingest_task['table_name'],
                'start_time': start_time.strftime('%Y/%m/%d %H:%M:%S'),
                'end_time': end_time.strftime('%Y/%m/%d %H:%M:%S'),
                'total_time_cost_seconds': (end_time-start_time).seconds
            }
        else:
            logging.info("[Stream ingestion] status={}, tenant_id={}, table_name={}, start_time={}, end_time={}, total_time_cost={} seconds".format(
                'failed',
                tenant_config['tenant_id'],
                ingest_task['table_name'],
                start_time.strftime('%Y/%m/%d %H:%M:%S'),
                end_time.strftime('%Y/%m/%d %H:%M:%S'),
                (end_time-start_time).seconds
                )
            )
            return {
                'status': 'failed',
                'tenant_id': tenant_config['tenant_id'],
                'table_name': ingest_task['table_name'],
                'start_time': start_time.strftime('%Y/%m/%d %H:%M:%S'),
                'end_time': end_time.strftime('%Y/%m/%d %H:%M:%S'),
                'total_time_cost_seconds': (end_time-start_time).seconds
            }