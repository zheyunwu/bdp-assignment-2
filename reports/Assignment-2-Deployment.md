# Deployment

## Environment

1. Make sure Cassandra cluster is running

2. Run RabbitMQ:

    ```shell
    docker run -d --hostname mysimbdp-rabbitmq --name bdp_rabbitmq -e RABBITMQ_DEFAULT_USER=mysimbdp -e RABBITMQ_DEFAULT_PASS=mysimbdp -p 5672:5672 -p 15672:15672 rabbitmq:3-management
    ```

3. Go to /code directory, install necessary python packages:

   ```shell
   pip3 install -r requirements.txt
   ```

4. Run daas API server:

    ```shell
    python3 mysimbdp-daas.py
    ```

## Batch ingest test

1. Start mysimbdp-batchingestmanager:

    ```shell
    python3 mysimbdp-batchingestmanager.py
    ```

2. Add csv file to customer's client-staging-input-directory

## Stream ingest test

1. Start mysimbdp-streamingestmanager:

    ```shell
    python3 mysimbdp-streamingestmanager.py
    ```

2. Start producing data to RabbitMQ

   ```shell
   python3 produce_data_to_rabbit.py
   ```
