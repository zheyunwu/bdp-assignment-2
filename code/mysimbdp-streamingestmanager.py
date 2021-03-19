import pika, sys, os, time, requests
import threading
import json
import importlib

RB_HOST = "35.228.109.23"
CRENDENTIALS = pika.PlainCredentials('mysimbdp', 'mysimbdp')


class ConsumerThread(threading.Thread):

    def __init__(self, host, tenant, *args, **kwargs):
        super(ConsumerThread, self).__init__(*args, **kwargs)

        self._host = host
        self._tenant = tenant

    def run(self):
        # connect to rabbitmq
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host, virtual_host=self._tenant['tenant_id'], credentials=CRENDENTIALS))
        channel = connection.channel()

        # declare exchange
        channel.exchange_declare(exchange='default', exchange_type='direct')

        # declare queues and bind it to exchange (one table has one queue)
        for table in self._tenant['tables']:
            # declare queue
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            # bind queue to exchange
            channel.queue_bind(exchange='default', queue=queue_name, routing_key=table)
            # define what to do when accept data from rabbitmq
            def callback(ch, method, properties, body):
                print(" [mysimbdp-streamingestmanager] <Tenant={} Table={}> Received msg".format(self._tenant['tenant_id'], table, body.decode()))
                current_directory = os.getcwd()
                # invoke customer's clientstreamingestapp
                clientstreamingestapp = importlib.import_module("clientingestapps.{}.clientstreamingestapp".format(self._tenant['tenant_id']))
                clientstreamingestapp.stream_ingest(table, body.decode())
                # ack the message
                ch.basic_ack(delivery_tag = method.delivery_tag)
            # consume the queue
            channel.basic_consume(queue=queue_name, on_message_callback=callback)

        print('[mysimbdp-streamingestmanager] Start consuming vhost: {}'.format(self._tenant['tenant_id']))
        channel.start_consuming()


def main():
    # stimulate asking daas API for tenants info
    tenants = [
        {
            'tenant_id': 'customer_1',
            'tables': ['listings']
        },
        {
            'tenant_id': 'customer_2',
            'tables': ['reviews']
        }
    ]

    # one tenant has one thread that runs his/her own rabbitmq vhost
    threads = [ConsumerThread(host=RB_HOST, tenant=tenant) for tenant in tenants]
    for thread in threads:
        thread.start()


if __name__ == '__main__':
    main()