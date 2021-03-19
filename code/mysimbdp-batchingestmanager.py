import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler

# watch the client-staging-input-directory
CURRENT_DIRECTORY = os.getcwd()
WATCH_PATH = CURRENT_DIRECTORY + '/../data/client-staging-input-directory'


class BatchIngestManagerHandler(FileSystemEventHandler):

    def __init__(self, **kwargs):
        super(BatchIngestManagerHandler, self).__init__(**kwargs)
    
    def on_created(self, event):
        # The directory structure is designed as:
        # 'client-staging-input-directory/tenant_id/table/data_files.csv'

        if event.is_directory:  # new directory detected
            # If the 2nd directory from last is 'client-staging-input-directory',
            # it means it is a new tenant directory.
            if event.src_path.split('/')[-2] == 'client-staging-input-directory':
                new_customer = event.src_path.split('/')[-1]
                print("[BatchIngestManager] New tenant detected: %s" % new_customer)

        else:  # new file detected
            # If the 4th directory from last is 'client-staging-input-directory',
            # it means it is a new data file that should be ingestd
            if event.src_path.split('/')[-4] == 'client-staging-input-directory':
                tenant_id = event.src_path.split('/')[-3]
                table_name = event.src_path.split('/')[-2]
                file_name = event.src_path.split('/')[-1]
                print("[BatchIngestManager] New file detected: {}/{}/{}".format(tenant_id, table_name, file_name))

                # invoke the customer's batchingestapp.py
                os.system("python3 {current}/clientingestapps/{tenant_id}/clientbatchingestapp.py {table_name} {file_name}".format(current=CURRENT_DIRECTORY, tenant_id=tenant_id, table_name=table_name, file_name=file_name))


if __name__ == '__main__':
    event_handler = BatchIngestManagerHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_PATH, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:  # pree ctrl+c to end this program
        observer.stop()
    observer.join()
