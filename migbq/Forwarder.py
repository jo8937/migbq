# -*- coding: utf-8 -*-
#!/opt/local/bin/python2.7

import datetime
import logging
from migbq.migutils import get_logger
from __builtin__ import basestring, False
from _mssql import decimal
import thread
from threading import Thread
import Queue

from MigrationRoot import MigrationRoot
from MigrationSet import MigrationSet
import shutil
import os
import pickle

class TimeoutQueue(Queue.Queue):
    def join_with_timeout(self, timeout):
        self.all_tasks_done.acquire()
        try:
            endtime = time.time() + timeout
            while self.unfinished_tasks:
                remaining = endtime - time.time()
                if remaining <= 0.0:
                    return
                self.all_tasks_done.wait(remaining)
        finally:
            self.all_tasks_done.release()
    
class Forwarder(MigrationRoot):
    def __init__(self, **options):
        self.log = get_logger("ForwardBase", config_file_path = options["config"].config_file_path )
        self.SELECT_LIMIT = 1000000
        
        self.first_q = TimeoutQueue()
        self.first_th = None
                
        self.filepath = "/tmp/migbq-data"
        self.filepath_end = "/tmp/migbq-data-end"

    def filter_row(self, row):
        for key in row:
            val = row[key]
            #if key == "IdForVendor":
            #    print isinstance(val, basestring)
            #    print val.strip()
            if isinstance(val, datetime.datetime):
                row[key] = val.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(val, decimal.Decimal):
                row[key] = long(val)
            elif isinstance(val, basestring):
                row[key] = val.strip()
                
    def count_range(self, tablename, pk_range, pk_name):
        self.log.error(" Forwarder count_range() is must be OVERRIDED ")
        return 0
    
    def retrive_pk_range_in_table(self, tablename, pk_name=None):
        self.log.error(" Forwarder retrive_pk_range_in_table() is must be OVERRIDED !! ")
        return (0,0,0)

    def execute(self, migset):
        datalist, tablename, pkname, pk_range, col_type_map, log_idx = migset.values()
        self.log.error(" Forwarder execute() is must be OVERRIDED ")
        return -1
    
    def execute_async(self, migset):
        datalist, tablename, pkname, pk_range, col_type_map, log_idx = migset.values()
        self.log.info("run async execute")
        
        if self.first_th is None: 
            self.log.info("# Start run Thread 1,2")
            self.first_th = Thread(target=self.execute_async_consumer_direct_upload, args=())
            self.first_th.start()
            
        try:
            self.log.info("## Start Queueing Forward.... ")
            self.first_q.put(migset)
        except:
            self.log.info("Interrupted ~! Waiting for All Process Queue flush !", exc_info=True)
            self.log.info("Queue Size : %s" % (self.first_q.qsize()))
            self.first_q.join_with_timeout(5) # queue 를 다 처리할때까지 대기...
            self.log.info("End ~! remain : %s" % (self.first_q.qsize()))
            
        return 0        
    
    def wait_for_queue_complete(self):
        self.log.info("wait for thread (1/1) stop... ")
        self.log.info("send FIN massage to job queue")
        # send FIN Message
        self.send_fin_message()
        
        if self.first_th is not None:
            self.log.info("## all mig finish. wait for queue complete...")
            self.first_th.join()
        else:
            self.log.info("## all mig finish. finish sync job~!")
    
    def send_fin_message(self):
        self.first_q.put(None)
        
    # 두번째 큐에서 읽어서 생성된 피클 파일을 읽어서 csv 만들고 업로드.. 
    def execute_async_consumer_upload(self):
        self.execute_async_consumer(self.second_q, self.dq_execute_from_pickle)
            
    def execute_async_consumer_direct_upload(self):
        self.execute_async_consumer(self.first_q, self.execute)

    # q 에 있는걸 읽어서 consume_callback 이 처리함...    
    def execute_async_consumer(self, q, consume_callback):
        try:
            self.log.info("## Start Read From Queue Loop ... ")
            while True:
                self.log.info("# Wait For Queue Pop ... ")
                chunk = q.get()
                self.log.info("# Queue Pop OK, Start Execute Consume Callback ... next waited Q(%s) ", q.qsize())
 
                if chunk is None:
                    q.task_done()
                    self.log.info("Queue receives FIN Message (None) ... finish process. Q(%s)", q.qsize())
                    break
                
                try:
                    r = consume_callback(chunk)
                except:
                    self.log.error("## [execute ERROR] ##", exc_info=True)
                    q.task_done()
                    continue
                
                self.log.info("# Queue Task Done, Next Loop~ Q(%s) ", q.qsize())
                q.task_done()
                                
        except:
            self.log.info("## Queue loop throws exception ! ##")
            if q.unfinished_tasks:
                q.task_done()
            self.log.info("################################################################")
            self.log.info("#################### [ERROR] consume error ~! ################# ", exc_info=True)
            self.log.info("################################################################")

    def select_pk_value_list(self, tablename, pk_range, pk_name):
        self.log.error(" Forwarder select_pk_value_list() must be OVERRIDED ")
        return [1]
     
########################################################################################################################

def test():
    f = Forwarder()
    f.filter_row({"a":1})
    print 1
    
if __name__ == '__main__':
    test()

    