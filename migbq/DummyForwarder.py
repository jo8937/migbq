# -*- coding: utf-8 -*-
#!/opt/local/bin/python2.7

from migbq.migutils import get_logger
from Forwarder import Forwarder

class DummyForwarder(Forwarder):
    def __init__(self, **options):
        super( DummyForwarder, self).__init__(**options)
        self.dataset_name = None
        self.table_prefix = None
        self.table_map = {}  
        self.log = get_logger("DummyForwarder_" + options.get("logname",""), config_file_path = options["config"].config_file_path)
        self.bq = None
        self.dataset = None
        self.lastJobId = None
        self.csvpath = options.get("csvpath")
            
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False
    
    def create_bq_schema(self, col_type_map):
        return None
    
    def get_table_create_if_needed(self, tablename=None, col_type_map = None):
        return None
    
    def save_json_data(self, tbl, datalist, tablename=None, pkname=None, pk_range=None, col_type_map = None):
        return None
        
    def save_csv_data(self, tbl, datalist, tablename=None, pkname=None, pk_range=None, col_type_map = None):
        return "/tmp/test.csv"
    
    def execute(self, migset):
        return 0
    
    def get_last_jobId(self):
        return None
    
    def execute_streaming_api(self, datalist, tablename=None, pkname=None, pk_range=None, col_type_map = None):
        return 0

    def select_pk_value_list(self, tablename, pk_range, pk_name):
        return [1]
    
    def query_one_row(self, sql):
        return {}
        
    def count_exact(self, tablename, pk_name):
        return 1
        
    def count_range(self, tablename, pk_range, pk_name):
        return 1
            
    def count_all(self, tablename, pkname):
        return 1
            
    def retrive_pk_range_in_table(self, tablename, pkname):
        return (1,1,1)
        
    def check_job_complete(self, jobId):
        return True
    
    def get_job_state(self, jobId):
        return "DONE" 
   

