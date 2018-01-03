'''
Created on 2017. 11. 27.

@author: jo8937
'''
import unittest
import migbq
from migbq.MigrationMetadataManager import MigrationMetadataManager,\
    MigrationMetadataDatabase
from migbq.DummyForwarder import DummyForwarder
from migbq.BigQueryForwarder import BigQueryForwarder
from migbq.migutils import *
from migbq.MsSqlDatasource import MsSqlDatasource
from migbq.MigrationSet import *
import logging
from os import getenv
from migbq.BigQueryJobChecker import *
from migbq.BQMig import commander, commander_executer
import time
import datetime

        
class TestBigquery(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        super(TestBigquery, self).__init__(*args, **kwargs)
        self.datasource = MigrationMetadataManager(meta_db_config = dict(sqlite_filename="data/test.sqlite"), stop_when_no_more_data=True) 
        self.bq = BigQueryForwarder(dataset = get_config(getenv("pymig_config_path")).source["out"]["dataset"] , prefix="", config = get_config( getenv("pymig_config_path")  ))
        self.datasource.log.setLevel(logging.DEBUG)
        self.bq.log.setLevel(logging.DEBUG)
        
    def test_data_migration(self):    
        print "--------------------------------"
        with self.datasource as sel:
            sel.reset_table()
            with self.bq as bq:
                sel.execute_next("test",bq.execute )
            sel.unset_migration_finish("test")

    def test_data_sync(self):    
        print "--------------------------------"
        with self.datasource as sel:
            with self.bq as bq:
                print sel.validate_pk_sync("test", bq)
            
    def test_data_validation(self):
        print "--------------------------------"    
        with self.datasource as sel:
            with self.bq as bq:
                print sel.validate_pk_sync_all(bq)
                
if __name__ == '__main__':
    sys.argv.append("TestBigquery")
    unittest.main()
                