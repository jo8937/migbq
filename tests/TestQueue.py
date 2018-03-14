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
from TestRoot import TestRoot

class TestQueue(TestRoot):
    
    def setUp(self):
        super(TestQueue,self).setUp()
        
    def test_queue(self):
        datasource = MsSqlDatasource(db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
                             meta_db_type = "mssql",
                             meta_db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
                             config = migbq.migutils.get_config(getenv("pymig_config_path"))
                             )  
        bq = BigQueryForwarder(dataset = get_config(getenv("pymig_config_path")).source["out"]["dataset"] , prefix="", config = get_config( getenv("pymig_config_path")  ))
        datasource.log.setLevel(logging.DEBUG)
        bq.log.setLevel(logging.DEBUG)
        commander(["run_range_queued",getenv("pymig_config_path"),"--tablenames","persons9","--range","0,10+12,15+20,30+100,101"])
            
if __name__ == '__main__':
    #sys.argv.append("TestMig.test_05_meta")
    sys.argv.append("TestQueue.test_queue")
    unittest.main()
    