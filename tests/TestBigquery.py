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
    
    def setUp(self):
        self.datasource = MigrationMetadataManager(
            db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
            meta_db_config = dict(sqlite_filename="data/test.sqlite"),
            config = migbq.migutils.get_config(getenv("pymig_config_path")),
            stop_when_no_more_data=True
            ) 
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
                
    def test_long_query(self):
        print "--------------------------------"
        sql = """
        SELECT count(*)
FROM (
  SELECT
      datano,
      ROW_NUMBER()
          OVER (PARTITION BY datano)
          row_number,
  FROM dwtest5.xxx
  where
    datano > 1
    AND
    datano <= 99999999999999
)
WHERE row_number = 1
        """    
        with self.bq as bq:
            print bq.query_one_row(sql)
                
if __name__ == '__main__':
    sys.argv.append("TestBigquery")
    unittest.main()
                