'''
Created on 2017. 11. 27.

@author: jo8937
'''
import unittest
import migbq
from migbq.MigrationMetadataManager import MigrationMetadataManager
from migbq.DummyForwarder import DummyForwarder
from migbq.BigQueryForwarder import BigQueryForwarder
from migbq.migutils import *
from migbq.MsSqlDatasource import MsSqlDatasource
from migbq.MigrationSet import *
import logging
from os import getenv
from migbq.BigQueryJobChecker import *
from migbq.BQMig import commander, commander_executer

        
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

class TestMig(unittest.TestCase):
    
    configfile = getenv("pymig_config_path_jinja")
    
    def test_00_reset(self):
        commander_executer("reset_for_debug", self.configfile)
    
    def test_01_mig(self):    
        commander(["run", self.configfile])
                
    def test_02_check(self):
        commander(["check", self.configfile])
            
    def test_03_sync(self):
        commander(["sync", self.configfile])

    def test_04_meta(self):
        commander(["meta", self.configfile])

    def test_99_error_pk_not_numeric(self):    
        commander(["run_with_no_retry", self.configfile,"--tablenames","companycode","persons9"])
    
    def test_99_error_pk_not_numeric_raise(self):
        try:
            commander(["run_with_no_retry", self.configfile,"--tablenames","companycode"])
            self.assertTrue(False, "this routin must raise exception")
        except:
            print sys.exc_info()
            print "FAIL is OK~!"
            
class TestMigUtils(unittest.TestCase):
    
    configfile = getenv("pymig_config_path")
        
    def test_get_config(self):    
        conf = getenv("pymig_config_path_jinja")
        print conf.__dict__
        self.assertIsNotNone(conf)

class TestJobChecker():
    def test_job_check_function(self):
        from MigrationSet import MigrationSet
        m = MigrationSet([],tablename="test",pkname="idx",pk_range=(0,1), col_type_map = None, log_idx = 0)
        print retry_error_job(m)
        print check_job_finish(m)    
        
if __name__ == '__main__':
    #sys.argv.append("TestMigUtils.test_get_config")
#     sys.argv.append("TestMig.test_00_mig")
#     sys.argv.append("TestMig.test_01_check")
#    sys.argv.append("TestMig.test_00_reset")
    #sys.argv.append("TestMig.test_99_error_pk_not_numeric_raise")
    sys.argv.append("TestMig")
    unittest.main()
    