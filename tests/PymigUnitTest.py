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
from migbq.BQMig import commander

class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)

def testfunc(migset):
    if migset.log_idx == 1:
        return MigrationSetJobResult(migset.log_idx,100)
    elif migset.log_idx == 2:
        return MigrationSetJobResult(migset.log_idx,-1,msg="xxx")
    else:
        return MigrationSetJobResult(migset.log_idx,0)


########################################################################################################################

class TestMigMeta(unittest.TestCase):

    def atest_conn(self):
        
        from migutils import get_connection_info 
        
        def test_callback(datalist, **data):
            print datalist
            return len(datalist)
        
        with MigrationMetadataManager(meta_db_config = dict(sqlite_filename="/tmp/testdb.sqlite"), stop_when_no_more_data=True) as mig:
            mig.log.setLevel(logging.INFO)
            mig.execute_next("test", test_callback)
            
        with MigrationMetadataManager(meta_db_type="mssql", meta_db_config = get_connection_info(getenv("pymig_config_path")), stop_when_no_more_data=True) as mig:
            mig.log.setLevel(logging.INFO)
            mig.execute_next("test", test_callback)
            cnt = mig.update_last_pk_in_meta()

    def atest_validate(self):
        from Forwarder import Forwarder  
        
        f = Forwarder()
        
        with MigrationMetadataManager(meta_db_config = dict(sqlite_filename="/tmp/testdb.sqlite"), stop_when_no_more_data=True) as mig:
            mig.log.setLevel(logging.DEBUG)
            print mig.validate_pk_sync("test", f)

    def test_job_check(self):
        #with MigrationMetadataManager(meta_db_config = dict(sqlite_filename="testdb.sqlite"), stop_when_no_more_data=True) as mig:
        #    print mig.check_job_finish([1,2,3,4,5,6,7])
        with MigrationMetadataManager(meta_db_config = dict(sqlite_filename="/tmp/testdb.sqlite"), stop_when_no_more_data=True) as mig:
            mig.check_job_process(4, [
                Struct(**dict(tableName="test",pkName="id",pkUpper=10, pkLower=1, idx=1, jobId="xx")),
                Struct(**dict(tableName="test",pkName="id",pkUpper=10, pkLower=1, idx=2, jobId="xx")),
                Struct(**dict(tableName="test",pkName="id",pkUpper=10, pkLower=1, idx=3, jobId="xx"))
                ], testfunc)
                        
    def test_log(self):
        with MigrationMetadataManager(meta_db_config = dict(sqlite_filename="/tmp/testdb.sqlite"), stop_when_no_more_data=True) as mig:
            print [(m.idx, m.checkComplete, m.jobComplete, m.errorMessage, str(m.endDate), m.cnt) for m in mig.select_all_log()]
            

########################################################################################################################

class TestMssql(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        super(TestMssql, self).__init__(*args, **kwargs)
        
        self.datasource = MsSqlDatasource(db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
                                     meta_db_config = dict(sqlite_filename="mssql_mig_meta.sqlite"),
                                     config = migbq.migutils.get_config(getenv("pymig_config_path"))
                                     ) 
        self.bq = DummyForwarder()
        self.datasource.log.setLevel(logging.DEBUG)
        
    def test_empty_range(self):    
        datasource = self.datasource
        td = self.bq
        
        def test_callback(datalist, **data):
                print datalist
                return len(datalist)
            
        with datasource as sel:
            sel.reset_table()
            with td as fl:
                sel.execute_next("persons7",fl.execute )
                
    def test_validate(self):
        with self.datasource as sel:
            with self.bq as f:
                print sel.validate_pk_sync("persons7", f)

    def test_validate_counter(self):
        with self.datasource as sel:
            print sel.count_range("persons7",(0,10))
    
    def test_generate_query(self):
        print "-----------"
        with self.datasource as sel:
            print sel.generate_where_query_in("a",(1,2,3,4,5,19,10,20,21,23,25))

        
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
        
    def test_00_mig(self):    
        commander(["run",self.configfile])
                
    def test_01_check(self):
        commander(["check",self.configfile])
            
    def test_03_meta(self):
        commander(["meta",self.configfile])

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
#     sys.argv.append("TestMig.test_03_meta")
    sys.argv.append("TestMig")
    unittest.main()
    