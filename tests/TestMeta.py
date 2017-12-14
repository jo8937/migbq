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
from datetime import datetime

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

class TestMeta(unittest.TestCase):

    def setUp(self):
        self.mig = MigrationMetadataManager(
            db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
            meta_db_type = "mssql",
            meta_db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
            config = migbq.migutils.get_config(getenv("pymig_config_path"))
            )

    def test_conn(self):
        
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

    def test_validate(self):
        from migbq.Forwarder import Forwarder  
        f = Forwarder()
        with self.mig as mig:
            mig.log.setLevel(logging.DEBUG)
            print mig.validate_pk_sync("test", f)

    def test_job_check(self):
        with self.mig as mig:
            mig.check_job_process(4, [
                Struct(**dict(tableName="test",pkName="id",pkUpper=10, pkLower=1, idx=1, jobId="xx")),
                Struct(**dict(tableName="test",pkName="id",pkUpper=10, pkLower=1, idx=2, jobId="xx")),
                Struct(**dict(tableName="test",pkName="id",pkUpper=10, pkLower=1, idx=3, jobId="xx"))
                ], testfunc)
                        
    def test_log(self):
        with self.mig as mig:
            print [(m.idx, m.checkComplete, m.jobComplete, m.errorMessage, str(m.endDate), m.cnt) for m in mig.select_all_log()]
    
    def test_incomplete_log(self):
        with self.mig as mig:
            l = mig.select_incomplete_range("persons7")
            print l
                
    def test_incomplete_log_range(self):
        
        with self.mig as mig:
            mig.meta_log.delete().where(mig.meta_log.tableName == "persons7").execute()
            mig.meta_log.delete().where(mig.meta_log.tableName == "persons6").execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 10,pkLower = 1,pkCurrent = 10)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 20,pkLower = 10,pkCurrent = 10, jobId="ok")._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 30,pkLower = 20,pkCurrent = 20)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 30,pkLower = 20,pkCurrent = 20)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 30,pkLower = 20,pkCurrent = 20)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 40,pkLower = 30,pkCurrent = 30)._execute()
            
            self.assertIsNot(mig.select_incomplete_range_groupby("persons6"))
            l = mig.select_incomplete_range_groupby("persons7")
            print l
            self.assertEqual(len(l), 3)
            for row in l:
                self.assertIsNotNone(row.tableName)
                self.assertIsNotNone(row.pkName)
                self.assertIsNotNone(row.pkUpper)
                self.assertIsNotNone(row.pkLower)
                self.assertIsNotNone(row.idx)
                self.assertIsNotNone(row.cnt)
                self.assertIsNotNone(row.maxpk)
                self.assertIsNotNone(row.minpk)
                
    def test_remain_day(self):
        with self.mig as mig:
            mig.log.setLevel(logging.DEBUG)
            mig.meta_log.delete().where(mig.meta_log.tableName == "persons6").execute()
            mig.meta_log.delete().where(mig.meta_log.tableName == "persons7").execute()
            mig.meta.delete().where(mig.meta.tableName == "persons7").execute()
            mig.meta.insert(tableName = "persons7", lastPk = 140, currentPk = 40)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 10,pkLower = 1,pkCurrent = 10, endDate = datetime.strptime("2017-11-25 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 20,pkLower = 10,pkCurrent = 10, endDate = datetime.strptime("2017-11-26 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 30,pkLower = 20,pkCurrent = 20, endDate = datetime.strptime("2017-11-27 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 30,pkLower = 20,pkCurrent = 20, endDate = datetime.strptime("2017-11-28 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 30,pkLower = 20,pkCurrent = 20, endDate = datetime.strptime("2017-11-29 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()
            mig.meta_log.insert(tableName = "persons7", pkName = "id",pkUpper = 40,pkLower = 30,pkCurrent = 30, endDate = datetime.strptime("2017-11-30 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()


            remainDay = mig.estimate_remain_days(tablenames = ["persons7"])
            print "r : %s" % remainDay
            
            self.assertEqual(remainDay, 10)
            
            
            #mig.meta.delete().where(mig.meta.tableName == "persons7").execute()
    
    def test_custom_meta(self):
        confdict = migbq.migutils.get_config(getenv("pymig_config_path")).source
        
        if confdict.get("meta",{}).get("type"):
            mig = MigrationMetadataManager(
                db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
                config = migbq.migutils.get_config(getenv("pymig_config_path"))
                )
            with mig as m:
                print m.meta.select()
        else:
            self.mig.log.info("metadata connection not in config file. process to standard meta")
            with self.mig as m:
                print m.meta.select()
        
if __name__ == '__main__':
    #sys.argv.append("TestMigUtils.test_get_config")
#     sys.argv.append("TestMig.test_00_mig")
#     sys.argv.append("TestMig.test_01_check")
    #sys.argv.append("TestMeta.test_incomplete_log")
    sys.argv.append("TestMeta.test_incomplete_log_range")
    sys.argv.append("TestMeta.test_remain_day")
    sys.argv.append("TestMeta.test_custom_meta")
    unittest.main()
    