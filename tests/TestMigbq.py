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
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.migation = MigrationMetadataManager(
            db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
            meta_db_type = "mssql",
            meta_db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
            config = migbq.migutils.get_config(getenv("pymig_config_path"))
            )
        self.conf = migbq.migutils.get_config(getenv("pymig_config_path"))
        self.bq = bigquery.Client(project=self.conf.project)
        self.dataset = self.bq.dataset(self.conf.datasetname)
        self.forward = BigQueryForwarder(dataset=self.conf.datasetname,
                                           prefix="",
                                           csvpath = self.conf.csvpath,
                                           logname="test",
                                           config = self.conf)
        self.qdb = MigrationMetadataDatabase("mssql", 
                                   migbq.migutils.get_connection_info(getenv("pymig_config_path")),
                                   metadata_tablename = "migrationmetadata_prepare_meta", 
                                   metadata_log_tablename = "migrationmetadata_prepare_queue", log=self.migation.log)
        
        
    def test_00_reset(self):
        commander_executer("reset_for_debug", self.configfile)
    
    def test_01_mig(self):    
        commander(["run", self.configfile])
        print "----------------- wait for jobId full -------------"
        self.wait_for_mig_end()
        print "---------------- mig end --------------------------"
            
    def wait_for_mig_end(self):
        with self.migation as mig:
            mig.log.setLevel(logging.DEBUG)
            return self.wait_for_mig_end_inner(mig.meta_log)
    
    def wait_for_mig_end_inner(self, meta_log, retry_cnt = 0, logidxs=None): 
        retry_max = 10 
        if logidxs:
            rows = meta_log.select().where(meta_log.idx << logidxs)
        else:
            rows = meta_log.select()
        
        for row in rows:
            if row.jobId is None:
                print "idx(%s) jobId is null. wait 5 second for migbq finish..." % row.idx
                time.sleep(5)
                if retry_cnt > retry_max:
                    print "test [check] command error!!!!!!!!!!!! retry limit over"
                    return None
                return self.wait_for_mig_end_inner(meta_log, retry_cnt + 1, logidxs)
        return None
                
    def make_error_job(self):
        with self.migation as mig:
            mig.log.setLevel(logging.DEBUG)
            #print "..."
            mig.meta_log.update(jobComplete = -1).execute()
    
    def test_02_check(self):
        commander(["check", self.configfile])
            
    def test_02_retry(self):
        self.make_error_job()
        commander(["retry", self.configfile])
            
    def test_03_sync(self):
        commander(["sync", self.configfile])

    def test_04_meta(self):
        commander(["meta", self.configfile])

    def test_05_meta(self):
        commander(["remaindayall", self.configfile])

    def reset_bigquery_table(self, tablename):
        if tablename:
            tbl = self.dataset.table(tablename)
            print tbl.delete()
    
    def check_bigquery_count(self, tablename):
        if tablename:
            try:
                cnt = self.forward.count_all(tablename, "id")
                return cnt
            except:
                self.migation.log.error("bigquery error",exc_info=True)
                return 0 
            
    def reset_temp_db(self):
        tablename = self.get_tablename_in_queue()
        cnt = self.check_bigquery_count(tablename)
        if cnt > 0:
            self.reset_bigquery_table(tablename)
        self.qdb.meta_log.delete().execute()
        return self.qdb 
    
    def get_tablename_in_queue(self):
        tablename = next(iter(set([row.tableName for row in self.qdb.meta_log.select().execute()])),None)
        return tablename 
        
    def test_11_mig_range_queue(self):
        metadb = self.reset_temp_db()
        commander(["run_range_queued", self.configfile,"--range","0,234","--range_batch_size","100"])
        self.wait_for_mig_end_inner(metadb.meta_log, logidxs = [int(r.pageToken) for r in metadb.meta_log.select()])
        tablename = self.get_tablename_in_queue()
        cnt = self.check_bigquery_count(tablename)
        print "bigquery cnt : %s" % cnt
        self.assertGreater(cnt, 1)
        
    def test_99_error_pk_not_numeric(self):    
        commander(["run_with_no_retry", self.configfile,"--tablenames","companycode","persons9"])
    
    def test_99_error_pk_not_numeric_raise(self):
        try:
            commander(["run_with_no_retry", self.configfile,"--tablenames","companycode"])
            self.assertTrue(False, "this routin must raise exception")
        except:
            print sys.exc_info()
            print "FAIL is OK~!"
        
    def test_99_estimate_datasource(self):
        commander_executer("estimate_datasource_per_day", self.configfile)
                    
    def test_99_datasource_current_pk_day(self):
        commander_executer("datasource_current_pk_day", self.configfile)
        
    def test_99_updatepk(self):
        commander_executer("updatepk", self.configfile)
        
        
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
#     sys.argv.append("TestMig.test_00_reset")
    #sys.argv.append("TestMig.test_99_error_pk_not_numeric_raise")
    #sys.argv.append("TestMig.test_05_meta")
    #sys.argv.append("TestMig.test_99_estimate_datasource")
    #sys.argv.append("TestMig.test_99_datasource_current_pk_day")
    #sys.argv.append("TestMig.test_05_meta")
    #sys.argv.append("TestMig.test_01_mig")
    #sys.argv.append("TestMig.test_02_check")
    #sys.argv.append("TestMig.test_02_retry")
    sys.argv.append("TestMig.test_11_mig_range_queue")
    #sys.argv.append("TestMig")
    unittest.main()
    