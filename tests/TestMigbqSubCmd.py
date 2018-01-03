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

class TestJobChecker():
    def test_job_check_function(self):
        from MigrationSet import MigrationSet
        m = MigrationSet([],tablename="test",pkname="idx",pk_range=(0,1), col_type_map = None, log_idx = 0)
        print retry_error_job(m)
        print check_job_finish(m)    

        
class TestMigSubCmd(TestRoot):
    
    def setUp(self):
        super(TestMigSubCmd,self).setUp()
        
    def test_04_meta(self):
        commander(["meta", self.configfile])

    def test_05_meta(self):
        commander(["remaindayall", self.configfile])
        
    def test_11_mig_range_queue(self):
        metadb = self.reset_temp_db()
        commander(["run_range_queued", self.configfile,"--range","0,234","--range_batch_size","100"])
        self.wait_for_mig_end_inner(metadb.meta_log, logidxs = [int(r.pageToken) for r in metadb.meta_log.select()])
        tablename = self.get_tablename_in_queue()
#         cnt = self.check_bigquery_count(tablename)
#         print "bigquery cnt : %s" % cnt
#         self.assertGreater(cnt, 1)
    
    def add_field_mssql(self, colname):
        with self.migration as ds:
            ds.conn.execute_query("alter table persons9 add %s int" % colname)

    def drop_field_mssql(self, colname):
        with self.migration as ds:
            ds.conn.execute_query("alter table persons9 drop column %s" % colname)
    
    def test_99_sync_schema(self):
        colname = "tag_%s" % datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.add_field_mssql(colname)     
        commander(["sync_schema_all_in_meta", self.configfile])
        self.drop_field_mssql(colname)
        
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
        
        
if __name__ == '__main__':
    #sys.argv.append("TestMigSubCmd")
    sys.argv.append("TestMigSubCmd.test_99_sync_schema")
    unittest.main()
    