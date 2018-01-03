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

class TestRoot(unittest.TestCase):
    
    configfile = getenv("pymig_config_path_jinja")
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.migration = MsSqlDatasource(db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
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
                                   metadata_log_tablename = "migrationmetadata_prepare_queue", log=self.migration.log)
        
    ####################################################################################################
    
    def wait_for_mig_end(self):
        with self.migration as mig:
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
        with self.migration as mig:
            mig.log.setLevel(logging.DEBUG)
            #print "..."
            mig.meta_log.update(jobComplete = -1).execute()
    
    
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
                self.migration.log.error("bigquery error",exc_info=True)
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
    