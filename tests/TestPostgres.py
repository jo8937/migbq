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
from peewee import *
import datetime
import random
from datetime import timedelta

########################################################################################################################

class TestPostgres(unittest.TestCase):
    
    def setUp(self):
        self.conf = migbq.migutils.get_config(getenv("pymig_config_path_pgsql"))
        self.datasource = MsSqlDatasource(db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path_pgsql")),
                            meta_db_type = "pgsql",
                            meta_db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path_pgsql")),
                            config = self.conf,
                            tablenames = self.conf.source["in"]["tables"]
                              )
        
        self.forward = BigQueryForwarder(dataset=self.conf.datasetname,
                                           prefix="",
                                           csvpath = self.conf.csvpath,
                                           logname="pgtest",
                                           config = self.conf)
        self.log = get_logger("test")
        
    def test_meta(self):
#         mig = MigrationMetadataManager(db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
#                                      meta_db_type = "pgsql",
#                                      meta_db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path_pgsql")),
#                                      config = migbq.migutils.get_config(getenv("pymig_config_path_pgsql"))
#                                      )
        with self.datasource as m:
            print [r.tablename for r in m.meta.select()]
            
    def test_migration(self):
        conf = self.conf
        self.datasource.csvpath = conf.csvpath
        self.datasource.log.setLevel(logging.DEBUG)
                
        with self.datasource as ds:
            
            # update max pk
            self.log.info("....... CHECK Max PK in Metadata Tables .........")
            ds.update_last_pk_in_tablenames(ds.tablenames)
            
            with self.forward as td:
                #ds.bq_table_map = {}
                #for tname in self.tablenames:
                #    ds.bq_table_map[tname] = td.get_table_create_if_needed(tname, ds.col_map[tname])
                
                ds.sync_field_list_src_and_dest(td)
                
                results = [True]
                
                self.log.info("start...")
                for tname in ds.tablenames:
                    ds.execute_left_logs(tname, td.execute_async, self.conf)
                    
                while any(results):
                    results = []
                    for tname in ds.tablenames:
                        ret = ds.execute_next(tname, td.execute_async)
                        #ret = ds.execute_next(tname, td.execute)
                        results.append( ret )
                        
                self.log.info("finish...")
                
            
if __name__ == '__main__':
    #sys.argv.append("TestMigUtils.test_get_config")
#     sys.argv.append("TestMig.test_00_mig")
#     sys.argv.append("TestMig.test_01_check")
    sys.argv.append("TestPostgres.test_migration")
    unittest.main()
    