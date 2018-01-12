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

class TestMig(TestRoot):
    
    def setUp(self):
        super(TestMig,self).setUp()
        
    def test_00_reset(self):
        commander_executer("reset_for_debug", self.configfile)
    
    def test_01_mig(self):    
        commander(["run", self.configfile])
        print "----------------- wait for jobId full -------------"
        self.wait_for_mig_end()
        print "---------------- mig end --------------------------"
            
    def test_02_check(self):
        commander(["check", self.configfile])
            
    def test_02_retry(self):
        self.make_error_job()
        commander(["retry", self.configfile])
            
    def test_03_sync(self):
        commander(["sync", self.configfile])

    def test_04_meta(self):
        commander(["meta", self.configfile])
        
if __name__ == '__main__':
    #sys.argv.append("TestMig.test_05_meta")
    sys.argv.append("TestMig.test_03_sync")
    unittest.main()
    