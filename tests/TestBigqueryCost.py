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

class TestBigqueryCost(TestRoot):
    
    def __init__(self, *args, **kwargs):
        super(TestBigqueryCost, self).__init__(*args, **kwargs)
    
    def setUp(self):
        super(TestBigqueryCost,self).setUp()
    
    def test_sync_range(self):    
        pk_range = (73545687728,112281154982)
        unsync_pk_range_list = []
        d = self.set_pk_range_list(pk_range, unsync_pk_range_list, 1)
        #print len(unsync_pk_range_list)
        print d
        
    
    def set_pk_range_list(self, pk_range, unsync_pk_range_list, depth):
        size = pk_range[1] - pk_range[0]
        mid_pk = int( (pk_range[0] + pk_range[1]) / 2 )
        
        if pk_range[0] == mid_pk:
            unsync_pk_range_list.append( (pk_range[0],pk_range[1]) )
            return depth
        
        if size < 1000000:
            unsync_pk_range_list.append( (pk_range[0],pk_range[1]) )
            return depth
        
        lower_range = ( pk_range[0], mid_pk)
        upper_range = ( mid_pk, pk_range[1])
        
        d1 = self.set_pk_range_list(lower_range, unsync_pk_range_list, depth + 1)
        d2 = self.set_pk_range_list(upper_range, unsync_pk_range_list, depth + 1)
   
        return max([d1,d2])
   
    def test_gen_query(self):
        print self.forward.generate_pk_select_insert_sql("table",(0,100), "idx")
        
    def test_temp_table(self):
        with self.forward as f:
            f.clear_all_temp_sync_table()
            print "---------------"
            cnt = f.count_range("persons8", (-1,200), "id", parent_pk_range=None)
            print cnt
            f.clear_temp_sync_tables()
            cnt = f.count_range("persons8", (100,600), "id", parent_pk_range=(100,600))
            print cnt
            f.clear_temp_sync_tables()
            
            cntmap = {}
            
            cnt = f.count_range("persons8", (-1,600), "id", parent_pk_range=(-1,600))
            cntmap["0_600"] = cnt
            
            cnt = f.count_range("persons8", (-1,300), "id", parent_pk_range=(-1,600))
            cntmap["0_300"] = cnt

            cnt = f.count_range("persons8", (-1,150), "id", parent_pk_range=(-1,300))
            cntmap["0_150"] = cnt
                        
            cnt = f.count_range("persons8", (150,300), "id", parent_pk_range=(-1,300))
            cntmap["151_300"] = cnt
            
            print cntmap
            
            f.clear_temp_sync_tables()
        
if __name__ == '__main__':
    sys.argv.append("TestBigqueryCost.test_temp_table")
    unittest.main()
                