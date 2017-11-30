'''
Created on 2017. 11. 27.

@author: jo8937
'''
import unittest
from migbq.migutils import *
from datetime import datetime

class TestUtils(unittest.TestCase):

    def test_utils(self):
        print 1
    
if __name__ == '__main__':
    #sys.argv.append("TestMigUtils.test_get_config")
#     sys.argv.append("TestMig.test_00_mig")
#     sys.argv.append("TestMig.test_01_check")
    #sys.argv.append("TestMeta.test_incomplete_log")
    sys.argv.append("TestUtils.test_utils")
    unittest.main()
    