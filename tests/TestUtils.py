'''
Created on 2017. 11. 27.

@author: jo8937
'''
import unittest
from migbq.migutils import *
from datetime import datetime

class TestUtils(unittest.TestCase):

    def test_utils(self):
        conf = get_config_sample({
                      "type": "mssql",
                      "host": "127.0.0.2",
                      "user": "test",
                      "password": "test",
                      "port": 1433,
                      "database": "TEST",
                      "tables": ["persons1"],
                      "batch_size": 200,
                      "temp_csv_path": "/tmp/pymig_csv",
                      "temp_csv_path_complete": "/tmp/pymig_csv_complete"
                      })
        print conf.__dict__
    
if __name__ == '__main__':
    #sys.argv.append("TestMigUtils.test_get_config")
#     sys.argv.append("TestMig.test_00_mig")
#     sys.argv.append("TestMig.test_01_check")
    #sys.argv.append("TestMeta.test_incomplete_log")
    sys.argv.append("TestUtils.test_utils")
    unittest.main()
    