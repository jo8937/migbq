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

class TestMssql(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        super(TestMssql, self).__init__(*args, **kwargs)
        
        self.datasource = MsSqlDatasource(db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
                                     meta_db_type = "mssql",
                                     meta_db_config = migbq.migutils.get_connection_info(getenv("pymig_config_path")),
                                     config = migbq.migutils.get_config(getenv("pymig_config_path"))
                                     ) 
        self.bq = DummyForwarder(config = migbq.migutils.get_config(getenv("pymig_config_path")))
        self.datasource.log.setLevel(logging.DEBUG)
    
    def test_insert_sample_tables(self):
        tbls = {}
        with self.datasource as ds:
            for i in range(1,11):
                tablename = 'persons%s' % i
                class Persons(Model):
                    id = IntegerField(primary_key=True)
                    name = CharField(null=True)
                    point = BigIntegerField(null=True)
                    regDate = DateTimeField(default=datetime.datetime.now, index=True)
                    modDate = DateTimeField(null=True)
                    class Meta:
                        database = ds.DB
                        db_table = tablename
                tbl = Persons
                tbls[tablename] = tbl 
                try:
                    ds.DB.create_tables([tbl], safe=True)
                except:
                    ds.log.error("## error creat test data ", exc_info=True)

                cnt = tbl.select().count()             
                if cnt == 0:
                    for i in xrange(1, random.randint(10, 1000)):
                        tbl.insert( id = i, name = "Tester-%s-%s" % (tablename,i), point = random.randint(1, 2000000), modDate = datetime.datetime.now() - timedelta(days=random.randint(0, 100)) ).execute()
                else:
                    print "table [%s] count is : %s" % (tablename, cnt)                    
            
            class CompanyCode(Model):
                code = CharField(primary_key=True)
                name = CharField(null=True)
                point = BigIntegerField(null=True)
                regDate = DateTimeField(default=datetime.datetime.now, index=True)
                modDate = DateTimeField(null=True)
                class Meta:
                    database = ds.DB
            tbl = CompanyCode
            try:
                ds.DB.create_tables([tbl], safe=True)
            except:
                ds.log.error("## error creat test data ", exc_info=True)
                
            cnt = tbl.select().count()             
            if cnt == 0:
                for i in xrange(1, random.randint(10, 20)):
                    tbl.insert( code = "code-%s" % i, name = "Tester-%s-%s" % (tablename,i), point = random.randint(1, 2000000), modDate = datetime.datetime.now() - timedelta(days=random.randint(0, 100)) ).execute()
            else:
                print "table [%s] count is : %s" % (tablename, cnt)                    
                   
        
    def test_sample_tables(self):
        print 1
            
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

    def test_remain_day(self):
        from datetime import datetime
        with self.datasource as mig:
            mig.log.setLevel(logging.DEBUG)
            mig.DB.execute_sql("delete from persons5",())
            for i in xrange(0,50):
                mig.DB.execute_sql("insert into persons5(id, name, regDate) values(%s,'%s', getdate())" % (i,"test-%s"%i),())
            for i in xrange(100,151):
                mig.DB.execute_sql("insert into persons5(id, name, regDate) values(%s,'%s', getdate())" % (i,"test-%s"%i),())    
                                
            mig.meta_log.delete().where(mig.meta_log.tableName == "persons5").execute()
            mig.meta.delete().where(mig.meta.tableName == "persons5").execute()
            mig.meta.insert(tableName = "persons5", lastPk = 151, currentPk = 51)._execute()
            mig.meta_log.insert(tableName = "persons5", cnt=10, pkName = "id",pkUpper = 10,pkLower = 1,pkCurrent = 10, endDate = datetime.strptime("2017-11-25 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()
            mig.meta_log.insert(tableName = "persons5", cnt=10, pkName = "id",pkUpper = 20,pkLower = 11,pkCurrent = 10, endDate = datetime.strptime("2017-11-26 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()
            mig.meta_log.insert(tableName = "persons5", cnt=10, pkName = "id",pkUpper = 30,pkLower = 21,pkCurrent = 20, endDate = datetime.strptime("2017-11-27 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()
            mig.meta_log.insert(tableName = "persons5", cnt=10, pkName = "id",pkUpper = 40,pkLower = 31,pkCurrent = 30, endDate = datetime.strptime("2017-11-30 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()
            mig.meta_log.insert(tableName = "persons5", cnt=10, pkName = "id",pkUpper = 50,pkLower = 41,pkCurrent = 30, endDate = datetime.strptime("2017-11-30 01:02:03","%Y-%m-%d %H:%M:%S"), jobId="ok", jobComplete = 1)._execute()

            remainDay = mig.estimate_remain_days(tablenames = ["persons5"], realCount=True)
            print "r : %s" % remainDay
            
            self.assertEqual(remainDay, 5)
            
            
if __name__ == '__main__':
    #sys.argv.append("TestMigUtils.test_get_config")
#     sys.argv.append("TestMig.test_00_mig")
#     sys.argv.append("TestMig.test_01_check")
    sys.argv.append("TestMssql.test_remain_day")
    unittest.main()
    