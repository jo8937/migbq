# -*- coding: utf-8 -*-
#!/opt/local/bin/python2.7

import sys
import signal
import datetime

import ujson
import os
import time
import csv
import numbers
import pymssql
import _mssql
import gzip

import sqlite3

from peewee import *

from peewee_mssql_custom import MssqlDatabase  

from migbq.migutils import get_logger, estimate_bigquery_type
from sys import exc_info

from playhouse.migrate import *
import logging
from playhouse.sqlite_ext import PrimaryKeyAutoIncrementField
from time import sleep
import multiprocessing

from MigrationRoot import MigrationRoot
from MigrationSet import MigrationSet, MigrationSetJobResult
from __builtin__ import False
#from apache_beam.internal.clients.bigquery.bigquery_v2_messages import Table

# DB-API 이용하는건 PyMssql 이라서... 음 ... 뭘 쓰지.
# MSSQL
# http://www.pymssql.org/en/stable/intro.html
# 
# https://wiki.python.org/moin/HigherLevelDatabaseProgramming
# MsSQL / MySQL 에서 테이블을 읽어서 어디론가 보내기 위한...

class MigrationMetadataDatabase(object):
    def __init__(self, meta_db_type, parent_meta_db_config, metadata_tablename = None, metadata_log_tablename = None,  log=None):
        self.log = log or logging.getLogger('Metadata')
        self.log.info("Metadata DB Info : %s", parent_meta_db_config)
        DB = None
        
        if meta_db_type == "mysql":
            self.log.info("init MySQL ... ")
            DB = MySQLDatabase( **parent_meta_db_config )
            PKField = MysqlBigIntPrimaryKeyAutoIncrementField(null=False)
        elif meta_db_type == "mssql":
            self.log.info("init MSSQL ... %s",parent_meta_db_config)
            #parent_meta_db_config["use_legacy_datetime"] = False
            DB = MssqlDatabase( **parent_meta_db_config )
            PKField = MssqlPrimaryKeyAutoIncrementField(null=False)
        elif meta_db_type == "pgsql":
            self.log.info("init Postgresql ... %s",parent_meta_db_config)
            DB = PostgresqlDatabase( **parent_meta_db_config )
            PKField = PostgresqlBigIntPrimaryKeyAutoIncrementField(null=False)            
        else:
            self.log.info("meta data type [%s] not found... init SQLITE .... ",  meta_db_type)
            
            sqlite_filename = parent_meta_db_config.get("sqlite_filename")
            
            if sqlite_filename is None:
                sqlite_filename = parent_meta_db_config.get("database")
                    
            if sqlite_filename is None:
                raise ValueError("SQLITE File not found ... sql lite 디비파일을 지정해야합니다")

            currentpath = os.path.join( os.path.dirname(os.path.realpath(__file__)), "log" )
            sqlite_file = os.path.join(currentpath, sqlite_filename)
            
            self.log.info("SQLITE File : %s", sqlite_file)
            
            DB = SqliteDatabase(sqlite_file, autocommit=True)
            PKField = PrimaryKeyAutoIncrementField(null=False)
            
        logger = logging.getLogger('peewee')
        logger.setLevel(logging.DEBUG)
        #logger.setLevel(logging.INFO)
        
        class MigrationMetadata(Model):
            tableName = CharField(primary_key=True)
            firstPk = BigIntegerField(null=True)
            lastPk = BigIntegerField(null=True)
            currentPk = BigIntegerField(null=True)
            regDate = DateTimeField(default=datetime.datetime.now)
            modDate = DateTimeField(default=datetime.datetime.now)
            endDate = DateTimeField(null=True)
            pkName = CharField(null=True)
            rowCnt = BigIntegerField(null=True)
            pageTokenCurrent = CharField(null=True)
            pageTokenNext = CharField(null=True)
            rowCntDest = BigIntegerField(null=True)
            dbname = CharField(null=True)
            dataset = CharField(null=True)
            tag1 = CharField(null=True)
            tag2 = CharField(null=True)
            tag3 = CharField(null=True)
            cntDate = DateTimeField(null=True)
            tagint  = BigIntegerField(null=True)
            class Meta:
                database = DB
                
        if metadata_tablename:
            MigrationMetadata._meta.db_table = metadata_tablename
        
        """
        CREATE INDEX idx_tblname ON migrationmetadatalog (tableName); 
        CREATE INDEX idx_jobid ON migrationmetadatalog (jobId); 
        CREATE INDEX idx_checkComplete ON migrationmetadatalog (checkComplete); 
        CREATE INDEX idx_jobComplete ON migrationmetadatalog (jobComplete); 
        """
        class MigrationMetadataLog(Model):
            idx = PKField
            tableName = CharField(null=False, index = True)
            regDate = DateTimeField(default=datetime.datetime.now)
            endDate = DateTimeField(null=True)
            pkName = CharField(null=True)
            cnt = BigIntegerField(null=True)
            pkUpper = BigIntegerField(null=True)
            pkLower = BigIntegerField(null=True)
            pkCurrent = BigIntegerField(null=True)
            jobId = CharField(null=True, index = True)
            errorMessage = CharField(null=True, max_length=4000)
            checkComplete = SmallIntegerField(null=False, default=0, index = True)
            jobComplete = SmallIntegerField(null=False, default=0, index = True)
            pageToken = CharField(null=True)
            class Meta:
                database = DB
        #MigrationMetadataLog._meta.auto_increment = True
        #self.DB.register_fields({'primary_key': 'BIGINT IDENTITY'})
        if metadata_log_tablename:
            MigrationMetadataLog._meta.db_table = metadata_log_tablename
            
        self.DB = DB
        self.meta = MigrationMetadata
        self.meta_log = MigrationMetadataLog
        
    def check_and_create_table(self):
        try:
            if not self.meta.table_exists() or not self.meta_log.table_exists():
                self.DB.create_tables([self.meta, self.meta_log], safe = True)
        except OperationalError as err:
            # if table exists... run, else exit
            if self.meta.table_exists() and self.meta_log.table_exists():
                self.log.error("Table Already Exists")
            else:
                self.log.error("## Table Not Exists !! But Create Error Also!!!??")
                raise err
        

class MigrationMetadataManager(MigrationRoot):
    def __init__(self, **data):
        pname = multiprocessing.current_process().name
        self.logname = "MigMeta_"+pname
        
        self.log = get_logger(self.logname, config_file_path = data["config"].config_file_path)
        self.log.setLevel(logging.INFO)
        self.meta_db_type = data.get("meta_db_type","sqlite")
        self.parent_meta_db_config = data.get("meta_db_config")
        self.select_size = data.get("listsize")
        self.tablenames = data.get("tablenames",None)
        self.stop_when_no_more_data = data.get("stop_when_no_more_data",False)
        self.stop_when_error = data.get("stop_when_error",False)
        self.stop_when_datasource_error = data.get("stop_when_datasource_error",False)
        self.wait_second_for_next = data.get("wait_second_for_next",0)
        self.wait_second_for_nodata = data.get("wait_second_for_nodata",10)
        self.retry_cnt = 0
        self.retry_cnt_limit = 10
        self.forward_retry_cnt = 0
        self.forward_retry_cnt_limit = 10
        self.forward_retry_wait = 1
        self.meta_cache = {}
        self.dest_table_field_list_map = {}
        
        if self.select_size is None:
            self.select_size = data["config"].source.get("in",{}).get("batch_size",10000)
        
        self.conf = data["config"]
        metaconf = data["config"].source.get("meta",{}) or {}
        
        self.metadata_tablename = metaconf.get("table")
        self.metadata_log_tablename = metaconf.get("log_table")
        
        if all([metaconf.get("type"), metaconf.get("host"), metaconf.get("user")]):
            from migbq.migutils import get_connection_info_dict
            self.meta_db_type = metaconf.get("type")
            self.parent_meta_db_config = get_connection_info_dict(data["config"].source, parentkey="meta")
            #metaconf
        
        if self.parent_meta_db_config is None:
            raise NameError("meta_db_config not found...")
        
        self.init_peewee()
        
    def __enter__(self):
        self.log.info("connect and init metadata : %s",self.logname)
        self.init_create_metadata_table()
        self.init_schema()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

########################################################################################################################                    
    def init_peewee(self):
        metadatabase = MigrationMetadataDatabase(self.meta_db_type, 
                                                 self.parent_meta_db_config, 
                                                 self.metadata_tablename, 
                                                 self.metadata_log_tablename,
                                                 self.log)
        self.DB = metadatabase.DB
        self.meta = metadatabase.meta
        self.meta_log = metadatabase.meta_log 
        #self.add_columns()
        
    def init_create_metadata_table(self):
        try:
            self.DB.create_tables([self.meta, self.meta_log], safe = True)
        except OperationalError as err:
            # if table exists... run, else exit
            if self.meta.table_exists() and self.meta_log.table_exists():
                self.log.error("Table Already Exists")
            else:
                self.log.error("## Table Not Exists !! But Create Error Also!!!??")
                raise err
            
        #self.add_columns()
            
    def reset_table(self):
        self.log.error("!!!! RESET TABLES !!!!!")
        self.DB.drop_tables([self.meta, self.meta_log], safe=False)
        self.init_create_metadata_table()
        self.init_schema()
        
    def remove_all_metadata_and_tablenames(self):
        self.log.error("!!!! REMOVE ALL META TABLES !!!!!")
        self.DB.drop_tables([self.meta, self.meta_log], safe=False)
        self.tablenames = None        
        
    def add_columns(self):
        self.log.debug("start check add columns")
        self.conn.execute_query("alter table MigrationMetadata add modDate datetime2")
    
    def init_schema(self):
        if self.tablenames is None:
            
            if self.meta is not None and self.meta.table_exists() and self.meta.select().count() > 0:
                self.tablenames = [tbl.tableName for tbl in self.meta.select()]
                self.log.debug("TABLES in Metas: %s", self.tablenames)
            else:
                self.tablenames = self.select_table_list()
                self.log.debug("TABLES : %s", self.tablenames)    
        else:
            self.log.debug("Param 'tablenames' : %s", self.tablenames)
        
        self.pk_map = self.select_pk_list(self.tablenames) 
        self.log.debug("PK Map : %s", self.pk_map)
        
        self.col_map = self.select_column_type_map(self.tablenames) 
        self.log.debug("COL Map : %s", self.col_map)
        
        self.validate_integer_pk_and_normalize()
        
        self.log.debug("####### Finish Init Schemas In Memory : %s", self.tablenames)
       
    def init_metadata(self):
        for tbl in self.col_map:
            self.log.debug("# Save Table Info %s", tbl)
            self.init_table_metadata(tbl)
    
    def validate_integer_pk_and_normalize(self):
        invalid_tablenames = []
        for tablename in self.pk_map:
            pkname = self.pk_map[tablename]
            coltype = self.col_map[tablename][pkname] 
            if estimate_bigquery_type(coltype) not in ["INTEGER","FLOAT"]:
                invalid_tablenames.append(tablename)
                #raise TypeError("Table Primary Key is not Numeric [%s] %s %s" % (tablename, pkname, coltype))
        for tablename in invalid_tablenames:
            self.tablenames.remove(tablename)
            del self.pk_map[tablename]
            del self.col_map[tablename]
            errormessage = "!!! [ERROR] Table Primary Key is NOT Numeric [%s].[%s] = (%s)" % (tablename, pkname, coltype)
            self.log.error(errormessage)
            self.log.error("##################################")
            self.log.error("# delete tablename [%s]" % tablename)
            self.log.error("##################################")
            # 그 외 지정한 
            if not self.tablenames or len(self.tablenames) == 0:
                raise TypeError(errormessage)
                
    ########################################################################################################################
    # SQLITE 혹은 기타 어느 DB 로든...
    # 마이그레이션 메타데이터를 저장하는 함수군.
    def get_meta(self, tablename):
        meta = self.meta_cache[tablename] 
        return meta if meta else retrive_table_metadata(tablename)
        
    def retrive_table_metadata(self, tablename, try_cnt=1):
        if(try_cnt > 3):
            self.log.error("! Metadata for [%s] NOT FOUND ! exit...  %s" % (tablename, try_cnt),exc_info=True)
            return None
        try:
            self.log.info("retrive_table_metadata_query [%s] try ..." % tablename)
            meta = self.retrive_table_metadata_query(tablename)
            self.meta_cache[tablename] = meta
            return meta
        except DoesNotExist as err:
            self.log.info("Table [%s] not found in Metadata. try create " % tablename)
            self.init_table_metadata(tablename)
            return self.retrive_table_metadata(tablename, try_cnt + 1)

    def retrive_table_metadata_query(self, tablename):
        return self.meta.get(self.meta.tableName == tablename) 

    def retrive_next_pk_range(self, tablename):
        meta = self.retrive_table_metadata(tablename)
        return (meta.currentPk , meta.currentPk+self.select_size, meta.pageTokenNext)

    def save_pk_range_finish(self, tablename, pk_range, rowcnt):
        self.save_pk_range(tablename, (pk_range[0],pk_range[0],""), rowcnt)
    
    def save_pk_range(self, tablename, next_range, rowcnt):
        current = next_range[0]
        token = next_range[2]
        q = self.meta.update(currentPk = current, 
                             modDate = datetime.datetime.now(),
                             rowCnt = self.meta.rowCnt + rowcnt,
                             pageTokenNext = token
                             ).where(self.meta.tableName == tablename)
        q.execute()
        self.log.debug("## update mig info : %s / %s / %s " % (tablename, current, rowcnt) )
        #dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
    def set_migration_finish(self, tablename):
        q = self.meta.update(endDate = datetime.datetime.now()).where(self.meta.tableName == tablename)
        q.execute()

    def unset_migration_finish(self, tablename):
        q = self.meta.update(endDate = None).where(self.meta.tableName == tablename)
        q.execute()        
        
    def delete_meta(self, tablename):
        q = self.meta.delete().where(self.meta.tableName == tablename)
        q.execute()
            
    def init_table_metadata(self, tablename):
        
        if tablename not in self.pk_map:
            self.log.error("""#### [%s]'s Primary Key Map not found. 
            you must check that table is in Database. 
            Or That table has Numeric Prrimary key.  
            """ % tablename)
            raise ValueError("{} Primary Key is not Numeric".format(tablename))
        
        pkname = self.pk_map[tablename]
        mmc = self.retrive_pk_range_in_table(tablename)
        meta = self.meta(tableName = tablename, 
                         firstPk = mmc[0], 
                         lastPk= mmc[1], 
                         currentPk = mmc[0], 
                         pkName = pkname,
                         rowCnt = 0,
                         dbname = self.conf.dbconf.get("database"),
                         dataset = self.conf.datasetname,
                         )
        try:
            
            meta.save(force_insert=True)
        except:
            self.log.error("METADATA Save Error : [%s]" % tablename, exc_info=True)
            self.DB.set_autocommit(True)
            
    def insert_log_once(self, tablename, pk_range, jobId):
        current = 0
        q = self.meta_log.insert(
                                 tableName = tablename,
                                pkName = self.pk_map[tablename],
                                pkUpper = pk_range[1],
                                pkLower = pk_range[0],
                                pkCurrent = current,
                                jobId = jobId
                                 )
        c = q._execute()
    
    def insert_log(self, tablename, pk_range):
        current = pk_range[1]
        q = self.meta_log.insert(
                                 tableName = tablename,
                                pkName = self.pk_map[tablename],
                                pkUpper = pk_range[1],
                                pkLower = pk_range[0],
                                pkCurrent = current
                                 )
        c = q._execute()
        pk = c.lastrowid
        self.log.info("last log pk : %s",pk )
        
        
        self.log.debug("## insert log of : %s " % dict(tableName = tablename,
                                pkName = self.pk_map[tablename],
                                pkUpper = pk_range[1],
                                pkLower = pk_range[0],
                                pkCurrent = current, last_insert_id=pk) )
        return pk

    def update_insert_log(self, idx, rowcnt, jobId=None, checkComplete=1, pageToken=None):
        self.log.info("update_insert_log(%s, %s, %s)", idx, rowcnt, jobId)
        if jobId is None:
            q = self.meta_log.update(checkComplete = checkComplete, cnt = rowcnt, pageToken = pageToken).where(self.meta_log.idx == idx)
        else:
            q = self.meta_log.update(jobId = jobId, checkComplete = checkComplete, cnt = rowcnt, pageToken = pageToken).where(self.meta_log.idx == idx)
        q.execute()

    def select_running_jobids(self):
        if self.tablenames:
            query = self.meta_log.select().where(self.meta_log.jobId.is_null(False) & 
                                                 (self.meta_log.jobComplete <= 0) & 
                                                 (self.meta_log.jobId <> '') &
                                                 (self.meta_log.tableName << self.tablenames)
                                                 )
        else:
            query = self.meta_log.select().where(self.meta_log.jobId.is_null(False) &
                                                  (self.meta_log.jobComplete <= 0) & 
                                                  (self.meta_log.jobId <> ''))
        return [row for row in query]
     
    def select_error_jobids(self):
        if self.tablenames:
            query = self.meta_log.select().where(self.meta_log.jobId.is_null(False) & 
                                                 (self.meta_log.jobComplete == -1) &
                                                 (self.meta_log.tableName << self.tablenames)
                                                 )
        else:
            query = self.meta_log.select().where(self.meta_log.jobId.is_null(False) &
                                                  (self.meta_log.jobComplete == -1))
            
        return [row for row in query]

    def select_all_log(self):
        query = self.meta_log.select()
        return [row for row in query]
    
    def update_job_info_for_retry_job(self, migret_list):
        for migret in migret_list:
            logrow = self.meta_log.get(self.meta_log.idx == migret.log_idx)
            if logrow.jobId <> migret.jobId:
                self.log.info("maybe retry job... jobId Differance : log_idx : %s",migret.log_idx)
                self.insert_log_once(logrow.tableName, (logrow.pkLower,logrow.pkUpper), migret.jobId)
                    
    def update_job_info(self, migret_list):
        for migret in migret_list:
            if migret.cnt > 0:
                q = self.meta_log.update(jobComplete = 1, endDate = datetime.datetime.now(), cnt=migret.cnt).where(self.meta_log.idx == migret.log_idx)
            elif migret.cnt == 0:
                q = self.meta_log.update(checkComplete = self.meta_log.checkComplete + 1, endDate = datetime.datetime.now()).where(self.meta_log.idx == migret.log_idx)
            else:
                msg = migret.errorMessage
                if len(msg) > 4000:
                    msg = msg[:4000]
                    self.log.warn("idx[%s] error message truncated : %s", migret.log_idx, msg)
                q = self.meta_log.update(jobComplete = -1, endDate = datetime.datetime.now(), errorMessage = msg).where(self.meta_log.idx == migret.log_idx)
                    
            self.log.debug("update_job_finish : %s", migret.log_idx)
            q.execute()

    def select_incomplete_range(self, tablename = None):
        if tablename is not None:
            query = self.meta_log.select().where((self.meta_log.jobId.is_null() | (self.meta_log.jobComplete < 0)) & (self.meta_log.tableName == tablename) )
        else:
            query = self.meta_log.select().where( self.meta_log.jobId.is_null() | (self.meta_log.jobComplete < 0) )
        l = [row for row in query]
        return l
    
    def select_complete_range(self, tablenames = None, limit = -1):
        if tablenames is not None:
            query = self.meta_log.select().where(self.meta_log.jobId.is_null(False) & (self.meta_log.jobComplete > 0) & (self.meta_log.tableName << tablenames) )
        else:
            query = self.meta_log.select().where( self.meta_log.jobId.is_null(False) & (self.meta_log.jobComplete > 0) )
            
        if limit > 0:
            query = query.order_by(self.meta_log.idx.desc()).limit(limit)
            
        l = [row for row in query]
        return l
    
    def update_duplicate_range_logs(self, tablename, groupbylist):
        
        for row in groupbylist:
            if row.cnt > 1:
                self.log.info("update duplicate [%s] %s ~ %s ::: (%s) max idx %s",row.tableName, row.pkLower, row.pkUpper, row.cnt, row.idx)
                query = self.meta_log.update(jobId = "", pageToken = "duplicated")\
                                     .where(
                                         (self.meta_log.jobId.is_null() | 
                                             (self.meta_log.jobComplete < 0)) & 
                                         (self.meta_log.tableName == tablename)
                                         &
                                         (self.meta_log.pkLower == row.pkLower)
                                         &
                                         (self.meta_log.pkUpper == row.pkUpper)
                                         &
                                         (self.meta_log.idx <> row.idx)
                                         )
                query.execute()
            
    
        
    def select_incomplete_range_groupby(self, tablename):
        query = self.meta_log.select(
                             self.meta_log.tableName, 
                             self.meta_log.pkName,
                             self.meta_log.pkUpper,
                             self.meta_log.pkLower,
                             fn.Max(self.meta_log.idx).alias('idx'),
                             fn.Count(self.meta_log.idx).alias('cnt'),
                             fn.Max(self.meta_log.idx).alias('maxpk'),
                             fn.Min(self.meta_log.idx).alias('minpk'),
                             )\
                             .group_by(
                                 self.meta_log.tableName,
                                 self.meta_log.pkUpper,
                                 self.meta_log.pkLower,
                                 self.meta_log.pkName
                                 )\
                             .where((self.meta_log.jobId.is_null() | 
                                     (self.meta_log.jobComplete < 0)) & 
                                    (self.meta_log.tableName == tablename) )
        if query:
            l = [row for row in query]
            self.update_duplicate_range_logs(tablename, l)
            return l
        else:
            return []
    """
    def update_job_error(self, idxs):
        self.log.debug("update_job_error : %s", idx)
        q = self.meta_log.update(jobComplete = -1).where(self.meta_log.idx << idxs)
        q.execute()
    """
    
########################################################################################################################
    def validate_pk_sync_all(self, forwarder, tablenames=None):
        unsync_pk_map = {}
        
        if tablenames is None:
            tablenames = self.tablenames
            if tablenames is None:    
                tablenames = [tbl.tableName for tbl in self.meta.select()]

        self.log.info("Start PK Sync TableList = %s ... ", tablenames)

        for tblnm in tablenames:
            self.log.info("Start PK Sync ... : %s", tblnm)
            unsync_pk_map[tblnm] = []
            r = self.validate_pk_sync( tblnm, forwarder)
            unsync_pk_map[tblnm].extend(r)
            
        self.log.info("unsync pk list : %s", unsync_pk_map)
        
        return unsync_pk_map
    
    def intersect_pk_range(self, pk_range, tablename, pk_range_forward, pk_name):
        
        self.log.info("## Dest PK Range %s", pk_range_forward)
        
        pk_min = pk_range[0]
        pk_max = pk_range[1]
        pk_cnt = pk_range[2]
        
        changed = False
        
        # choose all smaller count, larger range...
        if pk_min < pk_range_forward[0]: 
            pk_min = pk_range_forward[0]
            changed = True
              
        if pk_max > pk_range_forward[1]: 
            pk_max = pk_range_forward[1]
            changed = True
            
        if pk_cnt > pk_range_forward[2]:
            pk_cnt = pk_range_forward[2]
            changed = True
        
        new_range = (pk_min, pk_max, pk_cnt)
        
        if changed:
            self.log.info("PK Range Changed %s -> %s", pk_range, new_range)
        
        return new_range 
    
    def convert_to_mig_range(self, unsync_pk_range_list):
        return [(pk_range[0]-1,pk_range[1]) for pk_range in unsync_pk_range_list]
    
    def validate_pk_sync(self, tablename, forwarder, pk_range = None):
        
        if pk_range is None:
            try:
                meta = self.meta.get(self.meta.tableName == tablename)
                pk_range = (meta.firstPk - 1, meta.lastPk, meta.lastPk - meta.firstPk)
            except DoesNotExist:
                self.log.debug("### Table [%s]'s info not found in meta. re-get." % tablename, exc_info=True)
                pk_range = self.retrive_pk_range_in_table(tablename)
                if pk_range is None:
                    self.log.error("### Table [%s]'s info not found in origin" % tablename)
                    return []
            
        pk_name = self.pk_map[tablename]
        
        if pk_name is None:
            self.log.error("### Table [%s]'s PK info not in metadata. exit. " % tablename)
            return []

        self.log.info("# get min, max, count ... for Forward ")
        #pk_range_forward = forwarder.retrive_pk_range_in_table(tablename, pk_name) # bigquery count
        forward_cnt = forwarder.count_range(tablename, pk_range, pk_name)
        pk_range_forward = (pk_range[0],pk_range[1],forward_cnt)

        # 1차적으로 카운트가 같은지 봄. 같다면 더 할거 없음..
        if forward_cnt == pk_range[2]:
            self.log.info("### [OK] Table [%s] 의 row count 가 일치합니다! sync 를 진행할 필요가 없습니다.", tablename)
            return []
        else:
            self.log.info("### Table [%s] 의 row count 가 다르므로 sync 를 진행합니다 ", tablename)
            self.log.info("### Datasource : %s ###", pk_range[2])
            self.log.info("### Target     : %s ###", forward_cnt)
        
        self.log.info("# Datasource - Forward 's PK Range normalize ... to same min value ")
        pk_range = self.intersect_pk_range(pk_range, tablename, pk_range_forward, pk_name) ## API Call
        
        self.log.info("## Start searching table ... %s.%s ... Range %s" , tablename, pk_name, pk_range)
                
        #count_pk_range_in_target_callback = forwarder.count_range
        
        unsync_pk_range_list = self.validate_pk_sync_inner(tablename, pk_name, forwarder, pk_range) 
        
        self.log.info("!!! UnSync PK Range Count %s",len(unsync_pk_range_list))
        
        #self.execute_mig_unsync_pk_list(unsync_pk_range_list, tablename, pk_range, forwarder)
        
        return self.convert_to_mig_range(unsync_pk_range_list)
    
    def execute_mig_unsync_pk_list(self, unsync_pk_range_list, tablename, pk_range, forwarder):
        if len(unsync_pk_range_list) > 0:
            unsync_pk_range_list_spots = []
            for rng in unsync_pk_range_list:
                unsync_pk_range_list_spots.extend(list(rng))
                    
            self.log.info("! Start Migration Unsync Data .... %s ~ %s", min(unsync_pk_range_list_spots),max(unsync_pk_range_list_spots))
            log_idx = self.insert_log(tablename, pk_range)
            datalist = self.select_datalist_in_use_range_list(tablename, unsync_pk_range_list)
            migset = MigrationSet(
                                datalist, 
                                tablename=tablename, 
                                pkname= self.pk_map[tablename], 
                                pk_range=( min(unsync_pk_range_list_spots),max(unsync_pk_range_list_spots) ), 
                                col_type_map = self.col_map[tablename], 
                                log_idx = log_idx,
                                update_insert_log_callback = self.update_insert_log
                                ) 
            sendrowcnt = forwarder.execute(migset)
            
            self.log.info("sync jobId : %s",migset.jobId)
            self.log.info("sync count : %s",sendrowcnt)
            return migset.jobId
        else:
            self.log.info("### All Data Row Count is Synced")
            return ""
    
    def validate_pk_sync_inner(self, tablename, pk_name, forwarder, pk_range, depth = 1, retry_cnt = 0):
        SELECT_LIMIT = forwarder.SELECT_LIMIT
        MAX_RETRY_VALID_CNT = 3
        unsync_pk_range_list = []

        if depth == 1 and pk_range[2] > 0:
            datasource_cnt = long( self.count_all(tablename) )
            target_cnt = forwarder.retrive_pk_range_in_table(tablename, pk_name)[2]
            self.log.info("count all is approximate value. check all count")
        else:
            datasource_cnt = self.count_range(tablename, pk_range, pk_name )
            target_cnt = forwarder.count_range(tablename, pk_range, pk_name )
        
        self.log.debug("[%s] pk_range : %s, (1) datasource cnt %s  /  (2) dest cnt %s ", depth, pk_range, datasource_cnt, target_cnt)
        
        if datasource_cnt == target_cnt:
            self.log.info("## [%s] ## Count Equals %s. No Problem in %s",depth, datasource_cnt, pk_range)
        else:
            self.log.info("!! [%s] !! Count NOT Equals __ %s <> %s __ range : %s",depth, datasource_cnt, target_cnt, pk_range)
            # 한쪽이 0 이면... 모든 PK 를 채워넣음
            if depth > 1:
                if datasource_cnt == 0:
                    self.log.info("## [%s]: src cnt %s ... dest cnt %s ... in Range %s  ... stop requirsive",depth,  datasource_cnt, target_cnt, pk_range)
                    #src_pk_list = self.select_pk_value_list(tablename, pk_range, pk_name)
                    unsync_pk_range_list.extend( pk_range )
                    return unsync_pk_range_list
                
                if target_cnt == 0:
                    self.log.info("## [%s]: src cnt %s ... dest cnt %s ...  in Range %s ... stop requirsive",depth,  datasource_cnt, target_cnt, pk_range)
                    #dest_pk_list = forwarder.select_pk_value_list(tablename, pk_range, pk_name)
                    unsync_pk_range_list.extend( pk_range )
                    return unsync_pk_range_list
            
            # 한쪽이 음수면 에러라서 재시도..
            if datasource_cnt < 0 or target_cnt < 0:
                if retry_cnt > MAX_RETRY_VALID_CNT:
                    self.log.info("(!) ERROR when src cnt %s / dest cnt %s ...  retry limit over... RAISE! %s",  datasource_cnt, target_cnt, pk_range)
                    raise ValueError('HA HA HA.........')
                else:
                    self.log.info("(!) ERROR when src cnt %s / dest cnt %s ...  waiting 5 second and retry %s",  datasource_cnt, target_cnt, pk_range)
                    time.sleep(5)
                    return self.validate_pk_sync_inner(tablename, pk_name, forwarder, pk_range, retry_cnt + 1)

            # 만약 앙쪽모두 100만건 미만으로 나온다면... divive 체크 없이 전체 로딩
            if datasource_cnt < SELECT_LIMIT and target_cnt < SELECT_LIMIT:
                
                src_pk_list = self.select_pk_value_list(tablename, pk_range, pk_name)
                dest_pk_list = forwarder.select_pk_value_list(tablename, pk_range, pk_name)
                
                # 100만건끼리 메모리상에서 diff 비교
                diffset = set(src_pk_list).symmetric_difference(set(dest_pk_list))
                
                self.log.info("### Difference In %s Set Count %s", pk_range, len(diffset))
                self.log.info("### Difference Set %s ~ %s", min(diffset), max(diffset))
                
                unsync_pk_range_list.extend(   self.zip_array_to_range_list(list(diffset))  )
                
            else:
                # 양쪽 다 어느정도 값을 갖고있음에도 불구하고 수치가 틀리면.... 반으로 나ㅇ눠서 비교 .            
                half_pk = int( (pk_range[1] + pk_range[0]) / 2 )
                # 절반이 결국 최소값이라면..??   3+4 / 2 = 3
                if pk_range[0] == half_pk:
                    unsync_pk_range_list.append( (pk_range[0],pk_range[1]) )
                    return unsync_pk_range_list
                
                lower_range = ( pk_range[0], half_pk)
                upper_range = ( half_pk, pk_range[1])
                 
                self.log.info("[%s] Start count src ... Count %s , Reduce Range %s to %s ... ",depth , datasource_cnt, pk_range, lower_range)
                self.log.info("[%s] Start count dest .. Count %s , Reduce Range %s to %s ... ",depth, target_cnt , pk_range, upper_range)

                lower_range_result = self.validate_pk_sync_inner(tablename, pk_name, forwarder, pk_range = lower_range, depth = depth + 1)
                upper_range_result = self.validate_pk_sync_inner(tablename, pk_name, forwarder, pk_range = upper_range, depth = depth + 1)
                
                if len(lower_range_result) > 0:
                    unsync_pk_range_list.extend(lower_range_result)
                if len(upper_range_result) > 0:  
                    unsync_pk_range_list.extend(upper_range_result) 

        return unsync_pk_range_list
            
    def zip_array_to_range_list(self,unsync_pk_list):
        unsync_pk_list.append(max(unsync_pk_list))
        
        minval = unsync_pk_list[0]
        preval = unsync_pk_list[0] - 1
        range_list = []
        cnt = len(unsync_pk_list)
        for index, val in enumerate(sorted(unsync_pk_list)):
            if val != preval + 1 or index + 1 == cnt:
                range_list.append((minval,unsync_pk_list[index-1]))
                minval = val
            preval = val
                
        return range_list
                
    ######################################################################################################
    
    def update_last_pk_in_meta(self):
        allCnt = 0
        self.log.info("#### get max Primary Key for all table and update metadata... ")
        tablelist = self.meta.select()
        for tbl in tablelist:
            cnt = self.update_last_pk_in_table(tbl.tableName, oldLastPk = tbl.lastPk)
            allCnt = allCnt + cnt
        return allCnt
    
    def update_last_pk_in_tablenames(self, tablenames):
        allCnt = 0
        self.log.info("#### update last PK for all tablenames...[%s]", tablenames)
        for tbl in tablenames:
            cnt = self.update_last_pk_in_table(tbl)
            allCnt = allCnt + cnt
        return allCnt
    
    def update_last_pk_in_table(self, tablename, oldLastPk = None):
        #old_meta = self.meta.select().where(self.meta.tableName == tablename)
        if oldLastPk is None:
            try:
                old_meta = self.meta.get(self.meta.tableName == tablename)
                if old_meta:
                    oldLastPk = old_meta.lastPk
                else:
                    self.log.error("# Metadata is null [%s] " % tablename)
                    return 0
            except:
                self.log.error("# Metadata Not found in Table [%s] " % tablename)
                return 0
        
        new_range = self.retrive_pk_range_in_table(tablename)
        
        if oldLastPk < new_range[1]:
            self.log.info("LAST PK is updated in [%s] table. %s -> %s" % (tablename, oldLastPk, new_range[1]))
            q = self.meta.update(lastPk = new_range[1], modDate = datetime.datetime.now()).where(self.meta.tableName == tablename)
            q.execute()
            return 1
        else:
            self.log.debug("LAST PK SAME in [%s] %s = %s" % (tablename, oldLastPk, new_range[1]))
            return 0
        
    ######################################################################################################
    
    
    def retrive_next_range(self, tablename, next_range, rowcnt):
        self.log.info("!!! retrive_next_range() must OVERRIDE")
        return next_range
        
    def select_table_list(self):
        self.log.info("!!! select_table_list() must OVERRIDE")
        return ["test"]
    
    def select_pk_list(self, tablenames):
        self.log.info("!!! select_pk_list() must OVERRIDE")
        pkmap = {}
        for tname in tablenames:
            pkmap[tname] = "id"
        return pkmap
     
    def select_column_type_map(self, tablenames):
        self.log.info("!!! select_column_type_map() must OVERRIDE")
        schema = {}
        
        for tname in tablenames:
            schema[tname] = {"id":"int","name":"varchar"}
        
        self.log.debug("## 임의의 스키마 정보 : %s " % schema)
        
        return schema
     
    # min / max / count
    def retrive_pk_range_in_table(self, tablename, pkname = None):
        self.log.info("!!! retrive_pk_range_in_table() must OVERRIDE")
        return (1, 2, 1)

    def count_all(self, tablename):
        self.log.info("!!! count_all() must OVERRIDE")
        return 1
        
    def count_range(self, tablename, pk_range, pk_name):
        self.log.info("!!! count_range() must OVERRIDE")
        if 1 in range(pk_range[0], pk_range[1]):
            return 1
        else:
            return 0

    def select_pk_value_list(self, tablename, pk_range, pk_name):
        self.log.info("!!! select_pk_value_list() must OVERRIDE")
        return [1]

    def select_datalist(self,tablename, pk_range):
        self.log.info("!!! select_datalist() must OVERRIDE")
        if pk_range[0] <= 1:
            return [{"id":1,"name":"testewr"}]
        else:
            return []
    
    def select_datalist_in(self,tablename, pk_range):
        self.log.info("!!! select_datalist_in() must OVERRIDE")
        if 1 in pk_range:
            return [{"id":1,"name":"testewr"}]
        else:
            return []
        
    def select_datalist_in_use_hashlist(self,tablename, pk_range):
        self.log.info("!!! select_datalist_in_use_hashlist() must OVERRIDE")
        if 1 in pk_range:
            return [{"id":1,"name":"testewr"}]
        else:
            return []
        
        
    def select_datalist_and_next_range(self,tablename, pk_range):
        datalist = self.select_datalist(tablename, pk_range)
        next_range = (pk_range[1], pk_range[1] + self.select_size, None)
        return (datalist, next_range)

    def callback_mock(self, migset):
        self.log.info("callback_mock : %s", migset)
        if migset:
            return migset.cnt
        else:
            return -1
                    
    def execute_range(self, tablename, pk_range, callback, log_idx):
        pkname = self.pk_map[tablename]
        self.log.info("start process table [%s] %s : ( %s ~ %s ) ... ",tablename, pkname, pk_range[0], pk_range[1])
    
        datalist, next_range = self.select_datalist_and_next_range(tablename, pk_range) 
        datacnt = len(datalist)
        if datacnt == 0:
            self.log.error("# No more data in table [%s] ( %s ~ %s )",tablename, pk_range[0], pk_range[1])
            if pk_range[0] < pk_range[1]:
                self.set_migration_finish(tablename)

        if callback is None:
            self.log.error("# execute_range() callback NOT FOUND. execute as MOCK .......... ")
            callback = self.callback_mock
            
        # 데이터 목록을 통해 뭔가 처리함..
        sendrowcnt = callback(
            MigrationSet(
                datalist, 
                tablename=tablename, 
                pkname=pkname, 
                pk_range=pk_range, 
                col_type_map = self.col_map[tablename], 
                log_idx = log_idx, 
                update_insert_log_callback = self.update_insert_log)
                            )

        # 처리가 뭔가 성공했을때만 range 저장 
        if sendrowcnt >= 0 :
            return (sendrowcnt, next_range, datacnt)
        else:
            self.log.error("# execute_next 's callback FAIL : %s ()" % callback)
            return (sendrowcnt, next_range, datacnt)

    ####################################################################################
        
    def execute_migration(self, forwarder):
        results = [True]
        self.log.info("start...")
        
        while any(results):
            results = []
            for tname in self.tablenames:
                ret = self.execute_next(tname, forwarder.execute)
                results.append( ret )
                if ret:
                    self.log.info("execute next %s ... ", tname)
                
        self.log.info("finish...")
    
    def check_gzip_csv_file_linesize(self, temp_filename):
        try:
            i = -1
            with gzip.open(temp_filename, 'rb') as f:        
                for i, l in enumerate(f):
                    pass
            return i + 1
        except:
            self.log.error("error read %s . SKIP Read FRom file and retry Select - Upload " % temp_filename, exc_info=True)
            return 0
    
    # 마이그레이션이 kill 로 죽었다면, 다음 마이그레이션 시작시 남아있는 로그들을 처리해야함.
    def execute_left_logs(self, tablename, callback=None, conf = None):
        fileexistslist, notexistslist = self.get_left_logs(tablename, conf)
        currentMigset = MigrationSet([])
        try:
            from BigQueryJobChecker import retry_error_job
            from multiprocessing import Pool
            
            # 파일이 있다면 병렬로 올림
            process_cnt = 8
            div_migset_list = self.get_divided_list(fileexistslist, cnt = process_cnt)
            for i, migset_div in enumerate(div_migset_list, start=1):
                self.log.info("bigquery load job set... (%s/%s)", i, len(div_migset_list))
                p = None
                try:
                    p = Pool(processes=process_cnt)
                    
                    # 각 프로세스 끝날때까지 대기
                    jobresults = p.map(retry_error_job, tuple(migset_div))
                    
                    # 결과를 로그DB에 성공/실패/에러 업데이트 
                    for m, jobret in enumerate(jobresults):
                        # 파일로 jobid 업로드 성공했다면 jobid 남김.
                        if jobret and jobret.cnt > -1: 
                            self.update_insert_log(jobret.log_idx, 0, jobId=jobret.jobId, checkComplete=2)
                            self.log.info("file retry ok : %s", jobret )
                            self.retry_cnt = 0
                        else:
                            self.log.error("### file retry error : %s", jobret )
                            notexistslist.append(migset_div[m])
                            
                finally:
                    if p:
                        p.close()
                        p.join()
            
            # 파일이 없다면 하나씩 다시 처리        
            for migset in notexistslist:
                currentMigset = migset
                # 파일이 없다면... 데이터 source 에서 다시 읽어서 넣어줌...
                self.log.info("re-execute ")    
                #datasource_pk_range = (migset.pk_range[0]-1,migset.pk_range[1])            
                datasource_pk_range = migset.pk_range
                sendrowcnt, next_range, datacnt = self.execute_range(migset.tablename, datasource_pk_range, callback, migset.log_idx)
                self.update_insert_log(migset.log_idx, sendrowcnt)
                self.retry_cnt = 0
                 
        except:
            self.log.error("error from select_incomplete_range... : %s : %s" % (currentMigset.tablename, currentMigset.pk_range) ,exc_info=True)
            self.retry_cnt = self.retry_cnt + 1
            if(self.retry_cnt > self.retry_cnt_limit):
                self.log.error("! execute_not_complete() retry limit over (%s) : %s : %s" % (self.retry_cnt, currentMigset.tablename, currentMigset.pk_range))
                raise
            else:
                self.log.info("! execute_not_complete() sleep %s and retry... (%s) ...  %s : %s" % (self.retry_cnt, self.retry_cnt, currentMigset.tablename, currentMigset.pk_range))
                sleep(self.retry_cnt)
                return self.execute_left_logs(currentMigset.tablename, callback, conf)
            
            
    def get_left_logs(self, tablename, conf = None):
        logList = self.select_incomplete_range_groupby(tablename)
        fileexistslist = []
        notexistslist = []
        
        self.log.info("execute_left_logs :: access to migration log :: %s", len(logList))
        
        for l in logList:
            job_idx = l.idx
            pk_range = (l.pkLower,l.pkUpper)
            
            migset = MigrationSet([], tablename=l.tableName, 
                                                           pkname=l.pkName, 
                                                           pk_range=( l.pkLower, l.pkUpper ), 
                                                           col_type_map = self.col_map[tablename], 
                                                           log_idx = l.idx,
                                                           conf = conf
                                                           )
            migset.csvfile_del_path = conf.csvpath_complete
            migset.bq_dataset = conf.datasetname
            migset.bq_project = conf.project
            migset.csvfile = conf.csvpath
            
            # 일단 파일을 미리 생성했는지 봄...                
            temp_filename = os.path.join(conf.csvpath,"migbq-%s-%s-%s-%s" % (tablename, self.pk_map[tablename], pk_range[0], pk_range[1]))
            if os.path.isfile(temp_filename):
                self.log.info("Check CSV FILE ... %s ... ", temp_filename)            
                linecount = self.check_gzip_csv_file_linesize(temp_filename)
                self.log.info("%s ::: %s / %s", temp_filename, linecount, self.select_size)
                if linecount >= self.select_size:
                    self.log.info("start upload %s ...", temp_filename)
                    fileexistslist.append(migset)
                    continue
                
            # 파일이 없거나 잘못된 파일이라면.. 
            notexistslist.append(migset)
        
        return fileexistslist, notexistslist
            
    def execute_next(self, tablename, callback=None):
        try:
            pk_range = self.retrive_next_pk_range(tablename)
        except:
            self.log.error("PK range selct error : %s " % (tablename))
            raise
        
        try:
            job_idx = self.insert_log(tablename, pk_range)
            
            sendrowcnt, next_range, datacnt = self.execute_range(tablename, pk_range, callback, job_idx)
            
            self.retry_cnt = 0
        except:
            self.log.error("data load errorr : %s : %s" % (tablename, pk_range) ,exc_info=True)
            
            if self.stop_when_datasource_error:
                raise
            
            self.retry_cnt = self.retry_cnt + 1
            if(self.retry_cnt > self.retry_cnt_limit):
                self.log.error("! retry limit over (%s) : %s : %s" % (self.retry_cnt, tablename, pk_range))
                raise
            else:
                self.log.info("sleep %s and retry... (%s) ...  %s : %s" % (self.retry_cnt, self.retry_cnt, tablename, pk_range))
                sleep(self.retry_cnt)
                return self.execute_next(tablename, callback)
            
        # 보낸게 0 건이라도 일단 다음 pk 를 가져오도록 함... -1 만 아니면 됨.
        if(sendrowcnt > 0):
            self.save_pk_range(tablename, next_range, sendrowcnt)
            self.update_insert_log(job_idx, sendrowcnt)
            self.log.info("ok")
            # 다음걸 불러오기 전에 한템포 sleep...        
            if self.wait_second_for_next > 0:
                self.log.info("wait %s second ... ",self.wait_second_for_next)
                time.sleep(self.wait_second_for_next)
            
            return True
        
        elif sendrowcnt == 0:
            # PK 가 중간에 비어있는 경우를 대비...
            # max pk 를 다시 검색하고 ... max 보다 작은데도 불구하고 row 가 0 이라면 그대로 진핸.
            meta = self.get_meta(tablename)
            # max 다시 검색
            if pk_range[1] < meta.lastPk:
                meta = self.retrive_table_metadata(tablename)
            
            # 근데도 작다면? 세이브하고 다음 범위 검색
            if pk_range[1] < meta.lastPk:
                self.log.info("row zero. but execute next. max PK is bigger then current... ")
                # 중간 pk 가 비어있다면... 다음 pk 를 검색하도록 함.
                real_next_range = next_range
                # 보낸것도 0 인데, datasource 에서도 0 이 왔다면....
                if datacnt == 0:
                    # 빈 지역이 있는지 검색...
                    real_next_range = self.retrive_next_range(tablename, pk_range, sendrowcnt)
                    
                self.save_pk_range(tablename, real_next_range, sendrowcnt)
                return True
            
            # 진짜로 PK 에 더 이상 데이터가 없을다면...
            # 마이그레이션 중단하라는 옵션...
            if self.stop_when_no_more_data:
                self.log.info("@@ no more data. no more next.")
                if meta.lastPk <= pk_range[1]:
                    self.log.info("@@ no more data. no more next ::: retrive last next pk....")
                    real_min_max = self.retrive_pk_range_in_table(tablename)
                    if real_min_max[1] <= next_range[0]:
                        self.log.info("### ok ...  SET FINISH MIGRATION : %s ###" % tablename)
                        # 끝냄. 다시 실행안되도록...
                        self.save_pk_range_finish(tablename, (real_min_max[1],real_min_max[1],""), sendrowcnt)
                        return False
                    else:
                        self.log.info("MAX PK Larger then next range....!")
                        self.update_last_pk_in_table(tablename)
                        return True
                else:
                    self.log.info("LAST PK is Larger then current range...next ")
                    return True
            else:
                # 데이터 없어도 N 초 기다리고 재개하라는 옵션...
                if self.wait_second_for_nodata > 0:
                    time.sleep(self.wait_second_for_nodata)
                    return True 
        else:
            # 데이터를 음수로 -1, -10 등을 받았다면 ...
            # stop_when_error 옵션이 있다면 그냥 종료.. 
            if self.stop_when_error:
                self.log.error("error occur. forward returns %s. force exit" % sendrowcnt)
                return False
            # 그게 아니면 forward retry 시도.... 혹시 retry 리미트걸였으면 종료. False 리턴.
            elif self.forward_retry_cnt_limit < self.forward_retry_cnt:
                self.log.error("forward returns %s and retry limit over %s force exit" % (sendrowcnt,self.forward_retry_cnt))
                self.forward_retry_cnt = 0
                return False
            # N초 기다린 후 forward 리트라이 가능하도록 True 리턴 
            else:
                self.forward_retry_cnt = self.forward_retry_cnt + 1
                self.log.error("forward returns %s ... retry after wait %s second ..." % (sendrowcnt,self.forward_retry_wait))
                time.sleep(self.forward_retry_wait)
                return True

    def get_divided_list(self, datalist, cnt=4):            
        div_list = []
        for i in range(0, 1 + (len(datalist)-1) / cnt):
            min_index = i * cnt
            max_index = min_index + cnt
            a = datalist[min_index:max_index]
            div_list.append(a)
            #self.log.debug("[%s] cnt : %s (%s ~ %s)" % (i, len(a), a[0], a[-1]) )
        return div_list
    
    # 해당 async job 이 끝났는지 체크
    # 결과는 MigrationJobResult 
    """
    [idx],[tableName],[regDate],[endDate],[pkName],[cnt],[pkUpper],[pkLower],[pkCurrent],[jobId],[errorMessage],[checkComplete],[jobComplete],[pageToken]
    """
    def check_job_finish(self, is_job_finished_callback_func, migset_process_func=None):
        logList = self.select_running_jobids()
        return self.check_job_process(8, logList, is_job_finished_callback_func, migset_process_func)
    
    def check_job_retry(self, is_job_retry_callback_func, migset_process_func=None):
        logList = self.select_error_jobids()
        return self.check_job_process(8, logList, is_job_retry_callback_func, migset_process_func, post_process_func = self.update_job_info_for_retry_job)
            
    def check_job_process(self, process_cnt, logList, do_job_callback_func, migset_process_func=None, post_process_func=None):
        
        migsetList = []
        for l in logList:
            if l.jobId is not None:
                migset = MigrationSet([], 
                                        tablename=l.tableName, 
                                        pkname=l.pkName, 
                                        pk_range=( l.pkLower, l.pkUpper ) , 
                                        col_type_map = None, 
                                        log_idx = l.idx,
                                        jobId = l.jobId
                                        )
                if migset_process_func is not None:
                    migset_process_func(migset)
                
                migsetList.append(migset)

        if len(migsetList) < 1:
            self.log.error("job check process fail : no data ")
            return None
            
        div_migset_list = self.get_divided_list(migsetList, cnt = process_cnt)
        result_list = []
        from multiprocessing import Pool
        for i, migset_div in enumerate(div_migset_list, start=1):
            
            self.log.info("check job set... (%s/%s)", i, len(div_migset_list))
            
            try:
                # 병렬 프로세스 생성
                p = Pool(processes=process_cnt)
                
                # 각 프로세스 끝날때까지 대기
                jobresults = p.map(do_job_callback_func, tuple(migset_div))
                
                # 결과를 로그DB에 성공/실패/에러 업데이트 
                self.update_job_info(jobresults)
                
                # 결과 기록 이후 수행할게 더 있으면 실행하는거.. 주로 retry_job 에 이용.
                if post_process_func is not None:
                    post_process_func(jobresults)
                
                result_list += zip(migset_div, jobresults)    
                
            finally:
                p.close()
                p.join()
        
        return result_list
    
    def get_estimate_remain_days_remain_rows_rough(self, tablenames=None):
        metaq = self.meta.select(fn.Sum(self.meta.lastPk - self.meta.currentPk))
       
        if tablenames:
            metaq = metaq.where(self.meta.tableName << tablenames)
             
        remain_rows = metaq.scalar()
        remain_rows_in_log = self.get_remain_rows_in_log(tablenames)
        return (remain_rows, remain_rows_in_log)

    def get_estimate_remain_days_remain_rows_real(self, tablenames=None):
        metarows = self.meta.select()
        remain_rows = 0
        for row in metarows:
            if tablenames and (row.tableName in tablenames):
                realcnt = self.count_range(row.tableName, (row.currentPk, row.lastPk), row.pkName)
                self.log.info("TABLE [%s] %s ~ %s COUNT() : %s ",row.tableName,row.currentPk, row.lastPk, realcnt)
                remain_rows += realcnt 
        
        remain_rows_in_log = self.get_remain_rows_in_log(tablenames)
        return (remain_rows, remain_rows_in_log)
    
    def get_remain_rows_in_log(self, tablenames=None):
        #logq = self.meta_log.select(fn.Sum(self.meta_log.cnt))
        logq = self.meta_log.select(fn.Sum(self.meta_log.pkUpper - self.meta_log.pkLower))
        
        if tablenames:
            logq = logq.where((self.meta_log.jobId.is_null() | (self.meta_log.jobComplete < 0)) & (self.meta_log.tableName << tablenames))
        else:
            logq = logq.where(self.meta_log.jobId.is_null() | (self.meta_log.jobComplete < 0))
             
        remain_rows_in_log = logq.scalar() or 0
        
        return remain_rows_in_log 
    
    def sync_field_list_src_and_dest(self, forward):
        for tname in self.tablenames:
            self.dest_table_field_list_map[tname] = forward.get_table_field_and_type_list(tname, self.col_map[tname])
    
    def select_complete_range_impl(self, tablenames=None, realCount=False, maxidx=1):
        self.log.warn("select_complete_range_impl must be inherit")
        return None
    
    def estimate_remain_days(self, tablenames=None, realCount=False):
        try:
            #maxidx = self.meta_log.select(fn.Max(self.meta_log.idx)).scalar()
            if realCount:
                remain_rows, remain_rows_in_log = self.get_estimate_remain_days_remain_rows_real(tablenames)
            else:
                remain_rows, remain_rows_in_log = self.get_estimate_remain_days_remain_rows_rough(tablenames)
            
            aggrlist = self.select_complete_range_impl(tablenames = tablenames, realCount = realCount)

#             daysum = {}
#             for row in complete_list:
#                 if row.endDate:
#                     #dt = row.endDate.strftime("%Y-%m-%d")
#                     dt = row.endDate[:10]
#                     if dt not in daysum:
#                         daysum[dt] = 0 
#                     daysum[dt] += row.cnt if row.cnt > 0 else row.pkUpper - row.pkLower 
# 
#             aggrlist = []
#             for dt in daysum:
#                 aggrlist.append( {"dt":dt,"cnt":daysum[dt]})
            
            aggrlist = sorted(aggrlist, key=lambda k: k['dt'])
            
            for row in aggrlist:
                self.log.debug("[%s] : %s",row["dt"],row["cnt"])
                             
            # remove first and last day
            if len(aggrlist) > 2:
                aggrlist = aggrlist[1:-1]
                
            aggrlist = sorted(aggrlist, key=lambda k: k['cnt']) 
            # remove min. max cnt    
            if len(aggrlist) > 2:
                aggrlist = aggrlist[1:-1]
            
            if len(aggrlist) <= 0:
                self.log.debug("aggr list is 0... ")
                return 0

            self.log.debug("avg for : %s", ujson.dumps(aggrlist, indent=4))
            cntlist = [row["cnt"] for row in aggrlist]
            row_per_day = sum(cntlist) / len(cntlist)
            
            self.log.debug("Remain Row : %s",remain_rows)
            self.log.debug("Remain Row in Log : %s",remain_rows_in_log)
            self.log.debug("Avg processed row per days : %s",row_per_day)
            
            remain_day = (remain_rows_in_log + remain_rows) / row_per_day 
            remain_day = int(remain_day)
            
            return remain_day
        except:
            self.log.error("remain days",exc_info=True)

########################################################################################################################

class MssqlPrimaryKeyAutoIncrementField(BigIntegerField):
    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        super(MssqlPrimaryKeyAutoIncrementField, self).__init__(*args, **kwargs)
        
    def __ddl__(self, column_type):
        ddl = super(MssqlPrimaryKeyAutoIncrementField, self).__ddl__(column_type)
        return ddl + [SQL('IDENTITY')]
    

########################################################################################################################

class MysqlBigIntPrimaryKeyAutoIncrementField(BigIntegerField):
    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        super(MysqlBigIntPrimaryKeyAutoIncrementField, self).__init__(*args, **kwargs)
        
    def __ddl__(self, column_type):
        ddl = super(MysqlBigIntPrimaryKeyAutoIncrementField, self).__ddl__(column_type)
        return ddl + [SQL('auto_increment')]
        
########################################################################################################################

class PostgresqlBigIntPrimaryKeyAutoIncrementField(BigIntegerField):
    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        super(PostgresqlBigIntPrimaryKeyAutoIncrementField, self).__init__(*args, **kwargs)
        self.db_field = 'bigserial'
            
########################################################################################################################

