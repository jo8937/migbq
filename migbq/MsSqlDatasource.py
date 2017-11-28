# -*- coding: utf-8 -*-
#!/opt/local/bin/python2.7

import datetime
import os
import csv
import numbers
import pymssql
import _mssql
import gzip

from peewee import *
from peewee_mssql_custom import MssqlDatabase  
from migbq.migutils import get_logger
from sys import exc_info

from playhouse.migrate import *
import logging
from playhouse.sqlite_ext import PrimaryKeyAutoIncrementField
#from apache_beam.internal.clients.bigquery.bigquery_v2_messages import Table

# DB-API 이용하는건 PyMssql 이라서... 음 ... 뭘 쓰지.
# MSSQL
# http://www.pymssql.org/en/stable/intro.html
# 
# https://wiki.python.org/moin/HigherLevelDatabaseProgramming
# MsSQL / MySQL 에서 테이블을 읽어서 어디론가 보내기 위한...
from MigrationMetadataManager import MigrationMetadataManager
from MigrationSet import MigrationSet

class MsSqlDatasource(MigrationMetadataManager):
    def __init__(self, db_config, **data):
        super( MsSqlDatasource, self).__init__(**data)
        self.log = get_logger("MsSqlSelector_" + data.get("logname",""), config_file_path=data["config"].config_file_path )
        self.log.setLevel(logging.INFO)
        self.db_config = db_config
        self.select_size = data.get("listsize",10)
        self.bq_table_map = None
        self.csvpath = data["config"].csvpath
        
    def __enter__(self):
        self.log.info("## Connecting Database ...  ## %s ## "% datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.connect()
        super( MsSqlDatasource, self).__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        super( MsSqlDatasource, self).__exit__(exc_type, exc_value, traceback)
        self.close()
        return False
            
########################################################################################################################
    def connect(self):
        self.log.info("# start # DB Connection Enter {}".format( self.db_config.get("server") ))
        try:
            #self.conn = mysql.connector.connect(pool_name = "mypool",pool_size = 3,**self.mysql_config)
            self.conn = _mssql.connect(**self.db_config)
            #self.cursor = self.conn.cursor(prepared=True)
            #self.conn.autocommit = True
        except:
            self.log.error("error when connect()", exc_info=True)
        #self.conn.close()
        
    def close(self):
        self.log.info("# end # DB Connection Closed : {}".format( self.db_config.get("server") ))
        try:
            self.conn.close()
        except:
            self.log.error("error on close")
        
    ########################################################################################################################
    # SQLITE 혹은 기타 어느 DB 로든...
    # 마이그레이션 메타데이터를 저장하는 함수군.
    
    def retrive_pk_range_in_table(self, tablename):
        #sql = "select max(%s) as mx, min(%s) as mn, count(*) as cnt from %s (nolock)" % (self.pk_map[tablename], self.pk_map[tablename], tablename)
        sql = "select max(%s) as mx, min(%s) as mn, 0 as cnt from %s (nolock)" % (self.pk_map[tablename], self.pk_map[tablename], tablename)
        self.log.debug("############# get  max(PK) min(PK) ###############")
        self.log.debug(sql)
        self.conn.execute_query(sql)
        
        first_pk = 0
        last_pk = 0
        total_rows = 0
        for row in self.conn:
            first_pk = row["mn"]        
            last_pk = row["mx"]
            #total_rows = row["cnt"]
            total_rows = last_pk - first_pk 
        
        return (first_pk - 1, last_pk, total_rows)
    
        
    ########################################################################################################################
    # ms SQL 기반으로 테이블 스키마 정보를 불러오는 쿼리들...

    def select_table_list(self):
        tablenames = []
        #conn.execute_query("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'%s';" % table.name)
        sql = """
            SELECT 
            TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT 
            FROM INFORMATION_SCHEMA.COLUMNS
            """
        self.log.debug("############# get all table list in database ###############")
        self.log.debug(sql)
        self.conn.execute_query(sql)
        for row in self.conn:
            tablenames.append(row["TABLE_NAME"])
        return tablenames
        
    def select_pk_list(self, tablenames):
        self.log.debug( "retrive PK ")
        
        sql = """
            SELECT
                Col.Column_Name as col, Tab.Table_Name as tab 
            FROM 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
                INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
            WHERE 
                Col.Constraint_Name = Tab.Constraint_Name
                AND Col.Table_Name = Tab.Table_Name
                AND Constraint_Type = 'PRIMARY KEY'
                AND Col.Table_Name in (%s)
            """        % ",".join(["'%s'" % nm for nm in tablenames])
        
        self.log.debug("############# get [Primary Key] for selected tables ###############")
        self.log.debug(sql)
        
        self.conn.execute_query(sql)
        
        table_pk_map = {}
        
        for row in self.conn:
            table_pk_map[row["tab"]] = row["col"]
            
        self.log.debug("## Primary Key : %s " % table_pk_map)
        
        return table_pk_map
            
    def select_column_type_map(self, tablenames):
        self.log.debug( "retrive type")
        sql = """
            SELECT 
                TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT 
            FROM 
                INFORMATION_SCHEMA.COLUMNS 
            WHERE 
                TABLE_NAME in (%s)
            """ % ",".join(["N'%s'" % nm for nm in tablenames])
            
        self.log.debug("############# Get Schema (field-type) list for selected tables ###############")
        self.log.debug(sql)
        self.conn.execute_query(sql)
        table_pk_map = {}
        
        for row in self.conn:
            if row["TABLE_NAME"] not in table_pk_map:
                table_pk_map[row["TABLE_NAME"]] = {}
            table_pk_map[row["TABLE_NAME"]][row["COLUMN_NAME"]] = row["DATA_TYPE"]
            
        self.log.debug("## all schema : %s " % table_pk_map)
        
        return table_pk_map
        

    # 실제 데이터 가져오는 쿼리 함수 
    def generate_select_query(self, tablename, pk_range):    
        
        if self.col_map[tablename] is None:
            self.log.error("Table Schema Not Exists : %s", tablename)
            return None
        
        query = """
SELECT 
    %s 
FROM 
    %s (nolock) 
WHERE 
    %s > %d  
    AND 
    %s <= %d 
""" % ( ",".join([nm for nm in self.col_map[tablename].keys()]) 
        , tablename
        , self.pk_map[tablename]
        , pk_range[0]
        , self.pk_map[tablename]
        , pk_range[1]) 

        self.log.debug("SQL : %s", query)
        return query        
    
    def zip_select_query_in_length(self,pk_range):
        pk_range = list(pk_range)
        pk_range.append(max(pk_range) + 1)
        
        minval = pk_range[0]
        preval = pk_range[0] - 1
        range_list = []
        cnt = len(pk_range)
        for index, val in enumerate(sorted(pk_range)):
            if val != preval + 1 or index + 1 == cnt:
                range_list.append((minval,pk_range[index-1]))
                minval = val
            preval = val
                
        print range_list
        return range_list
            
    def generate_where_query_in(self, pkname, pk_range):
        zip_range_list = self.zip_select_query_in_length(pk_range)
        query_list = []
        dep_list = []
        for zip_range in zip_range_list:
            if zip_range[0] == zip_range[1]:
                dep_list.append(zip_range[0])
            else:
                query_list.append(" %s between %s and %s" % (pkname, zip_range[0], zip_range[1]))
                
        if len(dep_list) > 0:
            query_list.append( pkname + " IN (" + ",".join([str(i) for i in dep_list]) + ")")
            
        return " OR ".join(query_list)
    
    def generate_select_query_in(self, tablename, pk_range):    
        
        if self.col_map[tablename] is None:
            self.log.error("Table Schema Not Exists : %s", tablename)
            return None
        
        if len(pk_range) > 10000:
            query = """
    SELECT 
        %s 
    FROM 
        %s (nolock) 
    WHERE 
        %s  
    """ % ( ",".join([nm for nm in self.col_map[tablename].keys()]) 
            , tablename
            , self.generate_where_query_in(self.pk_map[tablename], pk_range) ) 
        else:
            query = """
    SELECT 
        %s 
    FROM 
        %s (nolock) 
    WHERE 
        %s in (%s) 
    """ % ( ",".join([nm for nm in self.col_map[tablename].keys()]) 
            , tablename
            , self.pk_map[tablename]
            , ",".join([str(i) for i in pk_range])) 

        self.log.debug("SQL : %s", query)
        return query          

    def retrive_table_metadata_query(self, tablename):
        return self.meta.get(self.meta.tableName == tablename) 

    def select_datalist(self, tablename, pk_range):
        return self.select_datalist_inner(tablename, pk_range, self.generate_select_query)

    def select_datalist_in(self, tablename, pk_range):
        return self.select_datalist_inner(tablename, pk_range, self.generate_select_query_in)
    
    def select_datalist_in_use_hashlist(self, tablename, pk_range):
        return self.select_datalist_inner_use_hashlist(tablename, pk_range, self.generate_select_query_in)
    
    def select_datalist_inner_use_hashlist(self, tablename, pk_range, generate_callback):
        #query = self.generate_select_query(tablename, pk_range)
        query = generate_callback(tablename, pk_range)
        #cols = self.col_map[tablename].keys()
        self.conn.execute_query(query)
        datalist = []
        for row in self.conn:
            newrow = {}
            #for col in cols:
            for key in row:
                if not isinstance(key, numbers.Number):
                    newrow[key] = row[key]
            datalist.append(newrow)
        return datalist
    
    # 받은걸 csv 로 그냥 넣어버림
    def select_datalist_inner(self, tablename, pk_range, generate_callback):
        #query = self.generate_select_query(tablename, pk_range)
        query = generate_callback(tablename, pk_range)
        #cols = self.col_map[tablename].keys()
        self.conn.execute_query(query)
        
        migset = MigrationSet([])
        migset.csvfile = os.path.join(self.csvpath,"migbq-%s-%s-%s-%s" % (tablename, self.pk_map[tablename], pk_range[0], pk_range[1]))
        
        col_type_map = self.col_map[tablename]
        col_list = []
        if self.bq_table_map is not None:
            col_list = [field.name for field in self.bq_table_map[tablename].schema]
        else:
            self.log.error("BigQuery Map is none//")
            col_list = col_type_map.keys()
        
        cnt = 0
        with gzip.open(migset.csvfile, 'wb') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            for rowori in self.conn:
                row = []
                for key in col_list:
                    val = rowori.get(key)
                    
                    if col_type_map[key] == "INTEGER":
                        row.append(long(val))
                    elif col_type_map[key] == "FLOAT":
                        row.append(float(val))
                    elif col_type_map[key] == "TIMESTAMP":
                        row.append(unicode(val).encode("utf-8"))
                    else:
                        if val is None:
                            row.append( "" )
                        else:
                            row.append( unicode(val).encode("utf-8") )
                writer.writerow(tuple(row))
                cnt = cnt + 1
            
        migset.cnt = cnt
        
        return migset    
        
    
    def retrive_next_range(self, tablename, next_range, rowcnt):
        
                
        if self.pk_map[tablename] is None:
            self.log.error("Table pk Not Exists : %s", tablename)
            return next_range
                
        lastmaxpk = next_range[0]
        
        query = """
SELECT 
    min(%s) 
FROM 
    %s (nolock) 
WHERE 
    %s >= %d   
""" % ( self.pk_map[tablename] 
        , tablename
        , self.pk_map[tablename]
        , lastmaxpk) 

        self.log.debug("SQL : %s", query)
        
        minpk = self.conn.execute_scalar(query)
        
        if minpk:
            
            l = []
            for col in next_range:
                l.append(col)
            l[0] = minpk
            l[1] = minpk + self.select_size
            tt = tuple(l)
            
            self.log.info("search NEXT PK : %s", tt)
            
            return tt; 
        else:
            return next_range;
        
    def count_all(self, tablename, pkname=None):
        sql = """SELECT CAST(p.rows AS float)
FROM sys.tables AS tbl
INNER JOIN sys.indexes AS idx ON idx.object_id = tbl.object_id and idx.index_id < 2
INNER JOIN sys.partitions AS p ON p.object_id=CAST(tbl.object_id AS int)
AND p.index_id=idx.index_id
WHERE ((tbl.name=N'{tablename}'
AND SCHEMA_NAME(tbl.schema_id)='dbo'))
    """.format(tablename = tablename)
        self.log.debug("count all approximate SQL : %s", sql)
        cnt = self.conn.execute_scalar(sql)
        return cnt
    
    def count_exact(self, tablename, pk_name):
        sql = "SELECT count_big(*) from " + tablename + " (nolock) "
        self.log.debug("count all SQL : %s", sql)
        cnt = self.conn.execute_scalar(sql)
        return cnt
    
        #return query
    def count_range(self, tablename, pk_range, pk_name=None):
        if pk_name is None:
            pk_name = self.pk_map[tablename]
            if pk_name is None:
                self.log.error("Count Error = Table Schema Not Exists : %s", tablename)
                return None
        
        query = """
SELECT 
    count_big(*)
FROM 
    %s (nolock) 
WHERE 
    %s > %d  
    AND 
    %s <= %d 
""" % (  tablename
        , pk_name
        , pk_range[0]
        , pk_name
        , pk_range[1]) 

        self.log.debug("SQL : %s", query)
        cnt = self.conn.execute_scalar(query)
        return cnt
       

    def select_pk_value_list(self, tablename, pk_range, pk_name):
        if self.pk_map[tablename] is None:
                self.log.error("Count Error = Table Schema Not Exists : %s", tablename)
                return None
            
        query = """
SELECT 
    %s
FROM 
    %s (nolock) 
WHERE 
    %s > %d  
    AND 
    %s <= %d 
""" % (  pk_name
        , tablename
        , pk_name
        , pk_range[0]
        , pk_name
        , pk_range[1]) 

        self.log.info("SQL : %s", query)
        self.conn.execute_query(query)
        pklist = []
        for row in self.conn:
            pklist.append(row[pk_name])
        return pklist
    