# -*- coding: utf-8 -*-
#!/opt/local/bin/python2.7

import sys
import datetime

import ujson
import os
import time
import csv
import gzip

from migbq.migutils import get_logger

import logging
from google.cloud import bigquery
from Forwarder import Forwarder

class BigQueryForwarder(Forwarder):
    def __init__(self, **options):
        super( BigQueryForwarder, self).__init__(**options)
        self.dataset_name = options.get("dataset")
        self.table_prefix = options.get("prefix","t_tmp_")
        self.table_map = {}  
        self.log = get_logger("BigQueryForwarder_" + options.get("logname",""))
        self.bq = None
        self.dataset = None
        self.lastJobId = None
        self.projectId = options["config"].project
        self.csvpath = options.get("csvpath")
        
        if self.csvpath == "": 
            default_csvpath = os.path.join( os.path.dirname(os.path.realpath(__file__)), "csvdata" )
            self.csvpath = default_csvpath 
            self.log.info("use default csv path : %s" % self.csvpath)
        
        if self.csvpath:
            if not os.path.exists(self.csvpath):
                os.mkdir(self.csvpath)
            
    def __enter__(self):
        self.bq = bigquery.Client(project=self.projectId)
        self.dataset = self.bq.dataset(self.dataset_name)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False
    
    def create_bq_schema(self, col_type_map):
        field_list  = []
        for col in col_type_map:
            tp = col_type_map[col].lower();
            
            if tp.startswith(("int","bigint","tinyint","smallint")):
                fieldType = "INTEGER"
            elif tp.startswith(("float","double")):
                fieldType = "FLOAT"                    
            elif tp.startswith(("timestamp","datetime")):
                fieldType = "TIMESTAMP"
            else:
                fieldType = "STRING"
                
            #self.log.debug("%s = %s", tp, fieldType)
            
            field = bigquery.schema.SchemaField(col, fieldType)
            field_list.append(field)
        return field_list
    
    def get_table_create_if_needed(self, tablename=None, col_type_map = None):
        tbl = self.table_map.get(tablename)
        bq_tablename = "%s%s" % (self.table_prefix, tablename)
        if tbl is None:
            tbl = self.dataset.table(bq_tablename);
            if tbl.exists():
                self.log.debug("table exists. load table schema...")
                tbl.reload()
            else:
                self.log.debug("table NOT exists. create table! ")
                #tbl.friendly_name = UPDATED_FRIENDLY_NAME
                #tbl.description = UPDATED_DESCRIPTION
                tbl.schema = self.create_bq_schema(col_type_map)
                tbl.partitioning_type = "DAY"
                
                if self.log.isEnabledFor(logging.DEBUG):
                    self.log.debug("create table schema : %s " % ujson.dumps(tbl))
                
                tbl.create()
                tbl.reload()
                self.log.debug("create table %s ... OK " %  bq_tablename)
            self.table_map[tablename] = tbl
        return tbl
    
    def save_json_data(self, tbl, datalist, tablename=None, pkname=None, pk_range=None, col_type_map = None):
        
        jobId = "migbq-%s-%s-%s-%s" % (tablename,  
                               pkname,
                               pk_range[0],
                               pk_range[1]
                               #,datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")                                   
                               ) 
        try:
            rowcnt_processed = 0
            csvfile = os.path.join(self.csvpath, jobId)
            
            with gzip.open(csvfile, 'w') as outfile:
                for row in datalist:
                    outfile.write(ujson.dumps(row))
                    rowcnt_processed = rowcnt_processed + 1
                    
            if rowcnt_processed > 0:
                return csvfile
            else:
                self.log.error( "empty rows ..." ) 
                return None
        except:
            self.log.error( "-------------------", exc_info=True)
            return None
        
    def save_csv_data(self, tbl, datalist, tablename=None, pkname=None, pk_range=None, col_type_map = None):
        
        jobId = "migbq-%s-%s-%s-%s" % (tablename,  
                               pkname,
                               pk_range[0],
                               pk_range[1]
                               #,datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")                                   
                               ) 
        
        try:
            rowcnt_processed = 0
            csvfile = os.path.join(self.csvpath, jobId)
                
            with gzip.open(csvfile, 'wb') as outfile:
                    writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                    for rowori in datalist:
                        row = []
                        for field in tbl.schema:
                            key = field.name
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
                        rowcnt_processed = rowcnt_processed + 1
                    
            if rowcnt_processed > 0:
                return csvfile
            else:
                self.log.error( "empty rows ..." ) 
                return None
        except:
            self.log.error( "-------------------", exc_info=True)
            return None
    
    def execute(self, migset):
        datalist, tablename, pkname, pk_range, col_type_map, log_idx = migset.values()
        
        # 지정된 csv 경로가 없다면 그냥 스트리밍 인서트 실행
        if self.csvpath is None:
            self.log.error("conf.csvpath is None... process execute_streaming_api")
            return self.execute_streaming_api(datalist, tablename, pkname, pk_range, col_type_map)

        if migset.csvfile is None:
            if len(datalist) > 0:
                migset.csvfile = self.save_csv_data(migset.tablename, datalist, tablename, pkname, pk_range, col_type_map)
                if migset.csvfile is None:
                    self.log.error("csvfile create fail. no data. %s" % migset.jobId)
                    return 0
                self.log.info("Lazy Memory dump to CSV. Start Insert BigQuery [%s], Upload File : %s ..." % (migset.tablename, migset.csvfile)) 
            else:
                self.log.error("Hmm... No rows present in the request To BigQuery. return 0")
                migset.jobId = ""
                # 데이터가 없는건 전송 안하지만 종료된것이기 때문에...
                ret = migset.complete_callback() # insert 실행끝났다고 알려줌...
                if not ret:
                    self.log.error("ERROR on check complate1 %s" , migset.update_insert_log_callback) 
                return 0
        
        tbl = self.get_table_create_if_needed(tablename, col_type_map)

        with open(migset.csvfile, 'rb') as fp:
            # num_retries=6, 
            # allow_jagged_rows=None, 
            # allow_quoted_newlines=None, 
            # encoding=None, 
            # ignore_unknown_values=None, 
            # max_bad_records=None, 
            # quote_character=None, 
            # skip_leading_rows=None, 
            job = tbl.upload_from_file(fp, source_format='CSV',allow_quoted_newlines=True)

        # job.begin() # 백그라운드 job 실행 시작 명령 
        self.log.info("### [%s] Start Job : %s " % (log_idx,job.name))
        self.lastJobId = job.name
        migset.jobId = job.name
        ret = migset.complete_callback() # insert 실행끝났다고 알려줌...
        if not ret:
            self.log.error("ERROR on check complate %s" , migset.update_insert_log_callback) 
        
        return 0
    
    def get_last_jobId(self):
        return self.lastJobId
    
    def execute_streaming_api(self, datalist, tablename=None, pkname=None, pk_range=None, col_type_map = None):
        
        jobId = "migbq-%s-%s-%s-%s-%s" % (tablename,  
                               pkname,
                               pk_range[0],
                               pk_range[1],
                               datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")                                   
                               ) 
        
        tbl = self.get_table_create_if_needed(tablename, col_type_map)

        if len(datalist) == 0:
            self.log.error("Hmm... No rows present in the request To BigQuery. return 0")
            return 0

        for row in datalist:
            self.filter_row(row)

        self.log.debug("start insert ... %s ... rows" % len(datalist))
                
        #rows = [tuple(row.values()) for row in datalist]
        row_ids = [row[pkname] for row in datalist]
        rows = []
        for row in datalist:
            onerow = []
            for col in col_type_map:
                onerow.append(row[col])
            rows.append( tuple(onerow) )

        try:
            packSize = 10000
            loopmax = 1 + len(datalist) / packSize
            for i in range(0, loopmax):
                lower = i * packSize
                upper = pk_range[0] + packSize
                self.log.debug("start insert streaming to bigquery %s/%s  ... %s" % (i,loopmax, datetime.datetime.now() ))
                ret = tbl.insert_data(rows[lower:upper], row_ids=row_ids[lower:upper])
        except:
            self.log.error("ERROR BigQuery Insert !!",exc_info=True)
            return -10
        #ret = tbl.insert_data(rows, row_ids=row_ids, skip_invalid_rows=True, ignore_unknown_values=True)
        
        # job.begin() # 백그라운드 job 실행 시작 명령 
        #print "start Job : %s " % jobId
        print "insert data Returns : %s " % ret
        
        return 0 

    def select_pk_value_list(self, tablename, pk_range, pk_name):
        self.log.info("START select_pk_value_list(%s,%s,%s) from BigQuery",tablename, pk_range, pk_name)
        query = """
SELECT 
    %s
FROM 
    %s.%s 
WHERE 
    %s > %d  
    AND 
    %s <= %d 
""" % (  pk_name
        , self.dataset_name
        , tablename
        , pk_name
        , pk_range[0]
        , pk_name
        , pk_range[1]) 

        self.log.debug("SQL : %s", query)
        query = self.bq.run_sync_query(query)
        query.timeout_ms = 10000
        query.max_results = self.SELECT_LIMIT
        query.run()
        
        pklist = []
        trycnt = 1
        if query.complete:
            rows = query.rows
            token = query.page_token
            
            while True:
                for row in rows:
                    pklist.append(row[0])
                    
                if token is None:
                    self.log.debug("fetch finish ")
                    break
                
                self.log.debug("[%s] start fetch next %s ... ", trycnt, token)
                rows, total_count, token = query.fetch_data(page_token=token)       # API request
                self.log.debug("[%s] fetch result : total_count : %s", trycnt, total_count)
                trycnt = trycnt + 1
                
            return pklist
        else:
            self.log.error("select_pk_value_list() query not end!")
            return []   
         
    ######################### 결과 한줄만 리턴하는 빅쿼리..
    def query_one_row(self, sql):
        self.log.debug(sql)
        
        query = self.bq.run_sync_query(sql)
        query.timeout_ms = 300000 
        query.max_results = 1
        query.run()
        
        if not query.complete:
            job = query.job
            job.reload()                          # API rquest
            retry_count = 0
            
            while retry_count < 10 and job.state != u'DONE':
                second = 2**retry_count
                self.log.info("jod : %s (%s) ... not end ... wait for %s seconds ... ", job.name, job.state, second)
                time.sleep(second)      # exponential backoff
                retry_count += 1
                job.reload()                      # API request
    
        if query.errors:
            self.log.error("query not end. state : %s, BQ Error : %s", query.state, query.errors)
            return None
        
        #query.page_token is not None
        #len(query.rows) == PAGE_SIZE
        #rows, total_count, token = query.fetch_data(page_token=token)       # API request
        rows = query.rows
        #token = query.page_token
        self.log.info("rows : %s", rows)
        return rows[0]
        
    def count_exact(self, tablename, pk_name):
        sql = """
SELECT count(*)
FROM (
  SELECT
      {pk}
  FROM {dataset}.{tablename}  
  group by {pk} 
)
""".format(pk = pk_name, dataset = self.dataset_name, tablename = tablename) 

        self.log.debug("BigQuery Count SQL by GroupBy ... ")
        row = self.query_one_row(sql)
        if row:
            return row[0]
        else:
            self.log.error("query not end...!")
            return -1
        
    def count_range(self, tablename, pk_range, pk_name):
        sql = """
SELECT count(*)
FROM (
  SELECT
      %s,
      ROW_NUMBER()
          OVER (PARTITION BY %s)
          row_number,
  FROM %s.%s  
  where 
    %s > %d  
    AND 
    %s <= %d 
)
WHERE row_number = 1 

""" % ( pk_name, pk_name, self.dataset_name, tablename
        , pk_name
        , pk_range[0]
        , pk_name
        , pk_range[1]) 

        self.log.debug("BigQuery Range Count SQL : %s",sql)
        row = self.query_one_row(sql)
        if row:
            return row[0]
        else:
            self.log.error("query not end...!")
            return -1
            
    def count_all(self, tablename, pkname):
        pkrange = self.retrive_pk_range_in_table(tablename, pkname)
        return pkrange[2]
            
    def retrive_pk_range_in_table(self, tablename, pkname):
        #sql = "select max(%s) as mx, min(%s) as mn, count(*) as cnt from %s.%s " % (pkname, pkname, self.dataset_name, tablename)
        sql = sql = """
SELECT max({pk}) as mx, min({pk}) as mn, count({pk}) as cnt
FROM (
  SELECT
      {pk},
      ROW_NUMBER()
          OVER (PARTITION BY {pk})
          row_number,
  FROM {dataset}.{tablename}  
)
WHERE row_number = 1 

""".format(**{"pk":pkname, "dataset":self.dataset_name, "tablename":tablename}) 

        self.log.debug("############# get max(PK) and min(PK) ###############")

        row = self.query_one_row(sql)
        if row:
            self.log.info("row : %s", row)
            return (row[1], row[0], row[2])
        else:
            return (0,0,0)
        
    def check_job_complete(self, jobId):
        return self.get_job_state(jobId) == "DONE"
    
    def get_job_state(self, jobId):
        job = _AsyncJob(jobId, client=self.bq)
        job.reload()
        state = job._properties.get("status",{}).get("state","")
        return state     
         

