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
from migbq import migutils
import uuid
from sys import exc_info
from google.cloud.exceptions import NotFound

class BigQueryForwarder(Forwarder):
    def __init__(self, **options):
        super( BigQueryForwarder, self).__init__(**options)
        self.dataset_name = options.get("dataset")
        self.table_prefix = options.get("prefix","t_tmp_")
        self.table_map = {}  
        self.log = get_logger("BigQueryForwarder_" + options.get("logname",""), config_file_path=options["config"].config_file_path)
        self.bq = None
        self.dataset = None
        self.lastJobId = None
        self.projectId = options["config"].project
        self.csvpath = options.get("csvpath")
        self.temp_dataset_tablenames = set([])
        self.RETRY_INTERVAL_SECOND = 10
        self.RETRY_LIMIT = 60 
        
        
        if self.csvpath == "": 
            default_csvpath = os.path.join( os.path.dirname(os.path.realpath(__file__)), "csvdata" )
            self.csvpath = default_csvpath 
            self.log.info("use default csv path : %s" % self.csvpath)
        
        if self.csvpath:
            if not os.path.exists(self.csvpath):
                os.mkdir(self.csvpath)
                
        self.conf = options["config"]
        self.temp_dataset_name = self.conf.source.get("out",{}).get("temp_dataset", self.dataset_name)
        self.sync_accept_size = self.conf.source.get("out",{}).get("sync_accept_size",1000000)

    def __enter__(self):
        self.bq = bigquery.Client(project=self.projectId)
        self.dataset = self.bq.dataset(self.dataset_name)
        self.temp_dataset = self.bq.dataset(self.temp_dataset_name) 
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.log.info("exit finally block for BigqueryForwarder...")
        super( BigQueryForwarder, self).send_fin_message()
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
    
    def get_table_field_and_type_list(self, tablename=None, src_col_type_map = None):
        tbl = self.get_table_create_if_needed(tablename, src_col_type_map)
        return [(field.name, field.field_type) for field in tbl.schema]
        
    def get_table_create_if_needed(self, tablename=None, col_type_map = None):
        tbl = self.table_map.get(tablename)
        bq_tablename = "%s%s" % (self.table_prefix, tablename)
        if tbl is None:
            tbl = self.dataset.table(bq_tablename);
            if tbl.exists():
                self.log.debug("table exists. load table schema...")
                tbl.reload()
                self.check_and_auto_add_field(tbl, col_type_map)
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
    
    def check_and_auto_add_field(self, bqtbl, col_type_map):
        #bigquery_tabletbl
        #col_type_map
        #NEW_SCHEMA = bqtbl.schema[:]
    
        fields = [] 
        bqcols = [field.name for field in bqtbl.schema] 
        for col, tp in col_type_map.items():
            if col not in bqcols:
                self.log.info("field not exists in bigquery %s (%s)", col, tp)
                fields.append(bigquery.schema.SchemaField(col, migutils.estimate_bigquery_type(tp), description="add %s" % datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S") ))
                
        if fields and len(fields) > 0:
            fields = bqtbl.schema[:] + fields
            bqtbl.schema = fields
            bqtbl.update()
            bqtbl.reload()
            self.log.info("table_map updated %s", bqtbl.name)
            self.table_map[bqtbl.name] = bqtbl
        
    
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
                                                        
                            if col_type_map.get(key) == "INTEGER":
                                row.append(long(val))
                            elif col_type_map.get(key) == "FLOAT":
                                row.append(float(val))
                            elif col_type_map.get(key) == "TIMESTAMP":
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
                #self.log.error("migset.csvfile is None and datalist exists ... process execute_streaming_api")
                #return self.execute_streaming_api(datalist, tablename, pkname, pk_range, col_type_map)
                tbl = self.bq.dataset(self.dataset_name).table(migset.tablename)
                tbl.reload()
                migset.cnt = len(datalist)
                migset.csvfile = self.save_csv_data(tbl, datalist, tablename, pkname, pk_range, col_type_map)
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

        if migset.cnt == 0:
            self.log.error("# ! # Row count is 0 in TABLE [%s] %s ~ %s" , tablename, pk_range[0], pk_range[1])
            migset.jobId = ""
            ret = migset.complete_callback() # insert 실행끝났다고 알려줌...
            if not ret:
                self.log.error("ERROR on check complate2 %s" , migset.update_insert_log_callback) 
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

    def select_table_data_col_value(self,datasetname,tablename,colname):
        tbl = self.bq.dataset(datasetname).table(tablename)
        tbl.reload()
        target_index = -1
        values = []
        next_page_token = None
        while True:
            res = tbl.fetch_data(max_results=10000, page_token = next_page_token)
            
            if target_index < 0:
                target_index = 0
                for f in res.schema:
                    if f.name == colname:
                        break
                    target_index += 1   
            
            for row in res:
                values.append(row[target_index])
                
            next_page_token = res.next_page_token
            if next_page_token is None:
                break

        return values
            
            
    def select_pk_value_list(self, tablename, pk_range, pk_name):
        
        temp_table = self.get_range_tablename(tablename, pk_range)
        # 먼저 temp table 이 있는지 조회.
        if self.bq.dataset(self.temp_dataset_name).table(temp_table).exists():
            pklist = self.select_table_data_col_value(self.temp_dataset_name, temp_table, pk_name)
            self.log.info("select tabledata in %s %s ::: size : %s",temp_table, pk_name, len(pklist))
            return pklist
        else:
            self.log.error("NOT Exists Temptable %s.%s", self.temp_dataset_name, temp_table)
            
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
        query.timeout_ms = 120000
        query.max_results = self.sync_accept_size
        query.run()
        
        pklist = []
        if query.complete:
            total_count = query.total_rows
            self.log.debug("SQL result count : %s", total_count)
            rows = query.fetch_data()
            for row in rows:
                pklist.append(row[0])
                
            return pklist
        else:
            self.log.error("select_pk_value_list() query not end!")
            return []   
         
    ######################### 결과 한줄만 리턴하는 빅쿼리..
    def query_standard(self, sql):
        self.log.debug(sql)
        jobId = "migbq-fd-" + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f") + "-" + str(uuid.uuid4())
        job = self.bq.run_async_query(jobId,sql)
        job.timeout_ms = 300000
        job.max_results = 1
        job.use_legacy_sql = False 
        job.begin()
        jobList = self.wait_for_jobids([jobId])
        res = jobList[0]
        errors = res._properties.get("status",{}).get("errors",[])
        if errors:
            self.log.error("(!) query error # %s # %s", sql, errors)
            return False
        return True

    def get_job(self, jobId, bq):
        from google.cloud.bigquery.job import _AsyncJob, QueryJob
        job = _AsyncJob(jobId, client=bq)
        self.execute_with_retry(lambda : job.reload())        
        return job
    
    def execute_with_retry(self, func):
        for i in xrange(0, self.RETRY_LIMIT):
            try:
                func()
                break
            except:
                self.log.error("error occur, retry after %s second (%s)" % (self.RETRY_INTERVAL_SECOND, i),exc_info=True)
            time.sleep(self.RETRY_INTERVAL_SECOND)    
    
    def wait_for_jobids(self,jobIdList):
        bq = self.bq
        remainJobIdSet = set(jobIdList)
        resultList = []
        cnt = 1
        print "waiting job finish..."
        while True:
            jobList = []
            for jobId in remainJobIdSet:
                job = self.get_job(jobId, bq)
                jobList.append( job )
            
            for job in jobList:
                if job.state == 'DONE':
                    remainJobIdSet.remove(job.name)
                    resultList.append( job )
                if job.error_result:
                    print job.errors
    
            if len(remainJobIdSet) == 0:
                break
            else:
                time.sleep(5)
                print "%s\r" % cnt
                cnt = cnt + 1
                
        print "all ok"
        
        return jobList
        
    ######################### 결과 한줄만 리턴하는 빅쿼리..
    def query_one_row(self, sql, use_legacy=None):
        self.log.debug(sql)
        
        query = self.bq.run_sync_query(sql)
        query.timeout_ms = 300000 
        #query.timeout_ms = 3000
        query.max_results = 1
        
        if use_legacy:
            query.use_legacy_sql = use_legacy 
        
        query.run()
        
        if query.complete:
            rows = query.rows
        else:
            job = query.job
            job.reload()                          # API rquest
            retry_count = 0
            
            while retry_count < 10 and job.state != u'DONE':
                second = 2**retry_count
                self.log.info("jod : %s (%s) ... not end ... wait for %s seconds ... ", job.name, job.state, second)
                time.sleep(second)      # exponential backoff
                retry_count += 1
                job.reload()                      # API request
    
            query = job.query_results()
            rows = query.rows
    
        if query.errors:
            self.log.error("query not end. state : %s, BQ Error : %s", query.state, query.errors)
            return None
        
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
    
    def count_range(self, tablename, pk_range, pk_name, parent_pk_range=None):
        forward_cnt = -1
        for i in xrange(0,self.RETRY_LIMIT):
            try:
                forward_cnt = self.count_range_inner(tablename, pk_range, pk_name, parent_pk_range)
                break
            except:
                self.log.error("error bq count range. wait %s second ... and retry (%s)" % (self.RETRY_INTERVAL_SECOND,i), exc_info=True)
                time.sleep(self.RETRY_INTERVAL_SECOND)
                
        if forward_cnt < 0:
            raise ValueError("Bigquery count_range error. maybe connection error. check log")
        
        return forward_cnt
        
    def count_range_inner(self, tablename, pk_range, pk_name, parent_pk_range):
        
        if parent_pk_range is None:
            parent_pk_range = pk_range

        # 대상 테이블네임
        temp_tablename = self.get_range_tablename(tablename, pk_range)
        # 상위 테이블네임
        parent_tablename = self.get_range_tablename(tablename, parent_pk_range)
            
        # 먼저 temp table 이 있는지 조회.
        range_cnt = self.count_temp_range_table(self.temp_dataset_name, temp_tablename)
        if range_cnt >= 0:
            return range_cnt
        
        if not self.bq.dataset(self.temp_dataset_name).table(parent_tablename).exists():
            sql = self.generate_pk_select_insert_sql(self.dataset_name, tablename, parent_pk_range, pk_name)
            self.log.debug("[Make Origin] BigQuery Range Select Insert SQL : %s",sql)
            # 최상위 중복 제거 테이블 만듬. 
            self.select_insert(sql, self.temp_dataset_name, parent_tablename)

        # 상위와 하위 지정이 같다면... 그냥 상위 테이블카운트 위에서 했으니까 그냥 리턴.
        if pk_range == parent_pk_range:
            range_cnt = self.count_temp_range_table(self.temp_dataset_name, parent_tablename)
            if range_cnt >= 0:
                self.log.info("return parent table count. that equals to temp_tablename ")
                return range_cnt
            
        if not self.bq.dataset(self.temp_dataset_name).table(temp_tablename).exists():
            sql = self.generate_pk_select_insert_sql(self.temp_dataset_name, parent_tablename, pk_range, pk_name)
            # 상위테이블에서 일부를 select 해서 범위임시테이블에 넣음.
            self.log.debug("BQ Range Select Insert SQL : %s",sql)
            self.select_insert(sql, self.temp_dataset_name, temp_tablename)
        #self.log.info("pk_range == parent_pk_range. maybe first time of [sync] command. %s %s" % (tablename, pk_range) )
            
        # 결과 임시테이블에서 select
        range_cnt = self.count_temp_range_table(self.temp_dataset_name, temp_tablename)
        if range_cnt >= 0:
            self.temp_dataset_tablenames.add(temp_tablename)
            return range_cnt
        else:
            raise ValueError("select insert FAIL :: %s.%s -> %s.%s" % (self.dataset_name, tablename, self.temp_dataset_name, temp_tablename))

    def clear_temp_data(self):
        self.clear_temp_sync_tables()
        
    def clear_temp_sync_tables(self):
        for tname in self.temp_dataset_tablenames:
            self.log.info("execute remmove table %s.%s", self.temp_dataset_name, tname)
            self.bq.dataset(self.temp_dataset_name).table(tname).delete()
        self.temp_dataset_tablenames.clear()
    
    def clear_all_temp_sync_table(self, tablename):
        tables = self.bq.dataset(self.temp_dataset_name).list_tables(max_results=65536)
        for t in tables:
            if t.name.startswith("migbqtmp_%s_%s_" % (self.dataset_name, tablename)):
                self.temp_dataset_tablenames.add(t.name)
        self.clear_temp_sync_tables()
        
    def clear_all_temp_sync_table_in_dataset(self):
        tables = self.bq.dataset(self.temp_dataset_name).list_tables(max_results=65536)
        for t in tables:
            self.temp_dataset_tablenames.add(t.name)
        self.clear_temp_sync_tables()
            
    def select_insert(self, sql, datasetname, desttable):
        jobId = "migbq-" + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f") + "-" + str(uuid.uuid4()).replace("-","")
        job = self.bq.run_async_query(jobId,sql) # QueryJob
        job.destination = self.bq.dataset(datasetname).table(desttable)
        job.use_legacy_sql = True
        job.write_disposition = 'WRITE_TRUNCATE'
        job.create_disposition = 'CREATE_IF_NEEDED'
        job.begin() # 백그라운드 job 실행 시작 명령 
        print "start select insert Job : %s " % jobId
        self.wait_for_jobids([jobId])
        return jobId

    def generate_pk_select_insert_sql_not_dedup(self, datasetname, tablename, pk_range, pk_name):
        sql = """
  SELECT
      {pkname}
  FROM {dataset}.{tablename}  
  where 
    {pkname} > {lower}  
    AND 
    {pkname} <= {upper}
""".format(pkname=pk_name, 
           tablename = tablename, 
           dataset = datasetname, 
           lower = pk_range[0], 
           upper = pk_range[1]) 
        return sql

    def generate_pk_select_insert_sql(self, datasetname, tablename, pk_range, pk_name):
        sql = """
SELECT {pkname}
FROM (
  SELECT
      {pkname},
      ROW_NUMBER()
          OVER (PARTITION BY {pkname})
          row_number,
  FROM {dataset}.{tablename}  
  where 
    {pkname} > {lower}  
    AND 
    {pkname} <= {upper}
)
WHERE row_number = 1 

""".format(pkname=pk_name, 
           tablename = tablename, 
           dataset = datasetname, 
           lower = pk_range[0], 
           upper = pk_range[1]) 
        return sql
        
    def get_range_tablename(self,tablename, pk_range):
        if pk_range and len(pk_range) > 1 and pk_range[0] >= -1 and pk_range[1] >= 0:
            return "migbqtmp_%s_%s_%s_%s" % (self.dataset_name, tablename, pk_range[0] + 1, pk_range[1])
        else:
            self.log.error("invalid pk_range ...")
            self.log.error(pk_range)
            return None
     
    def count_temp_range_table(self, datasetname, rangetablename):
        tbl = self.bq.dataset(datasetname).table(rangetablename)
        try:
            tbl.reload()
            return tbl.num_rows
        except NotFound as e:
            return -1
        
    def count_range_real(self, tablename, pk_range, pk_name):
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
         

