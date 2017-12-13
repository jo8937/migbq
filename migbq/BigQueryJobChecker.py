#!/opt/local/bin/python2.7
# -*- coding: utf-8 -*-

import sys
import ujson
import os

from MigrationSet import *
from google.cloud import bigquery
from google.cloud.bigquery.job import _AsyncJob
import copy
import shutil
import traceback

from MigrationSet import MigrationSetJobResult
import gzip
    
def check_job_finish(migset):
    #BQ_TEMP_CSVPATH = migset.csvfile
    #BQ_TEMP_DEL_PATH = migset.csvfile_del_path
        
    datalist, tablename, pkname, pk_range, col_type_map, log_idx = migset.values()
    jobId = migset.jobId
    if jobId is None:
        print >> sys.stderr, "jobid is null %s" % log_idx

    try:
        print "start check jobId : %s" % jobId 
        bq = bigquery.Client(migset.bq_project)
        job = _AsyncJob(jobId, client=bq)
        job.reload()
        state = job._properties.get("status",{}).get("state","")
        if state == "DONE":
            errors = job._properties.get("status",{}).get("errors",[])
            if len(errors) > 0:
                # 에러
                errmsg = ujson.dumps(errors)
                print "jodIb :%s has error : %s" % (jobId,errmsg) 
                
                
                filename = "migbq-%s-%s-%s-%s" % (tablename,  pkname, pk_range[0], pk_range[1])
                csvfile = os.path.join(migset.csvfile, filename) 
                if os.path.isfile(csvfile) and os.path.exists(csvfile):
                    print "!! check job found error. delete error file : %s" % csvfile     
                    os.remove(csvfile)
            
                ret = MigrationSetJobResult(migset.log_idx, -1, msg = errmsg)
            else:
                # 성공
                rowcnt = job._properties.get("statistics",{}).get("load",{}).get("outputRows",-1)
                ret = MigrationSetJobResult(migset.log_idx, rowcnt)
                
                print "jodIb : %s success" % jobId
                
                filename = "migbq-%s-%s-%s-%s" % (tablename,  pkname, pk_range[0], pk_range[1])
                csvfile = os.path.join(migset.csvfile, filename) 
                
                print "jodIb : %s ... delete temp file : %s " % (jobId,csvfile)
                
                if os.path.isfile(csvfile):
                    del_path_file = os.path.join(migset.csvfile_del_path,os.path.basename(csvfile))
                    print "remove file ... %s to %s" % (csvfile, del_path_file)
                    shutil.move(csvfile, del_path_file)
                    
        else:
            ret =  MigrationSetJobResult(migset.log_idx, 0)
            print "jodIb :%s not DONE : %s " % (jobId, state)
    except:
        errormsg = traceback.format_exc()
        print >> sys.stderr, "jobId [%s] error : %s" % (jobId, errormsg)
        ret =  MigrationSetJobResult(migset.log_idx, -1, errormsg)
        
    return ret

def validate_gzip_csv_file_linesize(temp_filename):
    try:
        with gzip.open(temp_filename, 'rb') as f:        
            for i, l in enumerate(f):
                pass
        return True
    except:
        print "error read %s . SKIP Read FRom file and retry Select - Upload " % temp_filename
        return False


def retry_error_job(migset):
    try:
            
        datalist, tablename, pkname, pk_range, col_type_map, log_idx = migset.values()
        # george-gv_game_asset_var_origin-log_id-8125175190-8125275190

        if migset.csvfile is None:
            raise NameError("migset.csvfile is None! %s %s %s %s %s" % (tablename, pkname, pk_range, col_type_map, log_idx))
        
        if os.path.isdir(migset.csvfile):
            filename = "migbq-%s-%s-%s-%s" % (tablename,  pkname, pk_range[0], pk_range[1])
            csvfile = os.path.join(migset.csvfile, filename)
        else:
            csvfile = migset.csvfile
        
        #if os.path.isfile(csvfile) and os.path.exists(csvfile) and validate_gzip_csv_file_linesize(csvfile):
        if os.path.isfile(csvfile) and os.path.exists(csvfile):
            bq = bigquery.Client(project=migset.bq_project)
            tbl = bq.dataset(migset.bq_dataset).table(tablename)
            if tbl.exists():
                tbl.reload()
                with open(csvfile, 'rb') as fp:
                    #upload_from_file(file_obj, source_format, rewind=False, size=None, num_retries=6, allow_jagged_rows=None, allow_quoted_newlines=None, create_disposition=None, encoding=None, field_delimiter=None, ignore_unknown_values=None, max_bad_records=None, quote_character=None, skip_leading_rows=None, write_disposition=None, client=None)
                    job = tbl.upload_from_file(fp, source_format='CSV',allow_quoted_newlines=True) # allowQuotedNewlines
                print("## retry_error_job() ## [%s] Start Retry Job : %s " % (log_idx,job.name))
                return MigrationSetJobResult(log_idx, 0, jobId = job.name)
            else:
                return MigrationSetJobResult(log_idx, -2, msg = "Table Not Exists : %s" % tablename)
        else:
            print "## retry_error_job() ## ERROR : FILE NOT FOUND %s" % csvfile
            return MigrationSetJobResult(log_idx, -1, msg = "File Not Exists : %s" % csvfile)
    except:
        import traceback 
        errmsg = traceback.format_exc()
        print >> sys.stderr, "## retry_error_job() ##"
        print >> sys.stderr, errmsg
        return MigrationSetJobResult(log_idx, -2, msg = errmsg)
    
