#!/opt/local/bin/python2.7
# -*- coding: utf-8 -*-

import sys
import signal
from migbq.migutils import get_logger
import datetime
import hashlib

import ujson
import os
import time
import gc

import httplib2
import csv
import numbers
import pymssql
import _mssql
import gzip

import sqlite3

# pip install peewee
from peewee import *

# pip install peewee-mssql
from peewee_mssql import MssqlDatabase  
import __main__

from migbq.migutils import *
from sys import exc_info

from playhouse.migrate import *
import logging
from playhouse.sqlite_ext import PrimaryKeyAutoIncrementField

from MigrationMetadataManager import MigrationMetadataManager 
from MigrationSet import MigrationSetJobResult
from google.cloud import bigquery
import copy
import uuid
import shutil
import yaml
import argparse
from os import getenv
from setuptools.command.setopt import config_file

class MigrationChildProcess(object):
    def __init__(self,conf):
        self.conf = conf
        
    def migset_process(self,migset):
        migset.csvfile = self.conf.csvpath
        migset.csvfile_del_path = self.conf.csvpath_complete
        migset.bq_dataset = self.conf.datasetname
        migset.bq_project = self.conf.project
        
    def run_check_job_finish(self, tablenames = None):
        from BigQueryJobChecker import check_job_finish
        mig = MigrationMetadataManager(meta_db_config = self.conf.meta_db_config, meta_db_type = self.conf.meta_db_type, tablenames = tablenames, config=self.conf)
        print "start check_job_finish() ..."
        with mig as m:
            m.check_job_finish(check_job_finish, self.migset_process)
        print "end check_job_finish()"
        
    def run_retry_error_job(self, tablenames = None):
        from BigQueryJobChecker import retry_error_job
        mig = MigrationMetadataManager(meta_db_config = self.conf.meta_db_config, meta_db_type = self.conf.meta_db_type, tablenames = tablenames, config=self.conf)
        print "start run_retry_error_job() ..."
        with mig as m:
            result_list = m.check_job_retry(retry_error_job, self.migset_process)
            
            if result_list:
                for ret in result_list:
                    # if file not found...
                    if ret[1].cnt == -1:
                        self.execure_range(ret)
            
        print "end run_retry_error_job()"
    
    def execure_range(self, migset_and_jobresult):
        migset, jobresult = migset_and_jobresult
        print "----------------------------------------------------------------"
        print "! File not found. Retry from select and download ... log idx : %s " % migset.log_idx
        from MsSqlDatasource import MsSqlDatasource
        from BigQueryForwarder import BigQueryForwarder
        conf = copy.deepcopy(self.conf)
        print conf.__dict__
        if os.name == "nt" and "use_legacy_datetime" in conf.dbconf:
            del conf.dbconf["use_legacy_datetime"]
        ds = MsSqlDatasource(conf.dbconf,
                                        meta_db_type=conf.meta_db_type,
                                        meta_db_config=conf.meta_db_config,
                                        listsize = conf.listsize,
                                        stop_when_no_more_data = True,
                                        tablenames = [migset.tablename],
                                        logname = "retryselect",
                                        config = conf 
                                        )
        
        fw = BigQueryForwarder(dataset=conf.datasetname,
                                           prefix="",
                                           csvpath = conf.csvpath,
                                           logname="retryselectforward",
                                           config = conf)
        
        ds.csvpath = conf.csvpath
        ds.log.setLevel(logging.DEBUG)
        with ds:
            with fw:
                job_idx = ds.insert_log(migset.tablename, migset.pk_range)
                ds.execute_range(migset.tablename, migset.pk_range, fw.execute, job_idx)
                q = ds.meta_log.update(checkComplete = 3, jobComplete = 2).where(ds.meta_log.idx == migset.log_idx)
                q.execute()

class BQMig(object):
    
    def __init__(self, config_path, custom_config_dict=None, custom_log_name=None, cmd=None):
        
        from MigrationConfig import MigrationConfig
        
        self.conf = MigrationConfig(config_path)
        
        print "pre config : %s" % ujson.dumps(self.conf.source, indent=4)
        
        if custom_config_dict:
            if custom_config_dict.get("in"):
                self.conf.source["in"].update(custom_config_dict.get("in"))
            if custom_config_dict.get("out"):
                self.conf.source["out"].update(custom_config_dict.get("out"))
        
        self.conf.init_config()
        
            
        #dbname = self.conf.source["in"]["database"]
        tablenames = self.conf.source["in"]["tables"]
        datasetname = self.conf.datasetname
        
        cmd = cmd or ""
        
        config_file_base_name = get_config_file_base_name(config_path)
        
        self.logname = custom_log_name or ("migbq_%s_%s_%s_%s" % (config_file_base_name , cmd, datasetname, "all" )) 
            
        if len(tablenames) > 0:
            md5key = hashlib.md5("-".join(tablenames)).hexdigest()
            self.tablenames = tablenames
            self.logname = "migbq_%s_%s_%s_%s" % (config_file_base_name, cmd, datasetname, md5key )
            self.log = get_logger(self.logname, config_file_path=config_path)
        else:
            self.tablenames = None
            self.log = get_logger(self.logname, config_file_path=config_path)
        
        if not os.path.exists(self.conf.csvpath):
            raise NameError("CVS Path [%s] not exists~!!" % self.conf.csvpath)
        
        self.log.setLevel(logging.INFO)
        self.log.info("config : %s", ujson.dumps(self.conf.source, indent=4))
        
        
        
    def init_migration(self):
        
        #from BigQueryDatasource import BigQueryDatasource 
        #from FluentdForwarder import FluentdForwarder
        from MsSqlDatasource import MsSqlDatasource
        from BigQueryForwarder import BigQueryForwarder
        
        conf = self.conf
        # c2s_game_asset_var_origin 은 5/17 들어옴
        
        self.datasource = MsSqlDatasource(conf.dbconf,
                                        meta_db_type=conf.meta_db_type,
                                        meta_db_config=conf.meta_db_config,
                                        listsize = conf.listsize,
                                        stop_when_no_more_data = True,
                                        tablenames = self.tablenames,
                                        logname = self.logname,
                                        config = conf
                                        ) 
        
        self.tdforward = BigQueryForwarder(dataset=conf.datasetname,
                                           prefix="",
                                           csvpath = conf.csvpath,
                                           logname=self.logname,
                                           config = conf)
        self.datasource.csvpath = conf.csvpath
        self.datasource.log.setLevel(logging.DEBUG)
        
        
        
        
    def start_migration(self):
        with self.datasource as ds:
            
            # update max pk
            self.log.info("....... CHECK Max PK in Metadata Tables .........")
            ds.update_last_pk_in_tablenames(self.tablenames)
            
            with self.tdforward as td:
                ds.bq_table_map = {}
                for tname in self.tablenames:
                    ds.bq_table_map[tname] = td.get_table_create_if_needed(tname, ds.col_map[tname])
                
                results = [True]
                
                self.log.info("start...")
                for tname in self.tablenames:
                    ds.execute_left_logs(tname, td.execute_async, conf = self.conf)
                    
                while any(results):
                    results = []
                    for tname in self.tablenames:
                        ret = ds.execute_next(tname, td.execute_async)
                        #ret = ds.execute_next(tname, td.execute)
                        results.append( ret )
                        
                self.log.info("finish...")
                
        self.tdforward.wait_for_queue_complete()
                
    def run_migration(self):
        self.init_migration();
        self.start_migration();
        
    
    def reset_for_debug(self):
        self.init_migration();
        if os.name == "nt":
            with self.datasource as ds:
                self.datasource.reset_table();
        else:
            print "cannot execute 'reset' in linux..."
        
    def run_migration_some_pk(self, tablename, pklist_arg):
        self.init_migration();
        pklist = [long(pk) for pk in pklist_arg]
        with self.datasource as ds:
            with self.tdforward as td:
                pk_range = (min(pklist), max(pklist))
                log_idx = ds.insert_log(tablename, pk_range)
                datalist = ds.select_datalist_in_use_hashlist(tablename, pklist)
                td.execute_streaming_api(datalist, tablename, ds.pk_map[tablename], pk_range, ds.col_map[tablename])
                self.log.info("finish...%s , %s", tablename, pklist)
                ds.update_insert_log(log_idx, len(datalist), checkComplete=999)
                
        self.tdforward.wait_for_queue_complete()
                
    def start_jobid_check_process(self):
        p = MigrationChildProcess(self.conf)
        p.run_check_job_finish()
        
    def start_jobid_check_and_retry_process(self):
        p = MigrationChildProcess(self.conf)
        p.run_check_job_finish(tablenames=self.tablenames)
        p.run_retry_error_job(tablenames=self.tablenames)
    
    def retry_error_job(self):
        p = MigrationChildProcess(self.conf)
        p.run_retry_error_job()
        
    def print_remain_days(self):
        try:
            conn = _mssql.connect(**self.conf.dbconf)
            maxidx = conn.execute_scalar("select max(idx) as mx from migrationmetadatalog")
            remain_rows = conn.execute_scalar("SELECT sum(lastPk - currentPk) FROM migrationmetadata")
            remain_rows_in_log = conn.execute_scalar("SELECT sum(case when cnt > 0 then cnt else pkUpper - pkLower end) FROM migrationmetadatalog where jobId is null or jobComplete < 0")
            remain_rows_in_log = 0 if remain_rows_in_log is None else 0
            conn.execute_query("""
            SELECT top 7 convert(varchar, enddate, 112)  as dt, sum(pkUpper - pkLower) as cnt
FROM migrationmetadatalog
group by convert(varchar, enddate, 112) 
order by dt desc  
            """)
            cntlist = []
            for row in conn:
                cntlist.append(row["cnt"])
            
            if len(cntlist) > 1:
                cntlist = cntlist[1:-1]
                
            if len(cntlist) <= 0:
                print 0
                return 0

            row_per_day = sum(cntlist) / len(cntlist)
            remain_day = (remain_rows_in_log + remain_rows) / row_per_day 
            remain_day = int(remain_day)
            print remain_day
            return remain_day
        except:
            self.log.error("remain days",exc_info=True)
        finally:
            conn.close()
            
    def print_migration_progress(self):

        try:
            conn = _mssql.connect(**self.conf.dbconf)
            conn.execute_query("select 100 * sum(currentpk) / sum(lastpk) as p from migrationmetadata")
            for row in conn:
                print row["p"]
        except:
            self.log.error("remain days",exc_info=True)
        finally:
            conn.close()
            
    def print_metadata(self):
        m = MigrationMetadataManager(meta_db_config = self.conf.meta_db_config, meta_db_type = self.conf.meta_db_type, config=self.conf)
        with m as mig:
            for row in mig.meta.select():
                print "%s,%s,%s" % (row.tableName, row.currentPk, row.lastPk)
                
    def syncdata(self,tablename):
        self.init_migration()
        with self.datasource as ds:
            with self.tdforward as f:
                ds.validate_pk_sync(tablename, f)
                        
    def diff_approximate(self):
        return self.diff("count_all")
    
    def diff_exact(self):
        return self.diff("count_exact")
    
    def diff(self, method_name="count_all"):
        self.init_migration()
        self.log.info("count diff approximate ...")
        
        results = []
        
        with self.datasource as ds:
            with self.tdforward as f:
                for tname in self.tablenames:
                     
                    pkname = ds.pk_map[tname]
                    src = long(getattr(ds,method_name)(tname, pkname))
                    dest = long(getattr(f,method_name)(tname, pkname))
                    
                    ret = {"tablename":tname, "src":src, "dest":dest, "diff": src - dest}
                    if src == dest:
                        ret["result"] = "OK"
                        self.log.info("# (ok) # [%s] cnt equals approximatly, %s", tname, src)
                    else:
                        ret["result"] = "--"
                        self.log.info("# (xx) # [%s] cnt NOT equals approximatly ............... ", tname)
                        self.log.info("SQLServer : %s", src)
                        self.log.info("BigQuery  : %s", dest)
                        self.log.info("diff      : %s", src - dest)
                    self.log.info("#####################################################")
                    results.append(ret)
                    
        for ret in results:
            self.log.info("# {result} # [{tablename}] \t{src}\t{dest}\t{diff}".format(**ret))
        
    def run_forever(self):
        try:
            self.log.info("## START RUN ... ##")
            self.run_migration()
        except:
            self.log.error("########### ERROR in Migration #######################", exc_info=True)
            self.log.error("## retry after 3 minute ... ##")
            time.sleep(180)
            self.log.error("## run again ~! ##")
            self.run_forever()
            
#########################
# 이거 나중에 많아지면 리플랙션으로 바꾸자...
#########################
def generate_lock_name(arg):
    cmd = arg.cmd
    tablenames = arg.tablenames
    dataset = arg.dataset
    return "{}_{}_{}".format(cmd,"_".join(tablenames),dataset)
    
def commander(array_command=None):
    
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", help="command", choices=('check', 'run', 'some', 'sync', 'meta', 'retry', 'run_with_no_retry'))
    parser.add_argument("config_file", help="source database info KEY (in MigrationConfig.py)")
    parser.add_argument("--tablenames", help="source table names", nargs="+", required=False)
    parser.add_argument("--dataset", help="destination bigquery dataset name", required=False)
    parser.add_argument("--lockname", help="custom lockname name", required=False)
    arg = parser.parse_args(args = array_command)

    cmd = arg.cmd
    config_file = arg.config_file
    
    migconf = get_config(config_file)
    
    custom_config_dict = {}
    
    if arg.tablenames:
        tablenames = arg.tablenames
        custom_config_dict["in"] = {}
        custom_config_dict["in"]["tables"] = arg.tablenames 
    else:
        tablenames = migconf.source["in"]["tables"]
        
    if arg.dataset:
        custom_config_dict["out"] = {}
        custom_config_dict["out"]["dataset"] = arg.dataset
    
    if os.name == "nt":
        commander_executer(cmd, arg.config_file, arg.lockname, custom_config_dict)
    else:
        # for crontab prevent duplicate process
        import fcntl
        
        md5key = hashlib.md5( "-".join(tablenames)).hexdigest()
        lockfile = "/tmp/bqmig_%s_%s_%s.pid" % (get_config_file_base_name(arg.config_file), cmd, md5key)
        try:
            print "lock file : " + lockfile
            x = open(lockfile,"w+")
            fcntl.flock(x, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as err:
            print "# process already running # > %s " % " ".join(sys.argv)
            print err
            return None
        
        commander_executer(cmd, arg.config_file, arg.lockname, custom_config_dict)
        fcntl.flock(x, fcntl.LOCK_UN)
        
def get_config_file_base_name(config_path):
    config_file_base_name = os.path.splitext(os.path.basename(config_path))[0]
    if config_file_base_name.endswith(".j2"):
        config_file_base_name = os.path.splitext(config_file_base_name)[0]
    return config_file_base_name

def commander_executer(cmd, config_file, lockname=None, custom_config_dict=None):
              
    mig = BQMig(config_file, 
                custom_config_dict = custom_config_dict if custom_config_dict and len(custom_config_dict) > 0 else None, 
               custom_log_name = lockname,
               cmd = cmd 
                )
    tablenames = mig.tablenames
    
    if cmd == "check":
        mig.start_jobid_check_and_retry_process()
    elif cmd == "retry":
        mig.retry_error_job()
    elif cmd == "remainday":
        mig.print_remain_days()
    elif cmd == "progress":
        mig.print_migration_progress() 
    elif cmd == "meta":
        mig.print_metadata()
    elif cmd == "sync":
        mig.syncdata(tablenames[0])
    elif cmd == "run":
        if len(mig.tablenames) > 0:
            mig.run_forever()
        else:
            print "select table like ... [BQMig.py mig DBNAME tablename1 tablename2] ... "
    elif cmd == "run_with_no_retry":
        mig.run_migration()
    elif cmd == "some":
        if len(tablenames) > 0:
            mig.run_migration_some_pk(mig.conf.source["in"]["pk_lower"], mig.conf.source["in"]["pk_upper"])
        else:
            print "select table like ... [BQMig.py mig DBNAME tablename1 1 10 13] ... "
    elif cmd == "approximate":
        mig.diff_approximate()
    elif cmd == "exact":
        mig.diff_exact()
    elif cmd == "reset_for_debug":
        mig.reset_for_debug();
    else:
        print "comnmand not found. select one of (check/remainday/progress/mig)... now : %s" % cmd
    
if __name__ == '__main__':
    commander()
    
