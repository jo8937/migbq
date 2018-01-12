# -*- coding: utf-8 -*-
#!/usr/local/bin/python2.7

import sys
import os
import logging

def get_logger(name, rawData = False, timeRotate=False, dirs="log", config_file_path=None):
    log = logging.getLogger('migbq-log-' + name)

    try:
        if rawData:
            logfmt = logging.Formatter("%(message)s")
        else:
            logfmt = logging.Formatter("# %(asctime)-15s # %(message)s")
        logging.basicConfig(format="# %(message)s")

        if config_file_path:
            log_folder = os.path.join( os.path.dirname(os.path.abspath(config_file_path)), dirs )
        else:
            log_folder = os.path.join( os.path.dirname(os.path.realpath(__file__)), dirs )
        #log_filename = os.path.splitext(os.path.basename(__file__))[0] + ".log"
        log_filename = name + ".log"

        if not os.path.exists(log_folder):
            os.makedirs(log_folder)
            
        if timeRotate:
            from logging.handlers import TimedRotatingFileHandler as RFHandler
            concurrentHandler = RFHandler(os.path.join(log_folder,log_filename), when='H', interval=1, backupCount=100)
            concurrentHandler.suffix = "%Y-%m-%d_%H_" + str(os.getpid())
        else:
            try:
                #from concurrent_log_handler import ConcurrentRotatingFileHandler as RFHandler
                from concurrent_log_handler import ConcurrentRotatingFileHandler as RFHandler
            except ImportError:
                # Next 2 lines are optional:  issue a warning to the user
                from warnings import warn
                warn("ConcurrentLogHandler package not installed.  Using builtin log handler")
                from logging.handlers import RotatingFileHandler as RFHandler
            concurrentHandler = RFHandler(os.path.join(log_folder,log_filename), maxBytes=1024000, backupCount=100)
        
        concurrentHandler.setFormatter(logfmt)
        log.addHandler(concurrentHandler)

        #file_handler = handlers.RotatingFileHandler(os.path.join(log_folder,log_filename), maxBytes=1024000, backupCount=5)
        #file_handler.setFormatter(logfmt)
        #log.addHandler(file_handler)

        log.setLevel(logging.DEBUG)
    except:
        print "logger cannot initialized %s, %s, %s" % sys.exc_info()
    return log


def estimate_bigquery_type(tp):
    if tp.lower().startswith(("int","bigint","tinyint","smallint")):
        fieldType = "INTEGER"
    elif tp.lower().startswith(("float","double","numeric","decimal")):
        fieldType = "FLOAT"                    
    elif tp.lower().startswith(("timestamp","datetime")):
        fieldType = "TIMESTAMP"
    else:
        fieldType = "STRING"
    return fieldType

def convert_colname_with_convention(colname, col_name_convert_map={}):
    if colname in col_name_convert_map:
        return col_name_convert_map.get(colname)
    else:
        import stringcase
        return stringcase.camelcase(colname)
    
def get_config(config_file_path):
    from MigrationConfig import MigrationConfig
    conf = MigrationConfig(config_file_path)
    conf.init_config()
    return conf


def get_config_sample(conf_in=None, conf_out=None):
    from MigrationConfig import MigrationConfig
    
    defaultconf = {"in":
                      {
                      "type": "mssql",
                      "host": "127.0.0.1",
                      "user": "test",
                      "password": "test",
                      "port": 1433,
                      "database": "TEST",
                      "tables": ["persons1"],
                      "batch_size": 200,
                      "temp_csv_path": "/tmp/pymig_csv",
                      "temp_csv_path_complete": "/tmp/pymig_csv_complete"
                      }
                      ,
                      "out": 
                      {
                      "type": "bigquery",
                      "project": "bq",
                      "dataset": "test"
                      }
                  }
    if conf_in:
        defaultconf["in"].update(conf_in)
    if conf_out:
        defaultconf["out"].update(conf_out)
        
    conf = MigrationConfig(confdict = defaultconf)
    conf.init_config()
    return conf

def parse_config_file(config_file_path):
    import yaml

    if config_file_path.endswith(".j2.yml"):
        from jinja2 import Environment, FileSystemLoader
        j2 = Environment(loader=FileSystemLoader(os.path.dirname(os.path.abspath(config_file_path)))).get_template( os.path.basename(config_file_path) )
        conf = yaml.load(j2.render(env=os.environ))
    else:
        with open(config_file_path,"rb") as f:
            conf = yaml.load(f)    
        
    return conf 

def get_connection_info(config_file_path, parentkey = "in"):
    import yaml
    with open(config_file_path,"rb") as f:
        conf = yaml.load(f)
    return get_connection_info_dict(conf, parentkey)

def get_connection_info_dict(conf, parentkey = "in"):
    dbconfig = dict([(key,conf[parentkey][key]) for key in conf[parentkey] if key in ["host","user","password","port","database"]])
    
    if conf[parentkey]["type"] == "mssql":
        dbconfig["server"] = dbconfig["host"] 
        del dbconfig["host"]
        
    return dbconfig 

def union_many_config_file(config_file_basepath):
    configlist = []
    for filename in os.listdir(config_file_basepath):
        if filename.startswith("_"):
            continue
        elif filename.endswith(".j2.yml"):
            conf = parse_config_file( os.path.abspath( os.path.join(config_file_basepath, filename)) )
        elif filename.endswith(".yml"):
            conf = parse_config_file( os.path.abspath( os.path.join(config_file_basepath, filename)) )
        else:
            continue
            
        configlist.append(conf)
        
    return configlist
        
def get_all_tablenames_in_path(config_file_basepath):
    configlist = union_many_config_file(config_file_basepath)
    
    tablelist = []
    for config in configlist:
        tables = config.get("in",{}).get("tables",[])
        tablelist += tables

    return tablelist


def divide_queue_range(pk_range, batch_size=500000):
    range_list = []
    range_size = pk_range[1] - pk_range[0]
    chunk_size =  1 + ((range_size-1) / batch_size) 
    
    for i in xrange(0,chunk_size):
        lower = pk_range[0] + (i * batch_size)
        upper = pk_range[0] + ((i+1) * batch_size)
        if pk_range[1] < upper:
            upper = pk_range[1]  
        new_range = (lower, upper)
        range_list.append(new_range)
        
    return range_list
