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
    if tp.startswith(("int","bigint","tinyint","smallint")):
        fieldType = "INTEGER"
    elif tp.startswith(("float","double")):
        fieldType = "FLOAT"                    
    elif tp.startswith(("timestamp","datetime")):
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

def get_connection_info(config_file_path):
    import yaml
    with open(config_file_path,"rb") as f:
        conf = yaml.load(f)
    
    dbconfig = dict([(key,conf["in"][key]) for key in conf["in"] if key in ["host","user","password","port","database"]])
    
    if conf["in"]["type"] == "mssql":
        dbconfig["server"] = dbconfig["host"] 
        del dbconfig["host"]
        
    return dbconfig 
    
