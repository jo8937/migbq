# -*- coding: utf-8 -*-
#!/opt/local/bin/python2.7

import sys
import ujson
import os


class MigrationConfig(object):
    
    def __init__(self, config_file_path = None, confdict = None):
        
        if config_file_path:
            self.init_config_from_file(config_file_path)
        elif confdict:
            self.init_config_from_dict(confdict) 
        else:
            raise ValueError("MigrationConfig() inited from file or dict")
    
    def init_config_from_file(self, config_file_path):
        import migutils
        configdict = migutils.parse_config_file(config_file_path)
        self.source = configdict
        self.config_file_path = config_file_path
    
    def init_config_from_dict(self, configdict):
        self.source = configdict
        self.config_file_path = None
        
    def init_config(self):
        conf = self.source
        self.project = conf["out"]["project"]
        self.datasetname = conf["out"]["dataset"]
        self.dbconf = dict(server=conf["in"]["host"], 
                           port=conf["in"]["port"], 
                           user=conf["in"]["user"], 
                           password=conf["in"]["password"], 
                           database=conf["in"]["database"], 
                           charset='UTF-8')
        if os.name != "nt":
            self.dbconf["tds_version"] = "7.0"
                        
        self.listsize = conf["in"]["batch_size"]
        self.csvpath = conf["in"]["temp_csv_path"]
        self.csvpath_complete = conf["in"]["temp_csv_path_complete"]
        
        # datasource and metadata divide
        self.meta_db_type = conf["in"]["type"]
        self.meta_db_config = self.dbconf
        
    
    