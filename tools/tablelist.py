#!/opt/local/bin/python2.7
# -*- coding: utf-8 -*-


import migbq
from migbq.migutils import get_all_tablenames_in_path
from migbq.BQMig import BQMig
from os import getenv

for tbl in get_all_tablenames_in_path("/tmp/migbq_config"):
    print tbl
    
#mig = BQMig(getenv("pymig_config_path_jinja"), cmd = "estimate_datasource_per_day")
#print mig.estimate_rows_per_days()
