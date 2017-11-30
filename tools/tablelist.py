#!/opt/local/bin/python2.7
# -*- coding: utf-8 -*-


import migbq
from migbq.migutils import get_all_tablenames_in_path

for tbl in get_all_tablenames_in_path("/tmp/migbq_config"):
    print tbl