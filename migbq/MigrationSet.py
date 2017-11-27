# -*- coding: utf-8 -*-
       
class MigrationSet(object):
    def __init__(self, datalist, tablename=None, pkname=None, pk_range=None, col_type_map = None, log_idx=None, update_insert_log_callback = None, jobId=None, fileName=None, conf=None):
        
        if type(datalist) == MigrationSet:
            self.datalist = datalist.datalist
            self.cnt = datalist.cnt
            self.csvfile = datalist.csvfile
        else:
            self.datalist=datalist
            self.cnt = 0
            self.csvfile = None
                    
        self.tablename=tablename
        self.pkname=pkname
        self.pk_range=pk_range
        self.col_type_map=col_type_map
        self.jobId = jobId
        self.log_idx = log_idx
        self.update_insert_log_callback = update_insert_log_callback
        self.filename = fileName
        self.csvfile_del_path = None
        self.bq_dataset = None
        self.bq_project = None
        self.finish = False
        if conf is not None:
            self.bq_dataset = conf.datasetname
    
    def complete_callback(self):
        if self.update_insert_log_callback is not None:
            self.update_insert_log_callback(self.log_idx, 0, jobId = self.jobId, checkComplete = 2)
            return True
        else:
            return False
        
    def values(self):
        return (self.datalist, self.tablename, self.pkname, self.pk_range, self.col_type_map, self.log_idx)
    
    def __str__(self):
        return "%s (%s) (%s)" % (self.tablename, self.pk_range, len(self.datalist))
     
    def __len__(self):
        if self.datalist and len(self.datalist) > 0:
            return len(self.datalist)
        else:
            return self.cnt
        
    def __iter__(self):
        if self.datalist:
            return self.datalist
        else:
            raise TypeError("MigrationSet does not contains datalist")
    
    def next(self):
        if self.datalist:
            return self.datalist.next()
        else:
            raise TypeError("MigrationSet does not contains datalist")

    
class MigrationSetJobResult(object):
    def __init__(self, log_idx, rowcnt, msg = None, jobId = None):
        self.log_idx = log_idx
        self.cnt = rowcnt
        self.errorMessage = msg
        self.jobId = jobId
    
    def __str__(self):
        return "[%s] %s (%s) ... %s " % (self.log_idx, self.jobId, self.cnt, self.errorMessage)


            