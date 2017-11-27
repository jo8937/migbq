# migbq 

RDBMS Table data upload to Bigquery table.

## Requirement

* Python
  - CPython 2.7.x

* RDBMS (below, DB)  
  - Microsoft SQL Server
  - Mysql (development)
  
* Table Spec
  - All table must have Numeric Primary Key Field

* DB User Grant
  - SELECT, INSERT, UPDATE, CREATE
  - can access DB's metadata ([INFORMATION_SCHEMA] database) 
  - some metadata tables create in source RDBMS
  - (If you don't want create table in source, you can use sqlite. fork this project and edit source)

* Google Cloud SDK 
  - install Google Cloud SDK must be required 
    - https://cloud.google.com/sdk/downloads
    - https://cloud.google.com/sdk/gcloud/reference/auth/login

* Pymssql freetds
  - http://www.pymssql.org/en/stable/
  
## Install

```
export PYMSSQL_BUILD_WITH_BUNDLED_FREETDS=1
pip install migbq
```

## Usage

### write Configuration File

* like embulk ( http://www.embulk.org ) 

### Example 

* config.yml 

```yml
in:
  type: mssql
  host: localhost
  user: USER
  password: PASSWORD
  port: 1433
  database: DATABASE
  tables: 
    - tbl
    - tbl2
    - tbl3
  batch_size: 50000
  temp_csv_path: /temp/pymig_csv
  temp_csv_path_complete: /temp/pymig_csv_complete 
out:
  type: bigquery
  project: GCP_PROJECT
  dataset: BQ_DATASET
```

### Run  

#### (1) Execute

```bash
bqmig run config.yml
```

#### (2) Check Job Complete

```bash
bqmig check config.yml
```

### Run Forever 

* you can add crontab 
* migbq have exclusive process lock. so you can add crontab every minute. 
* you must add both **run** and **check**  

## Description

### run command

**[1]** select RDBMS table metadata 
  - get table primary key name in RDBMS metadata table.
  - get column name and type fields in RDBMS metadata table.
   
**[2]** select RDBMS Primary key value range 
  - get min / max PK of table 
  
**[3]** select data in primary key range
  - select with pk min and min + batch_size
  
```sql
	select * from tbl where idx => 1 and idx < 100;
```

  - create file **pymig-tbl-idx-1-100** 
  - gzip csv  

**[4]** upload csv file to bigquery  
  - direct upload to bigquery table. not upload to GCS (quota exceed can occur)
  
**[5]** Repeat 1~4 until over the max primary key. 

For example, batch_size : 100, max pk is 321, then rdbms query execute like below.

```sql

select * from tbl where idx => 1 and idx < 100;
select * from tbl where idx => 100 and idx < 200;
select * from tbl where idx => 200 and idx < 300;
select * from tbl where idx => 300 and idx < 400;

-- end 

```

### check command

* check bigquery jobid end. 
* retry fail job.

## loadmap

* parallel loading not supported.  
