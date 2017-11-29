# migbq 

RDBMS 테이블에서 Bigquery 테이블로 데이터를 조금씩 이관하는 툴 .


## 전재 조건

* Python
  - CPython 2.7.x

* RDBMS (이하 DB)  
  - Microsoft SQL Server
  - Mysql (개발 중)
  
* 테이블 형태
  - 모든 테이블은 숫자형 PK 를 갖고있어야한다.

* DB 유저 권한 
  - 해당 DB에 메타데이터 테이블과 로그 테이블 생성 권한이 있어야한다.
  - (없을 경우 sqlite 에 기록도 가능하지만, 소스상에서 설정을 수정해야함)

* Google Cloud SDK 설치
  - 실행할 머신에 Google Cloud SDK 설치 필요
    - https://cloud.google.com/sdk/downloads
    - https://cloud.google.com/sdk/gcloud/reference/auth/login
    
* Pymssql 환경변수
  - http://www.pymssql.org/en/stable/
   
## 설치


```
export PYMSSQL_BUILD_WITH_BUNDLED_FREETDS=1
pip install pymig
```

## 사용법

### 설정파일 만들기

* embulk 의 설정형식을 따라 함.

### 예제

#### 일반 설정파일 

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

#### jinja2 template 형식

* config.j2.yml
 - jinja2 template 도 지원함. variable 은 현재 환경변수 (env) 만 지원함
 - .j2.yml 로 끝나는 확장자만 지원.

```yml
in:
  type: mssql
{% include "mssql-connect.yml" %}
  tables: 
    - tbl
    - tbl2
    - tbl3
  batch_size: 50000
  temp_csv_path: /temp/pymig_csv
  temp_csv_path_complete: /temp/pymig_csv_complete 
out:
  type: bigquery
  project: {{ env.GCP_PROJECT }}
  dataset: BQ_DATASET
```

### 실행  

* 이관 

```bash
pymig mig config.yml
```

* job 확인

```bash
pymig check config.yml
```

* 최종적으로 count 가 동일한지 확인 

```bash
pymig sync config.yml
```

### 지속적으로 업로드가 필요하다면 crontab 에 걸어 놓기

* 내부적으로 중복 프로세스 실행은 방지하므로 1분마다 크론텝 걸어도 됨.
* 다만 mig 와 check 커맨드를 같이 걸어주어야 함

## 동작방식

### mig 커맨드의 동작 순서

**[1]** RDBMS 테이블 메타데이터 select
  - RDB 에서 테이블 metadata 를 불러와서 테이블 목록, 컬럼-타입목록, Primary Key 정보를 가져옴.
   
**[2]** RDBMS 테이블 데이터 범위 select
  - 해당 Primary Key 의 min / max 를 구함. 
  
**[3]** RDBMS 데이터를 임시 csv 파일에 저장
  - 해당 PK 를 조건에 넣고 0부터 batch_size 만큼  select 하고, 그 결과를 csv 에 저장.
  
```sql
	select * from tbl where idx => 1 and idx < 100;
```

  - 이것을 pymig-tbl-idx-1-100 이라는 임시 압축된 csv 파일을 만들어서 임시 저장 (경로는 설정파일에)  

**[4]** 임시파일을 빅쿼리에 업로드 
  - (3) 에서 나온 압축 csv 파일을 빅쿼리 테이블에 upload
  - GCS 임시파일은 거치지 않음. (quota exceed 가 뜰 수 있음)
  
**[5]** 위의 과정으로 Primary Key 값이 max 가 될때까지 반복 

예를들면 batch_size : 100, max pk 가 321 일 경우, 이하와 같은 실행 순서를 가진다.
이 경우 실행 순서.

```sql

select * from tbl where idx => 1 and idx < 100;
select * from tbl where idx => 100 and idx < 200;
select * from tbl where idx => 200 and idx < 300;
select * from tbl where idx => 300 and idx < 400;

-- end 

```

### check 커맨드 동작순서

* mig 명령어로 실행된 구글 빅쿼리의 jobId 를 체크함. (기본적으로 프로세스 8개로 동시 체크)
* 빅쿼리 입력 실패한 데이터가 있다면 업로드 재시도. (병렬이 아니므로 느림)


### 로그 파일

* 프로그램 로그파일은 설정파일이 있는 디랙토리 이하의 log 폴더를 생성해서 만들어짐. 

### Pid file of program

* pid 파일은 /tmp 이하에 생성되는데, crontab 에 걸릴 경우를 대비하여 exclusive file lock 을 수행.

## 로드맵

* select 문을 N 개로 병렬 실행 가능하도록 변경 예정. 