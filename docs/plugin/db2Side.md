## 1.格式：

通过建表语句中的` PERIOD FOR SYSTEM_TIME`将表标识为维表，其中`PRIMARY KEY(keyInfo)`中的keyInfo，表示用来和源表进行关联的字段，
  维表JOIN的条件必须与`keyInfo`字段一致。

```sql
  通过建表语句中的` PERIOD FOR SYSTEM_TIME`将表标识为维表，其中`PRIMARY KEY(keyInfo)`中的keyInfo，表示用来和源表进行关联的字段，
  维表JOIN的条件必须与`keyInfo`字段一致。
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type='db2',
     url='jdbcUrl',
     userName='dbUserName',
     password='dbPwd',
     tableName='tableName',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
  );
```

## 2.支持版本

 db2 9.X

## 3.表结构定义

 [维表参数信息](docs/plugin/sideParams.md)

db2独有的参数

| 参数名称 | 含义               | 是否必填 | 默认值 |
| -------- | ------------------ | -------- | ------ |
| type     | 维表类型， db2     | 是       |        |
| url      | 连接数据库 jdbcUrl | 是       |        |
| userName | 连接用户名         | 是       |        |
| password | 连接密码           | 是       |        |

## 4.参数
  
  
 [维表参数信息](docs/plugin/sideParams.md)
 db2独有的参数配置：
  
 |参数名称|含义|
 |----|---|
| type | 维表类型， db2 |是||
| url | 连接数据库 jdbcUrl |是||
| userName | 连接用户名 |是||
| password | 连接密码|是||
| schema | 表所属scheam|否||
 

### ALL全量维表定义

```sql
// 定义全量维表
CREATE TABLE sideTable(
    id INT,
    name VARCHAR,
    PRIMARY KEY(id) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='db2',
    url ='jdbc:db2://172.16.8.104:50000/test?charset=utf8',
    userName ='dtstack',
    password ='abc123',
    tableName ='all_test_db2',
    cache ='ALL',
    cacheTTLMs ='60000',
    parallelism ='1'
 );
```
### LRU异步维表定义

## 4.样例

```sql
create table sideTable(
    channel varchar,
    xccount int,
    PRIMARY KEY(channel),
###  ALL全量维表定义
```
 // 定义全量维表
CREATE TABLE sideTable(
    id INT,
    name VARCHAR,
    PRIMARY KEY(id) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='db2',
    url='jdbc:db2://172.16.10.251:50000/mqTest',
    userName='DB2INST1',
    password='abc123',
    tableName='lru_test_db2',
    tableName='USER_INFO2',
    schema = 'DTSTACK'
    cache ='ALL',
    cacheTTLMs ='60000',
    parallelism ='2'
 );

```
### LRU异步维表定义

```
CREATE TABLE sideTable(
    id INT,
    name VARCHAR,
    PRIMARY KEY(id) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='db2',
    url='jdbc:db2://172.16.10.251:50000/mqTest',
    userName='DB2INST1',
    password='abc123',
    tableName='USER_INFO2',
    schema = 'DTSTACK'
    partitionedJoin ='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    asyncPoolSize ='3',
    parallelism ='2'
 );
```

```


### DB2异步维表关联输出到Console
```

CREATE TABLE source1 (
    id int,
    name VARCHAR
)WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='mqTest03',
    timezone='Asia/Shanghai',
    topicIsPattern ='false'
 );



CREATE TABLE source2(
    id int,
    address VARCHAR,
    PERIOD FOR SYSTEM_TIME
)WITH(
    type='db2',
    url='jdbc:db2://172.16.10.251:50000/mqTest',
    userName='DB2INST1',
    password='abc123',
    tableName='USER_INFO2',
    schema = 'DTSTACK',
    batchSize = '1'
);

### Db2异步维表关联

```sql
CREATE TABLE MyTable(
    id int,
    name varchar
 )WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='cannan_yctest01',
    timezone='Asia/Shanghai',
    enableKeyPartitions ='false',
    topicIsPattern ='false',
    parallelism ='1'
 );

CREATE TABLE MyResult(
    id INT,
    name VARCHAR
 )WITH(
    type='db2',
    url='jdbc:db2://172.16.8.104:50000/test?charset=utf8',
    userName='dtstack',
    password='abc123',
    tableName ='test_db2_zf',
    updateMode ='append',
    parallelism ='1',
    batchSize ='100',
    batchWaitInterval ='1000'
 );

CREATE TABLE sideTable(
    id INT,
    name VARCHAR,
    PRIMARY KEY(id) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='db2',
    url='jdbc:db2://172.16.8.104:50000/test?charset=utf8',
    userName='dtstack',
    password='abc123',
    tableName ='test_db2_10',
    partitionedJoin ='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    asyncPoolSize ='3',
    parallelism ='1'
 );
CREATE TABLE MyResult(
    id int,
    name VARCHAR,
    address VARCHAR,
    primary key (id)
)WITH(
    type='console'
);

insert into MyResult
select 
	s1.id,
	s1.name,
	s2.address
from 
	source1 s1
left join
	source2 s2
on 	
	s1.id = s2.id

```

DB2维表字段信息

```aidl
-- DTSTACK.USER_INFO2 definition

CREATE TABLE "DTSTACK "."USER_INFO2"  (
		  "ID" INTEGER , 
		  "NAME" VARCHAR(50 OCTETS) , 
		  "ADDRESS" VARCHAR(50 OCTETS) )   
		 IN "USERSPACE1"  
		 ORGANIZE BY ROW
 ;

GRANT CONTROL ON TABLE "DTSTACK "."USER_INFO2" TO USER "DB2INST1" 
;
```

insert   
into
    MyResult
    select
        m.id,
        s.name     
    from
        MyTable  m    
    join
        sideTable s             
            on m.id=s.id;

```
维表数据：(1001,maqi,hz)

源表数据：{"name":"maqi","id":1001}


输出结果： (1001,maqi,hz)