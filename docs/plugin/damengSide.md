
## 1.格式：

  通过建表语句中的` PERIOD FOR SYSTEM_TIME`将表标识为维表，其中`PRIMARY KEY(keyInfo)`中的keyInfo，表示用来和源表进行关联的字段，
  维表JOIN的条件必须与`keyInfo`字段一致。  
  
  注意：dameng维表使用的字段大小写，需要和dameng中定义的保持一致。
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type='dameng',
     url='jdbcUrl',
     userName='dbUserName',
     password='dbPwd',
     tableName='tableName',
     cache ='LRU',
     schema = 'MQTEST',
     parallelism ='1',
     partitionedJoin='false'
  );
```

# 2.支持版本
 dameng7
 
## 3.表结构定义
   [维表参数](sideParams.md)
    
   |参数名称|含义|
   |----|---|
   | tableName| dameng表名称|
   | colName | 列名称|
   | colType | 列类型 [colType支持的类型](../colType.md)|
   
## 4.dameng独有的参数配置：
   
   |参数名称|含义|是否必填|默认值|
   |----|---|---|----|
   | type | 维表类型， dameng |是||
   | url | 连接数据库 jdbcUrl |是||
   | userName | 连接用户名 |是||
   | password | 连接密码|是||
   | schema| 表空间|否||

## 5.样例

###  ALL全量维表定义
```
CREATE TABLE sideTable(
    ID char(20),  //  dameng定义了char(20)
    NAME varchar,
    PRIMARY KEY (ID),
    PERIOD FOR SYSTEM_TIME 
 )WITH(
    type='dameng',
    url =' jdbc:dm://172.16.8.178:5236/DMTEST?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=utf-8',
    userName = 'SYSDBA', 
    password = 'SYSDBA', 
    tableName = 'ID_NAME_MESSAGE',
    schema = 'TIEZHU',
    cache = 'ALL',
    cacheTTLMs ='60000'
 );
```

### LRU异步维表定义

```
create table sideTable(
    channel char,
    xccount int,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='dameng',
    url =' jdbc:dm://172.16.8.178:5236/DMTEST?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=utf-8',
    userName='xx',
    password='xx',
    tableName='sidetest',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    cacheMode='unordered',
    asyncCapacity='1000',
    asyncTimeout='10000'
    parallelism ='1',
    partitionedJoin='false',
    schema = 'MQTEST'
 );

```

### dameng异步维表关联

```
CREATE TABLE MyTable(
    id varchar,
    name varchar
    --ts timestamp,
    --tsDate Date
 )WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='mqTest01',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );


CREATE TABLE sideTable(
    ID char(20),
    NAME varchar,
    PRIMARY KEY (ID),
    PERIOD FOR SYSTEM_TIME 
 )WITH(
    type='dameng',
    url =' jdbc:dm://172.16.8.178:5236/DMTEST?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=utf-8',
    userName = 'SYSDBA', 
    password = 'SYSDBA', 
    tableName = 'SIDETEST1',
    --schema = 'dtstack',
    cache = 'LRU',
    asyncPoolSize ='3'
 );


CREATE TABLE MyResult(
    NAME varchar,
    ID char(20),
    PRIMARY KEY (ID)
 )WITH(
    --type ='console',
    type='dameng',
    url =' jdbc:dm://172.16.8.178:5236/DMTEST?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=utf-8',
    userName = 'SYSDBA', 
    password = 'SYSCBA', 
    tableName = 'SINK_TEST',
    batchSize='1'
 );



INSERT INTO MyResult
SELECT  
    s.ID as ID,
    m.name as NAME
FROM MyTable m
LEFT JOIN
      sideTable s
ON
      m.id=s.ID
```


