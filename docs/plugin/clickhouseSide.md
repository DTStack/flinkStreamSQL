## 1.格式：

通过建表语句中的` PERIOD FOR SYSTEM_TIME`将表标识为维表，其中`PRIMARY KEY(keyInfo)`中的keyInfo，表示用来和源表进行关联的字段，
  维表JOIN的条件必须与`keyInfo`字段一致。

```sql
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type='clickhouse',
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

# 2.支持版本

 19.14.x、19.15.x、19.16.x

## 3.表结构定义

 [维表参数信息](docs/plugin/sideParams.md)

clinkhose独有的参数信息

| 参数名称 | 含义                  | 是否必填 | 默认值 |
| -------- | --------------------- | -------- | ------ |
| type     | 维表类型， clinkhouse | 是       |        |
| url      | 连接数据库 jdbcUrl    | 是       |        |
| userName | 连接用户名            | 是       |        |
| password | 连接密码              | 是       |        |

## 4.样例

------

## ALL全量维表定义

```sql
// 定义全量维表
CREATE TABLE sideTable(
    id INT,
    name VARCHAR,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='clickhouse',
    url ='jdbc:clickhouse://172.16.8.104:3306/zftest',
    userName ='dtstack',
    password ='abc123',
    tableName ='all_test_clinkhousesql',
    cache ='ALL',
    cacheTTLMs ='60000',
    parallelism ='1'
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
    type ='clickhousesql',
    url ='jdbc:clickhousesql://172.16.8.104:3306/zftest',
    userName ='dtstack',
    password ='abc123',
    tableName ='lru_test_clickhousesql',
    partitionedJoin ='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    asyncPoolSize ='3',
    parallelism ='1'
 );
```

### ClickHouseSQL异步维表关联

```sql
CREATE TABLE MyTable(
    id int,
    name varchar
 )WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='cannan_zftest01',
    timezone='Asia/Shanghai',
    enableKeyPartitions ='false',
    topicIsPattern ='false',
    parallelism ='1'
 );

CREATE TABLE MyResult(
    id INT,
    name VARCHAR
 )WITH(
    type ='clickhousesql',
    url ='jdbc:clickhousesql://172.16.8.104:3306/zftest',
    userName ='dtstack',
    password ='abc123',
    tableName ='test_clickhouse_zf',
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
    type ='clickhousesql',
    url ='jdbc:clickhousesql://172.16.8.104:3306/zftest',
    userName ='dtstack',
    password ='abc123',
    tableName ='test_clickhouse_10',
    partitionedJoin ='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    asyncPoolSize ='3',
    parallelism ='1'
 );

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

