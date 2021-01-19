## 1.格式：

```sql
CREATE TABLE flinkTableName(
    colName colType,
    ...
    colNameX colType,
    PRIMARY KEY (colName, ..., colNameX)
 )WITH(
    type ='redis',
    url = 'ip:port',
    database ='dbName',
    password ='pwd',
    redisType='1/2/3',
    tableName ='tableName',
    parallelism ='parallelismNum'
 );
```

## 2.支持版本

redis5.x, redis6.x

## 3.表结构定义

在redis结果表中，实际的数据使用hash的格式存储，其中，key的组合为tableName_primaryKeys。详细请看《7.Redis数据说明》

|参数名称|含义|
|----|---|
| flinkTableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称
| colName | 列名称|
| colType | 列类型|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
| type | 表名 输出表类型[mysq&#124;hbase&#124;elasticsearch&#124;redis]|是||
| url | redis 的地址;格式ip:port[,ip:port]|是||
| password | redis 的密码 |是||
| redisType | redis模式（1 单机，2 哨兵， 3 集群）| 是 |
| masterName | 主节点名称（哨兵模式下为必填项） | 否 |
| database | redis 的数据库地址|否||
| tableName | redis 的表名称|是||
| parallelism | 并行度设置|否|1|
| timeout | 连接超时时间|否|10000|
| maxTotal |最大连接数|否|8|
| maxIdle |最大空闲连接数|否|8|
| minIdle |最小空闲连接数|否||0|
| masterName | 哨兵模式下的masterName|否||
| keyExpiredTime |redis sink的key的过期时间。默认是0（永不过期），单位是s。|否||

## 5.样例：

```sql
 CREATE TABLE MyResult
 (
     channel VARCHAR,
     pv      VARCHAR,
     PRIMARY KEY (pv)
 ) WITH (
       type = 'redis',
       redisType = '1',
       url = '172.16.8.109:6379',
       tableName = 'resultTable',
       partitionedJoin = 'false',
       parallelism = '1',
       database = '0',
       timeout = '10000',
       maxTotal = '60000',
       maxIdle = '8',
       minIdle = '0'
       );

```

## 6.redis完整样例

```sql
-- source
CREATE TABLE SourceOne
(
    id        INT,
    age       BIGINT,
    birth     TIMESTAMP,
    todayTime TIME,
    todayDate DATE,
    price     DECIMAL,
    name      VARCHAR,
    phone     VARCHAR,
    wechat    VARCHAR,
    qq        VARCHAR
) WITH (
      type = 'kafka11',
      bootstrapServers = 'kudu1:9092',
      zookeeperQuorum = 'kudu1:2181',
      offsetReset = 'latest',
      topic = 'tiezhu_in',
      enableKeyPartitions = 'false',
      topicIsPattern = 'false'
      );


CREATE TABLE SinkOne
(
    id        INT,
    age       BIGINT,
    birth     TIMESTAMP,
    todayTime TIME,
    todayDate DATE,
    price     DECIMAL,
    name      VARCHAR,
    phone     VARCHAR,
    wechat    VARCHAR,
    qq        VARCHAR,
    PRIMARY KEY (id, name)
) WITH (
      type = 'redis',
      url = 'kudu1:6379',
      database = '0',
      -- （1 单机，2 哨兵， 3 集群）
      redisType = '1',
      tableName = 'demo',
      partitionedJoin = 'false'
      );

CREATE VIEW ViewOne AS
SELECT id,
       age,
       birth,
       todayTime,
       todayDate,
       price,
       name,
       phone,
       wechat,
       qq
FROM SourceOne SO;

INSERT INTO SinkOne
SELECT *
FROM ViewOne;

```

## 7.redis数据说明

redis使用散列类型 hash 数据结构，key=tableName_primaryKey1_primaryKey2,value={column1=value1, column2=value2}
如果以班级class表为例，id和name作为联合主键，那么redis的结构为 <class_1_john ,{id=1, name=john, age=12}>

### 源表数据内容

```json
{
    "qq":63595541541,
    "todayTime":"10:19:40",
    "wechat":"微信号81850",
    "birth":"2021-01-19 10:19:40.075",
    "todayDate":"2021-01-19",
    "phone":18649852461,
    "price":1.4,
    "name":"tiezhu2",
    "id":2,
    "age":19
}
```

### redis实际数据内容

```shell
kudu1_redis_docker:0>keys *
1) "demo_2_tiezhu2"
2) "demo_4_tiezhu2"
3) "demo_3_yuange"
kudu1_redis_docker:0>hgetall demo_2_tiezhu2
1)  "qq"
2)  "63595541541"
3)  "todayTime"
4)  "10:19:40"
5)  "phone"
6)  "18649852461"
7)  "price"
8)  "1.400000000000000000"
9)  "name"
10) "tiezhu2"
11) "wechat"
12) "微信号81850"
13) "birth"
14) "2021-01-19 10:19:40.075"
15) "id"
16) "2"
17) "todayDate"
18) "2021-01-19"
19) "age"
20) "19"
kudu1_redis_docker:0>
```