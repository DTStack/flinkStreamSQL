## 1.格式：
```

CREATE TABLE tableName(
    colName colType,
    ...
    function(colNameX) AS aliasName,
    WATERMARK FOR colName AS withOffset( colName , delayTime )
 )WITH(
    type ='kafka09',
    bootstrapServers ='ip:port,ip:port...',
    zookeeperQuorum ='ip:port,ip:port/zkparent',
    offsetReset ='latest',
    topic ='topicName',
    groupId='test',
    parallelism ='parllNum',
    timezone='Asia/Shanghai',
    sourcedatatype ='dt_nest' #可不设置
 );
```

## 2.支持的版本
    kafka09,kafka10,kafka11及以上版本    
 **kafka读取和写入的版本必须一致，否则会有兼容性错误。**

## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](../colType.md)|
| function(colNameX) as aliasName | 支持在定义列信息的时候根据已有列类型生成新的列(函数可以使用系统函数和已经注册的UDF)|
| WATERMARK FOR colName AS withOffset( colName , delayTime ) | 标识输入流生的watermake生成规则,根据指定的colName(当前支持列的类型为Long &#124; Timestamp) 和delayTime生成waterMark 同时会在注册表的使用附带上rowtime字段(如果未指定则默认添加proctime字段);注意：添加该标识的使用必须设置系统参数 time.characteristic:EventTime; delayTime: 数据最大延迟时间(ms)|

## 4.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是|kafka09、kafka10、kafka11、kafka(对应kafka1.0及以上版本)|
|groupId | 需要读取的 groupId 名称|否||
|bootstrapServers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|zookeeperQuorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|topic | 需要读取的 topic 名称|是||
|topicIsPattern | topic是否是正则表达式格式(true&#124;false)  |否| false
|offsetReset  | 读取的topic 的offset初始位置[latest&#124;earliest&#124;指定offset值({"0":12312,"1":12321,"2":12312},{"partition_no":offset_value})]|否|latest|
|offsetEnd  | 任务停止时的offset位置|否|无|
|parallelism | 并行度设置|否|1|
|sourcedatatype | 数据类型,avro,csv,json,dt_nest。dt_nest为默认JSON解析器，能够解析嵌套JSON数据类型，其他仅支持非嵌套格式|否|dt_nest|
|schemaInfo | avro类型使用的schema信息|否||
|fieldDelimiter |csv类型使用的数据分隔符|否| | |
|timezone|时区设置[timezone支持的参数](../timeZone.md)|否|'Asia/Shanghai'
**kafka相关参数可以自定义，使用kafka.开头即可。**
```
kafka.consumer.id
kafka.socket.timeout.ms
kafka.fetch.message.max.bytes
kafka.num.consumer.fetchers
kafka.auto.commit.enable
kafka.auto.commit.interval.ms
kafka.queued.max.message.chunks
kafka.rebalance.max.retries
kafka.fetch.min.bytes
kafka.fetch.wait.max.ms
kafka.rebalance.backoff.ms
kafka.refresh.leader.backoff.ms
kafka.consumer.timeout.ms
kafka.exclude.internal.topics
kafka.partition.assignment.strategy
kafka.client.id
kafka.zookeeper.session.timeout.ms
kafka.zookeeper.connection.timeout.ms
kafka.zookeeper.sync.time.ms
kafka.offsets.storage
kafka.offsets.channel.backoff.ms
kafka.offsets.channel.socket.timeout.ms
kafka.offsets.commit.max.retries
kafka.dual.commit.enabled
kafka.partition.assignment.strategy
kafka.socket.receive.buffer.bytes
kafka.fetch.min.bytes

###kerberos认证相关参数
kafka.security.protocal
kafka.sasl.mechanism
kafka.sasl.kerberos.service.name
```

## 5.样例：
```
CREATE TABLE MyTable(
    name varchar,
    channel varchar,
    pv INT,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng
 )WITH(
    type ='kafka09',
    bootstrapServers ='172.16.8.198:9092',
    zookeeperQuorum ='172.16.8.198:2181/kafka',
    offsetReset ='latest',
    topic ='nbTest1,nbTest2,nbTest3',
    --topic ='mqTest.*',
    --topicIsPattern='true'
    parallelism ='1',
    sourcedatatype ='json' #可不设置
 );
 
 CREATE TABLE two
(
    id      int,
    name    string,
    message string
) WITH (
      type = 'kafka11',
      bootstrapServers = 'kudu1:9092,kudu2:9092,kudu3:9092',
      zookeeperQuorum = 'kudu1:2181,kudu2:2181,kudu3:2181/kafka',
      offsetReset = '{"0": 0,"1": 0,"2":0}',
--       offsetReset = '{"0": 34689}',
--     offsetReset = 'earliest',
      offsetEnd = '{"0": 100,"1": 100,"2":100}',
--       offsetEnd = '{"0": 34789}',
      topic = 'kafka'
      );
```
## 6.支持嵌套json、数据类型字段解析

嵌套json解析示例

json: {"name":"tom", "obj":{"channel": "root"}, "pv": 4, "xctime":1572932485}
```
CREATE TABLE MyTable(
    name varchar,
    obj.channel varchar as channel,
    pv INT,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng
 )WITH(
    type ='kafka09',
    bootstrapServers ='172.16.8.198:9092',
    zookeeperQuorum ='172.16.8.198:2181/kafka',
    offsetReset ='latest',
    groupId='nbTest',
    topic ='nbTest1,nbTest2,nbTest3',
    --- topic ='mqTest.*',
    ---topicIsPattern='true',
    parallelism ='1'
 );
```

#### 数组类型字段解析示例

声明数据类型为Array可以使用CROSS JOIN UNNEST语句对Array类型进行展开。（新模式，推荐）

json样例
```
{
    "id": 2,
    "some_users": [
        "foo",
        "bar"
    ]
}
```

SQL样例
```
CREATE TABLE ods (
    id INT,
    some_users ARRAY<STRING>
) WITH (
    ...
);

CREATE TABLE dwd (
    id INT,
    user_no INT,
    user_info VARCHAR
) WITH (
    type ='console',
);

INSERT INTO dwd
    SELECT id, user_info
    FROM ods_foo
    CROSS JOIN UNNEST(ods_foo.some_users) AS A (user_info);
```

json样例
```
{
    "id": 4,
    "some_users": [
        {
            "user_no": 12,
            "user_info": "foo"

        },
        {
            "user_no": 14,
            "user_info": "bar"
        }
    ]
}
```

SQL样例
```
CREATE TABLE ods (
    id INT,
    some_users ARRAY<ROW<user_no INT, user_info STRING>>
) WITH (
    ...
);

CREATE TABLE dwd (
    id INT,
    user_no INT,
    user_info VARCHAR
) WITH (
    type ='console',
);

INSERT INTO dwd
    SELECT id, user_no, user_info
    FROM ods_foo
    CROSS JOIN UNNEST(ods_foo.some_users) AS A (user_no, user_info);
```
##### 旧模式

json: {"name":"tom", "obj":{"channel": "root"}, "user": [{"pv": 4}, {"pv": 10}], "xctime":1572932485}
```
CREATE TABLE MyTable(
    name varchar,
    obj.channel varchar as channel,
    user[1].pv INT as pv,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng
 )WITH(
    type ='kafka09',
    bootstrapServers ='172.16.8.198:9092',
    zookeeperQuorum ='172.16.8.198:2181/kafka',
    offsetReset ='latest',
    groupId='nbTest',
    topic ='nbTest1,nbTest2,nbTest3',
    --- topic ='mqTest.*',
    ---topicIsPattern='true',
    parallelism ='1'
 );
```
or

json: {"name":"tom", "obj":{"channel": "root"}, "pv": [4, 7, 10], "xctime":1572932485}
```
CREATE TABLE MyTable(
    name varchar,
    obj.channel varchar as channel,
    pv[1] INT as pv,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng
 )WITH(
    type ='kafka09',
    bootstrapServers ='172.16.8.198:9092',
    zookeeperQuorum ='172.16.8.198:2181/kafka',
    offsetReset ='latest',
    groupId='nbTest',
    topic ='nbTest1,nbTest2,nbTest3',
    --- topic ='mqTest.*',
    ---topicIsPattern='true',
    parallelism ='1'
 );
```

## 7.csv格式数据源


```
CREATE TABLE MyTable(
    name varchar,
    channel varchar,
    pv INT,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng
 )WITH(
    type ='kafka09',
    bootstrapServers ='172.16.8.198:9092',
    zookeeperQuorum ='172.16.8.198:2181/kafka',
    offsetReset ='latest',
    topic ='nbTest1',
    --topic ='mqTest.*',
    --topicIsPattern='true'
    parallelism ='1',
    sourceDatatype ='csv'
 );
 ```
## 8.avro格式数据源

```
CREATE TABLE MyTable(
    channel varchar,
    pv varchar
    --xctime bigint
 )WITH(
   type='kafka',
   bootstrapServers='172.16.8.107:9092',
   groupId='mqTest01',
   offsetReset='latest',
   topic='mqTest01',
   parallelism ='1',
   topicIsPattern ='false',
   kafka.group.id='mqTest',
   sourceDataType ='avro',
   schemaInfo = '{"type":"record","name":"MyResult","fields":[{"name":"channel","type":"string"},{"name":"pv","type":"string"}]}'
 );

```

