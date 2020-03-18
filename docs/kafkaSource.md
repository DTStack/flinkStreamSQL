## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    function(colNameX) AS aliasName,
    WATERMARK FOR colName AS withOffset( colName , delayTime )
 )WITH(
    type ='kafka11',
    bootstrapServers ='ip:port,ip:port...',
    zookeeperQuorum ='ip:port,ip:port/zkparent',
    offsetReset ='latest',
    topic ='topicName',
    groupId='test',
    parallelism ='parllNum',
    --timezone='America/Los_Angeles',
    timezone='Asia/Shanghai',
    sourcedatatype ='json' #可不设置
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
| colType | 列类型 [colType支持的类型](colType.md)|
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
|parallelism | 并行度设置|否|1|
|sourcedatatype | 数据类型|否|json|
|timezone|时区设置[timezone支持的参数](timeZone.md)|否|'Asia/Shanghai'
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
    type ='kafka11',
    bootstrapServers ='172.16.8.198:9092',
    zookeeperQuorum ='172.16.8.198:2181/kafka',
    offsetReset ='latest',
    topic ='nbTest1,nbTest2,nbTest3',
    --topic ='mqTest.*',
    --topicIsPattern='true'
    parallelism ='1',
    sourcedatatype ='json' #可不设置
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

数组类型字段解析示例

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
# 二、csv格式数据源
根据字段分隔符进行数据分隔，按顺序匹配sql中配置的列。如数据分隔列数和sql中配置的列数相等直接匹配；如不同参照lengthcheckpolicy策略处理。
## 1.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是||
|bootstrapServers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|zookeeperQuorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|topic | 需要读取的 topic 名称|是||
|offsetReset | 读取的topic 的offset初始位置[latest&#124;earliest]|否|latest|
|parallelism | 并行度设置 |否|1|
|sourcedatatype | 数据类型|是 |csv|
|fielddelimiter | 字段分隔符|是 ||
|lengthcheckpolicy | 单行字段条数检查策略 |否|可选，默认为SKIP,其它可选值为EXCEPTION、PAD。SKIP：字段数目不符合时跳过 。EXCEPTION:字段数目不符合时抛出异常。PAD:按顺序填充，不存在的置为null。|
**kafka相关参数可以自定义，使用kafka.开头即可。**

## 2.样例：
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
    sourcedatatype ='csv',
    fielddelimiter ='\|',
    lengthcheckpolicy = 'PAD'
 );
 ```
