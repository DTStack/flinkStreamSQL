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

## 4.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是|kafka09、kafka10、kafka11、kafka(对应kafka1.0及以上版本)|
|groupId | 需要读取的 groupId 名称|否||
|bootstrapServers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|zookeeperQuorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|topic | 需要读取的 topic 名称|是||
|parallelism | 并行度设置|否|1|
|partitionKeys | 用来分区的字段|否||
|updateMode | 回溯流数据下发模式，append,upsert.upsert模式下会将是否为回溯信息以字段形式进行下发。|否|append|
|sinkdatatype | 写入kafka数据格式，json,avro,csv|否|json|
|fieldDelimiter | csv数据分隔符|否| \ |


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

### json格式：
```
CREATE TABLE MyResult(
    channel varchar,
    pv varchar
 )WITH(
    type='kafka',
    bootstrapServers='172.16.8.107:9092',
    topic='mqTest02',
    parallelism ='2',
    partitionKeys = 'channel,pv',
    updateMode='upsert'
 );
 
upsert模式下发的数据格式：{"channel":"zs","pv":"330",retract:true}
append模式下发的数据格式：{"channel":"zs","pv":"330"}

```

### avro格式：

如果updateMode='upsert',schemaInfo需要包含retract属性信息。

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
   topicIsPattern ='false'
 );

create table sideTable(
    channel varchar,
    xccount int,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='mysql',
    url='jdbc:mysql://172.16.8.109:3306/test?charset=utf8',
    userName='dtstack',
    password='abc123',
    tableName='sidetest',
    cache = 'LRU',
    cacheTTLMs='10000',
    parallelism ='1'

 );


CREATE TABLE MyResult(
    channel varchar,
    pv varchar
 )WITH(
    --type='console'
    type='kafka',
    bootstrapServers='172.16.8.107:9092',
    topic='mqTest02',
    parallelism ='1',
    updateMode='upsert',
    sinkdatatype = 'avro',
    schemaInfo = '{"type":"record","name":"MyResult","fields":[{"name":"channel","type":"string"}
    ,{"name":"pv","type":"string"},{"name":"channel","type":"string"},
    {"name":"retract","type":"boolean"}]}'

 );

 
insert 
into
    MyResult   
    select
        a.channel as channel,
        a.pv as pv
    from
            MyTable a
```
### csv格式：

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
   parallelism ='2',
   topicIsPattern ='false'
 );

create table sideTable(
    channel varchar,
    xccount int,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='mysql',
    url='jdbc:mysql://172.16.8.109:3306/test?charset=utf8',
    userName='dtstack',
    password='abc123',
    tableName='sidetest',
    cache = 'LRU',
    cacheTTLMs='10000',
    parallelism ='1'

 );


CREATE TABLE MyResult(
    channel varchar,
    pv varchar
 )WITH(
    type='kafka',
    bootstrapServers='172.16.8.107:9092',
    topic='mqTest02',
    parallelism ='2',
    updateMode='upsert',
    sinkdatatype = 'csv',
    fieldDelimiter='*'
    
   

 );

 
insert 
into
    MyResult   
    select
        a.channel as channel,
        a.pv as pv
    from
            MyTable a
```
