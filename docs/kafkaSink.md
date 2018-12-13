# 一、json格式结果表
## 1.格式：
```
数据现在支持json格式{"xx":"bb","cc":"dd"}

CREATE TABLE tableName(
    colName colType,
    ...
    function(colNameX) AS aliasName
 )WITH(
    type ='kafka09',
    kafka.bootstrap.servers ='ip:port,ip:port...',
    kafka.zookeeper.quorum ='ip:port,ip:port/zkparent',
    kafka.topic ='topicName',
    parallelism ='parllNum',
    sourcedatatype ='json' #可不设置
 );
```

## 2.支持的版本
    kafka08,kafka09,kafka10,kafka11

## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](colType.md)|
| function(colNameX) as aliasName | 支持在定义列信息的时候根据已有列类型生成新的列(函数可以使用系统函数和已经注册的UDF)|

## 4.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是||
|kafka.bootstrap.servers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|kafka.zookeeper.quorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|kafka.topic | 需要读取的 topic 名称|是||
|parallelism | 并行度设置|否|1|
|sourcedatatype | 数据类型|否|json|
**kafka相关参数可以自定义，使用kafka.开头即可。**

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
    kafka.bootstrap.servers ='172.16.8.198:9092',
    kafka.zookeeper.quorum ='172.16.8.198:2181/kafka',
    kafka.topic ='nbTest1',
    parallelism ='1',
    sourcedatatype ='json' #可不设置
 );
```
# 二、csv格式数据源
根据字段分隔符进行数据分隔，按顺序匹配sql中配置的列。如数据分隔列数和sql中配置的列数相等直接匹配；如不同参照lengthcheckpolicy策略处理。
## 1.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是||
|kafka.bootstrap.servers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|kafka.zookeeper.quorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|kafka.topic | 需要读取的 topic 名称|是||
|parallelism | 并行度设置 |否|1|
|sourcedatatype | 数据类型|是 |csv|
|fielddelimiter | 字段分隔符|是 ||
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
    kafka.bootstrap.servers ='172.16.8.198:9092',
    kafka.zookeeper.quorum ='172.16.8.198:2181/kafka',
    kafka.auto.offset.reset ='latest',
    kafka.topic ='nbTest1',
    parallelism ='1',
    sourcedatatype ='csv',
    fielddelimiter ='\|'
 );
