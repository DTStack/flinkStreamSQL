## 1.格式：
```
数据现在支持json格式{"xx":"bb","cc":"dd"}

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
    parallelism ='parllNum'
 );
```

## 2.支持的版本
  kafka09,kafka10,kafka11  

## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](colType.md)|
| function(colNameX) as aliasName | 支持在定义列信息的时候根据已有列类型生成新的列(函数可以使用系统函数和已经注册的UDF)|
| WATERMARK FOR colName AS withOffset( colName , delayTime ) | 标识输入流生的watermake生成规则,根据指定的colName(当前支持列的类型为Long \| Timestamp) 和delayTime生成waterMark 同时会在注册表的使用附带上rowtime字段(如果未指定则默认添加proctime字段);注意：添加该标识的使用必须设置系统参数 time.characteristic:EventTime; delayTime: 数据最大延迟时间(ms)|

## 4.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是||
|bootstrapServers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|zookeeperQuorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|topic | 需要读取的 topic 名称|是||
|offsetReset | 读取的topic 的offset初始位置[latest\|earliest]|否|latest|
|parallelism | 并行度设置|否|1|
  
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
    topic ='nbTest1',
    parallelism ='1'
 );
```
