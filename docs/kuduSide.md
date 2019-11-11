
## 1.格式：
All:
```
create table sideTable(
    id int,
    tablename1 VARCHAR,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='kudu',
    kuduMasters ='ip1,ip2,ip3',
    tableName ='impala::default.testSide',
    cache ='ALL',
	primaryKey='id，xx',
	lowerBoundPrimaryKey='10,xx',
	upperBoundPrimaryKey='15,xx',
	workerCount='1',
    defaultOperationTimeoutMs='600000',
    defaultSocketReadTimeoutMs='6000000',
    batchSizeBytes='100000000',
    limitNum='1000',
    isFaultTolerant='false',
    partitionedJoin='false'
 );
```
LRU:
```
create table sideTable(
    id int,
    tablename1 VARCHAR,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='kudu',
    kuduMasters ='ip1,ip2,ip3',
    tableName ='impala::default.testSide',
    cache ='LRU',
    workerCount='1',
    defaultOperationTimeoutMs='600000',
    defaultSocketReadTimeoutMs='6000000',
    batchSizeBytes='100000000',
    limitNum='1000',
    isFaultTolerant='false',
    partitionedJoin='false'
 );
 ```

## 2.支持版本
kudu 1.9.0+cdh6.2.0 

## 3.表结构定义
   
 |参数名称|含义|
 |----|---|
 | tableName | 注册到flink的表名称(可选填;不填默认和hbase对应的表名称相同)|
 | colName | 列名称|
 | colType | 列类型 [colType支持的类型](colType.md)|
 | PERIOD FOR SYSTEM_TIME | 关键字表明该定义的表为维表信息|
 | PRIMARY KEY(keyInfo) | 维表主键定义;多个列之间用逗号隔开|
  
## 3.参数


|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
|type | 表明维表的类型[hbase&#124;mysql&#124;kudu]|是||
| kuduMasters | kudu master节点的地址;格式ip[ip，ip2]|是||
| tableName | kudu 的表名称|是||
| workerCount | 工作线程数 |否||
| defaultOperationTimeoutMs | 写入操作超时时间 |否||
| defaultSocketReadTimeoutMs | socket读取超时时间 |否||
| primaryKey | 需要过滤的主键 ALL模式独有 |否||
| lowerBoundPrimaryKey | 需要过滤的主键的最小值 ALL模式独有 |否||
| upperBoundPrimaryKey | 需要过滤的主键的最大值(不包含) ALL模式独有 |否||
| workerCount | 工作线程数 |否||
| defaultOperationTimeoutMs | 写入操作超时时间 |否||
| defaultSocketReadTimeoutMs | socket读取超时时间 |否||
| batchSizeBytes |返回数据的大小 | 否||
| limitNum |返回数据的条数 | 否||
| isFaultTolerant |查询是否容错  查询失败是否扫描第二个副本  默认false  容错 | 否||
| cache | 维表缓存策略(NONE/LRU/ALL)|否|NONE|
| partitionedJoin | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|


--------------
> 缓存策略
  * NONE: 不做内存缓存
  * LRU:
    * cacheSize: 缓存的条目数量
    * cacheTTLMs:缓存的过期时间(ms)

## 4.样例
All:
```
create table sideTable(
    id int,
    tablename1 VARCHAR,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='kudu',
    kuduMasters ='ip1,ip2,ip3',
    tableName ='impala::default.testSide',
    cache ='ALL',
	primaryKey='id，xx',
	lowerBoundPrimaryKey='10,xx',
	upperBoundPrimaryKey='15,xx',
    partitionedJoin='false'
 );
```
LRU:
```
create table sideTable(
    id int,
    tablename1 VARCHAR,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='kudu',
    kuduMasters ='ip1,ip2,ip3',
    tableName ='impala::default.testSide',
    cache ='LRU',
    partitionedJoin='false'
 );
 ```

