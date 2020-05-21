## 1.格式
  通过建表语句中的` PERIOD FOR SYSTEM_TIME`将表标识为维表，其中`PRIMARY KEY(keyInfo)`中的keyInfo，表示用来和源表进行关联的字段，
  维表JOIN的条件必须与`keyInfo`字段一致。
```
 CREATE TABLE tableName(
    colName cloType,
    ...
    PRIMARY KEY(colName1,colName2) ,
    PERIOD FOR SYSTEM_TIME
  )WITH(
    type ='kudu',
    kuduMasters ='ip1,ip2,ip3',
    tableName ='impala::default.testSide',
    primaryKey='id，xx',
    lowerBoundPrimaryKey='10,xx',
    upperBoundPrimaryKey='15,xx',
    workerCount='1',
    defaultOperationTimeoutMs='600000',
    defaultSocketReadTimeoutMs='6000000',
    batchSizeBytes='100000000',
    limitNum='1000',
    isFaultTolerant='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1',
    partitionedJoin='false'
  );
```
## 2.支持版本
 kudu 1.10.0+cdh6.2.0

## 3.表结构定义
   
 |参数名称|含义|
 |----|---|
 | tableName | 注册到flink的表名称(可选填;不填默认和hbase对应的表名称相同)|
 | colName | 列名称|
 | colType | 列类型 [colType支持的类型](colType.md)|
 | PERIOD FOR SYSTEM_TIME | 关键字表明该定义的表为维表信息|
 | PRIMARY KEY(keyInfo) | 维表主键定义;多个列之间用逗号隔开|
  
## 4.参数

参数详细说明请看[参数详细说明](./sideParams.md)

|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
| type | 表明维表的类型[hbase&#124;mysql&#124;kudu]|是||
| kuduMasters | kudu master节点的地址;格式ip[ip，ip2]|是||
| tableName | kudu 的表名称|是||
| workerCount | 工作线程数 |否||
| defaultOperationTimeoutMs | scan操作超时时间 |否||
| defaultSocketReadTimeoutMs | socket读取超时时间 |否||
| primaryKey | 需要过滤的主键 ALL模式独有 |否||
| lowerBoundPrimaryKey | 需要过滤的主键的最小值 ALL模式独有 |否||
| upperBoundPrimaryKey | 需要过滤的主键的最大值(不包含) ALL模式独有 |否||
| batchSizeBytes |返回数据的大小 | 否||
| limitNum |返回数据的条数 | 否||
| isFaultTolerant |查询是否容错  查询失败是否扫描第二个副本  默认false  容错 | 否||
| cache | 维表缓存策略(NONE/LRU/ALL)|否|NONE|
| partitionedJoin | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|
--------------

## 5.样例
### LRU维表示例
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
### ALL维表示例
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

## 6.kudu异步关联完整样例

```
CREATE TABLE MyTable(
    id bigint,
    name varchar,
    address varchar
)WITH(
    type = 'kafka10',
    bootstrapServers = '172.16.101.224:9092',
    zookeeperQuorm = '172.16.100.188:2181/kafka',
    offsetReset = 'latest',
    topic = 'tiezhu_test_in2',
    timezone = 'Asia/Shanghai',
    topicIsPattern = 'false',
    parallelism = '1'
);

CREATE TABLE sideTable(
    id int,
    message varchar,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
)WITH(
    type='kudu',
    kuduMasters ='ip1,ip2,ip3',
    tableName ='impala::default.testSide',
    cache ='LRU',
    partitionedJoin='false'
);

CREATE TABLE MyResult(
    id bigint,
    name varchar,
    address varchar,
    message varchar
)WITH(
    type ='console',
    address ='192.168.80.106:9042,192.168.80.107:9042',
    userName='cassandra',
    password='cassandra',
    database ='tiezhu',
    tableName ='stu_out',
    parallelism ='1'
);

insert
into
    MyResult
    select
        t1.id AS id,
        t1.name AS name,
        t1.address AS address,
        t2.message AS message
    from(
    select
        id,
        name,
        address
        from
            MyTable
        ) t1
    join sideTable t2
    on t1.id = t2.id;     
```

