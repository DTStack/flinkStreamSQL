
## 1.格式：
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
    type ='cassandra',
    address ='ip:port[,ip:port]',
     userNae='dbUserName',
     password='dbPwd',
     tableName='tableName',
     database='database',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
  );
```

# 2.支持版本
 cassandra-3.6.x
 
## 3.表结构定义
  
 |参数名称|含义|
 |----|---|
 | tableName | 注册到flink的表名称(可选填;不填默认和hbase对应的表名称相同)|
 | colName | 列名称|
 | colType | 列类型 [colType支持的类型](docs/colType.md)|
 | PERIOD FOR SYSTEM_TIME | 关键字表明该定义的表为维表信息|
 | PRIMARY KEY(keyInfo) | 维表主键定义;多个列之间用逗号隔开|
 
## 4.参数

  |参数名称|含义|是否必填|默认值|
  |----|---|---|----|
  | type |表明 输出表类型 cassandra|是||
  | address | 连接cassandra数据库 jdbcUrl |是||
  | userName | cassandra连接用户名|否||
  | password | cassandra连接密码|否||
  | tableName | cassandra表名称|是||
  | database  | cassandra表名称|是||
  | cache | 维表缓存策略(NONE/LRU)|否|NONE|
  | partitionedJoin | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|
  | maxRequestsPerConnection | 每个连接最多允许64个并发请求|否|NONE|
  | coreConnectionsPerHost   | 和Cassandra集群里的每个机器都至少有2个连接|否|NONE|
  | maxConnectionsPerHost    | 和Cassandra集群里的每个机器都最多有6个连接|否|NONE|
  | maxQueueSize             | Cassandra队列大小|否|NONE|
  | readTimeoutMillis        | Cassandra读超时|否|NONE|
  | connectTimeoutMillis     | Cassandra连接超时|否|NONE|
  | poolTimeoutMillis        | Cassandra线程池超时|否|NONE|
  
  ----------
  > 缓存策略
  * NONE: 不做内存缓存
  * LRU:
    * cacheSize: 缓存的条目数量
    * cacheTTLMs:缓存的过期时间(ms)
  

## 5.样例
```
create table sideTable(
    CHANNEL varchar,
    XCCOUNT int,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='cassandra',
    address ='172.21.32.1:9042,172.21.32.1:9042',
    database ='test',
    tableName ='sidetest',
    cache ='LRU',
    parallelism ='1',
    partitionedJoin='false'
 );


```


