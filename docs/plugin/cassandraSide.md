
## 1.格式：

  通过建表语句中的` PERIOD FOR SYSTEM_TIME`将表标识为维表，其中`PRIMARY KEY(keyInfo)`中的keyInfo，表示用来和源表进行关联的字段，
  维表JOIN的条件必须与`keyInfo`字段一致。
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
 cassandra-3.x
 
## 3.表结构定义
  
 |参数名称|含义|
 |----|---|
 | tableName | 注册到flink的表名称(可选填;不填默认和cassandra对应的表名称相同)|
 | colName | 列名称|
 | colType | 列类型|
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
  | maxRequestsPerConnection | 每个连接允许的并发请求数|否|1|
  | coreConnectionsPerHost   | 每台主机连接的核心数|否|8|
  | maxConnectionsPerHost    | Cassandra集群里的每个机器都最多连接数|否|32768|
  | maxQueueSize             | Cassandra队列大小|否|100000|
  | readTimeoutMillis        | Cassandra读超时|否|60000|
  | connectTimeoutMillis     | Cassandra连接超时|否|60000|
  | poolTimeoutMillis        | Cassandra线程池超时|否|60000|
  
  ----------
  > 缓存策略
-  NONE：不做内存缓存。每条流数据触发一次维表查询操作。
-  ALL:  任务启动时，一次性加载所有数据到内存，并进行缓存。适用于维表数据量较小的情况。
-  LRU:  任务执行时，根据维表关联条件使用异步算子加载维表数据，并进行缓存。
  

## 5.维表定义样例

### ALL全量维表定义
```
CREATE TABLE sideTable(
    id bigint,
    school varchar,
    home varchar,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
)WITH(
    type='mysql',
    url='jdbc:mysql://172.16.8.109:3306/tiezhu',
    userName='dtstack',
    password='abc123',
    tableName='stressTest',
    cache='ALL',
    parallelism='1'
);
```
### LRU异步维表定义
```
CREATE TABLE sideTable(
    id bigint,
    message varchar,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
)WITH(
    type ='cassandra',
    address ='192.168.80.106:9042, 192.168.80.107:9042',
    database ='tiezhu',
    tableName ='stu',
    userName='cassandra',
    password='cassandra',
    cache ='LRU',
    parallelism ='1',
    partitionedJoin='false'
);
```
## 6.完整样例
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
    id bigint,
    message varchar,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
)WITH(
    type ='cassandra',
    address ='192.168.80.106:9042, 192.168.80.107:9042',
    database ='tiezhu',
    tableName ='stu',
    userName='cassandra',
    password='cassandra',
    cache ='LRU',
    parallelism ='1',
    partitionedJoin='false'
);

CREATE TABLE MyResult(
    id bigint,
    name varchar,
    address varchar,
    message varchar
 )WITH(
    type ='cassandra',
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
    from
    (
        select
        id,
        name,
        address
        from
            MyTable
        ) t1
        join    sideTable t2
        on t1.id = t2.id;                                
```


