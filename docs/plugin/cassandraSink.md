## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='cassandra',
    address ='ip:port[,ip:port]',
    userName ='userName',
    password ='pwd',
    database ='databaseName',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 cassandra-3.x
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](../colType.md)|

## 4.参数

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表明 输出表类型 cassandra|是||
|address | 连接cassandra数据库 jdbcUrl |是||
|userName | cassandra连接用户名|否||
|password | cassandra连接密码|否||
|tableName | cassandra表名称|是||
|database  | cassandra表名称|是||
|parallelism | 并行度设置|否|1|
| maxRequestsPerConnection | 每个连接允许的并发请求数|否|1|
| coreConnectionsPerHost   | 每台主机连接的核心数|否|8|
| maxConnectionsPerHost    | Cassandra集群里的每个机器都最多连接数|否|32768|
| maxQueueSize             | Cassandra队列大小|否|100000|
| readTimeoutMillis        | Cassandra读超时|否|60000|
| connectTimeoutMillis     | Cassandra连接超时|否|60000|
| poolTimeoutMillis        | Cassandra线程池超时|否|60000|
  
## 5.完整样例：
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
### 6.结果表数据展示
```
cqlsh:tiezhu> desc stu_out

CREATE TABLE tiezhu.stu_out (
    id int PRIMARY KEY,
    address text,
    message text,
    name text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';


cqlsh:tiezhu> select * from stu_out limit 1;

 id | address    | message          | name
----+------------+------------------+----------
 23 | hangzhou23 | flinkStreamSql23 | tiezhu23

(1 rows)
```