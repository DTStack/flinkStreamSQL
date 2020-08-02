## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType,
     [primary key (colName)]
 )WITH(
    type ='kafka09',
    bootstrapServers ='ip:port,ip:port...',
    zookeeperQuorum ='ip:port,ip:port/zkparent',
    offsetReset ='latest',
    topic ='topicName',
    groupId='test',
    parallelism ='parllNum',
    timezone='Asia/Shanghai',
    sourcedatatype ='json' #可不设置
 );
```

## 2.支持版本
 kafka09,kafka10,kafka11及以上版本   
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| 结果表名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](../colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表名的输出表类型[kafka09&#124;kafka10&#124;kafka11&#124;kafka]|是||
|groupId | 需要读取的 groupId 名称|否||
|bootstrapServers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|zookeeperQuorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|topic | 需要读取的 topic 名称|是||
|topicIsPattern | topic是否是正则表达式格式(true&#124;false)  |否| false
|offsetReset  | 读取的topic 的offset初始位置[latest&#124;earliest&#124;指定offset值({"0":12312,"1":12321,"2":12312},{"partition_no":offset_value})]|否|latest|
|parallelism | 并行度设置|否|1|
|sourcedatatype | 数据类型|否|json|
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
```

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
    groupId = 'flink_sql',
    timezone = 'Asia/Shanghai',
    topicIsPattern = 'false',
    parallelism = '1'
);

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

CREATE TABLE MyResult(
    id bigint,
    name varchar,
    address varchar,
    home varchar,
    school varchar
)WITH(
    type = 'kafka10',
    bootstrapServers = '172.16.101.224:9092',
    zookeeperQuorm = '172.16.100.188:2181/kafka',
    offsetReset = 'latest',
    topic = 'tiezhu_test_out2',
    parallelism = '1'
);

insert
into
    MyResult
    select
        t1.id AS id,
        t1.name AS name,
        t1.address AS address,
        t2.school AS school,
        t2.home AS home
    from(
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

## 6.结果表数据示例：
```
[root@node002 bin]# ./kafka-console-consumer.sh --bootstrap-server 172.16.101.224:9092 --topic tiezhu_test_out2
{"id":122,"name":"tiezhu122","address":"hangzhou122","home":"ganzhou122","school":" ecjtu122"}
{"id":123,"name":"tiezhu123","address":"hangzhou123","home":"ganzhou123","school":" ecjtu123"}
```