### 样例1:
注册自定义函数，并指定某一列作为eventTime;  
kafkaSource join hbaseDim ==> hbaseOut
     
```
CREATE scala FUNCTION CHARACTER_LENGTH WITH com.dtstack.Kun;

CREATE TABLE MyTable(
    name varchar,
    channel varchar,
    pv int,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng //自定义的函数
 )WITH(
    type ='kafka09',
    bootstrapServers ='172.16.8.198:9092',
    zookeeperQuorum ='172.16.8.198:2181/kafka',
    offsetReset ='latest',
    topic ='nbTest1',
    parallelism ='1'
 );

CREATE TABLE MyResult(
    channel varchar,
    pv varchar
 )WITH(
    type ='mysql',
    url ='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',
    userName ='dtstack',
    password ='abc123',
    tableName ='pv2',
    parallelism ='1'
 );

CREATE TABLE workerinfo(
    cast(logtime as TIMESTAMP) AS rtime,
    cast(logtime) AS rtime
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    tableName ='workerinfo',
    rowKey ='ce,de',
    parallelism ='1',
    zookeeperParent ='/hbase'
 );

CREATE TABLE sideTable(
    cf:name varchar as name,
    cf:info varchar as info,
    PRIMARY KEY(name),
    PERIOD FOR SYSTEM_TIME //维表标识
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    zookeeperParent ='/hbase',
    tableName ='workerinfo',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1'
 );

insert
into
    MyResult
    select
        d.channel,
        d.info
    from
        (      select
            a.*,b.info
        from
            MyTable a
        join
            sideTable b
                on a.channel=b.name
        where
            a.channel = 'xc2'
            and a.pv=10      ) as d
```

### 创建视图demo:
[参考视图文档](docs/createView.md)
