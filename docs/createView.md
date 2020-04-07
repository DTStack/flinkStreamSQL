## 语法
```
 CREATE VIEW viewName
        [ (columnName[ , columnName]*) ]
            AS queryStatement;
 或
 CREATE VIEW viewName [ (columnName[ , columnName]*) ];
 INSERT INTO viewName queryStatement;
```
## 样例
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

CREATE TABLE MyResult(
    channel VARCHAR,
    pv VARCHAR
 )WITH(
    type ='mysql',
    url ='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',
    userName ='dtstack',
    password ='abc123',
    tableName ='yx',
    parallelism ='1'
 );

CREATE TABLE workerinfo(
    cast(logtime as TIMESTAMP)AS rtime,
    cast(logtime)AS rtime
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    tableName ='workerinfo',
    rowKey ='ce,de',
    parallelism ='1',
    zookeeperParent ='/hbase'
 );

CREATE TABLE REDIS(
    name VARCHAR,
    pv VARCHAR
)WITH(
    type ='redis',
    url ='172.16.10.79:6379',
    databsae =0,
    password =''
);

CREATE TABLE sideTable(
    cf:name varchar as name,
    cf:info varchar as info,
    PRIMARY KEY(name),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    zookeeperParent ='/hbase',
    tableName ='workerinfo',
    cache ='ALL',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1'
 );
 CREATE VIEW abc1 AS SELECT * FROM MyTable;
 CREATE VIEW abc2 AS SELECT d.channel,
        d.info
    FROM
        (      SELECT
            a.*,b.info
        FROM
            MyTable a
        JION
            sideTable b
                ON a.channel=b.name
        ) as d;
CREATE VIEW abc3(name varchar, info varchar);
insert into abc3 select
        d.channel,
        d.info
    from
        abc2 as d;

insert
into
    MyResult
    select
        d.channel,
        d.info
    from
        abc3 as d;
```
