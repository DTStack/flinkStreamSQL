CREATE TABLE MyUserTable (
    name varchar,
    channel varchar,
    pv int
) WITH (
    type ='kafka',
    bootstrapServers ='10.88.0.227:9093',
    zookeeperQuorum ='10.88.0.227:2181',
    offsetReset ='latest',
    topic ='nbtest',
    sourcedatatype ='json',
    parallelism ='1'
 );

CREATE TABLE MyResult(
    name varchar,
    channel varchar,
    pv int
 )WITH(
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test',
    userName ='xxx',
    password ='xxx',
    tableName ='test',
    parallelism ='1'
 );

CREATE TABLE dwd (
    name varchar,
    channel varchar,
    pv int
) WITH (
    type ='console'
);

insert
into
    MyResult
    select
        a.name,
        a.channel,
        a.pv
    from
        MyUserTable as a