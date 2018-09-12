## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    function(channel) AS alias
 )WITH(
    type ='kafka09',
    bootstrapServers ='ip:port,ip:port...',
    zookeeperQuorum ='ip:port,ip:port/zkparent',
    offsetReset ='latest',
    topic ='nbTest1',
    parallelism ='1'
 );
```
## 2.参数：
  * type ==> kafka09
  * bootstrapServers
## 3.样例：
```
CREATE TABLE MyTable(
    name string,
    channel STRING,
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