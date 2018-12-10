
## 1.格式：
```
 CREATE TABLE tableName(
     columnFamily:columnName type as alias,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type ='hbase',
     zookeeperQuorum ='ip:port',
     zookeeperParent ='/hbase',
     tableName ='tableNamae',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
  );
```
## 2.支持版本
 hbase2.0

## 3.表结构定义
   
|参数名称|含义|
|----|---|
| tableName | 注册到flink的表名称(可选填;不填默认和hbase对应的表名称相同)|
| columnFamily:columnName | hbase中的列族名称和列名称 |
| alias | hbase 中的列对应到flink中注册的列名称 |
| PERIOD FOR SYSTEM_TIME | 关键字表明该定义的表为维表信息|
| PRIMARY KEY(keyInfo) | 维表主键定义;hbase 维表rowkey的构造方式;可选择的构造包括 md5(alias + alias), '常量',也包括上述方式的自由组合 |
  
## 3.参数

|参数名称|含义|是否必填|默认值|
|----|---|---|----|
| type | 表明维表的类型[hbase\|mysql]|是||
| zookeeperQuorum | hbase 的zk地址;格式ip:port[;ip:port]|是||
| zookeeperParent | hbase 的zk parent路径|是||
| tableName | hbase 的表名称|是||
| cache | 维表缓存策略(NONE/LRU)|否|NONE|
| partitionedJoin | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|

--------------
> 缓存策略
  * NONE: 不做内存缓存
  * LRU:
    * cacheSize: 缓存的条目数量
    * cacheTTLMs:缓存的过期时间(ms)

## 4.样例
```
CREATE TABLE sideTable(
    cf:name varchar as name,
    cf:info int as info,
    PRIMARY KEY(md5(name) + 'test'),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    zookeeperParent ='/hbase',
    tableName ='workerinfo',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1',
    partitionedJoin='true'
 );

```


