
## 1.格式：
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type ='redis',
     url = 'ip:port',
     password = 'redisPwd',
     database = 'dbName',
     tableName ='sideTableName',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000'
  );
```
## 2.支持版本
 redis5.0

## 3.表结构定义
   
|参数名称|含义|
|----|---|
| tableName | 注册到flink的表名称(可选填;不填默认和redis对应的表名称相同)|
| colName | 列名称，维表列名格式 表名:主键名:主键值:列名]|
| colType | 列类型，当前只支持varchart|
| PERIOD FOR SYSTEM_TIME | 关键字表明该定义的表为维表信息|
  
## 3.参数

|参数名称|含义|是否必填|默认值|
|----|---|---|----|
| type | 表明维表的类型[hbase\|mysql\|redis]|是||
| url | redis 的地址;格式ip:port[,ip:port]|是||
| password | redis 的密码 |是||
| database | reids 的数据库地址|否||
| tableName | redis 的表名称|是||
| cache | 维表缓存策略(NONE/LRU/ALL)|否|NONE|
| partitionedJoin | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|

--------------
> 缓存策略
  * NONE: 不做内存缓存
  * LRU:
    * cacheSize: 缓存的条目数量
    * cacheTTLMs:缓存的过期时间(ms)
  * ALL: 缓存全量表数据

## 4.样例
```
create table sideTable(
    channel varchar,
    xccount varchar,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='redis',
    url='172.16.10.79:6379',
    password='abc123',
    database='0',
    tableName='sidetest',
    cache = 'LRU',
    cacheTTLMs='10000'
 );

```


