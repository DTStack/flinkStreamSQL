
## 1.格式：
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(colName1,colName2) ,
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type ='redis',
     url = 'ip:port',
     password = 'redisPwd',
     database = 'dbName',
     tableName ='sideTableName',
     redisType = '1',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000'
  );
```
## 2.支持版本
 redis5.0

## 3.表结构定义
注意：redis中没有表和schema的概念，参数中tableName是指符合命名规则的key，具体规则请看[缓存redis的存储命名规则]
   
|参数名称|含义|
|----|---|
| tableName | 注册到flinkStreamSql的表名称(可选填;不填默认和redis对应的"表"名称相同)|
| colName | 列名称，对应redis对应"表"的field|
| colType | 列类型，当前只支持varchar|
| PRIMARY KEY |主键，多个字段做为联合主键时以逗号分隔|
| PERIOD FOR SYSTEM_TIME | 关键字，表明该定义的表为维表信息|
  
## 4.参数

参数详细说明请看[参数详细说明]()

|参数名称|含义|是否必填|默认值|
|----|---|---|----|
| type | 表明维表的类型[hbase&#124;mysql&#124;redis]|是||
| url | redis 的地址;格式ip:port[,ip:port]|是||
| password | redis 的密码 |否|空|
| redisType | redis模式（1 单机，2 哨兵， 3 集群）| 是 |
| masterName | 主节点名称（哨兵模式下为必填项） | 否 |
| database | reids 的数据库地址|否|0|
| tableName | redis 的“表”名称|是||
| cache | 维表缓存策略(NONE/LRU/ALL)|否|NONE|
| partitionedJoin | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|
--------------
> 缓存策略
  * NONE: 不做内存缓存
  * LRU:
    * cacheSize: 缓存的条目数量
    * cacheTTLMs:缓存的过期时间(ms)
  * ALL: 缓存全量表数据

## 5.样例
```
CREATE TABLE MyRedis(
    id varchar,
    message varchar,
    PRIMARY KEY(id),
    PERIOD FOR SYSTEM_TIME
)WITH(
    type='redis',
    url='172.16.10.79:6379',
    password='abc123',
    database='0',
    redisType = '1',
    tableName = 'sideTable',
    cache = 'LRU',
    cacheTTLMs='10000'
);

```
## 6.redis的存储命名规则

redis使用散列类型 hash 数据结构，key=tableName_primaryKey1_primaryKey2,value={column1=value1, column2=value2}
如果以班级class表为例，id和name作为联合主键，那么redis的结构为 <class_1_john ,{id=1, name=john, age=12}>

在样例中，tableName为sideTable，主键为id，column为id，message，所以对应的redis数据插入语句为<hset sideTable_5 id 5 message redis>

数据在redis中对应的数据存储情况为：
```
192.168.80.105:6379> hgetall sideTable_5
1) "id"
2) "5"
3) "message"
4) "redis"
```


