## 1.格式：
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type='polardb',
     url='jdbcUrl',
     userName='dbUserName',
     password='dbPwd',
     tableName='tableName',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
  );
```

# 2.支持版本
 mysql-8.0.16
 
## 3.表结构定义
  
 |参数名称|含义|
 |----|---|
 | tableName | polardb表名称|
 | colName | 列名称|
 | colType | 列类型 [colType支持的类型](colType.md)|
 | PERIOD FOR SYSTEM_TIME | 关键字表明该定义的表为维表信息|
 | PRIMARY KEY(keyInfo) | 维表主键定义;多个列之间用逗号隔开|
 
## 4.参数

  |参数名称|含义|是否必填|默认值|
  |----|---|---|----|
  | type | 表明维表的类型 polardb |是||
  | url | 连接polardb数据库 jdbcUrl |是||
  | userName | ploardb连接用户名 |是||
  | password | ploardb连接密码|是||
  | tableName | ploardb表名称|是||
  | cache | 维表缓存策略(NONE/LRU)|否|NONE|
  | partitionedJoin | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|
  
  ----------
  > 缓存策略
  * NONE: 不做内存缓存
  * LRU:
    * cacheSize: 缓存的条目数量
    * cacheTTLMs:缓存的过期时间(ms)
    * cacheMode: (unordered|ordered)异步加载是有序还是无序,默认有序。
    * asyncCapacity:异步请求容量，默认1000
    * asyncTimeout：异步请求超时时间，默认10000毫秒

## 5.样例
```
create table sideTable(
    channel varchar,
    xccount int,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='polardb',
    url='jdbc:mysql://xxx.xxx.xxx:3306/test?charset=utf8',
    userName='dtstack',
    password='abc123',
    tableName='sidetest',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    cacheMode='unordered',
    asyncCapacity='1000',
    asyncTimeout='10000'
    parallelism ='1',
    partitionedJoin='false'
 );
 ```