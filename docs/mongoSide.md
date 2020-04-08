
## 1.格式：
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
    type ='mongo',
    address ='ip:port[,ip:port]',
     userName='dbUserName',
     password='dbPwd',
     tableName='tableName',
     database='database',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
  );
```

# 2.支持版本
 mongo-3.8.2
 
## 3.表结构定义
  
 |参数名称|含义|
 |----|---|
 | tableName | 注册到flink的表名称(可选填;不填默认和hbase对应的表名称相同)|
 | colName | 列名称|
 | colType | 列类型 [colType支持的类型](colType.md)|
 | PERIOD FOR SYSTEM_TIME | 关键字表明该定义的表为维表信息|
 | PRIMARY KEY(keyInfo) | 维表主键定义;多个列之间用逗号隔开|
 
## 4.参数

  |参数名称|含义|是否必填|默认值|
  |----|---|---|----|
  | type |表明 输出表类型 mongo|是||
  | address | 连接mongo数据库 jdbcUrl |是||
  | userName | mongo连接用户名|否||
  | password | mongo连接密码|否||
  | tableName | mongo表名称|是||
  | database  | mongo表名称|是||
  | cache | 维表缓存策略(NONE/LRU)|否|NONE|
  | partitionedJoin | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|
  
  ----------
  > 缓存策略
  * NONE: 不做内存缓存
  * LRU:
    * cacheSize: 缓存的条目数量
    * cacheTTLMs:缓存的过期时间(ms)
  

## 5.样例
```
create table sideTable(
    CHANNEL varchar,
    XCCOUNT int,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='mongo',
    address ='172.21.32.1:27017,172.21.32.1:27017',
    database ='test',
    tableName ='sidetest',
    cache ='LRU',
    parallelism ='1',
    partitionedJoin='false'
 );


```


