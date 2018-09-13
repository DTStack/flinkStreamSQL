
## 1.格式：
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type='mysql',
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
  
## 2.参数

  * tableName ==> 注册到flink的表名称(可选填;不填默认和hbase对应的表名称相同)
  * colName ==> 列名称
  * colType ==> 列类型 [colType支持的类型](colType.md)
  * PERIOD FOR SYSTEM_TIME ==> 关键字表明该定义的表为维表信息
  * PRIMARY KEY(keyInfo) ==> 维表主键定义;多个列之间用逗号隔开
  * url ==> 连接mysql数据库 jdbcUrl 
  * userName ==> mysql连接用户名
  * password ==> mysql连接密码
  * tableName ==> mysql表名称
  * type ==> 表明维表的类型[hbase|mysql]
  
  * tableName ==> mysql 的表名称
  * cache ==> 维表缓存策略(NONE/LRU)
  
      > * NONE: 不做内存缓存
      > * LRU:
      > > cacheSize ==> 缓存的条目数量
      > > cacheTTLMs ==> 缓存的过期时间(ms)
  
  * partitionedJoin ==> 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)

## 3.样例
```
create table sideTable(
    channel String,
    xccount int,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='mysql',
    url='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',
    userName='dtstack',
    password='abc123',
    tableName='sidetest',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1',
    partitionedJoin='false'
 );


```


