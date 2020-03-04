
## 1.格式：
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type='elasticsearch6',
     address ='ip:port[,ip:port]',
     cluster='clusterName',
     estype ='esType',
     index ='index',
     authMesh='true',
     userName='dbUserName',
     password='dbPwd',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
  );
```

# 2.支持版本
 elasticsearch 6.8.6
 
## 3.表结构定义
  
 |参数名称|含义|
 |----|---|
 | tableName | elasticsearch表名称|
 | colName | 列名称|
 | colType | 列类型 [colType支持的类型](colType.md)|
 | PERIOD FOR SYSTEM_TIME | 关键字表明该定义的表为维表信息|
 | PRIMARY KEY(keyInfo) | 维表主键定义;多个列之间用逗号隔开|
 
## 4.参数

  |参数名称|含义|是否必填|默认值|
  |----|---|---|----|
  type|表明 输出表类型[elasticsearch6]|是||
  |address | 连接ES Transport地址(tcp地址)|是||
  |cluster | ES 集群名称 |是||
  |index | 选择的ES上的index名称|否||
  |esType | 选择ES上的type名称|否||
  |authMesh | 是否进行用户名密码认证 | 否 | false|
  |userName | 用户名 | 否，authMesh='true'时为必填 ||
  |password | 密码 | 否，authMesh='true'时为必填 ||
  | cache | 维表缓存策略(NONE/LRU)|否|NONE|
  | partitionedJoin | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|
  |parallelism | 并行度设置|否|1|
  
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
    type ='elasticsearch6',
    address ='172.16.10.47:9500',
    cluster='es_47_menghan',
    estype ='type1',
    index ='xc_es_test',
    authMesh='true',
    userName='dtstack',
    password='abc123',
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


