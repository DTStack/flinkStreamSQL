
## 1.格式：
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type='impala',
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
 2.10.0-cdh5.13.0
 
## 3.表结构定义
  
 |参数名称|含义|
 |----|---|
 | tableName | 注册到flink的表名称|
 | colName | 列名称|
 | colType | 列类型 [colType支持的类型](docs/colType.md)|
 | PERIOD FOR SYSTEM_TIME | 关键字表明该定义的表为维表信息|
 | PRIMARY KEY(keyInfo) | 维表主键定义;多个列之间用逗号隔开|
 
## 4.参数

  |参数名称|含义|是否必填|默认值|
  |----|---|---|----|
  | type | 表明维表的类型[impala] |是||
  | url | 连接postgresql数据库 jdbcUrl |是||
  | userName | postgresql连接用户名 |是||
  | password | postgresql连接密码|是||
  | tableName | postgresql表名称|是||
  | authMech | 身份验证机制 (0, 1, 2, 3), 暂不支持kerberos |是|0|
  | principal | kerberos用于登录的principal（authMech=1时独有） |authMech=1为必填|
  | keyTabFilePath | keytab文件的路径（authMech=1时独有） |authMech=1为必填 ||
  | krb5FilePath | krb5.conf文件路径（authMech=1时独有） |authMech=1为必填||
  | krbServiceName | Impala服务器的Kerberos principal名称（authMech=1时独有） |authMech=1为必填||
  | krbRealm | Kerberos的域名（authMech=1时独有） |否| HADOOP.COM |
  | enablePartition | 是否支持分区|否|false|
  | partitionfields | 分区字段名|否,enablePartition='true'时为必填||
  | partitionFieldTypes | 分区字段类型 |否,enablePartition='true'时为必填||
  | partitionValues | 分区值|否,enablePartition='true'时为必填||
  | cache | 维表缓存策略(NONE/LRU/ALL)|否|NONE|
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
    channel varchar,
    xccount int,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='impala',
    url='jdbc:impala://localhost:21050/mytest',
    userName='dtstack',
    password='abc123',
    tableName='sidetest',
    authMech='3',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1',
    partitionedJoin='false'
 );


```

## 6.分区样例

注：分区字段放在最后面，如下，name是分区字段，放在channel，xccount字段的后面

```
create table sideTable(
    channel varchar,
    xccount int,
    name varchar,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='impala',
    url='jdbc:impala://localhost:21050/mytest',
    userName='dtstack',
    password='abc123',
    tableName='sidetest',
    authMech='3',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1',
    enablePartition='true',
    partitionfields='name',
    partitionFieldTypes='varchar',
    partitionValues='{"name":["tom","jeck"]}',
    partitionedJoin='false'
 );

```


