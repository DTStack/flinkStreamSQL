## 关系型数据库维表参数

适用于`MYSQL`,`ORACLE`,`SQLSERVER`,`POSTGRESQL`,`DB2`,`POLARDB`,`CLICKHOUSE`,`IMPALA`维表插件

### 维表参数

|参数名称|含义|是否必填|默认值|
|----|---|---|----|
| type | 维表类型， 例如:mysql |是||
| url | 连接数据库 jdbcUrl |是||
| userName | 连接用户名 |是||
| password | 连接密码|是||
| tableName| 表名称|是||
| schema| 表空间|否||
| cache | 维表缓存策略(NONE/LRU/ALL)|否|LRU|
| partitionedJoin | 是否在維表join之前先根据设定的key 做一次keyby操作(可以減少维表的数据缓存量)|否|false|

### 缓存策略

-  NONE：不做内存缓存。每条流数据触发一次维表查询操作。
-  ALL:  任务启动时，一次性加载所有数据到内存，并进行缓存。适用于维表数据量较小的情况。
-  LRU:  任务执行时，根据维表关联条件使用异步算子加载维表数据，并进行缓存。

#### ALL全量维表参数

|参数名称|含义|默认值|
|----|---|----|
| cacheTTLMs | 缓存周期刷新时间 |60，单位s|

#### LRU异步维表参数

|参数名称|含义|默认值|
|----|---|----|
| cacheTTLMs | LRU缓存写入后超时时间 |60，单位s|
| cacheSize | LRU缓存大小 |10000|
| cacheMode | 异步请求处理有序还是无序，可选：ordered，unordered  |ordered|
| asyncCapacity | 异步线程容量 |100|
| asyncTimeout | 异步处理超时时间 |10000，单位毫秒|
| asyncPoolSize | 异步查询DB最大线程池，上限20 |min(20,Runtime.getRuntime().availableProcessors() * 2)|


