## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='cassandra',
    address ='ip:port[,ip:port]',
    userName ='userName',
    password ='pwd',
    database ='databaseName',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 cassandra-3.6.x
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表明 输出表类型 cassandra|是||
|address | 连接cassandra数据库 jdbcUrl |是||
|userName | cassandra连接用户名|否||
|password | cassandra连接密码|否||
|tableName | cassandra表名称|是||
|database  | cassandra表名称|是||
|parallelism | 并行度设置|否|1|
|maxRequestsPerConnection | 每个连接最多允许64个并发请求|否|NONE|
|coreConnectionsPerHost   | 和Cassandra集群里的每个机器都至少有2个连接|否|NONE|
|maxConnectionsPerHost    | 和Cassandra集群里的每个机器都最多有6个连接|否|NONE|
|maxQueueSize             | Cassandra队列大小|否|NONE|
|readTimeoutMillis        | Cassandra读超时|否|NONE|
|connectTimeoutMillis     | Cassandra连接超时|否|NONE|
|poolTimeoutMillis        | Cassandra线程池超时|否|NONE|
  
## 5.样例：
```
CREATE TABLE MyResult(
    channel VARCHAR,
    pv VARCHAR
 )WITH(
    type ='cassandra',
    address ='172.21.32.1:9042,172.21.32.1:9042',
    userName ='dtstack',
    password ='abc123',
    database ='test',
    tableName ='pv',
    parallelism ='1'
 )
 ```