## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='impala',
    url ='jdbcUrl',
    userName ='userName',
    password ='pwd',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 2.10.0-cdh5.13.0
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](docs/colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
| type |表明 输出表类型[impala]|是||
| url | 连接postgresql数据库 jdbcUrl |是||
| userName | postgresql连接用户名 |是||
| password | postgresql连接密码|是||
| tableName | postgresqll表名称|是||
| authMech | 身份验证机制 (0, 1, 2, 3),暂不支持kerberos |是|0|
| principal | kerberos用于登录的principal（authMech=1时独有） |authMech=1为必填|
| keyTabFilePath | keytab文件的路径（authMech=1时独有） |authMech=1为必填 ||
| krb5FilePath | krb5.conf文件路径（authMech=1时独有） |authMech=1为必填||
| krbHostFQDN | 主机的标准域名（authMech=1时独有） |authMech=1为必填 ||
| krbServiceName | Impala服务器的Kerberos principal名称（authMech=1时独有） |authMech=1为必填||
| krbRealm | Kerberos的域名（authMech=1时独有） |否| HADOOP.COM |
| enablePartition | 是否支持分区 |否|false|
| partitionFields | 分区字段名|否，enablePartition='true'时为必填||
| parallelism | 并行度设置|否|1|


## 5.样例：
```
CREATE TABLE MyTable(
      channel VARCHAR,
      pt int,
      xctime varchar,
      name varchar
 )WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='mqTest03'
 );

CREATE TABLE MyResult(
    a STRING,
    b STRING
 )WITH(
    type ='impala',
    url ='jdbc:impala://172.16.101.252:21050/hxbho_pub',
    userName ='root',
    password ='pwd',
    authMech ='3',
    tableName ='tb_result_4',
    parallelism ='1',
    -- 指定分区
    partitionFields  = 'pt=1001,name="name1001" ',
    batchSize = '1000',
    parallelism ='2'
 );

CREATE TABLE MyResult1(
    a STRING,
    b STRING,
    pt int,
    name STRING
 )WITH(
    type ='impala',
    url ='jdbc:impala://172.16.101.252:21050/hxbho_pub',
    userName ='root',
    password ='Wscabc123..@',
    authMech ='3',
    tableName ='tb_result_4',
    parallelism ='1',
    enablePartition ='true',
    -- 动态分区
    partitionFields  = 'pt,name ',
    batchSize = '1000',
    parallelism ='2'
 );


insert  
into
    MyResult1
    select
       xctime AS b,
       channel AS a,
       pt,
       name 
    from
        MyTable;



insert  
into
    MyResult
    select
       xctime AS b,
       channel AS a
    from
        MyTable;
        
        
        

 ```
