## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='oracle',
    url ='jdbcUrl',
    userName ='userName',
    password ='pwd',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
  10g 11g
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| oracle表名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](docs/colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表名 输出表类型[mysq&#124;hbase&#124;elasticsearch&#124;oracle]|是||
|url | 连接oracle数据库 jdbcUrl |是||
|userName | oracle连接用户名 |是||
| password | oracle连接密码|是||
| tableName | oracle表名称|是||
| schema | oracle 的schema|否|当前登录用户|
| parallelism | 并行度设置|否|1|
| batchSize | flush的大小|否|100|
| batchWaitInterval | flush的时间间隔，单位ms|否|1000|
| allReplace | true:新值替换旧值|否|false|
| updateMode | APPEND：不回撤数据，只下发增量数据，UPSERT：先删除回撤数据，然后更新|否|结果表设置主键则为UPSERT|

  
## 5.样例：
```
CREATE TABLE MyTable(
    name varchar,
    channel varchar,
    id int
 )WITH(
    type ='kafka10',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='mqTest01',
    timezone='Asia/Shanghai',
    updateMode ='append',
    enableKeyPartitions ='false',
    topicIsPattern ='false',
    parallelism ='1'
 );

 CREATE TABLE MyResult(
    primarykey_id int ,
    name VARCHAR,
    address VARCHAR
 )WITH(
    type ='oracle',
    url ='jdbc:oracle:thin:@172.16.8.178',
    userName ='system',
    password ='oracle',
    tableName ='YELUO_TEST_ORACLE_01',
    updateMode ='append',
    parallelism ='1',
    batchSize ='100',
    batchWaitInterval ='1000'
 );

insert          
into
    MyResult
    select
        id as primarykey_id,
        channel as address,
        name                                         
    from
        MyTable a       
 ```

## 6.数据示例
### 输入数据
```
{"name":"roc","id":11,"channel":"daishuyun"}
```
### 结果数据
```
+---------+------+------+-----------+
| primarykey_id | name   | address  |
+---------+------+------+----------+
|   11   | roc   |  daishuyun  |
```