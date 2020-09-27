## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='dameng',
    url ='jdbcUrl',
    userName ='userName',
    password ='pwd',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
  dameng7
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| dameng表名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](../colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表名 输出表类型[dameng]|是||
|url | 连接dameng数据库 jdbcUrl |是||
|userName | dameng连接用户名 |是||
| password | dameng连接密码|是||
| tableName | dameng表名称|是||
| schema | dameng 的schema|否|当前登录用户|
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
    type ='kafka11',
    bootstrapServers ='kerberos01:9092',
    zookeeperQuorum ='kerberos01:2181/kafka',
    offsetReset ='latest',
    topic ='tiezhu_in',
    timezone='Asia/Shanghai',
    updateMode ='append',
    enableKeyPartitions ='false',
    topicIsPattern ='false',
    parallelism ='1'
 );

 CREATE TABLE MyResult(
    ID int ,
    NAME VARCHAR,
    MESSAGE VARCHAR,
    PRIMARY KEY (ID)
 )WITH(
    type ='dameng',
    url =' jdbc:dm://172.16.8.178:5236/DMTEST?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=utf-8',
    userName = 'SYSDBA',
    password = 'SYSDBA',
    tableName = 'ID_NAME_MESSAGE',
    updateMode ='upsert',
    schema = 'TIEZHU',
    parallelism = '1',
    batchSize = '100',
    batchWaitInterval = '1000'
 );

insert
into
    MyResult
    select
        id AS ID,
        channel AS MESSAGE,
        name AS NAME
    from
        MyTable a

 ```

## 6.数据示例
### 输入数据
```
{"name":"wtz1000","channel":"message from kafka","id":1000}
```
### 结果数据
```
+---------+-------+------+-----------+
| id      | name  | message          |
+---------+-------+------+-----------+
|  1000   |wtz1000| message from kafka|
```