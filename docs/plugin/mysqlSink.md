## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType,
     [primary key (colName)]
 )WITH(
    type ='mysql',
    url ='jdbcUrl',
    userName ='userName',
    password ='pwd',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 mysql-5.6.35
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| 结果表名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](docs/colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表名 输出表类型[mysq&#124;hbase&#124;elasticsearch]|是||
|url | 连接mysql数据库 jdbcUrl |是||
|userName | mysql连接用户名 |是||
|password | mysql连接密码|是||
|tableName | mysql表名称|是||
|parallelism | 并行度设置|否|1|
|batchSize | flush的大小|否|100|
|batchWaitInterval | flush的时间间隔，单位ms|否|1000|
|allReplace| true:新值替换旧值|否|false|
|updateMode| APPEND：不回撤数据，只下发增量数据，UPSERT：先删除回撤数据，然后更新|否|结果表设置主键则为UPSERT|

## 5.完整样例：
```
CREATE TABLE MyTable(
    id int,
    channel varchar,
    pv varchar,
    xctime varchar,
    name varchar
 )WITH(
    type ='kafka10',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='es_test',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );

CREATE TABLE MyResult(
    pv VARCHAR,
    channel VARCHAR
 )WITH(
    type ='mysql',
    url ='jdbc:mysql://172.16.10.134:3306/test',
    userName ='dtstack',
    password ='abc123',
    tableName ='myresult',
    parallelism ='1'
 );


insert  
into
    MyResult
    select
        channel,
        pv
    from
        MyTable        
 ```

## 6.结果表数据示例：
```
mysql> desc myresult;
+---------+--------------+------+-----+---------+-------+
| Field   | Type         | Null | Key | Default | Extra |
+---------+--------------+------+-----+---------+-------+
| channel | varchar(255) | YES  |     | NULL    |       |
| pv      | varchar(11)  | YES  |     | NULL    |       |
+---------+--------------+------+-----+---------+-------+
2 rows in set (0.00 sec)

mysql> select *  from myresult limit 1;
+---------+------+
| channel | pv   |
+---------+------+
| aa    | mq6  |
```