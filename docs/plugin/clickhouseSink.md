## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='clickhouse',
    url ='jdbcUrl',
    userName ='userName',
    password ='pwd',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 19.14.x、19.15.x、19.16.x
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| clickhouse表名称|
| colName | 列名称|
| colType | clickhouse基本数据类型，不包括Array，Tuple,Nested等|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表明 输出表类型 clickhouse |是||
|url | 连接clickhouse 数据库 jdbcUrl |是||
|userName | clickhouse 连接用户名 |是||
| password | clickhouse 连接密码|是||
| tableName | clickhouse 表名称|是||
| parallelism | 并行度设置|否|1|
|updateMode| 只支持APPEND模式，过滤掉回撤数据|||

  
## 5.样例：


```

CREATE TABLE source1 (
    id int,
    name VARCHAR
)WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='mqTest03',
    timezone='Asia/Shanghai',
    topicIsPattern ='false'
 );



CREATE TABLE source2(
    id int,
    address VARCHAR
)WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='mqTest04',
    timezone='Asia/Shanghai',
    topicIsPattern ='false'
);


CREATE TABLE MyResult(
    id int,
    name VARCHAR,
    address VARCHAR
)WITH(
    type='clickhouse',
    url='jdbc:clickhouse://172.16.10.168:8123/tudou?charset=utf8',
    userName='dtstack',
    password='abc123',
    tableName='MyResult',
    updateMode = 'append'
);

insert into MyResult
select 
	s1.id,
	s1.name,
	s2.address
from 
	source1 s1
left join
	source2 s2
on 	
	s1.id = s2.id


 ```
 

**Clickhouse建表语句**

 
 ```aidl
CREATE TABLE tudou.MyResult (`id` Int64, `name` String, `address` String)
    ENGINE = MergeTree PARTITION BY address ORDER BY id SETTINGS index_granularity = 8192
```
 
 
 
 
数据结果：

向Topic mqTest03 发送数据  {"name":"maqi","id":1001}  插入 (1001,"maqi",null)

向Topic mqTest04 发送数据  {"address":"hz","id":1001} 插入  (1001,"maqi","hz")
 
 