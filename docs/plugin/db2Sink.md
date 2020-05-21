## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType,
    [primary key (colName)]
 )WITH(
    type ='db2',
    url ='jdbcUrl',
    userName ='userName',
    password ='pwd',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 db2 9.X
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| db2表名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](docs/colType.md)|
| primary key | updateMode为UPSERT时，需要指定的主键信息，不需要和数据库一致|


## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |结果表插件类型，必须为db2|是||
|url | 连接db2数据库 jdbcUrl |是||
|userName |db2连接用户名 |是||
|password | db2连接密码|是||
|tableName | db2表名称|是||
|schema | db2表空间|否||
|parallelism | 并行度设置|否|1|
|batchSize | flush的大小|否|100|
|batchWaitInterval | flush的时间间隔，单位ms|否|1000|
|allReplace| true:新值替换旧值|否|false|
|updateMode| APPEND：不回撤数据，只下发增量数据，UPSERT：先删除回撤数据，然后更新|否|结果表设置主键则为UPSERT|
  
## 5.样例：

回溯流删除

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
    address VARCHAR,
    primary key (id)
)WITH(
    type='db2',
    url='jdbc:db2://172.16.10.251:50000/mqTest',
    userName='DB2INST1',
    password='abc123',
    tableName='USER_INFO2',
    schema = 'DTSTACK',
    updateMode = 'upsert',   // 设置 primary key则默认为upsert
    batchSize = '1'
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
 
 
 DB2结果表建表语句：
 
 ```aidl
CREATE TABLE "DTSTACK "."USER_INFO2"  (
		  "ID" INTEGER , 
		  "NAME" VARCHAR(50 OCTETS) , 
		  "ADDRESS" VARCHAR(50 OCTETS) )   
		 IN "USERSPACE1"  
		 ORGANIZE BY ROW
 ;

GRANT CONTROL ON TABLE "DTSTACK "."USER_INFO2" TO USER "DB2INST1" 
;
```
 
 
 
数据结果：

向Topic mqTest03 发送数据  {"name":"maqi","id":1001}  插入 (1001,"maqi",null)

向Topic mqTest04 发送数据  {"address":"hz","id":1001} 删除 (1001,"maqi",null) 插入  (1001,"maqi","hz")