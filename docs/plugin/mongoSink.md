## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='mongo',
    address ='ip:port[,ip:port]',
    userName ='userName',
    password ='pwd',
    database ='databaseName',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 mongo-3.8.2
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](docs/colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表明 输出表类型 mongo|是||
|address | 连接mongo数据库 jdbcUrl |是||
|userName | mongo连接用户名|否||
|password | mongo连接密码|否||
|tableName | mongo表名称|是||
|database  | mongo表名称|是||
|parallelism | 并行度设置|否|1|
  
## 5.样例：

双流join并插入mongo

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
    type ='mongo',
    address ='172.16.8.193:27017',
    database ='dtstack',
    tableName ='userInfo'
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
 
 
 数据结果：
 
 向Topic mqTest03 发送数据  {"name":"maqi","id":1001}  插入 (1001,"maqi",null)
 
 向Topic mqTest04 发送数据  {"address":"hz","id":1001} 插入  (1001,"maqi","hz")