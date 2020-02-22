## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
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
| tableName| mysql表名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表名 输出表类型[mysq&#124;hbase&#124;elasticsearch]|是||
|url | 连接mysql数据库 jdbcUrl |是||
|userName | mysql连接用户名 |是||
| password | mysql连接密码|是||
| tableName | mysql表名称|是||
| parallelism | 并行度设置|否||
| updateMode | 回溯流的处理模式，update或者append，默认根据主键判断,update模式下需要指定主键|否||
| allReplace | 主键冲突时对数据的处理，全部替换or 非空值替换。|否|false|
| batchSize | 批插入数量|否|100|
| batchWaitInterval |自动触发刷新的间隔|否|10000，单位毫秒|








  
## 5.样例：
```
 CREATE TABLE MyResult (
 	 id varchar, 
 	 name varchar,
 	 city varchar,
 	 b3 varchar,
 	 primary key (id)
)  WITH (
 	type='mysql',
 	url='jdbc:mysql://172.16.8.109:3306/mqtest?charset=utf8',
    userName='dtstack',
    password='abc123',
    tableName='sink_table_02',
    batchSize='1',
 	updateMode = 'update',
 	updateMode = 'append',
 	--allReplace = 'false'
 	allReplace = 'true'
 );
 ```