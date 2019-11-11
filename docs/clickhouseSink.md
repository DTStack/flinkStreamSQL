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
| colType | 列类型 [colType支持的类型](colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表明 输出表类型 clickhouse |是||
|url | 连接clickhouse 数据库 jdbcUrl |是||
|userName | clickhouse 连接用户名 |是||
| password | clickhouse 连接密码|是||
| tableName | clickhouse 表名称|是||
| parallelism | 并行度设置|否|1|
  
## 5.样例：
```
CREATE TABLE MyResult(
    channel VARCHAR,
    pv VARCHAR
 )WITH(
    type ='clickhouse',
    url ='jdbc:clickhouse://172.16.8.104:3306/test?charset=utf8',
    userName ='dtstack',
    password ='abc123',
    tableName ='pv2',
    parallelism ='1'
 )
 ```