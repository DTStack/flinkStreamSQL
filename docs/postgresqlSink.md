## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='postgresql',
    url ='jdbcUrl',
    userName ='userName',
    password ='pwd',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 postgresql-9.5+
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
| type |表明 输出表类型[postgresql]|是||
| url | 连接postgresql数据库 jdbcUrl |是||
| userName | postgresql连接用户名 |是||
| password | postgresql连接密码|是||
| tableName | postgresqll表名称|是||
| parallelism | 并行度设置|否|1|

## 5.样例：
```
CREATE TABLE MyResult(
    channel VARCHAR,
    pv VARCHAR
 )WITH(
    type ='postgresql',
    url ='jdbc:postgresql://localhost:9001/test?sslmode=disable',
    userName ='dtstack',
    password ='abc123',
    tableName ='pv2',
    parallelism ='1'
 )
 ```
