## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='polardb',
    url ='jdbcUrl',
    userName ='userName',
    password ='pwd',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 mysql-8.0.16
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| polardb表名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](../colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表名 输出表类型 polardb|是||
|url | 连接polardb数据库 jdbcUrl |是||
|userName | polardb连接用户名 |是||
| password | polardb连接密码|是||
| tableName | polardb表名称|是||
| parallelism | 并行度设置|否|1|
  
## 5.样例：
```
CREATE TABLE MyResult(
    channel VARCHAR,
    pv VARCHAR
 )WITH(
    type ='polardb',
    url ='jdbc:mysql://xxx.xxx.xxx:3306/test?charset=utf8',
    userName ='dtstack',
    password ='abc123',
    tableName ='pv2',
    parallelism ='1'
 );
 ```