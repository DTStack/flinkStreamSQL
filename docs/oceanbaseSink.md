## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='oceanbase',
    url ='jdbcUrl',
    userName ='userName',
    password ='pwd',
    tableName ='tableName',
    parallelism ='parllNum'
 );

```

## 2.支持版本
 oceanbase 1.+, 2.+
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| oceanbase表名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表名 输出表类型|是||
|url | 连接oceanbase数据库 jdbcUrl |是||
|userName | oceanbase连接用户名 |是||
| password | oceanbase连接密码|是||
| tableName | oceanbase表名称|是||
| parallelism | 并行度设置|否|1|
  
## 5.样例：
```
CREATE TABLE MyResult(
    channel VARCHAR,
    pv VARCHAR
 )WITH(
    type ='oceanbase',
    url ='jdbc:mysql://172.16.8.104:3306/test',
    userName ='dtstack',
    password ='abc123',
    tableName ='pv2',
    parallelism ='1'
 )
 ```