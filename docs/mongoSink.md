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
| colType | 列类型 [colType支持的类型](colType.md)|

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
```
CREATE TABLE MyResult(
    channel VARCHAR,
    pv VARCHAR
 )WITH(
    type ='mongo',
    address ='172.21.32.1:27017,172.21.32.1:27017',
    userName ='dtstack',
    password ='abc123',
    database ='test',
    tableName ='pv',
    parallelism ='1'
 )
 ```