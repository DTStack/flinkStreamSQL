## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='console',
    parallelism ='parllNum'
 );

```

## 2.支持版本
没有限制
 
## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName| 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](docs/colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|type |表明 输出表类型[console]|是||
| parallelism | 并行度设置|否|1|

## 5.样例：
```
CREATE TABLE MyResult(
    name VARCHAR,
    channel VARCHAR
 )WITH(
    type ='console',
    parallelism ='1'
 )
 ```
 
 ## 6.输出结果:
 ```
 +------+---------+
 | name | channel |
 +------+---------+
 |  aa  |   02    |
 +------+---------+
 ```