## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='redis',
    url = 'ip:port',
    database ='dbName',
    password ='pwd',
    redisType='1',
    tableName ='tableName',
    parallelism ='parllNum'
 );


```

## 2.支持版本
redis5.0

## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称
| colName | 列名称，redis中存储为 表名:主键名:主键值:列名]|
| colType | 列类型，当前只支持varchar|
| PRIMARY KEY(keyInfo) | 结果表主键定义;多个列之间用逗号隔开|

## 4.参数：
  
|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
| type | 表明 输出表类型[mysql\|hbase\|elasticsearch\|redis\]|是||
| url | redis 的地址;格式ip:port[,ip:port]|是||
| password | redis 的密码 |是||
| redisType | redis模式（1 单机，2 哨兵， 3 集群）| 是 |
| masterName | 主节点名称（哨兵模式下为必填项） | 否 |
| database | reids 的数据库地址|否||
| tableName | redis 的表名称|是||
| parallelism | 并行度设置|否|1|
      
  
## 5.样例：
```
 CREATE TABLE MyResult(
    channel varchar,
    pv varchar,
    PRIMARY KEY(channel)
 )WITH(
    type='redis',
    url='172.16.10.79:6379',
    password='abc123',
    database='0',
    redisType='1',
    tableName='sinktoredis'
 );

 ```