## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='wredis',
    url = 'ip',
    key ='appName',
    password ='pwd',
    tableName ='tableName',
    timeout = '1000',
    parallelism ='parllNum'
 );


```

## 2.支持版本
wredis2.0+

## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称
| colName | 列名称，redis中存储为 表名:主键名:主键值:列名]|
| colType | 列类型，当前只支持varchar|
| PRIMARY KEY(keyInfo) | 结果表主键定义;多个列之间用逗号隔开|

## 4.参数：
参照wridis WIKI:http://wiki.58corp.com/index.php?title=WRedis
|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
|type | 表明 输出表类型[mysql\|hbase\|elasticsearch\|wredis\]|是||
| url | redis 的地址;格式ip（port默认）|是||
| key | redis 的app key |是||
| password | redis 的密码 |否|现在的wredis不设置密码也能成功访问|
| tableName | redis 的表名称|是||
| timeout | 连接超时时间|否|1000|
|parallelism | 并行度设置|否|1|
      
  
## 5.样例：
```
 CREATE TABLE MyResult(
    channel varchar,
    pv varchar,
    PRIMARY KEY(channel)
 )WITH(
    type='wredis',
    url='10.48.160.2',
    key='6029d61e1c1b75d28a2f9df6d88802dc',
    tableName='test_for_public',
    parallelism ='1'
 );

 ```