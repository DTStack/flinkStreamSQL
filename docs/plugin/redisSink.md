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

## 4.参数：
  
|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
| type | 表名 输出表类型[mysq&#124;hbase&#124;elasticsearch&#124;redis]|是||
| url | redis 的地址;格式ip:port[,ip:port]|是||
| password | redis 的密码 |是||
| redisType | redis模式（1 单机，2 哨兵， 3 集群）| 是 |
| masterName | 主节点名称（哨兵模式下为必填项） | 否 |
| database | reids 的数据库地址|否||
| tableName | redis 的表名称|是||
| parallelism | 并行度设置|否|1|
|timeout| 连接超时时间|否|10000|
|maxTotal|最大连接数|否|8|
|maxIdle|最大空闲连接数|否|8|
|minIdle|最小空闲连接数|否||0|
|masterName| 哨兵模式下的masterName|否||
|primarykeys|主键字段，多个字段以逗号分割|是||
|keyExpiredTime|redis sink的key的过期时间。默认是0（永不过期），单位是s。|否||
      
  
## 5.样例：
```
 CREATE TABLE MyTable(
     name varchar,
     channel varchar
  )WITH(
     type ='kafka10',
     bootstrapServers ='172.16.8.107:9092',
     zookeeperQuorum ='172.16.8.107:2181/kafka',
     offsetReset ='latest',
     topic ='mqTest01',
     timezone='Asia/Shanghai',
     updateMode ='append',
     enableKeyPartitions ='false',
     topicIsPattern ='false',
     parallelism ='1'
  );
 
 CREATE TABLE MyResult(
     channel VARCHAR,
     pv VARCHAR
  )WITH(
     type ='redis',
     primarykeys='name',
     redisType ='1',
     url ='172.16.8.109:6379',
     tableName ='resultTable',
     partitionedJoin ='false',
     parallelism ='1',
     database ='0',
     timeout ='10000',
     maxTotal ='60000',
     maxIdle='8',
     minIdle='0'
  );
 
 insert          
 into
     MyResult
     select
         channel,
         name as pv                                             
     from
         MyTable a                                        
 ```

## 6.redis完整样例
### redis数据说明
redis使用散列类型 hash 数据结构，key=tableName_primaryKey1_primaryKey2,value={column1=value1, column2=value2}
如果以班级class表为例，id和name作为联合主键，那么redis的结构为 <class_1_john ,{id=1, name=john, age=12}>

### 源表数据内容
```
{"name":"roc","channel":"daishu","age":2}
```
### redis实际数据内容
```
127.0.0.1:6379> keys *
1) "resultTable_roc"
127.0.0.1:6379> hgetall resultTable_roc
1) "channel"
2) "daishu"
3) "name"
4) "roc"
```