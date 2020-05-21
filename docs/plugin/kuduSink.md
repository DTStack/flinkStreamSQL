## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='kudu',
    kuduMasters ='ip1,ip2,ip3',
    tableName ='impala::default.test',
    writeMode='upsert',
    workerCount='1',
    defaultOperationTimeoutMs='600000',
    defaultSocketReadTimeoutMs='6000000',
    parallelism ='parllNum'
 );


```

## 2.支持版本
kudu 1.9.0+cdh6.2.0 

## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称
| colName | 列名称，redis中存储为 表名:主键名:主键值:列名]|
| colType | 列类型 [colType支持的类型](docs/colType.md)|


## 4.参数：
  
|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
| type | 表名 输出表类型[mysq&#124;hbase&#124;elasticsearch&#124;redis&#124;kudu]|是||
| kuduMasters | kudu master节点的地址;格式ip[ip，ip2]|是||
| tableName | kudu 的表名称|是||
| writeMode | 写入kudu的模式 insert&#124;update&#124;upsert |否 |upsert
| workerCount | 工作线程数 |否|
| defaultOperationTimeoutMs | 操作超时时间 |否|
| defaultSocketReadTimeoutMs | socket读取超时时间 |否|
| parallelism | 并行度设置|否|1|
      
  
## 5.样例：
```
CREATE TABLE MyTable(
    channel varchar,
    name varchar,
    pv varchar,
    a varchar,
    b varchar
 )WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='es_test',
    timezone='Asia/Shanghai',
    updateMode ='append',
    enableKeyPartitions ='false',
    topicIsPattern ='false',
    parallelism ='1'
 );

CREATE TABLE MyResult(
    a string,
    b string,
    c string,
    d string
 )WITH(
    type ='kudu',
    kuduMasters ='cdh03.cdhsite:7051',
    tableName ='myresult',
    writeMode='insert',
    parallelism ='1'
 );

CREATE TABLE sideTable(
    c string,
    d string,
    PRIMARY KEY(c) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='kudu',
    kuduMasters ='cdh03.cdhsite:7051',
    tableName ='sidetest4',
    partitionedJoin ='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1',
    primaryKey ='c',
    isFaultTolerant ='false'
 );

insert         
into
    MyResult
    select
        MyTable.a,
        MyTable.b,
        s.c,
        s.d                                
    from
        MyTable                                    
    join
        sideTable s                                                                                            
            on MyTable.a = s.c                                    
    where
        MyTable.a='2'                                                              
        and s.d='2'

 ```

## 6.数据示例
### 输入数据
```
{"channel":"daishuyun","name":"roc","pv":"10","a":"2","b":"2"}
```
### 结果数据
```
{"a":"2","b":"2","c":"3","d":"4"}
```