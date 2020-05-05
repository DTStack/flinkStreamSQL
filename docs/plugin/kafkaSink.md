## 1.格式：
```
CREATE TABLE MyResult(
    name varchar,
    channel varchar
 )WITH(
    type ='kafka',
    bootstrapServers ='localhost:9092',
    zookeeperQuorum ='localhost:2181/kafka',
    offsetReset ='latest',
    topic ='sink_test',
    parallelism ='1',
    sinkDataType ='protobuf',
    descriptorHttpGetUrl ='http://localhost:8080/v3/user/protodescsink'
 );

```

## 2.支持版本
    kafka09,kafka10,kafka11及以上版本    
 **kafka读取和写入的版本必须一致，否则会有兼容性错误。**
 
## 3.参数：

|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是|kafka09、kafka10、kafka11、kafka(对应kafka1.0及以上版本)|
|bootstrapServers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|zookeeperQuorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|topic | 需要读取的 topic 名称|是||
|topicIsPattern | topic是否是正则表达式格式(true&#124;false)  |否| false
|parallelism | 并行度设置|否|1|
|sinkdatatype | 数据类型|否|json|

## 4.样例：
```
CREATE TABLE MyResult(
    name varchar,
    channel varchar
 )WITH(
    type ='kafka',
    bootstrapServers ='localhost:9092',
    zookeeperQuorum ='localhost:2181/kafka',
    offsetReset ='latest',
    topic ='sink_test',
    parallelism ='1',
    sinkDataType ='protobuf',
    descriptorHttpGetUrl ='http://localhost:8080/protobuf/descriptor'
 );
 ```