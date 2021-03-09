## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    bb INT
 )WITH(
    type ='elasticsearch6',
    address ='ip:port[,ip:port]',
    cluster='clusterName',
    esType ='esType',
    index ='index',
    id ='num[,num]'(id = 'field[,field]'),
    authMesh = 'true',
    userName = 'userName',
    password = 'password',
    parallelism ='1'
 )
```
## 2.支持的版本
   elasticsearch 6.8.6

## 3.表结构定义
 
|参数名称|含义|
|----|---|
|tableName|在 sql 中使用的名称;即注册到flink-table-env上的名称|  
|colName|列名称|
|colType|列类型 [colType支持的类型](../colType.md)|
   
## 4.参数：
|参数名称|含义|是否必填|默认值|
|----|---|---|----|
|type|表明 输出表类型[elasticsearch6]|是||
|address | 连接ES Transport地址(tcp地址)|是||
|cluster | ES 集群名称 |是||
|index | 选择的ES上的index名称|是||
|esType | 选择ES上的type名称|是||
|id | 生成id的规则(当前是根据指定的字段名称(或者字段position)获取字段信息,拼接生成id)|否||
| |若id为空字符串或索引都超出范围，则随机生成id值)|||
|authMesh | 是否进行用户名密码认证 | 否 | false|
|userName | 用户名 | 否，authMesh='true'时为必填 ||
|password | 密码 | 否，authMesh='true'时为必填 ||
|parallelism | 并行度设置|否|1|
  
## 5.完整样例：
```
CREATE TABLE MyTable(
    channel varchar,
    pv INT,
    xctime bigint
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
    pv varchar,
    channel varchar
 )WITH(
    type ='elasticsearch6',
    address ='172.16.8.193:9200',
    authMesh='true',
    username='elastic',
    password='abc123',
    estype ='external',
    cluster ='docker-cluster',
    index ='myresult',
    id ='pv',
    updateMode ='append',
    parallelism ='1'
 );

CREATE TABLE sideTable(
    a varchar,
    b varchar,
    PRIMARY KEY(a) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='elasticsearch6',
    address ='172.16.8.193:9200',
    estype ='external',
    cluster ='docker-cluster',
    index ='sidetest',
    authMesh='true',
    username='elastic',
    password='abc123',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    partitionedJoin ='false',
    parallelism ='1'
 );

insert   
into
    MyResult
    select
        w.b as pv,
        s.channel as channel      
    from
        MyTable  s     
    join
        sideTable  w                        
            on s.pv = w.a           
    where
        w.a = '10'                   
        and s.channel='xc';
 ```
## 6.结果数据
### 输入数据
```
{"channel":"xc26","pv":10,"xctime":1232312}
```
### 输出数据
```
http://172.16.8.193:9200/myresult/_search
{"_index":"myresult","_type":"external","_id":"8aX_DHIBn3B7OBuqFl-i","_score":1.0,"_source":{"pv":"10","channel":"xc26"}}
```