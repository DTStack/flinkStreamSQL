## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    bb INT
 )WITH(
    type ='elasticsearch7',
    address ='ip:port[,ip:port]',
    index ='index',
    id = 'field[,field]',
    authMesh = 'true',
    userName = 'userName',
    password = 'password',
    parallelism ='1'
 )
```
## 2.支持的版本
   elasticsearch `7.x`

## 3.表结构定义
 
|参数名称|含义|
|----|---|
|tableName|在 sql 中使用的名称;即注册到flink-table-env上的名称|  
|colName|列名称|
|colType|列类型 [colType支持的类型](../colType.md)|
   
## 4.参数：
|参数名称|含义|是否必填|默认值|
|----|---|---|----|
|type|表明 输出表类型[elasticsearch7]|是||
|address | 连接ES Http地址|是||
|index | 选择的ES上的index名称,支持静态索引和动态索引，动态索引示例: `{user_name}`|是||
|index_definition| 为ES定义索引的字段类型、别名以及shard数量|否|
|id | 生成id的规则，根据字段名称定义文档ID|否|uuid|
|authMesh | 是否进行用户名密码认证（xpack） | 否 | false|
|userName | 用户名 | 否，authMesh='true'时为必填 ||
|password | 密码 | 否，authMesh='true'时为必填 ||
|parallelism | 并行度设置|否|1|
  
## 5.完整样例：
```
CREATE TABLE MyTable(
    channel varchar,
    pv int,
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
    channel varchar,
    pv int
 )WITH(
    type ='elasticsearch7',
    address ='172.16.8.193:9200',
    authMesh='true',
    username='elastic',
    password='abc123',
    estype ='external',
    cluster ='docker-cluster',
    index ='myresult',
--  index = '{pv}' # 动态索引写法
    id ='pv',
    parallelism ='1'
 );

CREATE TABLE sideTable(
    a varchar,
    b varchar,
    PRIMARY KEY(a) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='elasticsearch7',
    address ='172.16.8.193:9200',
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
http://localhost:9200/myresult/_search
{"_index":"myresult","_type":"external","_id":"8aX_DHIBn3B7OBuqFl-i","_score":1.0,"_source":{"pv":"10","channel":"xc26"}}
```