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
    id ='num[,num]',
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
|colType|列类型 [colType支持的类型](colType.md)|
   
## 4.参数：
|参数名称|含义|是否必填|默认值|
|----|---|---|----|
|type|表明 输出表类型[elasticsearch6]|是||
|address | 连接ES Transport地址(tcp地址)|是||
|cluster | ES 集群名称 |是||
|index | 选择的ES上的index名称|是||
|esType | 选择ES上的type名称|是||
|id | 生成id的规则(当前是根据指定的字段pos获取字段信息,拼接生成id;|是||
| |若id为空字符串或索引都超出范围，则随机生成id值)|||
|authMesh | 是否进行用户名密码认证 | 否 | false|
|userName | 用户名 | 否，authMesh='true'时为必填 ||
|password | 密码 | 否，authMesh='true'时为必填 ||
|parallelism | 并行度设置|否|1|
  
## 5.样例：
```
CREATE TABLE MyResult(
    aa INT,
    bb INT
 )WITH(
    type ='elasticsearch6',
    address ='172.16.10.47:9500',
    cluster='es_47_menghan',
    esType ='type1',
    index ='xc_es_test',
    authMesh = 'true',
    userName = 'elastic',
    password = 'abc123',
    id ='0,1',
    parallelism ='1'
 )
 ```