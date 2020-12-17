## 1.格式：
```
CREATE TABLE MyResult(
    colFamily:colName colType,
    ...
 )WITH(
    type ='hbase',
    zookeeperQuorum ='ip:port[,ip:port]',
    tableName ='tableName',
    rowKey ='colName[+colName]',
    parallelism ='1',
    zookeeperParent ='/hbase'
 )


```

## 2.支持版本
hbase2.0

## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称
| colFamily:colName | hbase中的列族名称和列名称
| colType | 列类型 [colType支持的类型](../colType.md)

## 4.参数：
  
|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
|type | 表明 输出表类型[mysq&#124;hbase&#124;elasticsearch]|是||
|zookeeperQuorum | hbase zk地址,多个直接用逗号隔开|是||
|zookeeperParent | zkParent 路径|是||
|tableName | 关联的hbase表名称|是||
|rowkey | hbase的rowkey关联的列信息'+'多个值以逗号隔开|是||
|updateMode|APPEND：不回撤数据，只下发增量数据，UPSERT：先删除回撤数据，然后更新|否|APPEND｜
|parallelism | 并行度设置|否|1|
|kerberosAuthEnable | 是否开启kerberos认证|否|false|
|regionserverPrincipal | regionserver的principal，这个值从hbase-site.xml的hbase.regionserver.kerberos.principal属性中获取|否||
|clientKeytabFile|client的keytab 文件|否|
|clientPrincipal|client的principal|否||
|zookeeperSaslClient | zookeeper.sasl.client值|否|true|
|securityKrb5Conf | java.security.krb5.conf值|否||
 另外开启Kerberos认证还需要在VM参数中配置krb5, -Djava.security.krb5.conf=/Users/xuchao/Documents/flinkSql/kerberos/krb5.conf
 同时在addShipfile参数中添加keytab文件的路径，参数具体细节请看[命令参数说明](../config.md)
## 5.样例：

### 普通结果表语句示例
```
CREATE TABLE MyTable(
    name varchar,
    channel varchar,
    age int
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
    cf:name varchar ,
    cf:channel varchar 
 )WITH(
	type ='hbase',
	zookeeperQuorum ='172.16.10.104:2181,172.16.10.224:2181,172.16.10.252:2181',
	zookeeperParent ='/hbase',
	tableName ='myresult',
	partitionedJoin ='false',
	parallelism ='1',
	rowKey='name+channel'
 );

insert          
into
    MyResult
    select
        channel,
        name                                            
    from
        MyTable a       

 
 ```

### kerberos认证结果表语句示例
```
CREATE TABLE MyTable(
    name varchar,
    channel varchar,
    age int
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
    cf:name varchar ,
    cf:channel varchar 
 )WITH(
	type ='hbase',
	zookeeperQuorum ='cdh2.cdhsite:2181,cdh4.cdhsite:2181',
	zookeeperParent ='/hbase',
	tableName ='myresult',
	partitionedJoin ='false',
	parallelism ='1',
	rowKey='name',
    kerberosAuthEnable='true',
    regionserverPrincipal='hbase/_HOST@DTSTACK.COM',
    clientKeytabFile='test.keytab',
    clientPrincipal='test@DTSTACK.COM',
    securityKrb5Conf='krb5.conf',
 );

insert          
into
    MyResult
    select
        channel,
        name                                            
    from
        MyTable a      

```

## 6.hbase数据
### 数据内容说明
hbase的rowkey 构建规则：以描述的rowkey字段值作为key，多个字段以'+'连接
### 数据内容示例
hbase(main):007:0> scan 'myresult'
    ROW                   COLUMN+CELL                                               
 roc-daishu           column=cf:channel, timestamp=1589183971724, value=daishu  
 roc-daishu           column=cf:name, timestamp=1589183971724, value=roc 