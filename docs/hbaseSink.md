## 1.格式：
```
CREATE TABLE MyResult(
    colFamily:colName colType,
    ...
 )WITH(
    type ='hbase',
    zookeeperQuorum ='ip:port[,ip:port]',
    tableName ='tableName',
    rowKey ='colFamily:colName[,colFamily:colName]',
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
| colType | 列类型 [colType支持的类型](colType.md)

## 4.参数：
  
|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
|type | 表明 输出表类型[mysq&#124;hbase&#124;elasticsearch]|是||
|zookeeperQuorum | hbase zk地址,多个直接用逗号隔开|是||
|zookeeperParent | zkParent 路径|是||
|tableName | 关联的hbase表名称|是||
|rowKey | hbase的rowkey关联的列信息|是||
|parallelism | 并行度设置|否|1|
      
> kerberos 配置

    *  hbase.security.authentication = 'kerberos', 
    *  hbase.security.authorization = 'true',
    *  hbase.master.kerberos.principal = 'hbase/cdh01@DTSTACK.COM',
    *  hbase.master.keytab.file = '/Users/hbase.keytab',
    *  hbase.regionserver.keytab.file = '/Users/hbase.keytab',
    *  hbase.regionserver.kerberos.principal = 'hbase/cdh01@DTSTACK.COM'
    *  (非必选)java.security.krb5.conf = '/etc/krb5.conf'
## 5.样例：
```

 CREATE TABLE MyResult(
    cf:info VARCHAR,
    cf:name VARCHAR,
    cf:channel varchar
 )WITH(
    type ='hbase',
    zookeeperQuorum ='172.16.10.104:2181,172.16.10.224:2181,172.16.10.252:2181',
    zookeeperParent ='/hbase',
    tableName ='workerinfo01',
    rowKey ='channel'

    ---hbase.security.authentication = 'kerberos',
    ---hbase.security.authorization = 'true',
    ---hbase.master.kerberos.principal = 'hbase/cdh01@DTSTACK.COM',
    ---hbase.master.keytab.file = '/Users/chuixue/Desktop/hbase.keytab',
    ---hbase.regionserver.keytab.file = '/Users/chuixue/Desktop/hbase.keytab',
    ---hbase.regionserver.kerberos.principal = 'hbase/cdh01@DTSTACK.COM'



 );


 ```