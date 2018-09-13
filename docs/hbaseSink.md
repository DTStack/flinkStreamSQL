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

## 2.参数：
  * tableName ==> 在 sql 中使用的名称;即注册到flink-table-env上的名称
  * colFamily:colName ==> hbase中的列族名称和列名称
  * colType ==> 列类型 [colType支持的类型](colType.md)
  
  * type ==> 表明 输出表类型[mysql|hbase|elasticsearch]
  * zookeeperQuorum ==> hbase zk地址,多个直接用逗号隔开
  * zookeeperParent ==> zkParent 路径
  * tableName ==> 关联的hbase表名称
  * rowKey ==> hbase的rowkey关联的列信息
  * parallelism ==> 并行度设置
      
  
## 3.样例：
```
CREATE TABLE MyResult(
    cf:channel STRING,
    cf:pv BIGINT
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    tableName ='workerinfo',
    rowKey ='cf:channel',
    parallelism ='1',
    zookeeperParent ='/hbase'
 )

 ```