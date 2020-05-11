## 1.格式：
```
CREATE TABLE MyResult(
    colFamily:colName colType,
    ...
 )WITH(
    type ='hbase',
    zookeeperQuorum ='ip:port[,ip:port]',
    tableName ='tableName',
    rowKey ='colName[,colName]',
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
| colType | 列类型 [colType支持的类型](docs/colType.md)

## 4.参数：
  
|参数名称|含义|是否必填|默认值|
|----|---|---|-----|
|type | 表明 输出表类型[mysq&#124;hbase&#124;elasticsearch]|是||
|zookeeperQuorum | hbase zk地址,多个直接用逗号隔开|是||
|zookeeperParent | zkParent 路径|是||
|tableName | 关联的hbase表名称|是||
|rowkey | hbase的rowkey关联的列信息,多个值以逗号隔开|是||
|updateMode|APPEND：不回撤数据，只下发增量数据，UPSERT：先删除回撤数据，然后更新|否|APPEND｜
|parallelism | 并行度设置|否|1|
      
  
## 5.样例：
```
 CREATE TABLE MyResult(
    cf:info VARCHAR,
    cf:name VARCHAR,
    cf:channel varchar
 )WITH(
    type ='hbase',
    zookeeperQuorum ='xx:2181',
    zookeeperParent ='/hbase',
    tableName ='workerinfo01',
    rowKey ='channel'
 );
 ```

## 6.hbase数据
### 数据内容说明
### 数据内容示例