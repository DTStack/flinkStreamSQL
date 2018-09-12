1: example：
CREATE TABLE sideTable(
    cf:name String as name,
    cf:info int as info,
    PRIMARY KEY(md5(name) + 'test'),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    zookeeperParent ='/hbase',
    tableName ='workerinfo',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1',
    partitionedJoin='true'
 );

2: 格式：
 CREATE TABLE tableName(
     columnFamily:columnName type as alias,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type ='hbase',
     zookeeperQuorum ='ip:port',
     zookeeperParent ='/hbase',
     tableName ='tableNamae',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
  );


tableName ==> 注册到flink的表名称(可选填;不填默认和hbase对应的表名称相同)
columnFamily:columnName ==> hbase中的列族名称和列名称
alias ===> hbase 中的列对应到flink中注册的列名称
PERIOD FOR SYSTEM_TIME ==> 关键字表明该定义的表为维表信息
PRIMARY KEY(keyInfo) ==> 维表主键定义;hbase 维表为rowkey的构造方式;
                         可选择的构造包括 md5(alias + alias), '常量',也包括上述方式的自由组合
type ==> 表明维表的类型
zookeeperQuorum ==> hbase 的zk地址;格式ip:port[;ip:port]
zookeeperParent ==> hbase 的zk parent路径
tableName ==> hbase 的表名称
cache ==> 维表缓存策略(NONE/LRU)
partitionedJoin ==> 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量)

NONE: 不做内存缓存

LRU:
cacheSize ==> 缓存的条目数量
cacheTTLMs ==> 缓存的过期时间(ms)


