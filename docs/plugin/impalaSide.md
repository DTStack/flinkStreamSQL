## 1.格式：

```sql
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
     type='impala',
     url='jdbcUrl',
     userName='dbUserName',
     password='dbPwd',
     tableName='tableName',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
  );
```

## 2.支持版本

 2.10.0-cdh5.13.0

## 3.表结构定义

 [维表参数信息](docs/plugin/sideParams.md)

impala独有的参数配置

| 参数名称            | 含义                                                         | 是否必填                          | 默认值     |
| ------------------- | ------------------------------------------------------------ | --------------------------------- | ---------- |
| type                | 表明维表的类型[impala]                                       | 是                                |            |
| url                 | 连接postgresql数据库 jdbcUrl                                 | 是                                |            |
| userName            | postgresql连接用户名                                         | 是                                |            |
| password            | postgresql连接密码                                           | 是                                |            |
| tableName           | postgresql表名称                                             | 是                                |            |
| authMech            | 身份验证机制 (0, 1, 2, 3), 暂不支持kerberos                  | 是                                | 0          |
| principal           | kerberos用于登录的principal（authMech=1时独有）              | authMech=1为必填                  |            |
| keyTabFilePath      | keytab文件的路径（authMech=1时独有）                         | authMech=1为必填                  |            |
| krb5FilePath        | krb5.conf文件路径（authMech=1时独有）                        | authMech=1为必填                  |            |
| krbServiceName      | Impala服务器的Kerberos principal名称（authMech=1时独有）     | authMech=1为必填                  |            |
| krbRealm            | Kerberos的域名（authMech=1时独有）                           | 否                                | HADOOP.COM |
| enablePartition     | 是否支持分区                                                 | 否                                | false      |
| partitionfields     | 分区字段名                                                   | 否,enablePartition='true'时为必填 |            |
| partitionFieldTypes | 分区字段类型                                                 | 否,enablePartition='true'时为必填 |            |
| partitionValues     | 分区值                                                       | 否,enablePartition='true'时为必填 |            |
| cache               | 维表缓存策略(NONE/LRU/ALL)                                   | 否                                | NONE       |
| partitionedJoin     | 是否在維表join之前先根据 設定的key 做一次keyby操作(可以減少维表的数据缓存量) | 否                                | false      |

## 4.样例

### ALL全量维表定义

```sql
 // 定义全量维表
CREATE TABLE sideTable(
    id INT,
    name VARCHAR,
    PRIMARY KEY(id) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='mysql',
    url ='jdbc:impala://localhost:21050/mqtest',
    userName ='dtstack',
    password ='1abc123',
    tableName ='test_impala_all',
    authMech='3',
    cache ='ALL',
    cacheTTLMs ='60000',
    parallelism ='2',
    partitionedJoin='false'
 );

```

### LRU异步维表定义

```sql
create table sideTable(
    channel varchar,
    xccount int,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='impala',
    url='jdbc:impala://localhost:21050/mytest',
    userName='dtstack',
    password='abc123',
    tableName='sidetest',
    authMech='3',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1',
    partitionedJoin='false'
 );


```

### MySQL异步维表关联

```sql
CREATE TABLE MyTable(
    id int,
    name varchar
 )WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='cannan_yctest01',
    timezone='Asia/Shanghai',
    enableKeyPartitions ='false',
    topicIsPattern ='false',
    parallelism ='1'
 );

CREATE TABLE MyResult(
    id INT,
    name VARCHAR
 )WITH(
    type='impala',
    url='jdbc:impala://localhost:21050/mytest',
    userName='dtstack',
    password='abc123',
    tableName ='test_impala_zf',
    updateMode ='append',
    parallelism ='1',
    batchSize ='100',
    batchWaitInterval ='1000'
 );

CREATE TABLE sideTable(
    id INT,
    name VARCHAR,
    PRIMARY KEY(id) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='impala',
    url='jdbc:impala://localhost:21050/mytest',
    userName='dtstack',
    password='abc123',
    tableName ='test_impala_10',
    partitionedJoin ='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    asyncPoolSize ='3',
    parallelism ='1'
 );

insert   
into
    MyResult
    select
        m.id,
        s.name     
    from
        MyTable  m    
    join
        sideTable s             
            on m.id=s.id;

```

### 分区样例

注：分区字段放在最后面，如下，name是分区字段，放在channel，xccount字段的后面

```sql
create table sideTable(
    channel varchar,
    xccount int,
    name varchar,
    PRIMARY KEY(channel),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='impala',
    url='jdbc:impala://localhost:21050/mytest',
    userName='dtstack',
    password='abc123',
    tableName='sidetest',
    authMech='3',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1',
    enablePartition='true',
    partitionfields='name',
    partitionFieldTypes='varchar',
    partitionValues='{"name":["tom","jeck"]}',
    partitionedJoin='false'
 );

```

