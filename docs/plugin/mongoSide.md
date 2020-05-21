
## 1.格式：
```
 CREATE TABLE tableName(
     colName cloType,
     ...
     PRIMARY KEY(keyInfo),
     PERIOD FOR SYSTEM_TIME
  )WITH(
    type ='mongo',
    address ='ip:port[,ip:port]',
     userName='dbUserName',
     password='dbPwd',
     tableName='tableName',
     database='database',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
  );
```

# 2.支持版本
 mongo-3.8.2
 
## 3.表结构定义
  
 [通用维表参数信息](docs/plugin/sideParams.md)
 
 
 mongo相关参数配置：

|参数名称|含义|是否必填|默认值|
|----|---|---|----|
| type |表明 输出表类型 mongo|是||
| address | 连接mongo数据库 jdbcUrl |是||
| userName | mongo连接用户名|否||
| password | mongo连接密码|否||
| tableName | mongo表名称|是||
| database  | mongo表名称|是||

## 4.样例


### 全量维表结构

```aidl
CREATE TABLE source2(
    id int,
    address VARCHAR,
    PERIOD FOR SYSTEM_TIME
)WITH(
    type ='mongo',
    address ='172.16.8.193:27017',
    database ='dtstack',
    tableName ='userInfo',
    cache ='ALL',
    parallelism ='1',
    partitionedJoin='false'
);
```

### 异步维表结构

```aidl
CREATE TABLE source2(
    id int,
    address VARCHAR,
    PERIOD FOR SYSTEM_TIME
)WITH(
    type ='mongo',
    address ='172.16.8.193:27017',
    database ='dtstack',
    tableName ='userInfo',
    cache ='LRU',
    parallelism ='1',
    partitionedJoin='false'
);

```

### 异步维表关联样例

```

CREATE TABLE source1 (
    id int,
    name VARCHAR
)WITH(
    type ='kafka11',
    bootstrapServers ='172.16.8.107:9092',
    zookeeperQuorum ='172.16.8.107:2181/kafka',
    offsetReset ='latest',
    topic ='mqTest03',
    timezone='Asia/Shanghai',
    topicIsPattern ='false'
 );


CREATE TABLE source2(
    id int,
    address VARCHAR,
    PERIOD FOR SYSTEM_TIME
)WITH(
    type ='mongo',
    address ='172.16.8.193:27017',
    database ='dtstack',
    tableName ='userInfo',
    cache ='ALL',
    parallelism ='1',
    partitionedJoin='false'
);


CREATE TABLE MyResult(
    id int,
    name VARCHAR,
    address VARCHAR,
    primary key (id)
)WITH(
    type='console'
);

insert into MyResult
select 
	s1.id,
	s1.name,
	s2.address
from 
	source1 s1
left join
	source2 s2
on 	
	s1.id = s2.id


```


维表数据：{"id": 1001,"address":"hz""}

源表数据：{"name":"maqi","id":1001}


输出结果： (1001,maqi,hz)


