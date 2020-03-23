  
## 技术交流
  > * 招聘大数据平台开发工程师，想了解岗位详细信息可以添加本人微信号ysqwhiletrue,注明招聘，如有意者发送简历至sishu@dtstack.com
  
  > * 可以搜索群号30537511或者可以扫下面的二维码进入钉钉群
<div align=center>
    <img src=https://user-images.githubusercontent.com/11881814/76816457-3df44800-683b-11ea-94cc-707fb0250a9a.png width=600 />
</div>


## flinkStreamSQL
> * 基于开源的flink，对其实时sql进行扩展   
>  >  * 自定义create table 语法（包括源表,输出表,维表）
>  >  * 自定义create view 语法
>  >  * 自定义create function 语法
>  >  * 实现了流与维表的join
>  >  * 支持原生FLinkSQL所有的语法
>  >  * 扩展了输入和输出的性能指标到promethus
 
## 已支持
  * 源表：kafka 0.9、0.10、0.11、1.x版本
  * 维表：mysql, SQlServer,oracle, hbase, mongo, redis, cassandra, serversocket, kudu, postgresql, clickhouse, impala, db2, sqlserver
  * 结果表：mysql, SQlServer, oracle, hbase, elasticsearch5.x, mongo, redis, cassandra, console, kudu, postgresql, clickhouse, impala, db2, sqlserver


## 1 快速起步
### 1.1 运行模式


* 单机模式：对应Flink集群的单机模式
* standalone模式：对应Flink集群的分布式模式
* yarn模式：对应Flink集群的yarn模式

### 1.2 执行环境

* Java: JDK8及以上
* Flink集群: 1.4,1.5,1.8（单机模式不需要安装Flink集群）
* 操作系统：理论上不限
* kerberos环境需要在flink-conf.yaml配置security.kerberos.login.keytab以及security.kerberos.login.principal参数，配置案例:
```
## hadoop配置文件路径
fs.hdfs.hadoopconf: /Users/maqi/tmp/hadoopconf/hadoop_250
security.kerberos.login.use-ticket-cache: true
security.kerberos.login.keytab: /Users/maqi/tmp/hadoopconf/hadoop_250/maqi.keytab
security.kerberos.login.principal: maqi@DTSTACK.COM
security.kerberos.login.contexts: Client,KafkaClient
zookeeper.sasl.service-name: zookeeper
zookeeper.sasl.login-context-name: Client

```

### 1.3 打包

进入项目根目录，使用maven打包：

```
mvn clean package -Dmaven.test.skip

```

打包完成后的包结构：

> * dt-center-flinkStreamSQL  
>  >  * bin: 任务启动脚本
>  >  * lib: launcher包存储路径，是任务提交的入口
>  >  * plugins:  插件包存储路径
>  >  * ........   :  core及插件代码

### 1.4 启动

#### 1.4.1 启动命令

```
sh submit.sh -sql D:\sideSql.txt  -name xctest -remoteSqlPluginPath /opt/dtstack/150_flinkplugin/sqlplugin   -localSqlPluginPath D:\gitspace\flinkStreamSQL\plugins   -addjar \["udf.jar\"\] -mode yarn -flinkconf D:\flink_home\kudu150etc  -yarnconf D:\hadoop\etc\hadoopkudu -confProp \{\"time.characteristic\":\"EventTime\",\"sql.checkpoint.interval\":10000\} -yarnSessionConf \{\"yid\":\"application_1564971615273_38182\"}
```

#### 1.4.2 命令行参数选项

* **mode**
	* 描述：执行模式，也就是flink集群的工作模式
		* local: 本地模式
		* standalone: 提交到独立部署模式的flink集群
		* yarn: 提交到yarn模式的flink集群(即提交到已有flink集群)
		* yarnPer: yarn per_job模式提交(即创建新flink application)
	* 必选：否
	* 默认值：local
	
* **name**
	* 描述：flink 任务对应名称。
	* 必选：是
	* 默认值：无	

* **sql**
	* 描述：执行flink sql 的主体语句。
	* 必选：是
	* 默认值：无
	
* **localSqlPluginPath**
	* 描述：本地插件根目录地址，也就是打包后产生的plugins目录。
	* 必选：是
	* 默认值：无
	
* **remoteSqlPluginPath**
    * 描述：flink执行集群上的插件根目录地址（将打包好的插件存放到各个flink节点上,如果是yarn集群需要存放到所有的nodemanager上）。
    * 必选：否
    * 默认值：无

* **addjar**
    * 描述：扩展jar路径,当前主要是UDF定义的jar；
    * 格式：json
    * 必选：否
    * 默认值：无
    
* **confProp**
    * 描述：一些参数设置
    * 格式: json
    * 必选：是 （如无参数填写空json即可）
    * 默认值：无
    * 可选参数:
        * sql.ttl.min: 最小过期时间,大于0的整数,如1d、1h(d\D:天,h\H:小时,m\M:分钟,s\s:秒)
        * sql.ttl.max: 最大过期时间,大于0的整数,如2d、2h(d\D:天,h\H:小时,m\M:分钟,s\s:秒),需同时设置最小时间,且比最小时间大5分钟
        * state.backend: 任务状态后端，可选为MEMORY,FILESYSTEM,ROCKSDB，默认为flinkconf中的配置。
        * state.checkpoints.dir: FILESYSTEM,ROCKSDB状态后端文件系统存储路径，例如：hdfs://ns1/dtInsight/flink180/checkpoints。
        * state.backend.incremental: ROCKSDB状态后端是否开启增量checkpoint,默认为true。
        * sql.env.parallelism: 默认并行度设置
        * sql.max.env.parallelism: 最大并行度设置
        * time.characteristic: 可选值[ProcessingTime|IngestionTime|EventTime]
        * sql.checkpoint.interval: 设置了该参数表明开启checkpoint(ms)
        * sql.checkpoint.mode: 可选值[EXACTLY_ONCE|AT_LEAST_ONCE]
        * sql.checkpoint.timeout: 生成checkpoint的超时时间(ms)
        * sql.max.concurrent.checkpoints: 最大并发生成checkpoint数
        * sql.checkpoint.cleanup.mode: 默认是不会将checkpoint存储到外部存储,[true(任务cancel之后会删除外部存储)|false(外部存储需要手动删除)]
        * flinkCheckpointDataURI: 设置checkpoint的外部存储路径,根据实际的需求设定文件路径,hdfs://, file://
        * jobmanager.memory.mb: per_job模式下指定jobmanager的内存大小(单位MB, 默认值:768)
        * taskmanager.memory.mb: per_job模式下指定taskmanager的内存大小(单位MB, 默认值:768)
        * taskmanager.num: per_job模式下指定taskmanager的实例数(默认1)
        * taskmanager.slots：per_job模式下指定每个taskmanager对应的slot数量(默认1)
        * savePointPath：任务恢复点的路径（默认无）
        * allowNonRestoredState：指示保存点是否允许非还原状态的标志（默认false）
        * logLevel: 日志级别动态配置（默认info）
        * [prometheus 相关参数](docs/prometheus.md) per_job可指定metric写入到外部监控组件,以prometheus pushgateway举例
    
	
* **flinkconf**
	* 描述：flink配置文件所在的目录（单机模式下不需要），如/hadoop/flink-1.4.0/conf
	* 必选：否
	* 默认值：无
	
* **yarnconf**
	* 描述：Hadoop配置文件（包括hdfs和yarn）所在的目录（单机模式下不需要），如/hadoop/etc/hadoop
	* 必选：否
	* 默认值：无
	
* **flinkJarPath**
	* 描述：per_job 模式提交需要指定本地的flink jar存放路径
	* 必选：否
	* 默认值：false	

* **queue**
	* 描述：per_job 模式下指定的yarn queue
	* 必选：否
	* 默认值：false	
	
* **pluginLoadMode**
	* 描述：per_job 模式下的插件包加载方式。classpath:从每台机器加载插件包，shipfile:将需要插件从提交的节点上传到hdfs,不需要每台安装插件
	* 必选：否
	* 默认值：classpath
	
* **yarnSessionConf**
	* 描述：yarn session 模式下指定的运行的一些参数，[可参考](https://ci.apache.org/projects/flink/flink-docs-release-1.8/ops/cli.html),目前只支持指定yid
	* 必选：否
	* 默认值：false	
	

## 2 结构
### 2.1 源表插件
* [kafka 源表插件](docs/kafkaSource.md)

### 2.2 结果表插件
* [elasticsearch 结果表插件](docs/elasticsearchSink.md)
* [hbase 结果表插件](docs/hbaseSink.md)
* [mysql 结果表插件](docs/mysqlSink.md)
* [oracle 结果表插件](docs/oracleSink.md)
* [mongo 结果表插件](docs/mongoSink.md)
* [redis 结果表插件](docs/redisSink.md)
* [cassandra 结果表插件](docs/cassandraSink.md)
* [kudu 结果表插件](docs/kuduSink.md)
* [postgresql 结果表插件](docs/postgresqlSink.md)
* [clickhouse 结果表插件](docs/clickhouseSink.md)
* [impala 结果表插件](docs/impalaSink.md)
* [db2 结果表插件](docs/db2Sink.md)
* [sqlserver 结果表插件](docs/sqlserverSink.md)

### 2.3 维表插件
* [hbase 维表插件](docs/hbaseSide.md)
* [mysql 维表插件](docs/mysqlSide.md)
* [oracle 维表插件](docs/oracleSide.md)
* [mongo 维表插件](docs/mongoSide.md)
* [redis 维表插件](docs/redisSide.md)
* [cassandra 维表插件](docs/cassandraSide.md)
* [kudu 维表插件](docs/kuduSide.md)
* [postgresql 维表插件](docs/postgresqlSide.md)
* [clickhouse 维表插件](docs/clickhouseSide.md)
* [impala 维表插件](docs/impalaSide.md)
* [db2 维表插件](docs/db2Side.md)
* [sqlserver 维表插件](docs/sqlserverSide.md)

## 3 性能指标(新增)

### kafka插件
* 业务延迟： flink_taskmanager_job_task_operator_dtEventDelay(单位s)  
   数据本身的时间和进入flink的当前时间的差值.
  
* 各个输入源的脏数据：flink_taskmanager_job_task_operator_dtDirtyData  
  从kafka获取的数据解析失败的视为脏数据  

* 各Source的数据输入TPS: flink_taskmanager_job_task_operator_dtNumRecordsInRate  
  kafka接受的记录数(未解析前)/s

* 各Source的数据输入RPS: flink_taskmanager_job_task_operator_dtNumRecordsInResolveRate  
  kafka接受的记录数(解析后)/s
  
* 各Source的数据输入BPS: flink_taskmanager_job_task_operator_dtNumBytesInRate  
  kafka接受的字节数/s

* Kafka作为输入源的各个分区的延迟数: flink_taskmanager_job_task_operator_topic_partition_dtTopicPartitionLag  
  当前kafka10,kafka11有采集该指标

* 各个输出源RPS: flink_taskmanager_job_task_operator_dtNumRecordsOutRate  
  写入的外部记录数/s
      
	
## 4 样例

```

CREATE (scala|table|aggregate) FUNCTION CHARACTER_LENGTH WITH com.dtstack.Kun;


CREATE TABLE MyTable(
    name varchar,
    channel varchar,
    pv int,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng //自定义的函数
 )WITH(
    type ='kafka09',
    bootstrapServers ='172.16.8.198:9092',
    zookeeperQuorum ='172.16.8.198:2181/kafka',
    offsetReset ='latest',
    topic ='nbTest1',
    parallelism ='1'
 );

CREATE TABLE MyResult(
    channel varchar,
    pv varchar
 )WITH(
    type ='mysql',
    url ='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',
    userName ='dtstack',
    password ='abc123',
    tableName ='pv2',
    parallelism ='1'
 );

CREATE TABLE workerinfo(
    cast(logtime as TIMESTAMP) AS rtime,
    cast(logtime) AS rtime
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    tableName ='workerinfo',
    rowKey ='ce,de',
    parallelism ='1',
    zookeeperParent ='/hbase'
 );

CREATE TABLE sideTable(
    cf:name varchar as name,
    cf:info varchar as info,
    PRIMARY KEY(name),
    PERIOD FOR SYSTEM_TIME //维表标识
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    zookeeperParent ='/hbase',
    tableName ='workerinfo',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    parallelism ='1'
 );

insert
into
    MyResult
    select
        d.channel,
        d.info
    from
        (      select
            a.*,b.info
        from
            MyTable a
        join
            sideTable b
                on a.channel=b.name
        where
            a.channel = 'xc2'
            and a.pv=10      ) as d
```


  
