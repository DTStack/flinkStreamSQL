# flinkStreamSQL
> * 基于开源的flink，对其实时sql进行扩展   
>  >  * 自定义create table 语法（包括源表,输出表,维表）
>  >  * 自定义create function 语法
>  >  * 实现了流与维表的join

# 后续开发计划
  * 增加全局缓存功能
  * sql增加临时表功能
  * 增加redis维表功能
  * 增加mongodb维表功能
  * 增加oracle维表，结果表功能
  * 增加SQlServer维表，结果表功能

## 1 快速起步
### 1.1 运行模式


* 单机模式：对应Flink集群的单机模式
* standalone模式：对应Flink集群的分布式模式
* yarn模式：对应Flink集群的yarn模式

### 1.2 执行环境

* Java: JDK8及以上
* Flink集群: 1.4（单机模式不需要安装Flink集群）
* 操作系统：理论上不限

### 1.3 打包

进入项目根目录，使用maven打包：

```
mvn clean package -Dmaven.test.skip

所有插件包会生成到plugins下
```

打包结束后，项目根目录下会产生plugins目录，plugins目录下存放编译好的数据同步插件包


### 1.4 启动

#### 1.4.1 命令行参数选项

* **model**
	* 描述：执行模式，也就是flink集群的工作模式
		* local: 本地模式
		* standalone: 独立部署模式的flink集群
		* yarn: yarn模式的flink集群
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
    * 必选：否
    * 默认值：无
    
* **confProp**
    * 描述：一些参数设置
    * 必选：否
    * 默认值：无
    * 可选参数:
        * sql.env.parallelism: 默认并行度设置
        * sql.max.env.parallelism: 最大并行度设置
        * time.characteristic: 可选值[ProcessingTime|IngestionTime|EventTime]
        * sql.checkpoint.interval: 设置了该参数表明开启checkpoint(ms)
        * sql.checkpoint.mode: 可选值[EXACTLY_ONCE|AT_LEAST_ONCE]
        * sql.checkpoint.timeout: 生成checkpoint的超时时间(ms)
        * sql.max.concurrent.checkpoints: 最大并发生成checkpoint数
        * sql.checkpoint.cleanup.mode: 默认是不会将checkpoint存储到外部存储,[true(任务cancel之后会删除外部存储)|false(外部存储需要手动删除)]
        * flinkCheckpointDataURI: 设置checkpoint的外部存储路径,根据实际的需求设定文件路径,hdfs://, file://
    
	
* **flinkconf**
	* 描述：flink配置文件所在的目录（单机模式下不需要），如/hadoop/flink-1.4.0/conf
	* 必选：否
	* 默认值：无
	
* **yarnconf**
	* 描述：Hadoop配置文件（包括hdfs和yarn）所在的目录（单机模式下不需要），如/hadoop/etc/hadoop
	* 必选：否
	* 默认值：无

## 2 结构
### 2.1 源表插件
* [kafka09 源表插件](docs/kafka09Source.md)

### 2.2 结果表插件
* [elasticsearch 结果表插件](docs/elasticsearchSink.md)
* [hbase 结果表插件](docs/hbaseSink.md)
* [mysql 结果表插件](docs/mysqlSink.md)

### 2.3 维表插件
* [hbase 维表插件](docs/hbaseSide.md)
* [mysql 维表插件](docs/mysqlSide.md)
	
## 3 样例

```
CREATE TABLE MyTable(
    name string,
    channel STRING,
    pv INT,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng
 )WITH(
    type ='kafka09',
    bootstrapServers ='172.16.8.198:9092',
    zookeeperQuorum ='172.16.8.198:2181/kafka',
    offsetReset ='latest',
    topic ='nbTest1',
    parallelism ='1'
 );

CREATE TABLE MyResult(
    channel VARCHAR,
    pv VARCHAR
 )WITH(
    type ='mysql',
    url ='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',
    userName ='dtstack',
    password ='abc123',
    tableName ='pv2',
    parallelism ='1'
 );

CREATE TABLE workerinfo(
    cast(logtime as TIMESTAMP)AS rtime,
    cast(logtime)AS rtime
 )WITH(
    type ='hbase',
    zookeeperQuorum ='rdos1:2181',
    tableName ='workerinfo',
    rowKey ='ce,de',
    parallelism ='1',
    zookeeperParent ='/hbase'
 );

CREATE TABLE sideTable(
    cf:name String as name,
    cf:info String as info,
    PRIMARY KEY(name),
    PERIOD FOR SYSTEM_TIME
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
