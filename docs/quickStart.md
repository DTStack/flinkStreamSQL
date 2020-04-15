### 1.1 运行模式

* 本地模式：通常用于本地开发调试(--mode:local)
* standalone模式：Flink 本身提供到集群分布式模式(--mode:standalone)
* yarn-session模式：在yarn上已经预先启动了flink集群(--mode:yarn)
* yarn-perjob 模式：每个任务单独启动一个yarn application,推荐使用该模式.(--mode:yarnPer)

### 1.2 执行环境

* Java: JDK8及以上
* Flink集群: 1.4,1.5,1.8,1.9,1.10（单机模式不需要安装Flink集群）
* 操作系统：理论上不限
* kerberos环境需要在flink-conf.yaml配置security.kerberos.login.keytab以及security.kerberos.login.principal参数，配置案例:
```
#提交到hadoop环境一定要配置fs.hdfs.hadoopconf参数
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

####可运行的目录结构:  
```
|
|-----bin
|     |--- submit.sh 任务启动脚本  
|-----lib: launcher包存储路径，是任务提交的入口
|     |--- sql.launcher.jar   
|-----sqlplugins:  插件包存储路径(mvn 打包之后会自动将jar移动到该目录下)  
|     |--- core.jar
|     |--- xxxsource
|     |--- xxxsink
|     |--- xxxside
```
### 1.4 启动

#### 1.4.1 启动命令

```shell script
# 脚本启动
sh submit.sh 
  -mode yarn
  -name flink1.10_yarnSession
  -sql F:\dtstack\stressTest\flinkStreamSql\1.10_dev\sql\flink1100.sql
  -localSqlPluginPath F:\dtstack\project\flinkStreamSQL\plugins
  -remoteSqlPluginPath F:\dtstack\project\flinkStreamSQL\plugins
  -flinkconf F:\dtstack\flink\flink-1.10.0\conf
  -yarnconf F:\dtstack\flinkStreamSql\yarnConf_node1
  -flinkJarPath F:\dtstack\flink\flink-1.10.0\lib
  -pluginLoadMode shipfile
  -confProp {\"time.characteristic\":\"eventTime\",\"logLevel\":\"info\"}
  -yarnSessionConf {\"yid\":\"application_1586851105774_0014\"}
```
```shell script
# 通过idea启动 程序入口类LaucherMain
# Run/Debug Configurations中设置Program arguments
-mode yarnPer
-sql /home/wen/Desktop/flink_stream_sql_conf/sql/stressTest.sql
-name stressTestAll
-localSqlPluginPath /home/wen/IdeaProjects/flinkStreamSQL/plugins
-remoteSqlPluginPath /home/wen/IdeaProjects/flinkStreamSQL/plugins
-flinkconf /home/wen/Desktop/flink_stream_sql_conf/flinkConf
-yarnconf /home/wen/Desktop/flink_stream_sql_conf/yarnConf_node1
-flinkJarPath /home/wen/Desktop/dtstack/flink-1.8.1/lib
-pluginLoadMode shipfile
-confProp {\"time.characteristic\":\"eventTime\",\"logLevel\":\"info\"}
-queue c
```
#### 1.4.2 命令参数说明
* **mode**
	* 描述：执行模式，也就是flink集群的工作模式
		* local: 本地模式
		* standalone: 提交到独立部署模式的flink集群
		* yarn: 提交到yarn模式的flink集群，该模式下需要提前启动一个yarn-session，使用默认名"Flink session cluster"
		* yarnPer: yarn per_job模式提交(即创建新flink application)，默认名为flink任务名称
	* 必选：否
	* 默认值：local
	
* **name**
	* 描述：flink 任务对应名称。
	* 必选：是
	* 默认值：无	

* **sql**
	* 描述：待执行的flink sql所在路径 。
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
    * 必选：是 （如无参数填写空json即可）
    * 默认值：{}
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
        * restore.enable：是否失败重启（默认是true）
        * failure.interval：衡量失败率的时间段，单位分钟（默认6m）
        * delay.interval：连续两次重启尝试间的间隔，单位是秒（默认10s）
    	* logLevel: 日志级别动态配置（默认info）
        * [prometheus 相关参数](docs/prometheus.md) per_job可指定metric写入到外部监控组件,以prometheus pushgateway举例
    
	
* **flinkconf**
	* 描述：flink配置文件所在的目录（单机模式下不需要），如/hadoop/flink-1.10.0/conf
	* 必选：否
	* 默认值：无
	
* **yarnconf**
	* 描述：Hadoop配置文件（包括hdfs和yarn）所在的目录（单机模式下不需要），如/hadoop/etc/hadoop
	* 必选：否
	* 默认值：无
	
* **flinkJarPath**
	* 描述：per_job 模式提交需要指定本地的flink jar存放路径
	* 必选：否
	* 默认值：无

* **queue**
	* 描述：per_job 模式下指定的yarn queue
	* 必选：否
	* 默认值：default
	
* **pluginLoadMode**
	* 描述：per_job 模式下的插件包加载方式。classpath:从每台机器加载插件包，shipfile:将需要插件从提交的节点上传到hdfs,不需要每台安装插件
	* 必选：否
	* 默认值：classpath
	
* **yarnSessionConf**
	* 描述：yarn session 模式下指定的运行的一些参数，[可参考](https://ci.apache.org/projects/flink/flink-docs-release-1.8/ops/cli.html),目前只支持指定yid
	* 必选：否
	* 默认值：{}
	
## 1.5 任务样例

```
# 一个kafka join all维表 sink kafka的样例
CREATE TABLE MyTable(
        id bigint,
        name varchar,
        address varchar
)WITH(
        type = 'kafka10',
        bootstrapServers = '172.16.101.224:9092',
        zookeeperQuorm = '172.16.100.188:2181/kafka',
        offsetReset = 'latest',
        topic = 'tiezhu_test_in',
        groupId = 'flink_sql',
        timezone = 'Asia/Shanghai',
        topicIsPattern = 'false',
        parallelism = '1'
);

CREATE TABLE sideTable(
        id bigint,
        school varchar,
        home varchar,
        PRIMARY KEY(id),
        PERIOD FOR SYSTEM_TIME
)WITH(
        type='mysql',
        url='jdbc:mysql://172.16.8.109:3306/tiezhu',
        userName='dtstack',
        password='you-guess',
        tableName='stressTest',
        cache='ALL',
        parallelism='1',
        asyncCapacity='100'
);

CREATE TABLE MyResult(
        id bigint,
        name varchar,
        address varchar,
        home varchar,
        school varchar
)WITH(
        type = 'kafka10',
        bootstrapServers = '172.16.101.224:9092',
        zookeeperQuorm = '172.16.100.188:2181/kafka',
        offsetReset = 'latest',
        topic = 'tiezhu_test_out',
        parallelism = '1'
);


insert
into
        MyResult
        select
                t1.id AS id,
                t1.name AS name,
                t1.address AS address,
                t2.school AS school,
                t2.home AS home
        from
        (
        select
        id,
        name,
        address
        from
                MyTable
        ) t1
        left join       sideTable t2
                on t2.id = t2.id;
```

# 招聘
1.大数据平台开发工程师，想了解岗位详细信息可以添加本人微信号ysqwhiletrue,注明招聘，如有意者发送简历至sishu@dtstack.com。
  
