### 命令行参数选项

```
sh submit.sh -key1 val1 -key2 val2
```
其中key的可选参数描述如下

* **mode**
	* 描述：执行模式，也就是flink集群的工作模式
		* local: 本地模式
		* standalone: 提交到独立部署模式的flink集群
		* yarn: 提交到yarn模式的flink集群，该模式下需要提前启动一个yarn-session，使用默认名"Flink session cluster"
		* yarnPer: yarn per_job模式提交(即创建新flink application)，默认名为flink任务名称
	* 必选：否
	* 默认值：local
	
* **pluginLoadMode**
	* 描述：yarnPer 模式下的插件包加载方式。
	   * classpath: 从节点机器加载指定remoteSqlPluginPath路径插件包,需要预先在每个运行节点下存放一份插件包。
	   * shipfile: 将localSqlPluginPath路径下的插件从本地上传到hdfs,不需要集群的每台机器存放一份插件包。
	* 必选：否
	* 默认值：classpath
		
	
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

* **addShipfile**
    * 描述：扩展上传的文件，比如开启；Kerberos认证需要的keytab文件和krb5.conf文件    
    * 必选：否
    * 默认值：无
    
* **confProp**
    * 描述：一些参数设置
    * 必选：否
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
        * logLevel: 日志级别动态配置（默认info）
        * [prometheus 相关参数](./prometheus.md) per_job可指定metric写入到外部监控组件,以prometheus pushgateway举例
    
	
* **flinkconf**
	* 描述：flink配置文件所在的目录（单机模式下不需要），如/hadoop/flink-1.10.0/conf
	* 必选：否
	* 默认值：无
	
* **yarnconf**
	* 描述：Hadoop配置文件（包括hdfs和yarn）所在的目录（单机模式下不需要），如/hadoop/etc/hadoop
	* 必选：否
	* 默认值：无
	
* **flinkJarPath**
	* 描述：yarnPer 模式提交需要指定本地的flink jar存放路径
	* 必选：否
	* 默认值：无

* **queue**
	* 描述：yarnPer 模式下指定的yarn queue
	* 必选：否
	* 默认值：default
	
* **yarnSessionConf**
	* 描述：yarn session 模式下指定的运行的一些参数，[可参考](https://ci.apache.org/projects/flink/flink-docs-release-1.8/ops/cli.html),目前只支持指定yid
	* 必选：否
	* 默认值：{}
	