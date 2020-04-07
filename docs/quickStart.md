### 1.1 运行模式

* 本地模式：通常用于本地开发调试(--mode:local)
* standalone模式：Flink 本身提供到集群分布式模式(--mode:standalone)
* yarn-session模式：在yarn上已经预先启动了flink集群(--mode:yarn)
* yarn-perjob 模式：每个任务单独启动一个yarn application,推荐使用该模式.(--mode:yarnPer)

### 1.2 执行环境

* Java: JDK8及以上
* Flink集群: 1.4,1.5,1.8（单机模式不需要安装Flink集群）
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
|-----lib
|     |--- sql.launcher.jar 包存储路径，是任务提交的入口（需要手动移动到该目录）  
|-----plugins:  插件包存储路径(mvn 打包之后会自动将jar移动到该目录下)  
|     |--- core.jar
|     |--- xxxsource
|     |--- xxxsink
|     |--- xxxside
```
### 1.4 启动

#### 1.4.1 启动命令

```
sh submit.sh -sql D:\sideSql.txt
-name xctest 
-remoteSqlPluginPath /opt/dtstack/150_flinkplugin/sqlplugin   
-localSqlPluginPath D:\gitspace\flinkStreamSQL\plugins   
-addjar \["udf.jar\"\] 
-mode yarn 
-flinkconf D:\flink_home\kudu150etc  
-yarnconf D:\hadoop\etc\hadoopkudu 
-confProp \{\"time.characteristic\":\"EventTime\",\"sql.checkpoint.interval\":10000\} 
-yarnSessionConf \{\"yid\":\"application_1564971615273_38182\"}
```