# 通知！！！
因该项目与FlinkX项目合并统一，即 2021-07-01 起， 1.12.x 版本之后，项目迁移到 FlinkX 项目中。

FlinkStreamSQL 项目推荐使用版本 1.10_release（为最新稳定版本）。

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
```yaml
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
```shell script
mvn clean package -Dmaven.test.skip
```

####可运行的目录结构:  
```
|
|-----bin
|     |--- submit.sh 任务启动脚本  
|-----lib: launcher包存储路径，是任务提交的入口
|     |--- sql.launcher.jar   
|-----plugins:  插件包存储路径(mvn 打包之后会自动将jar移动到该目录下)  
|     |--- core.jar
|     |--- xxxsource
|     |--- xxxsink
|     |--- xxxside
```
### 1.4 快速启动命令

#### local模式命令
```shell script
sh submit.sh
  -mode local
  -name local_test
  -sql F:\dtstack\stressTest\flinkStreamSql\stressTest.sql
  -localSqlPluginPath F:\dtstack\project\flinkStreamSQL\plugins
```

#### standalone模式命令
```shell script
sh submit.sh
  -mode standalone
  -sql F:\dtstack\flinkStreamSql\tiezhu\twodimjoin.sql
  -name wtz_standalone_flinkStreamSql
  -localSqlPluginPath F:\dtstack\project\flinkStreamSQL\plugins
  -remoteSqlPluginPath /home/admin/dtstack/flinkStreamSQL/plugins
  -flinkconf F:\dtstack\flinkStreamSql\localhost\flinkConf
  -yarnconf F:\dtstack\flinkStreamSql\localhost\hadoop
  -flinkJarPath F:\Java\flink-1.8.2-bin-scala_2.12\flink-1.8.2\lib
  -pluginLoadMode shipfile
  -confProp {\"time.characteristic\":\"eventTime\",\"logLevel\":\"info\"}
```

#### yarn模式命令
```shell script
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
  -yarnSessionConf {\"yid\":\"application_1586851105774_0014\"}
```

#### yarnPer模式命令
```shell script
sh submit.sh
  -mode yarnPer 
  -sql /home/wen/Desktop/flink_stream_sql_conf/sql/Test01.sql
  -name TestAll
  -localSqlPluginPath /home/wen/IdeaProjects/flinkStreamSQL/plugins
  -remoteSqlPluginPath /home/wen/IdeaProjects/flinkStreamSQL/plugins
  -flinkconf /home/wen/Desktop/flink_stream_sql_conf/flinkConf
  -yarnconf /home/wen/Desktop/flink_stream_sql_conf/yarnConf_node1
  -flinkJarPath /home/wen/Desktop/dtstack/flink-1.8.1/lib
  -pluginLoadMode shipfile
  -confProp {\"time.characteristic\":\"eventTime\",\"logLevel\":\"info\"}
  -queue c
```
参数具体细节请看[命令参数说明](./config.md)

任务sql详情请看[任务样例](./demo.md)

### 招聘
1.大数据平台开发工程师，想了解岗位详细信息可以添加本人微信号ysqwhiletrue,注明招聘，如有意者发送简历至sishu@dtstack.com。
  
