### 1. 自定义的性能指标(新增)

### 开启prometheus 需要设置的 confProp 参数
* metrics.reporter.promgateway.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter  
* metrics.reporter.promgateway.host: prometheus pushgateway的地址  
* metrics.reporter.promgateway.port：prometheus pushgateway的端口  
* metrics.reporter.promgateway.jobName: 实例名称
* metrics.reporter.promgateway.randomJobNameSuffix: 是否在实例名称后面添加随机字符串(默认:true)
* metrics.reporter.promgateway.deleteOnShutdown: 是否在停止的时候删除数据(默认false)

#### kafka插件
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
      