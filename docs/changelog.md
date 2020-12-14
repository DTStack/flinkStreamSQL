
* **1.支持不同模式下的维表join语法**
    * 描述：不同模式下维表join语法不同,可选值：dtstack，flink(默认)。查看[ 1.3 参数配置](../docs/config.md) 中planner
    	* dtstack：和flinkStreamsql v1.10.0语法一样
    	* flink：create table和create function跟flinkStreamsql v1.10.0语法一样。create view和insert into 语法和flink原生保持一致，最大区别在于和维表join，如下：
    	
        ```
        --  创建视图
        create view view_name as 
            select 
               *
            from source u
            left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
            on u.id = s.id;
            
        -- 输出到外部表
        insert into sink
            select 
               *
            from source u
            left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s
            on u.id = s.id;
        ```


* **2.支持Unaligned Checkpoint 功能,查看[ 1.3 参数配置](../docs/config.md)中confProp下的sql.checkpoint.unalignedCheckpoints**


* **3.修复watermark idle问题**
    * 描述：在事件时间语义下，假设kafka有两个partition，一个partition会生成大量数据，而另外一个久久不来数据。这时就会有一个问题。这时的watermark始终是最低的，导致窗口不触发计算。查看[ kafkaSource](../docs/plugin/kafkaSource.md) 中withIdleness


* **4.window支持early trigger功能**
    * 描述：在大时间窗口下提前触发窗口计算结果，查看[ 1.3 参数配置](../docs/config.md) 中confProp下的early.trigger


* **5.一些bug的修复和一些代码的优化**
