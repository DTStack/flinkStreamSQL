# flinkStreamSQL
> * 基于开源的flink，对其实时sql进行扩展   
>  >  * 自定义create table 语法（包括源表,输出表,维表）
>  >  * 自定义create view 语法
>  >  * 自定义create function 语法
>  >  * 实现了流与维表的join
>  >  * 支持原生FLinkSQL所有的语法
>  >  * 扩展了输入和输出的性能指标到promethus

 
# 内容更新

## 新特性：
 1. 支持从kafka读取嵌套JSON格式数据,暂不支持数组类型字段。例如：  info.name varchar as info_name。
 2. 支持kafka结果表数据写入。
 3. 支持为ROWTIME绑定时区，默认为本地时区。例如：timezone="America/New_York"
 4. 支持从kafka自定义偏移量中消费数据。

## BUG修复：

 1. oracle维表获取索引问题。
 2. Perjob模式下UDF类加载异常问题。
 3. queue 参数设置无效的问题。