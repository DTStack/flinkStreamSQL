# 一、json格式数据源
## 1.格式：
```
数据现在支持json格式{"xx":"bb","cc":"dd"}

CREATE TABLE tableName(
    colName colType,
    ...
    function(colNameX) AS aliasName,
    WATERMARK FOR colName AS withOffset( colName , delayTime )
 )WITH(
    type ='kafka09',
    kafka.bootstrap.servers ='ip:port,ip:port...',
    kafka.zookeeper.quorum ='ip:port,ip:port/zkparent',
    kafka.auto.offset.reset ='latest',
    kafka.topic ='topicName',
    parallelism ='parllNum',
    sourcedatatype ='json' #可不设置
 );
```

## 2.支持的版本
    kafka08,kafka09,kafka10,kafka11

## 3.表结构定义
 
|参数名称|含义|
|----|---|
| tableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](colType.md)|
| function(colNameX) as aliasName | 支持在定义列信息的时候根据已有列类型生成新的列(函数可以使用系统函数和已经注册的UDF)|
| WATERMARK FOR colName AS withOffset( colName , delayTime ) | 标识输入流生的watermake生成规则,根据指定的colName(当前支持列的类型为Long \| Timestamp) 和delayTime生成waterMark 同时会在注册表的使用附带上rowtime字段(如果未指定则默认添加proctime字段);注意：添加该标识的使用必须设置系统参数 time.characteristic:EventTime; delayTime: 数据最大延迟时间(ms)|

## 4.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是||
|kafka.group.id | 需要读取的 groupId 名称|否||
|kafka.bootstrap.servers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|kafka.zookeeper.quorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|kafka.topic | 需要读取的 topic 名称|是||
|patterntopic | topic是否是正则表达式格式(true|false)  |否| false
|kafka.auto.offset.reset  | 读取的topic 的offset初始位置[latest\|earliest\|指定offset值({"0":12312,"1":12321,"2":12312},{"partition_no":offset_value})]|否|latest|
|parallelism | 并行度设置|否|1|
|sourcedatatype | 数据类型|否|json|
**kafka相关参数可以自定义，使用kafka.开头即可。**

## 5.样例：
```
CREATE TABLE MyTable(
    name varchar,
    channel varchar,
    pv INT,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng
 )WITH(
    type ='kafka09',
    kafka.bootstrap.servers ='172.16.8.198:9092',
    kafka.zookeeper.quorum ='172.16.8.198:2181/kafka',
    kafka.auto.offset.reset ='latest',
    kafka.topic ='nbTest1,nbTest2,nbTest3',
    --kafka.topic ='mqTest.*',
    --patterntopic='true'
    parallelism ='1',
    sourcedatatype ='json' #可不设置
 );
```
# 二、csv格式数据源
根据字段分隔符进行数据分隔，按顺序匹配sql中配置的列。如数据分隔列数和sql中配置的列数相等直接匹配；如不同参照lengthcheckpolicy策略处理。
## 1.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是||
|kafka.bootstrap.servers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|kafka.zookeeper.quorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|kafka.topic | 需要读取的 topic 名称|是||
|kafka.auto.offset.reset | 读取的topic 的offset初始位置[latest\|earliest]|否|latest|
|parallelism | 并行度设置 |否|1|
|sourcedatatype | 数据类型|是 |csv|
|fielddelimiter | 字段分隔符|是 ||
|lengthcheckpolicy | 单行字段条数检查策略 |否|可选，默认为SKIP,其它可选值为EXCEPTION、PAD。SKIP：字段数目不符合时跳过 。EXCEPTION:字段数目不符合时抛出异常。PAD:按顺序填充，不存在的置为null。|
**kafka相关参数可以自定义，使用kafka.开头即可。**

## 2.样例：
```
CREATE TABLE MyTable(
    name varchar,
    channel varchar,
    pv INT,
    xctime bigint,
    CHARACTER_LENGTH(channel) AS timeLeng
 )WITH(
    type ='kafka09',
    kafka.bootstrap.servers ='172.16.8.198:9092',
    kafka.zookeeper.quorum ='172.16.8.198:2181/kafka',
    kafka.auto.offset.reset ='latest',
    kafka.topic ='nbTest1',
    --kafka.topic ='mqTest.*',
    --kafka.topicIsPattern='true'
    parallelism ='1',
    sourcedatatype ='csv',
    fielddelimiter ='\|',
    lengthcheckpolicy = 'PAD'
 );
 ```
# 三、text格式数据源UDF自定义拆分
Kafka源表数据解析流程：Kafka Source Table -> UDTF ->Realtime Compute -> SINK。从Kakfa读入的数据，都是VARBINARY（二进制）格式，对读入的每条数据，都需要用UDTF将其解析成格式化数据。
  与其他格式不同，本格式定义DDL必须与以下SQL一摸一样，表中的五个字段顺序务必保持一致：
  
## 1. 定义源表，注意：kafka源表DDL字段必须与以下例子一模一样。WITH中参数可改。
```
create table kafka_stream(
     _topic STRING,
     _messageKey STRING,
     _message STRING,
     _partition INT,
     _offset BIGINT,
) with (
     type ='kafka09',
     kafka.bootstrap.servers ='172.16.8.198:9092',
     kafka.zookeeper.quorum ='172.16.8.198:2181/kafka',
     kafka.auto.offset.reset ='latest',
     kafka.topic ='nbTest1',
     parallelism ='1',
     sourcedatatype='text' 
 ）
```
## 2.参数：
 
|参数名称|含义|是否必填|默认值|
|----|---|---|---|
|type | kafka09 | 是||
|kafka.bootstrap.servers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|kafka.zookeeper.quorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|kafka.topic | 需要读取的 topic 名称|是||
|kafka.auto.offset.reset | 读取的topic 的offset初始位置[latest\|earliest]|否|latest|
|parallelism | 并行度设置|否|1|
|sourcedatatype | 数据类型|否|text|
**kafka相关参数可以自定义，使用kafka.开头即可。**

## 2.自定义：
从kafka读出的数据，需要进行窗口计算。 按照实时计算目前的设计，滚窗/滑窗等窗口操作，需要（且必须）在源表DDL上定义Watermark。Kafka源表比较特殊。如果要以kafka中message字段中的的Event Time进行窗口操作， 
需要先从message字段，使用UDX解析出event time，才能定义watermark。 在kafka源表场景中，需要使用计算列。 假设，kafka中写入的数据如下：
2018-11-11 00:00:00|1|Anna|female整个计算流程为：Kafka SOURCE->UDTF->Realtime Compute->RDS SINK（单一分隔符可直接使用类csv格式模板，自定义适用于更复杂的数据类型，本说明只做参考）
   
**SQL**
```
-- 定义解析Kakfa message的UDTF
 CREATE FUNCTION kafkapaser AS 'com.XXXX.kafkaUDTF';
 CREATE FUNCTION kafkaUDF AS 'com.XXXX.kafkaUDF';
 -- 定义源表，注意：kafka源表DDL字段必须与以下例子一模一样。WITH中参数可改。
 create table kafka_src (
     _topic STRING,
     _messageKey STRING,
     _message STRING,
     _partition INT,
     _offset BIGINT,
     ctime AS TO_TIMESTAMP(kafkaUDF(_message)), -- 定义计算列，计算列可理解为占位符，源表中并没有这一列，其中的数据可经过下游计算得出。注意计算里的类型必须为timestamp才能在做watermark。
     watermark for ctime as withoffset(ctime,0) -- 在计算列上定义watermark
 ) WITH (
     type = 'kafka010',    -- Kafka Source类型，与Kafka版本强相关，目前支持的Kafka版本请参考本文档
     topic = 'test_kafka_topic',
     ...
 );
 create table rds_sink (
   name       VARCHAR,
   age        INT,
   grade      VARCHAR,
   updateTime TIMESTAMP
 ) WITH(
  type='mysql',
  url='jdbc:mysql://localhost:3306/test',
  tableName='test4',
  userName='test',
  password='XXXXXX'
 );
 -- 使用UDTF，将二进制数据解析成格式化数据
 CREATE VIEW input_view (
     name,
     age,
     grade,
     updateTime
 ) AS
 SELECT
     COUNT(*) as cnt,
     T.ctime,
     T.order,
     T.name,
     T.sex
 from
     kafka_src as S,
     LATERAL TABLE (kafkapaser _message)) as T (
         ctime,
         order,
         name,
         sex
     )
 Group BY T.sex,
         TUMBLE(ctime, INTERVAL '1' MINUTE);
 -- 对input_view中输出的数据做计算
 CREATE VIEW view2 (
     cnt,
     sex
 ) AS
 SELECT
     COUNT(*) as cnt,
     T.sex
 from
     input_view
 Group BY sex, TUMBLE(ctime, INTERVAL '1' MINUTE);
 -- 使用解析出的格式化数据进行计算，并将结果输出到RDS中
 insert into rds_sink
   SELECT 
       cnt,sex
   from view2;
 ```  
**UDF&UDTF**
```
package com.XXXX;
 import com.XXXX.fastjson.JSONObject;
 import org.apache.flink.table.functions.TableFunction;
 import org.apache.flink.table.types.DataType;
 import org.apache.flink.table.types.DataTypes;
 import org.apache.flink.types.Row;
 import java.io.UnsupportedEncodingException;
 /**
   以下例子解析输入Kafka中的JSON字符串，并将其格式化输出
 **/
 public class kafkaUDTF extends TableFunction<Row> {
     public void eval(byte[] message) {
         try {
           // 读入一个二进制数据，并将其转换为String格式
             String msg = new String(message, "UTF-8");
                 // 提取JSON Object中各字段
                     String ctime = Timestamp.valueOf(data.split('\\|')[0]);
                     String order = data.split('\\|')[1];
                     String name = data.split('\\|')[2];
                     String sex = data.split('\\|')[3];
                     // 将解析出的字段放到要输出的Row()对象
                     Row row = new Row(4);
                     row.setField(0, ctime);
                     row.setField(1, age);
                     row.setField(2, grade);
                     row.setField(3, updateTime);
                     System.out.println("Kafka message str ==>" + row.toString());
                     // 输出一行
                     collect(row);
             } catch (ClassCastException e) {
                 System.out.println("Input data format error. Input data " + msg + "is not json string");
             }
         } catch (UnsupportedEncodingException e) {
             e.printStackTrace();
         }
     }
     @Override
     // 如果返回值是Row，就必须重载实现这个方法，显式地告诉系统返回的字段类型
     // 定义输出Row()对象的字段类型
     public DataType getResultType(Object[] arguments, Class[] argTypes) {
         return DataTypes.createRowType(DataTypes.TIMESTAMP,DataTypes.STRING, DataTypes.Integer, DataTypes.STRING,DataTypes.STRING);
     }
 }                                                         
 
 package com.dp58;                                             
 package com.dp58.sql.udx;                              
 import org.apache.flink.table.functions.FunctionContext;         
 import org.apache.flink.table.functions.ScalarFunction;          
 public class KafkaUDF extends ScalarFunction {                   
     // 可选，open方法可以不写                                             
     // 需要import org.apache.flink.table.functions.FunctionContext;
     public String eval(byte[] message) {                         
          // 读入一个二进制数据，并将其转换为String格式                             
         String msg = new String(message, "UTF-8");               
         return msg.split('\\|')[0];                              
     }                                                            
     public long eval(String b, String c) {                       
         return eval(b) + eval(c);                                
     }                                                            
     //可选，close方法可以不写                                             
     @Override                                                    
     public void close() {                                        
         }                                                        
 }
 ```
