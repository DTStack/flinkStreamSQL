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
    bootstrapServers ='ip:port,ip:port...',
    zookeeperQuorum ='ip:port,ip:port/zkparent',
    offsetReset ='latest',
    topic ='topicName',
    parallelism ='parllNum'
 );
```

## 2.支持的版本
  kafka09,kafka10,kafka11  

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
|bootstrapServers | kafka bootstrap-server 地址信息(多个用逗号隔开)|是||
|zookeeperQuorum | kafka zk地址信息(多个之间用逗号分隔)|是||
|topic | 需要读取的 topic 名称|是||
|topicIsPattern |  topic是否是正则表达式格式|否| false
|groupId | 需要读取的 groupId 名称|否||
|offsetReset | 读取的topic 的offset初始位置[latest\|earliest\|指定offset值({"0":12312,"1":12321,"2":12312},{"partition_no":offset_value})]|否|latest|
|parallelism | 并行度设置|否|1|
  
## 5.样例：
```
JSON嵌套:
CREATE TABLE pft_report_order_two(
    message.after.id int AS id,
    message.after.date int AS oper_date,
    message.after.fid int AS fid,
    message.after.reseller_id int AS reseller_id,
    message.after.lid int AS lid,
    message.after.tid int AS tid,
    message.after.pid int AS pid,
    message.after.level int AS level,
    message.after.operate_id int AS operate_id,
    message.after.order_num int AS order_num,
    message.after.order_ticket int AS order_ticket,
    message.after.cancel_num int AS cancel_num,
    message.after.cancel_ticket int AS cancel_ticket,
    message.after.revoke_num int AS revoke_num,
    message.after.revoke_ticket int AS revoke_ticket,
    message.after.cost_money int AS cost_money,
    message.after.sale_money int AS sale_money,
    message.after.cancel_cost_money int AS cancel_cost_money,
    message.after.cancel_sale_money int AS cancel_sale_money,
    message.after.revoke_cost_money int AS revoke_cost_money,
    message.after.revoke_sale_money int AS revoke_sale_money,
    message.after.service_money int AS service_money,
    message.after.orders_info varchar AS orders_info,
    message.after.cancel_orders_info varchar AS cancel_orders_info,
    message.after.revoke_orders_info varchar AS revoke_orders_info,
    message.after.pay_way int AS pay_way,
    message.after.channel int AS channel,
    message.after.update_time int AS update_time,
    message.after.site_id int AS site_id
 )WITH(
    type ='kafka10',
    bootstrapServers = 'xxx:9092', 
    zookeeperQuorum = 'xx:2181/kafka',
    offsetReset = 'latest', 
    topic ='mqTest01',
    parallelism ='1'
 );

WATERMARK:

CREATE TABLE MyTable(
    channel varchar,
    pv INT,
    xctime bigint
    WATERMARK FOR xctime AS withOffset(xctime,3000)
 )WITH(
   type='kafka11',
   bootstrapServers='172.16.8.107:9092',
   groupId='mqTest',
   offsetReset='latest',
   topic='mqTest01'
 );

```
