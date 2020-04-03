## 支持UDF,UDTF,UDAT：

### UDTF使用案例

1. cross join：左表的每一行数据都会关联上UDTF 产出的每一行数据，如果UDTF不产出任何数据，那么这1行不会输出。
2. left join：左表的每一行数据都会关联上UDTF 产出的每一行数据，如果UDTF不产出任何数据，则这1行的UDTF的字段会用null值填充。 left join UDTF 语句后面必须接 on true参数。


场景：将某个字段拆分为两个字段。

```$xslt

create table function UDTFOneColumnToMultiColumn with cn.todd.flink180.udflib.UDTFOneColumnToMultiColumn;

CREATE TABLE MyTable (
	 userID VARCHAR , 
	 eventType VARCHAR, 
	 productID VARCHAR)
WITH (
	type = 'kafka11', 
	bootstrapServers = '172.16.8.107:9092', 
	zookeeperQuorum = '172.16.8.107:2181/kafka',
	offsetReset = 'latest', 
	topic ='mqTest03',
	topicIsPattern = 'false'
);

CREATE TABLE MyTable1 (
	 channel VARCHAR , 
	 pv VARCHAR, 
	 name VARCHAR)
WITH (
	type = 'kafka11', 
	bootstrapServers = '172.16.8.107:9092', 
	zookeeperQuorum = '172.16.8.107:2181/kafka',
	offsetReset = 'latest', 
	topic ='mqTest01',
	topicIsPattern = 'false'
);

CREATE TABLE MyTable2 (
	userID VARCHAR,
	eventType VARCHAR,
	productID VARCHAR,
	date1 VARCHAR,
	time1 VARCHAR
)
WITH (
	type = 'console', 
	bootstrapServers = '172.16.8.107:9092', 
	zookeeperQuorum = '172.16.8.107:2181/kafka',
	offsetReset = 'latest', 
	topic ='mqTest02',
	topicIsPattern = 'false'
);

## 视图使用UDTF
--create view udtf_table as
--	select	MyTable.userID,MyTable.eventType,MyTable.productID,date1,time1 
  --    from MyTable LEFT JOIN lateral table(UDTFOneColumnToMultiColumn(productID))
  -- as T(date1,time1) on true;
  
  
  
  
insert
	into
	MyTable2
select
	userID,eventType,productID,date1,time1 
from
	(
	select	MyTable.userID,MyTable.eventType,MyTable.productID,date1,time1 
      from MyTable ,lateral table(UDTFOneColumnToMultiColumn(productID)) as T(date1,time1)) as udtf_table;

```
一行转多列UDTFOneColumnToMultiColumn

```$xslt
public class UDTFOneColumnToMultiColumn extends TableFunction<Row> {
    public void eval(String value) {
        String[] valueSplits = value.split("_");

        //一行，两列
        Row row = new Row(2);
        row.setField(0, valueSplits[0]);
        row.setField(1, valueSplits[1]);
        collect(row);
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.STRING, Types.STRING);
    }
}
```

输入输出：


输入： {"userID": "user_5", "eventType": "browse", "productID":"product_5"}

输出：

    +--------+-----------+-----------+---------+-------+
    | userID | eventType | productID |  date1  | time1 |
    +--------+-----------+-----------+---------+-------+
    | user_5 |  browse   | product_5 | product |   5   |
    +--------+-----------+-----------+---------+-------+