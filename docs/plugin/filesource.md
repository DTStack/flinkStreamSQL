## 1.格式：

```
CREATE TABLE tableName(
    colName colType,
    ...
 )WITH(
    type ='file',
    format = 'csv',
    fieldDelimiter = ',',
    fileName = 'xxxx',
    filePath = 'xx/xxx',
    location = 'local',
    nullLiteral = 'null',
    allowComment = 'true',
    arrayElementDelimiter = ',',
    quoteCharacter = '"',
    escapeCharacter = '\\',
    ignoreParseErrors = 'true',
    hdfsSite = 'xxx/hdfs-site.xml',
    coreSite = 'xxx/core-site.xml',
    hdfsUser = 'root',
    charsetName = 'UTF-8'
 );
```

## 2.支持的格式

支持 HDFS、 Local 支持 Csv、Json、Arvo 格式文件

## 3.表结构定义

|参数名称|含义|
|----|---|
| tableName | 在 sql 中使用的名称;即注册到flink-table-env上的名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](../colType.md)|

## 4.参数

通用参数设置

|参数名称|默认值|是否必填|参数说明|
|----|---|---|---|
|type|file|是|当前表的类型|
|format|csv|是|文件格式，仅支持csv，json，Arvo类型|
|fileName|无|是|文件名|
|filePath|无|是|文件绝对路径|
|location|local|是|文件存储介质，仅支持HDFS、Local|
|charsetName|UTF-8|否|文件编码格式|

### 4.1 Csv 参数设置

|参数名称|默认值|是否必填|参数说明|
|----|---|---|---|
|ignoreParseErrors|true|否|是否忽略解析失败的数据|
|fieldDelimiter|,|否|csv数据的分割符|
|nullLiteral|"null"|否|填充csv数据中的null值|
|allowComments|true|否||
|arrayElementDelimiter|,|否||
|quoteCharacter|"|否||
|escapeCharacter|\|否||

### 4.2 Arvo 参数说明

|参数名称|默认值|是否必填|参数说明|
|----|---|---|---|
|avroFormat|无|是|在format = 'arvo'的情况下是必填项|

### 4.3 HDFS 参数说明

|参数名称|默认值|是否必填|参数说明|
|----|---|---|---|
|hdfsSite|${HADOOP_CONF_HOME}/hdfs-site.xml|是|hdfs-site.xml所在位置|
|coreSite|${HADOOP_CONF_HOME}/core-site.xml|是|core-site.xml所在位置|
|hdfsUser|root|否|HDFS访问用户，默认是[root]用户|

### 4.4 Json 参数说明

Json无特殊参数

## 5.样例

数据展示：

csv

```csv
712382,1/1/2017 0:00,1/1/2017 0:03,223,7051,Wellesley St E / Yonge St Green P,7089,Church St  / Wood St,Member
```

json

```json
{
  "trip_id": "712382",
  "trip_start_time": "1/1/2017 0:00",
  "trip_stop_time": "1/1/2017 0:03",
  "trip_duration_seconds": "223",
  "from_station_id": "7051",
  "from_station_name": "Wellesley St E / Yonge St Green P",
  "to_station_id": "7089",
  "to_station_name": "Church St  / Wood St",
  "user_type": "Member"
},

```

### 5.1 csv

```sql
CREATE TABLE SourceOne
(
    trip_id               varchar,
    trip_start_time       varchar,
    trip_stop_time        varchar,
    trip_duration_seconds varchar,
    from_station_id       varchar,
    from_station_name     varchar,
    to_station_id         varchar,
    to_station_name       varchar,
    user_type             varchar
) WITH (
      type = 'file',
      format = 'csv',
      fieldDelimiter = ',',
      fileName = '2017-Q1.csv',
      filePath = '/data',
      location = 'local',
      charsetName = 'UTF-8'
      );
```

### 5.2 json

```sql
CREATE TABLE SourceOne
(
    trip_id               varchar,
    trip_start_time       varchar,
    trip_stop_time        varchar,
    trip_duration_seconds varchar,
    from_station_id       varchar,
    from_station_name     varchar,
    to_station_id         varchar,
    to_station_name       varchar,
    user_type             varchar
) WITH (
      type = 'file',
      format = 'json',
      fieldDelimiter = ',',
      fileName = '2017-Q1.json',
      filePath = '/data',
      charsetName = 'UTF-8'
      );
```

### 5.3 HDFS

```sql
CREATE TABLE SourceOne
(
    trip_id               varchar,
    trip_start_time       varchar,
    trip_stop_time        varchar,
    trip_duration_seconds varchar,
    from_station_id       varchar,
    from_station_name     varchar,
    to_station_id         varchar,
    to_station_name       varchar,
    user_type             varchar
) WITH (
      type = 'file',
      format = 'json',
      fieldDelimiter = ',',
      fileName = '2017-Q1.json',
      filePath = 'hdfs://ns1/data',
      location = 'hdfs',
      hdfsSite = '/Users/wtz/dtstack/conf/yarn/kudu1/hdfs-site.xml',
      coreSite = '/Users/wtz/dtstack/conf/yarn/kudu1/core-site.xml',
      hdfsUser = 'admin',
      charsetName = 'UTF-8'
      );
```
