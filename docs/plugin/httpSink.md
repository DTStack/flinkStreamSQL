## 1.格式：
```
CREATE TABLE tableName(
    colName colType,
    ...
    colNameX colType
 )WITH(
    type ='http', 
    url ='http://xxx:8080/test/returnAll'
    ,flag ='aa' 
    ,delay = '10' 
 );

```


## 3.表结构定义

|参数名称|含义|
|----|---|
| tableName| http表名称|
| colName | 列名称|
| colType | 列类型 [colType支持的类型](../colType.md)|

## 4.参数：

|参数名称|含义|是否必填|默认值|
|----|----|----|----|
|http |结果表插件类型，必须为http|是||
|url | 地址 |是||
|flag | 结果返回标识符|否||
|delay |每条结果数据之间延时时间 |否|默认20毫秒|


## 5.样例：

```

-- {"name":"maqi","id":1001}
CREATE TABLE sourceIn (
    id int,
    name VARCHAR
)WITH(
    type  =  'kafka',  
    bootstrapServers  =  'localhost:9092',  
    topic ='test1'
 );

CREATE  TABLE  sinkOut  (
  id  int 
  , name varchar 
) WITH  (
  type ='http', 
  url ='http://xxx:8080/test/returnAll'
  ,flag ='aa' 
  ,delay = '10' 
);

insert into sinkOut select id,name from sourceIn;

 ```

发送数据：{"name":"maqi","id":1001}
</br>结果数据：
</br>1.flag不填或者为空串：{"name":"maqi","id":1001}
</br>2.flag有内容：{"flag":"11111111","name":"maqi","id":1001,"tableName":"sinkOut"}
