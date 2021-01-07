CREATE TABLE ProductTable (
  ID BIGINT,
  ProductName STRING,
  type INT
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    username ='xxx',
    password ='xxx',
    tableName ='product'
);

CREATE TABLE ProductTypeDim (
  ID INT,
  NAME STRING
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='product_type',
    parallelism ='1'
);

CREATE TABLE sink_table (
   ID BIGINT,
   ProductName STRING,
   ProductType STRING
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='product_result',
    parallelism ='1'
);

INSERT INTO sink_table SELECT a.ID, a.ProductName, b.NAME as ProductType FROM ProductTable a join ProductTypeDim b on a.type = b.ID;
