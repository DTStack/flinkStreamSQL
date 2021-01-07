CREATE TABLE ProductTable (
  ID BIGINT,
  ProductName STRING,
  type INT
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' ='xxx',
    'connector.password' ='xxx',
    'connector.table' ='product'
);

CREATE TABLE ProductTypeDim (
  ID INT,
  NAME STRING
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' ='xxx',
    'connector.password' ='xxx',
    'connector.table' ='product_type'
);

CREATE TABLE sink_table (
   ID BIGINT,
   ProductName STRING,
   ProductType STRING
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' ='xxx',
    'connector.password' ='xxx',
    'connector.table' ='product_result'
);

INSERT INTO sink_table SELECT a.ID, a.ProductName, b.NAME as ProductType FROM ProductTable a join ProductTypeDim b on a.type = b.ID;
