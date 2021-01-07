CREATE TABLE MyUserTable (
  id BIGINT,
  name STRING,
  channel STRING,
  pv BIGINT
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' ='xxx',
    'connector.password' ='xxx',
    'connector.table' ='test'
);

CREATE TABLE print_table (
   id BIGINT,
   name STRING,
   channel STRING,
   pv BIGINT
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' ='xxx',
    'connector.password' ='xxx',
    'connector.table' ='test_new'
);

-- scan data from the JDBC table
INSERT INTO print_table SELECT id, name, channel, pv FROM MyUserTable;
