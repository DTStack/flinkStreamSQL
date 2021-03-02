package com.dtstack.flink.sql.parse;

import com.dtstack.flink.sql.parser.CreateTableParser;
import com.dtstack.flink.sql.parser.SqlTree;
import org.junit.Test;

public class CreateTableParserTest {

    @Test
    public void parseSql(){
        String sql = "CREATE TABLE MyResult(\n" +
                "        id varchar,\n" +
                "        name varchar,\n" +
                "        address varchar,\n" +
                "        message varchar,\n" +
                "        info varchar\n" +
                ")WITH(\n" +
                "        type = 'console'\n" +
                ");";
        CreateTableParser parser = CreateTableParser.newInstance();
        parser.verify(sql);
        parser.parseSql(sql, new SqlTree());
    }

}
