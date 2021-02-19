package com.dtstack.flink.sql.parse;

import com.dtstack.flink.sql.parser.CreateFuncParser;
import com.dtstack.flink.sql.parser.SqlTree;
import org.junit.Test;

public class CreateFuncParserTest {

    @Test
    public void parseSql(){
        String sql = "create table function with xxxx";

        CreateFuncParser parser = CreateFuncParser.newInstance();
        parser.verify(sql);

        parser.parseSql(sql, new SqlTree());
    }

}