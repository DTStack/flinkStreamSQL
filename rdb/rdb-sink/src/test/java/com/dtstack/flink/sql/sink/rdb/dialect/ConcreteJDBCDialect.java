package com.dtstack.flink.sql.sink.rdb.dialect;

/**
 * @program: flink.sql
 * @author: wuren
 * @create: 2020/07/31
 **/
public class ConcreteJDBCDialect implements JDBCDialect {

    @Override
    public boolean canHandle(String url) {
        return false;
    }

}
