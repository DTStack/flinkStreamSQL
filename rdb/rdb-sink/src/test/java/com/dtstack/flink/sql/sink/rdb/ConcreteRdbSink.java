package com.dtstack.flink.sql.sink.rdb;

import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;

/**
 * @program: flink.sql
 * @author: wuren
 * @create: 2020/07/31
 **/
public class ConcreteRdbSink extends AbstractRdbSink {

    public ConcreteRdbSink(JDBCDialect jdbcDialect) {
        super(jdbcDialect);
    }

    @Override
    public JDBCUpsertOutputFormat getOutputFormat() {
        return null;
    }
}
