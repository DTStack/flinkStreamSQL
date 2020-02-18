package com.dtstack.flink.sql.sink.polardb;

import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;


public class PolardbSink extends RdbSink {
    public PolardbSink() {
        super(new PolardbDialect());
    }

    @Override
    public JDBCUpsertOutputFormat getOutputFormat() {
        JDBCOptions jdbcOptions = JDBCOptions.builder()
                .setDBUrl(dbURL).setDialect(jdbcDialect)
                .setUsername(userName).setPassword(password)
                .setTableName(tableName).build();

        return JDBCUpsertOutputFormat.builder()
                .setOptions(jdbcOptions)
                .setFieldNames(fieldNames)
                .setFlushMaxSize(batchNum)
                .setFlushIntervalMills(batchWaitInterval)
                .setFieldTypes(sqlTypes)
                .setKeyFields(primaryKeys)
                .setAllReplace(allReplace)
                .setUpdateMode(updateMode).build();
    }
}
