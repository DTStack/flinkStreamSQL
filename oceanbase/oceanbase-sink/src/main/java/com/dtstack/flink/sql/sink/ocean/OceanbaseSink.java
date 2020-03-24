package com.dtstack.flink.sql.sink.ocean;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.AbstractRdbSink;
import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;

/**
 * @author : tiezhu
 * @date : 2020/3/24
 */
public class OceanbaseSink extends AbstractRdbSink implements IStreamSinkGener<AbstractRdbSink> {

    private static final String OCEANBESE_DRIVER = "com.mysql.jdbc.Driver";

    public OceanbaseSink() {
        super(new OceanbaseDialect());
    }

    @Override
    public JDBCUpsertOutputFormat getOutputFormat() {
        JDBCOptions oceanbaseOptions = JDBCOptions.builder()
                .setDbUrl(dbUrl)
                .setDialect(jdbcDialect)
                .setUsername(userName)
                .setPassword(password)
                .setTableName(tableName)
                .build();

        return JDBCUpsertOutputFormat.builder()
                .setOptions(oceanbaseOptions)
                .setFieldNames(fieldNames)
                .setFlushMaxSize(batchNum)
                .setFlushIntervalMills(batchWaitInterval)
                .setFieldTypes(sqlTypes)
                .setKeyFields(primaryKeys)
                .setAllReplace(allReplace)
                .setUpdateMode(updateMode)
                .build();
    }
}
