package com.dtstack.flink.sql.sink.rdb.format;

import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.dialect.ConcreteJDBCDialect;
import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.sink.rdb.writer.AppendOnlyWriter;
import com.dtstack.flink.sql.sink.rdb.writer.JDBCWriter;
import com.dtstack.flink.sql.util.JDBCUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    JDBCUpsertOutputFormat.class,
    JDBCWriter.class,
    AppendOnlyWriter.class,
    JDBCUtils.class,
    DriverManager.class,
    Connection.class
})
public class JDBCUpsertOutputFormatTest {

    JDBCUpsertOutputFormat outputFormat;

    @Before
    public void setUp() throws SQLException {
        JDBCUpsertOutputFormat.Builder builder = JDBCUpsertOutputFormat.builder();

        JDBCOptions.Builder optionsBuilder = JDBCOptions.builder();
        final String diverName = "org.postgresql.Driver";
        final String dbUrl = "jdbc:postgres://";
        final String tableName = "TEST_tablename";
        final String username = "TEST_user";
        final String password = "TEST_pass";
        final String schema = "TEST_schema";
        final JDBCDialect dialect = new ConcreteJDBCDialect();
        JDBCOptions options = optionsBuilder
            .setDriverName(diverName)
            .setDbUrl(dbUrl)
            .setUsername(username)
            .setPassword(password)
            .setTableName(tableName)
            .setDialect(dialect)
            .setSchema(schema).build();

        final String[] fieldNames = new String[] {"id", "name"};

        outputFormat =
            builder
                .setOptions(options)
                .setFieldNames(fieldNames)
                .build();

        AppendOnlyWriter writer = Whitebox.newInstance(AppendOnlyWriter.class);
        Whitebox.setInternalState(outputFormat, "jdbcWriter", writer);
        suppress(method(JDBCUpsertOutputFormat.class, "establishConnection"));
        Connection connMock = mock(Connection.class);
        PowerMockito.doReturn(false).when(connMock).isValid(anyInt());
        Whitebox.setInternalState(outputFormat, "connection", connMock);
    }

    @Test
    public void testBuilder() {
        JDBCUpsertOutputFormat.Builder builder = JDBCUpsertOutputFormat.builder();

        JDBCOptions.Builder optionsBuilder = JDBCOptions.builder();
        final String diverName = "org.postgresql.Driver";
        final String dbUrl = "jdbc:postgres://";
        final String tableName = "TEST_tablename";
        final String username = "TEST_user";
        final String password = "TEST_pass";
        final String schema = "TEST_schema";
        final JDBCDialect dialect = new ConcreteJDBCDialect();
        JDBCOptions options = optionsBuilder
            .setDriverName(diverName)
            .setDbUrl(dbUrl)
            .setUsername(username)
            .setPassword(password)
            .setTableName(tableName)
            .setDialect(dialect)
            .setSchema(schema).build();

        final String[] fieldNames = new String[] {"id", "name"};
        JDBCUpsertOutputFormat outputFormat =
            builder
                .setOptions(options)
                .setFieldNames(fieldNames)
                .build();
    }

    @Test
    public void testOpen() throws IOException {
        suppress(method(
            JDBCUtils.class,
            "forName",
            String.class,
            ClassLoader.class));
        suppress(method(DriverManager.class, "getConnection", String.class));
        suppress(method(
            DriverManager.class,
            "getConnection",
            String.class,
            String.class,
            String.class
        ));

        suppress(method(Connection.class, "setAutoCommit"));

        suppress(method(JDBCUpsertOutputFormat.class, "initMetric"));
        suppress(method(AppendOnlyWriter.class, "open"));
        outputFormat.open(1, 1);
    }

    @Test
    public void testWriteRecord() throws NoSuchMethodException, IOException {
        try {
            outputFormat.writeRecord(null);
        } catch (RuntimeException e) { }

        suppress(method(JDBCUpsertOutputFormat.class, "checkConnectionOpen"));
        suppress(AppendOnlyWriter.class.getMethod("addRecord", Tuple2.class));
        suppress(JDBCWriter.class.getMethod("executeBatch", Connection.class));

        outputFormat.outRecords = new SimpleCounter();
        outputFormat.writeRecord(null);
    }

    @Test
    public void testClose() {
        try {
            outputFormat.close();
        } catch (IOException e) {}
    }
}