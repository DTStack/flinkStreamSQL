package com.dtstack.flink.sql.sink.mysql;

import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.powermock.api.support.membermodification.MemberModifier;

import java.util.Optional;

import static org.mockito.Mockito.*;

/**
 * @program: flink.sql
 * @author: wuren
 * @create: 2020/06/17
 **/
public class MysqlSinkTest {

    @Mock
    JDBCDialect jdbcDialect;

    @InjectMocks
    MysqlSink mysqlSink = new MysqlSink();;
    @Before
    public void setUp () {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetOutputFormat() throws IllegalAccessException {
        when(jdbcDialect.defaultDriverName()).thenReturn(Optional.of("dd"));

        MemberModifier.field(MysqlSink.class, "dbUrl").set(mysqlSink, "foo");
        MemberModifier.field(MysqlSink.class, "jdbcDialect").set(mysqlSink, jdbcDialect);
        MemberModifier.field(MysqlSink.class, "userName").set(mysqlSink, "foo");
        MemberModifier.field(MysqlSink.class, "password").set(mysqlSink, "foo");
        MemberModifier.field(MysqlSink.class, "tableName").set(mysqlSink, "foo");
        MemberModifier.field(MysqlSink.class, "fieldNames").set(mysqlSink, new String[]{"foo", "bar"});

        JDBCUpsertOutputFormat format = mysqlSink.getOutputFormat();
    }
}
