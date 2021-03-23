package com.dtstack.flink.sql.sink.postgresql;

import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.support.membermodification.MemberModifier;

import java.util.Optional;

import static org.mockito.Mockito.when;

public class PostgresqlSinkTest {

    @Mock
    JDBCDialect jdbcDialect;

    @InjectMocks
    PostgresqlSink sink = new PostgresqlSink();;
    @Before
    public void setUp () {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetOutputFormat() throws IllegalAccessException {
        when(jdbcDialect.defaultDriverName()).thenReturn(Optional.of("dd"));

        MemberModifier.field(PostgresqlSink.class, "dbUrl").set(sink, "foo");
        MemberModifier.field(PostgresqlSink.class, "jdbcDialect").set(sink, jdbcDialect);
        MemberModifier.field(PostgresqlSink.class, "userName").set(sink, "foo");
        MemberModifier.field(PostgresqlSink.class, "password").set(sink, "foo");
        MemberModifier.field(PostgresqlSink.class, "tableName").set(sink, "foo");
        MemberModifier.field(PostgresqlSink.class, "fieldNames").set(sink, new String[]{"foo", "bar"});

        JDBCUpsertOutputFormat format = sink.getOutputFormat();
    }

}