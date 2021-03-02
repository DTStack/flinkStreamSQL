package com.dtstack.flink.sql.sink.sqlserver;

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

public class SqlserverSinkTest {

    @Mock
    JDBCDialect jdbcDialect;

    @InjectMocks
    SqlserverSink sink = new SqlserverSink();;
    @Before
    public void setUp () {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetOutputFormat() throws IllegalAccessException {
        when(jdbcDialect.defaultDriverName()).thenReturn(Optional.of("dd"));

        MemberModifier.field(SqlserverSink.class, "dbUrl").set(sink, "foo");
        MemberModifier.field(SqlserverSink.class, "jdbcDialect").set(sink, jdbcDialect);
        MemberModifier.field(SqlserverSink.class, "userName").set(sink, "foo");
        MemberModifier.field(SqlserverSink.class, "password").set(sink, "foo");
        MemberModifier.field(SqlserverSink.class, "tableName").set(sink, "foo");
        MemberModifier.field(SqlserverSink.class, "fieldNames").set(sink, new String[]{"foo", "bar"});

        JDBCUpsertOutputFormat format = sink.getOutputFormat();
    }

}