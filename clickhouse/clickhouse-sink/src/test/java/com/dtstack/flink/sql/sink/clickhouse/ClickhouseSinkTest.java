package com.dtstack.flink.sql.sink.clickhouse;

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

public class ClickhouseSinkTest {

    @Mock
    JDBCDialect jdbcDialect;

    @InjectMocks
    ClickhouseSink sink = new ClickhouseSink();;
    @Before
    public void setUp () {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetOutputFormat() throws IllegalAccessException {
        when(jdbcDialect.defaultDriverName()).thenReturn(Optional.of("dd"));

        MemberModifier.field(ClickhouseSink.class, "dbUrl").set(sink, "foo");
        MemberModifier.field(ClickhouseSink.class, "jdbcDialect").set(sink, jdbcDialect);
        MemberModifier.field(ClickhouseSink.class, "userName").set(sink, "foo");
        MemberModifier.field(ClickhouseSink.class, "password").set(sink, "foo");
        MemberModifier.field(ClickhouseSink.class, "tableName").set(sink, "foo");
        MemberModifier.field(ClickhouseSink.class, "fieldNames").set(sink, new String[]{"foo", "bar"});

        JDBCUpsertOutputFormat format = sink.getOutputFormat();
    }

}