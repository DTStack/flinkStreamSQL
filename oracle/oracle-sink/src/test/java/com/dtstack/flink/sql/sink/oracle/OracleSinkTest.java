package com.dtstack.flink.sql.sink.oracle;

import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.support.membermodification.MemberModifier;
import java.util.Optional;
import static org.mockito.Mockito.when;

public class OracleSinkTest {

    @Mock
    OracleDialect jdbcDialect;

    @InjectMocks
    OracleSink sink = new OracleSink();;
    @Before
    public void setUp () {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetOutputFormat() throws IllegalAccessException {
        when(jdbcDialect.defaultDriverName()).thenReturn(Optional.of("dd"));

        MemberModifier.field(OracleSink.class, "dbUrl").set(sink, "foo");
        MemberModifier.field(OracleSink.class, "jdbcDialect").set(sink, jdbcDialect);
        MemberModifier.field(OracleSink.class, "userName").set(sink, "foo");
        MemberModifier.field(OracleSink.class, "password").set(sink, "foo");
        MemberModifier.field(OracleSink.class, "tableName").set(sink, "foo");
        MemberModifier.field(OracleSink.class, "fieldNames").set(sink, new String[]{"foo", "bar"});

        JDBCUpsertOutputFormat format = sink.getOutputFormat();
    }

}