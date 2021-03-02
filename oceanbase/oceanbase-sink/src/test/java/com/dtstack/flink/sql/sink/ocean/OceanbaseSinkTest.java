package com.dtstack.flink.sql.sink.ocean;

import com.dtstack.flink.sql.sink.oceanbase.OceanbaseSink;
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

public class OceanbaseSinkTest {

    @Mock
    JDBCDialect jdbcDialect;

    @InjectMocks
    OceanbaseSink sink = new OceanbaseSink();;
    @Before
    public void setUp () {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetOutputFormat() throws IllegalAccessException {
        when(jdbcDialect.defaultDriverName()).thenReturn(Optional.of("dd"));

        MemberModifier.field(OceanbaseSink.class, "dbUrl").set(sink, "foo");
        MemberModifier.field(OceanbaseSink.class, "jdbcDialect").set(sink, jdbcDialect);
        MemberModifier.field(OceanbaseSink.class, "userName").set(sink, "foo");
        MemberModifier.field(OceanbaseSink.class, "password").set(sink, "foo");
        MemberModifier.field(OceanbaseSink.class, "tableName").set(sink, "foo");
        MemberModifier.field(OceanbaseSink.class, "fieldNames").set(sink, new String[]{"foo", "bar"});

        JDBCUpsertOutputFormat format = sink.getOutputFormat();
    }

}