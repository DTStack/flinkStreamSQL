package com.dtstack.flink.sql.sink.impala;

import com.dtstack.flink.sql.sink.impala.table.ImpalaTableInfo;
import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.reflect.Whitebox;

import java.util.Optional;

import static org.mockito.Mockito.when;

public class ImpalaSinkTest {

    @Mock
    JDBCDialect jdbcDialect;

    @InjectMocks
    ImpalaSink sink = new ImpalaSink();;

    ImpalaTableInfo tableInfo = new ImpalaTableInfo();
    @Before
    public void setUp () {
        MockitoAnnotations.initMocks(this);
        tableInfo.setAuthMech(EAuthMech.NoAuthentication.ordinal());
        Whitebox.setInternalState(sink, "impalaTableInfo", tableInfo);
    }

}