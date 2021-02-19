package com.dtstack.flink.sql.side;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SidePredicatesParserTest {

    private SidePredicatesParser sidePredicatesParser = new SidePredicatesParser();
    

    @Test
    public void fillPredicateInfoToList() throws Exception {
        Method method = SidePredicatesParser.class.getDeclaredMethod("fillPredicateInfoToList", SqlBasicCall.class, List.class, String.class, SqlKind.class, int.class, int.class);
        method.setAccessible(true);
        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        SqlIdentifier sqlNode = mock(SqlIdentifier.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(sqlNode.toString()).thenReturn("2");
        sqlNode.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));
        when(sqlBasicCall.getOperands()).thenReturn(new SqlNode[]{sqlNode});

        method.invoke(sidePredicatesParser, sqlBasicCall, Lists.newArrayList(), "a", SqlKind.IN, 0, 0);
    }

}
