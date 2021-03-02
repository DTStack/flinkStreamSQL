package com.dtstack.flink.sql.parse;

import com.dtstack.flink.sql.parser.CreateTmpTableParser;
import com.dtstack.flink.sql.parser.FlinkPlanner;
import com.dtstack.flink.sql.parser.SqlTree;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FlinkPlanner.class, SqlKind.class})
public class CreateTmpTableParserTest {

    @Test
    public void parseSql(){
        String sql = "create view test as select a, b from tablea";
        CreateTmpTableParser parser = CreateTmpTableParser.newInstance();
        parser.verify(sql);

        PowerMockito.mockStatic(FlinkPlanner.class);
        FlinkPlanner flinkPlanner = mock(FlinkPlanner.class);
        CalciteParser calciteParser = mock(CalciteParser.class);
        when(flinkPlanner.getParser()).thenReturn(calciteParser);

        SqlNode sqlNode = mock(SqlNode.class);
        when(calciteParser.parse(anyString())).thenReturn(sqlNode);

        SqlKind sqlKind = mock(SqlKind.class);
        when(sqlNode.getKind()).thenReturn(sqlKind);

        parser.parseSql(sql, new SqlTree());
    }
}
