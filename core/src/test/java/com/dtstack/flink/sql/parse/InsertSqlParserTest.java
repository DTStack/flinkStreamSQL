package com.dtstack.flink.sql.parse;

import com.dtstack.flink.sql.parser.FlinkPlanner;
import com.dtstack.flink.sql.parser.InsertSqlParser;
import com.dtstack.flink.sql.parser.SqlTree;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(PowerMockRunner.class)
@PrepareForTest({FlinkPlanner.class, SqlKind.class})
public class InsertSqlParserTest {

    @Test
    public void parseSql() throws Exception {
        String sql = "insert into MyResult\n" +
                "    select\n" +
                "        t1.id AS id,\n" +
                "        t1.name AS name,\n" +
                "        t1.address AS address,\n" +
                "        t2.message AS message,\n" +
                "        t3.message as info\n" +
                "    from MyTable t1 \n" +
                "    join MyRedis t2\n" +
                "        on t1.id  = t2.id\n" +
                "    join redisSide t3\n" +
                "        on t1.id = t3.id";
        InsertSqlParser parser = InsertSqlParser.newInstance();
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
