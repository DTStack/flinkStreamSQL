package com.dtstack.flink.sql.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SqlNode.class})
public class FieldReplaceUtilTest {

    @Test
    public void replaceFieldName() throws SqlParseException {
        SqlParser.Config config = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .build();
        SqlParser sqlParser = SqlParser
                .create("insert into resultTable select id from source s left join sideTable t on s.id = t.id union select id from source2 order by id", config);
        SqlNode sqlNode = sqlParser.parseStmt();
        FieldReplaceUtil.replaceFieldName(sqlNode, "old", "new", Maps.newHashMap());
    }

    @Test
    public void replaceOrderByTableName() throws Exception {
        PowerMockito.spy(FieldReplaceUtil.class);

        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlIdentifier.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlIdentifier.names = ImmutableList.copyOf(Arrays.asList("old", "a"));
        Map<String, String> mappingField = Maps.newHashMap();
        mappingField.put("a", "b");
        when(sqlIdentifier.setName(anyInt(), anyString())).thenReturn(sqlIdentifier);
        PowerMockito.when(FieldReplaceUtil.class, "replaceOrderByTableName", sqlIdentifier, "old","new", mappingField).thenReturn(null);

        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.DEFAULT);
        List<SqlNode> sqlNodeList = Lists.newArrayList(sqlIdentifier);
        when(sqlBasicCall.getOperandList()).thenReturn(sqlNodeList);
        when(sqlBasicCall.getOperands()).thenReturn(new SqlNode[1]);
        PowerMockito.when(FieldReplaceUtil.class, "replaceOrderByTableName", sqlBasicCall, "old","new", mappingField).thenReturn(null);

        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.DEFAULT);
        PowerMockito.when(FieldReplaceUtil.class, "replaceOrderByTableName", sqlNode, "old","new", mappingField).thenReturn(null);
    }


    @Test
    public void replaceNodeInfo() throws Exception {
        PowerMockito.spy(FieldReplaceUtil.class);

        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlIdentifier.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlIdentifier.names = ImmutableList.copyOf(Arrays.asList("old", "a"));
        Map<String, String> mappingField = Maps.newHashMap();
        mappingField.put("a", "b");
        when(sqlIdentifier.setName(anyInt(), anyString())).thenReturn(sqlIdentifier);
        PowerMockito.when(FieldReplaceUtil.class, "replaceNodeInfo", sqlIdentifier, "old","new", mappingField).thenReturn(null);


        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.DEFAULT);
        List<SqlNode> sqlNodes = Lists.newArrayList(sqlIdentifier);
        when(sqlBasicCall.getOperandList()).thenReturn(sqlNodes);
        when(sqlBasicCall.getOperands()).thenReturn(new SqlNode[1]);
        PowerMockito.when(FieldReplaceUtil.class, "replaceNodeInfo", sqlBasicCall, "old","new", mappingField).thenReturn(null);

        SqlCase sqlCase = mock(SqlCase.class);
        when(sqlCase.getKind()).thenReturn(SqlKind.CASE);
        SqlNodeList sqlNodeList = mock(SqlNodeList.class);
        when(sqlNodeList.getList()).thenReturn(sqlNodes);
        when(sqlCase.getWhenOperands()).thenReturn(sqlNodeList);
        when(sqlNodeList.size()).thenReturn(1);

        when(sqlCase.getThenOperands()).thenReturn(sqlNodeList);

        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.LITERAL);
        when(sqlCase.getElseOperand()).thenReturn(sqlNode);

        try{
            PowerMockito.when(FieldReplaceUtil.class, "replaceNodeInfo", sqlCase, "old","new", mappingField).thenReturn(null);
        }catch (Exception e){

        }

        PowerMockito.when(FieldReplaceUtil.class, "replaceNodeInfo", sqlNode, "old","new", mappingField).thenReturn(null);
    }

    @Test
    public void createNewIdentify(){
        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        sqlIdentifier.names = ImmutableList.copyOf(Arrays.asList("old", "a"));
        Map<String, String> mappingField = Maps.newHashMap();
        mappingField.put("a", "b");
        when(sqlIdentifier.setName(anyInt(), anyString())).thenReturn(sqlIdentifier);
        FieldReplaceUtil.createNewIdentify(sqlIdentifier, "old", "new", mappingField);
    }

    @Test
    public void replaceSelectFieldName() {
        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlIdentifier.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlIdentifier.names = ImmutableList.copyOf(Arrays.asList("old", "a"));
        Map<String, String> mappingField = Maps.newHashMap();
        mappingField.put("a", "b");
        when(sqlIdentifier.setName(anyInt(), anyString())).thenReturn(sqlIdentifier);

        SqlBasicCall sqlNode = mock(SqlBasicCall.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.AS);

        SqlNode[] sqlNodes = new SqlNode[1];
        sqlNodes[0] = sqlIdentifier;
        when(sqlNode.getOperands()).thenReturn(sqlNodes);
        FieldReplaceUtil.replaceSelectFieldName(sqlNode, "old", "new", mappingField);


        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getOperands()).thenReturn(sqlNodes);


        SqlCase sqlCase = mock(SqlCase.class);
        when(sqlCase.getKind()).thenReturn(SqlKind.CASE);
        SqlNodeList sqlNodeList = mock(SqlNodeList.class);
        when(sqlNodeList.getList()).thenReturn(Lists.newArrayList(sqlIdentifier));
        when(sqlCase.getWhenOperands()).thenReturn(sqlNodeList);
        when(sqlNodeList.get(anyInt())).thenReturn(sqlIdentifier);
        when(sqlNodeList.size()).thenReturn(1);

        when(sqlCase.getThenOperands()).thenReturn(sqlNodeList);

        SqlNode sqlKindNode = mock(SqlNode.class);
        when(sqlKindNode.getKind()).thenReturn(SqlKind.LITERAL);
        when(sqlCase.getElseOperand()).thenReturn(sqlKindNode);

        FieldReplaceUtil.replaceSelectFieldName(sqlCase, "old", "new", mappingField);



        when(sqlNode.getKind()).thenReturn(SqlKind.LITERAL);
        FieldReplaceUtil.replaceSelectFieldName(sqlNode, "old", "new", mappingField);

        when(sqlNode.getKind()).thenReturn(SqlKind.OTHER);
        FieldReplaceUtil.replaceSelectFieldName(sqlNode, "old", "new", mappingField);

        try{
            when(sqlNode.getKind()).thenReturn(SqlKind.DEFAULT);
            FieldReplaceUtil.replaceSelectFieldName(sqlNode, "old", "new", mappingField);
        }catch (Exception e){

        }

    }
}
