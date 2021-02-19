package com.dtstack.flink.sql.util;

import com.dtstack.flink.sql.side.JoinInfo;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Map;
import java.util.Queue;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SqlSelect.class, SqlJoin.class, FieldReplaceUtil.class, TableUtils.class})
public class TableUtilTest {

    @Test
    public void parserSelectField() throws SqlParseException {
        SqlParser.Config config = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .build();
        SqlParser sqlParser = SqlParser
                .create("select id from source s left join sideTable t on s.id = t.id ", config);
        SqlNode sqlNode = sqlParser.parseStmt();
        TableUtils.parserSelectField((SqlSelect) sqlNode, Maps.newHashMap());
    }

    @Test
    public void extractSelectFieldToFieldInfo() {
        SqlIdentifier sqlNode = mock(SqlIdentifier.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(sqlNode.isStar()).thenReturn(false);
        sqlNode.names = ImmutableList.copyOf(Arrays.asList("a"));
        when(sqlNode.getComponent(0)).thenReturn(sqlNode);
        when(sqlNode.getSimple()).thenReturn("b");
        TableUtils.extractSelectFieldToFieldInfo(sqlNode, "", Lists.newArrayList(), Maps.newHashMap());


        when(sqlNode.isStar()).thenReturn(true);
        sqlNode.names = ImmutableList.copyOf(Arrays.asList("a", "b"));

        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlNode.skipLast(1)).thenReturn(sqlIdentifier);
        when(sqlIdentifier.getSimple()).thenReturn("ab");
        try {
            TableUtils.extractSelectFieldToFieldInfo(sqlNode, "", Lists.newArrayList(), Maps.newHashMap());
        } catch (RuntimeException e) {
            Map<String, Table> localTableCache = Maps.newHashMap();
            Table table = mock(Table.class);
            localTableCache.put("ab", table);
            TableSchema tableSchema = mock(TableSchema.class);
            when(table.getSchema()).thenReturn(tableSchema);
            when(tableSchema.getFieldNames()).thenReturn(Lists.newArrayList("a").toArray(new String[1]));
            TableUtils.extractSelectFieldToFieldInfo(sqlNode, "", Lists.newArrayList(), localTableCache);
        }

    }

    @Test
    public void extractSelectFieldToFieldInfoWithCoalesce() {
        SqlIdentifier sqlNode = mock(SqlIdentifier.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(sqlNode.isStar()).thenReturn(false);
        sqlNode.names = ImmutableList.copyOf(Arrays.asList("a"));
        when(sqlNode.getComponent(0)).thenReturn(sqlNode);
        when(sqlNode.getSimple()).thenReturn("b");

        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.COALESCE);
        SqlNode[] sqlNodes = new SqlNode[1];
        sqlNodes[0] = sqlNode;
        when(sqlBasicCall.getOperands()).thenReturn(sqlNodes);
        TableUtils.extractSelectFieldToFieldInfo(sqlBasicCall, "", Lists.newArrayList(), Maps.newHashMap());
    }


    @Test
    public void extractSelectFieldToFieldInfoWithAs() {
        SqlIdentifier sqlNode = mock(SqlIdentifier.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(sqlNode.isStar()).thenReturn(false);
        sqlNode.names = ImmutableList.copyOf(Arrays.asList("a"));
        when(sqlNode.getComponent(0)).thenReturn(sqlNode);
        when(sqlNode.getSimple()).thenReturn("b");

        SqlCase sqlCase = mock(SqlCase.class);
        when(sqlCase.getKind()).thenReturn(SqlKind.CASE);
        SqlNodeList sqlNodeList = mock(SqlNodeList.class);
        when(sqlNodeList.size()).thenReturn(1);
        when(sqlNodeList.get(anyInt())).thenReturn(sqlNode);
        when(sqlCase.getWhenOperands()).thenReturn(sqlNodeList);
        when(sqlCase.getThenOperands()).thenReturn(sqlNodeList);
        when(sqlCase.getElseOperand()).thenReturn(sqlNode);
        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.AS);
        SqlNode[] sqlNodes = new SqlNode[1];
        sqlNodes[0] = sqlCase;
        when(sqlBasicCall.getOperands()).thenReturn(sqlNodes);
    }


    @Test
    public void buildTableField() {
        TableUtils.buildTableField("a", "b");
    }

    @Test
    public void buildTableNameWithScope() {
        TableUtils.buildTableNameWithScope("a", "1");
        TableUtils.buildTableNameWithScope("a", null);
    }


    @Test
    public void buildAsNodeByJoinInfo() {
        JoinInfo joinInfo = mock(JoinInfo.class);
        when(joinInfo.getNewTableName()).thenReturn("new");
        when(joinInfo.getLeftTableAlias()).thenReturn("");
        when(joinInfo.getRightTableAlias()).thenReturn("b");

        SqlBasicCall sqlNode = mock(SqlBasicCall.class);
        when(joinInfo.getLeftNode()).thenReturn(sqlNode);
        when(sqlNode.getKind()).thenReturn(SqlKind.AS);
        SqlNode[] sqlNodes = new SqlNode[2];
        sqlNodes[0] = sqlNode;
        sqlNodes[1] = sqlNode;
        when(sqlNode.toString()).thenReturn("sqlnode");
        when(sqlNode.getOperands()).thenReturn(sqlNodes);

        TableUtils.buildAsNodeByJoinInfo(joinInfo, null, "roc");
    }

    @Test
    public void dealSelectResultWithJoinInfo() throws Exception {
        JoinInfo joinInfo = mock(JoinInfo.class);
        when(joinInfo.checkIsSide()).thenReturn(true);
        when(joinInfo.isRightIsSideTable()).thenReturn(true);
        when(joinInfo.getNewTableName()).thenReturn("new");
        when(joinInfo.getNewTableAlias()).thenReturn("ali");
        when(joinInfo.getLeftTableAlias()).thenReturn("a");
        when(joinInfo.getRightTableAlias()).thenReturn("b");
        HashBasedTable<String, String, String> fieldMapping = HashBasedTable.<String, String, String>create();
        when(joinInfo.getTableFieldRef()).thenReturn(fieldMapping);

        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.SELECT);
        when(joinInfo.getLeftNode()).thenReturn(sqlNode);


        SqlSelect sqlSelect = PowerMockito.mock(SqlSelect.class);
        SqlNodeList sqlNodeList = mock(SqlNodeList.class);
        when(sqlSelect.getSelectList()).thenReturn(sqlNodeList);


        PowerMockito.mockStatic(FieldReplaceUtil.class);
        PowerMockito.doNothing().when(FieldReplaceUtil.class, "replaceFieldName", anyObject(), anyString(), anyString(), anyObject());

        Queue<Object> queue = mock(Queue.class);
        TableUtils.dealSelectResultWithJoinInfo(joinInfo, sqlSelect, queue);
        when(joinInfo.isRightIsSideTable()).thenReturn(false);
        SqlNode sqlRightNode = mock(SqlNode.class);
        when(sqlRightNode.getKind()).thenReturn(SqlKind.SELECT);
        when(joinInfo.getRightNode()).thenReturn(sqlRightNode);
        TableUtils.dealSelectResultWithJoinInfo(joinInfo, sqlSelect, queue);
    }

    @Test
    public void getFromTableInfo() {
        SqlBasicCall sqlNode = mock(SqlBasicCall.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.AS);
        when(sqlNode.toString()).thenReturn("a");
        SqlNode[] sqlNodes = new SqlNode[2];
        sqlNodes[0] = sqlNode;
        sqlNodes[1] = sqlNode;
        when(sqlNode.getOperands()).thenReturn(sqlNodes);
        TableUtils.getFromTableInfo(sqlNode, Sets.newHashSet());

        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlIdentifier.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(sqlIdentifier.getSimple()).thenReturn("a");
        TableUtils.getFromTableInfo(sqlIdentifier, Sets.newHashSet());

        SqlJoin sqlJoin = PowerMockito.mock(SqlJoin.class);
        when(sqlJoin.getKind()).thenReturn(SqlKind.JOIN);
        when(sqlJoin.getLeft()).thenReturn(sqlIdentifier);
        when(sqlJoin.getRight()).thenReturn(sqlIdentifier);


        SqlSelect sqlSelect = PowerMockito.mock(SqlSelect.class);
        when(sqlSelect.getKind()).thenReturn(SqlKind.SELECT);
        when(sqlSelect.getFrom()).thenReturn(sqlJoin);
        TableUtils.getFromTableInfo(sqlSelect, Sets.newHashSet());

        try {
            when(sqlNode.getKind()).thenReturn(SqlKind.CAST);
            TableUtils.getFromTableInfo(sqlNode, Sets.newHashSet());
        } catch (Exception e) {

        }
    }


    @Test
    public void replaceSelectFieldTable() {
        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlIdentifier.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlIdentifier.names = ImmutableList.copyOf(Arrays.asList("old", "a"));
        HashBiMap<String, String> map = HashBiMap.create();
        map.put("a", "b");

        SqlNode sqlNodeNothing = mock(SqlNode.class);
        when(sqlNodeNothing.getKind()).thenReturn(SqlKind.LITERAL_CHAIN);

        SqlBasicCall sqlBasicCallNull = mock(SqlBasicCall.class);
        when(sqlBasicCallNull.getKind()).thenReturn(SqlKind.IS_NULL);
        when(sqlBasicCallNull.getOperands()).thenReturn(Lists.newArrayList(sqlNodeNothing).toArray(new SqlNode[1]));


        SqlBasicCall sqlBasicCallAs = mock(SqlBasicCall.class);
        when(sqlBasicCallAs.getKind()).thenReturn(SqlKind.AS);
        when(sqlBasicCallAs.getOperands()).thenReturn(Lists.newArrayList(sqlNodeNothing).toArray(new SqlNode[1]));
        TableUtils.replaceSelectFieldTable(sqlBasicCallAs, "old", "new", map);

        SqlCase sqlCase = mock(SqlCase.class);
        when(sqlCase.getKind()).thenReturn(SqlKind.CASE);
        SqlNodeList sqlNodeList = mock(SqlNodeList.class);
        when(sqlNodeList.getList()).thenReturn(Lists.newArrayList(sqlNodeNothing));
        when(sqlCase.getWhenOperands()).thenReturn(sqlNodeList);

        when(sqlNodeList.get(anyInt())).thenReturn(sqlNodeNothing);
        when(sqlNodeList.size()).thenReturn(1);

        when(sqlCase.getThenOperands()).thenReturn(sqlNodeList);

        SqlNode sqlKindNode = mock(SqlNode.class);
        when(sqlKindNode.getKind()).thenReturn(SqlKind.LITERAL);
        when(sqlCase.getElseOperand()).thenReturn(sqlKindNode);

        TableUtils.replaceSelectFieldTable(sqlCase, "old", "new", map);


        TableUtils.replaceSelectFieldTable(sqlNodeNothing, "old", "new", map);

        when(sqlNodeNothing.getKind()).thenReturn(SqlKind.OTHER);
        TableUtils.replaceSelectFieldTable(sqlNodeNothing, "old", "new", map);

        try {
            when(sqlNodeNothing.getKind()).thenReturn(SqlKind.DEFAULT);
            TableUtils.replaceSelectFieldTable(sqlNodeNothing, "old", "new", map);
        } catch (Exception e) {

        }


    }

    @Test(expected = Exception.class)
    public void replaceOneSelectField() throws Exception {
        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlIdentifier.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlIdentifier.names = ImmutableList.copyOf(Arrays.asList("old", "a"));
        HashBiMap<String, String> map = HashBiMap.create();
        map.put("a", "b");
        PowerMockito.spy(TableUtils.class);
        PowerMockito.when(TableUtils.class, "replaceOneSelectField", sqlIdentifier, "old", "new", map).thenReturn(null);

    }

    @Test
    public void replaceJoinFieldRefTableName(){
        SqlNode sqlNodeNothing = mock(SqlNode.class);
        when(sqlNodeNothing.getKind()).thenReturn(SqlKind.LITERAL);
        Map<String, String> fieldRef = Maps.newHashMap();
        fieldRef.put("ss", "aa.bb");
        TableUtils.replaceJoinFieldRefTableName(sqlNodeNothing, fieldRef);

        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.EQUALS);
        when(sqlBasicCall.getOperands()).thenReturn(Lists.newArrayList(sqlNodeNothing).toArray(new SqlNode[1]));
        TableUtils.replaceJoinFieldRefTableName(sqlBasicCall, fieldRef);

        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlIdentifier.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlIdentifier.names = ImmutableList.copyOf(Arrays.asList("old", "a"));
        when(sqlIdentifier.toString()).thenReturn("ss");

        when(sqlIdentifier.setName(anyInt(), anyObject())).thenReturn(sqlIdentifier);
        TableUtils.replaceJoinFieldRefTableName(sqlIdentifier, fieldRef);

    }

    @Test
    public void getTargetRefField() {
        Map<String, String> refFieldMap = Maps.newHashMap();
        refFieldMap.put("a", "b");
        TableUtils.getTargetRefField(refFieldMap, "a");
    }

    @Test
    public void replaceWhereCondition() {
        TableUtils.replaceWhereCondition(null, null, null, null);

    }

    @Test
    public void getConditionRefTable() {
        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlIdentifier.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(sqlIdentifier.toString()).thenReturn("test");
        TableUtils.getConditionRefTable(sqlIdentifier, Sets.newHashSet());

        when(sqlIdentifier.getKind()).thenReturn(SqlKind.LITERAL);
        TableUtils.getConditionRefTable(sqlIdentifier, Sets.newHashSet());

        when(sqlIdentifier.getKind()).thenReturn(SqlKind.LITERAL_CHAIN);
        TableUtils.getConditionRefTable(sqlIdentifier, Sets.newHashSet());

        when(sqlIdentifier.getKind()).thenReturn(SqlKind.OTHER);
        TableUtils.getConditionRefTable(sqlIdentifier, Sets.newHashSet());

        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.COALESCE);
        when(sqlBasicCall.getOperands()).thenReturn(Lists.newArrayList(sqlIdentifier).toArray(new SqlNode[1]));


        SqlCase sqlCase = mock(SqlCase.class);
        when(sqlCase.getElseOperand()).thenReturn(sqlIdentifier);

        SqlNodeList sqlNodeList = mock(SqlNodeList.class);
        when(sqlNodeList.getList()).thenReturn(Lists.newArrayList(sqlIdentifier));
        when(sqlCase.getThenOperands()).thenReturn(sqlNodeList);
        when(sqlCase.getWhenOperands()).thenReturn(sqlNodeList);

        when(sqlCase.getKind()).thenReturn(SqlKind.CASE);
        TableUtils.getConditionRefTable(sqlCase, Sets.newHashSet());

        try {
            when(sqlIdentifier.getKind()).thenReturn(SqlKind.DEFAULT);
            TableUtils.getConditionRefTable(sqlCase, Sets.newHashSet());
        } catch (Exception e) {

        }


    }


}
