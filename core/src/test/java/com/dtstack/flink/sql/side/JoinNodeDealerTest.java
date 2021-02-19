package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.parser.FlinkPlanner;
import com.dtstack.flink.sql.util.TableUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JoinNodeDealer.class, SqlJoin.class, TableUtils.class, FlinkPlanner.class})
public class JoinNodeDealerTest {

    private JoinNodeDealer joinNodeDealer;

    private SideSQLParser sideSQLParser;

    @Before
    public void init(){
        sideSQLParser = mock(SideSQLParser.class);
        joinNodeDealer = new JoinNodeDealer(sideSQLParser);
    }

    @Test
    public void dealJoinNode(){
        SqlParserPos pos = mock(SqlParserPos.class);
        SqlNode left = mock(SqlNode.class);
        SqlLiteral natural = mock(SqlLiteral.class);
        when(natural.getTypeName()).thenReturn(SqlTypeName.BOOLEAN);
        SqlLiteral joinType = mock(SqlLiteral.class);
        when(joinType.symbolValue(JoinType.class)).thenReturn(JoinType.LEFT);
        SqlNode right = mock(SqlNode.class);
        when(right.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(right.toString()).thenReturn("a");
        SqlLiteral conditionType = mock(SqlLiteral.class);
        when(conditionType.symbolValue(JoinConditionType.class)).thenReturn(JoinConditionType.ON);

        SqlNode condition = mock(SqlNode.class);
        SqlJoin joinNode = new SqlJoin(pos, left, natural, joinType, right, conditionType, condition);
        Set<String> sideTableSet = Sets.newHashSet();
        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();
        SqlNode parentWhere = mock(SqlNode.class);
        SqlNodeList parentSelectList = mock(SqlNodeList.class);
        SqlNodeList parentGroupByList = mock(SqlNodeList.class);
        Set<Tuple2<String, String>> joinFieldSet = Sets.newHashSet();
        Map<String, String> tableRef = Maps.newHashMap();
        Map<String, String> fieldRef = Maps.newHashMap();
        String scope = "1";

        joinNodeDealer.dealJoinNode(joinNode, sideTableSet, queueInfo, parentWhere, parentSelectList, parentGroupByList, joinFieldSet, tableRef, fieldRef, scope, Sets.newHashSet());
    }

    @Test
    public void extractJoinNeedSelectField(){

        SqlIdentifier leftNode = mock(SqlIdentifier.class);
        when(leftNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(leftNode.getSimple()).thenReturn("a");
        when(leftNode.toString()).thenReturn("a.b");
        leftNode.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));

        SqlBasicCall parentOther = mock(SqlBasicCall.class);
        when(parentOther.getKind()).thenReturn(SqlKind.OTHER);

        SqlBasicCall parentWhere = mock(SqlBasicCall.class);
        when(parentWhere.getKind()).thenReturn(SqlKind.AND);
        when(parentWhere.getOperands()).thenReturn(Lists.newArrayList(parentOther, parentOther).toArray(new SqlBasicCall[2]));

        SqlNodeList parentSelectList = mock(SqlNodeList.class);
        when(parentSelectList.getList()).thenReturn(Lists.newArrayList(leftNode));

        SqlNodeList parentGroupByList = mock(SqlNodeList.class);
        when(parentGroupByList.getList()).thenReturn(Lists.newArrayList(leftNode));
        Map<String, String> tableRef = Maps.newHashMap();
        Set<Tuple2<String, String>> joinFieldSet = Sets.newHashSet();
        Map<String, String> fieldRef = Maps.newHashMap();
        JoinInfo tableInfo = mock(JoinInfo.class);
        when(tableInfo.getNewTableAlias()).thenReturn("a");
        joinNodeDealer.extractJoinNeedSelectField(leftNode, leftNode, parentWhere, parentSelectList, parentGroupByList, tableRef, joinFieldSet, fieldRef, tableInfo);
    }

    @Test
    public void extractField(){
        SqlIdentifier sqlNode = mock(SqlIdentifier.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(sqlNode.getSimple()).thenReturn("a");
        sqlNode.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));

        SqlBasicCall parentOther = mock(SqlBasicCall.class);
        when(parentOther.getKind()).thenReturn(SqlKind.OTHER);

        SqlBasicCall parentWhere = mock(SqlBasicCall.class);
        when(parentWhere.getKind()).thenReturn(SqlKind.AND);
        when(parentWhere.getOperands()).thenReturn(Lists.newArrayList(parentOther, parentOther).toArray(new SqlBasicCall[2]));

        SqlNodeList parentSelectList = mock(SqlNodeList.class);
        when(parentSelectList.getList()).thenReturn(Lists.newArrayList(sqlNode));

        SqlNodeList parentGroupByList = mock(SqlNodeList.class);
        when(parentGroupByList.getList()).thenReturn(Lists.newArrayList(sqlNode));
        Map<String, String> tableRef = Maps.newHashMap();
        Set<Tuple2<String, String>> joinFieldSet = Sets.newHashSet();
        joinNodeDealer.extractField(sqlNode, parentWhere, parentSelectList, parentGroupByList, tableRef, joinFieldSet);
    }

    @Test
    public void dealNestJoin() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("dealNestJoin"
                , SqlJoin.class
                , Set.class
                , Queue.class
                , SqlNode.class
                , SqlNodeList.class
                , SqlNodeList.class
                , Set.class
                , Map.class
                , Map.class
                , String.class
                , Set.class);
        method.setAccessible(true);
        SqlParserPos pos = mock(SqlParserPos.class);
        SqlLiteral natural = mock(SqlLiteral.class);
        when(natural.getTypeName()).thenReturn(SqlTypeName.BOOLEAN);
        SqlLiteral joinType = mock(SqlLiteral.class);
        when(joinType.symbolValue(JoinType.class)).thenReturn(JoinType.LEFT);
        SqlNode right = mock(SqlNode.class);
        when(right.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(right.toString()).thenReturn("a");
        SqlLiteral conditionType = mock(SqlLiteral.class);
        when(conditionType.symbolValue(JoinConditionType.class)).thenReturn(JoinConditionType.ON);

        SqlNode condition = mock(SqlNode.class);

        SqlNode joinLeft = mock(SqlNode.class);
        when(joinLeft.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(joinLeft.toString()).thenReturn("a");
        SqlNode joinRight = mock(SqlNode.class);
        when(joinRight.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(joinRight.toString()).thenReturn("a");
        SqlJoin left = new SqlJoin(pos, joinLeft, natural, joinType, joinRight, conditionType, condition);

        SqlJoin joinNode = new SqlJoin(pos, left, natural, joinType, right, conditionType, condition);
        Set<String> sideTableSet = Sets.newHashSet();
        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();
        SqlNode parentWhere = mock(SqlNode.class);
        SqlNodeList parentSelectList = mock(SqlNodeList.class);
        SqlNodeList parentGroupByList = mock(SqlNodeList.class);
        Set<Tuple2<String, String>> joinFieldSet = Sets.newHashSet();
        Map<String, String> tableRef = Maps.newHashMap();
        Map<String, String> fieldRef = Maps.newHashMap();
        String scope = "1";
        method.invoke(joinNodeDealer, joinNode, sideTableSet, queueInfo, parentWhere, parentSelectList, parentGroupByList,
                joinFieldSet, tableRef, fieldRef, scope ,new HashSet<>());
    }

    @Test
    public void addSideInfoToExeQueue(){
        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();
        JoinInfo joinInfo = mock(JoinInfo.class);
        SqlJoin joinNode = mock(SqlJoin.class);
        SqlNodeList parentSelectList = mock(SqlNodeList.class);
        SqlNodeList parentGroupByList = mock(SqlNodeList.class);
        SqlNode parentWhere = mock(SqlNode.class);
        Map<String, String> tableRef = Maps.newHashMap();

        when(joinInfo.isRightIsSideTable()).thenReturn(false);
        joinNodeDealer.addSideInfoToExeQueue(queueInfo, joinInfo, joinNode, parentSelectList, parentGroupByList, parentWhere, tableRef, null);

        when(joinInfo.isRightIsSideTable()).thenReturn(true);
        SqlBasicCall buildAs = mock(SqlBasicCall.class);
        PowerMockito.mockStatic(TableUtils.class);
        when(TableUtils.buildAsNodeByJoinInfo(joinInfo, null, null)).thenReturn(buildAs);
        SqlIdentifier leftJoinNode = mock(SqlIdentifier.class);
        when(joinInfo.getLeftNode()).thenReturn(leftJoinNode);

        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlNode.toString()).thenReturn("q");
        when(buildAs.getOperands()).thenReturn(Lists.newArrayList(sqlNode, sqlNode).toArray(new SqlNode[2]));
        when(leftJoinNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(leftJoinNode.getSimple()).thenReturn("q");
        leftJoinNode.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));
        joinNodeDealer.addSideInfoToExeQueue(queueInfo, joinInfo, joinNode, parentSelectList, parentGroupByList, parentWhere, tableRef, null);

    }

    @Test
    public void replaceSelectAndWhereField(){

        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlNode.toString()).thenReturn("q");
        SqlBasicCall buildAs = mock(SqlBasicCall.class);
        when(buildAs.getOperands()).thenReturn(Lists.newArrayList(sqlNode, sqlNode).toArray(new SqlNode[2]));

        SqlIdentifier leftJoinNode = mock(SqlIdentifier.class);
        when(leftJoinNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(leftJoinNode.getSimple()).thenReturn("q");
        leftJoinNode.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));
        SqlNodeList parentSelectList = mock(SqlNodeList.class);
        when(parentSelectList.getList()).thenReturn(Lists.newArrayList(leftJoinNode));
        SqlNodeList parentGroupByList = mock(SqlNodeList.class);
        when(parentGroupByList.getList()).thenReturn(Lists.newArrayList(leftJoinNode));
        SqlNode parentWhere = null;
        joinNodeDealer.replaceSelectAndWhereField(buildAs, leftJoinNode, Maps.newHashMap(), null, parentGroupByList, null, null);
    }

    // @Test
    public void extractTemporaryQuery() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("extractTemporaryQuery", SqlNode.class, String.class, SqlBasicCall.class,
                SqlNodeList.class, Queue.class, Set.class, Map.class, Map.class);
        method.setAccessible(true);

        SqlIdentifier node = mock(SqlIdentifier.class);
        when(node.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(node.getSimple()).thenReturn("a");
        when(node.toString()).thenReturn("a.b");
        node.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));

        SqlBasicCall parentWhere = mock(SqlBasicCall.class);
        when(parentWhere.getKind()).thenReturn(SqlKind.OTHER);

        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.AND);
        when(sqlBasicCall.getOperands()).thenReturn(Lists.newArrayList(parentWhere, parentWhere).toArray(new SqlBasicCall[2]));


        SqlNodeList parentSelectList = mock(SqlNodeList.class);
        when(parentSelectList.getList()).thenReturn(Lists.newArrayList(node));


        String tableAlias = "a";
        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();
        Set<Tuple2<String, String>> joinFieldSet = Sets.newHashSet();
        Map<String, String> tableRef = Maps.newHashMap();
        Map<String, String> fieldRef = Maps.newHashMap();


        method.invoke(joinNodeDealer, node, tableAlias, sqlBasicCall, parentSelectList, queueInfo, joinFieldSet, tableRef, fieldRef);

    }

    @Test
    public void extractSelectFields() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("extractSelectFields", SqlNodeList.class, Set.class, Map.class);
        method.setAccessible(true);

        SqlIdentifier sqlNode = mock(SqlIdentifier.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlNode.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));
        SqlNodeList sqlNodeList = mock(SqlNodeList.class);
        when(sqlNodeList.getList()).thenReturn(Lists.newArrayList(sqlNode));
        method.invoke(joinNodeDealer, sqlNodeList, Sets.newHashSet(), Maps.newHashMap());
    }

    @Test
    public void extractSelectFieldFromJoinCondition() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("extractSelectFieldFromJoinCondition", Set.class, Set.class, Map.class);
        method.setAccessible(true);
        Tuple2 tuple2 = new Tuple2("a", "b");
        Map map = Maps.newHashMap();
        map.put("a", "b");
        method.invoke(joinNodeDealer, Sets.newHashSet(tuple2), Sets.newHashSet("a"), map);

    }

    @Test
    public void extractFieldFromGroupByList() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("extractFieldFromGroupByList", SqlNodeList.class, Set.class, Map.class);
        method.setAccessible(true);
        method.invoke(joinNodeDealer, null, Sets.newHashSet(), Maps.newHashMap());


        SqlIdentifier sqlNode = mock(SqlIdentifier.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlNode.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));
        SqlNodeList sqlNodeList = mock(SqlNodeList.class);
        when(sqlNodeList.getList()).thenReturn(Lists.newArrayList(sqlNode));
        method.invoke(joinNodeDealer, sqlNodeList, Sets.newHashSet(), Maps.newHashMap());
    }

    @Test
    public void extractJoinField() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("extractJoinField", SqlNode.class, Set.class);
        method.setAccessible(true);
        method.invoke(joinNodeDealer, null, Sets.newHashSet());

        SqlIdentifier sqlNode = mock(SqlIdentifier.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlNode.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));
        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.LIKE);
        when(sqlBasicCall.getOperands()).thenReturn(Lists.newArrayList(sqlNode).toArray(new SqlNode[1]));
        method.invoke(joinNodeDealer, sqlBasicCall, Sets.newHashSet());
    }

    @Test
    public void extractSelectField() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("extractSelectField", SqlNode.class, Set.class, Set.class, Map.class);
        method.setAccessible(true);
        SqlIdentifier sqlNode = mock(SqlIdentifier.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlNode.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));
        method.invoke(joinNodeDealer,sqlNode, Sets.newHashSet(), Sets.newHashSet("a"), Maps.newHashMap());

        SqlBasicCall sqlBasicCallAs = mock(SqlBasicCall.class);
        when(sqlBasicCallAs.getKind()).thenReturn(SqlKind.AS);
        when(sqlBasicCallAs.getOperands()).thenReturn(Lists.newArrayList(sqlNode).toArray(new SqlIdentifier[1]));
        method.invoke(joinNodeDealer, sqlBasicCallAs,  Sets.newHashSet(), Sets.newHashSet(), Maps.newHashMap());


        SqlBasicCall sqlBasicCallLike = mock(SqlBasicCall.class);
        when(sqlBasicCallAs.getKind()).thenReturn(SqlKind.LIKE);
        when(sqlBasicCallAs.getOperands()).thenReturn(Lists.newArrayList(sqlNode).toArray(new SqlIdentifier[1]));
        method.invoke(joinNodeDealer, sqlBasicCallLike,  Sets.newHashSet(), Sets.newHashSet(), Maps.newHashMap());

        SqlCase sqlCase = mock(SqlCase.class);
        when(sqlCase.getKind()).thenReturn(SqlKind.CAST);
        SqlNodeList sqlNodeList = mock(SqlNodeList.class);
        when(sqlNodeList.size()).thenReturn(1);
        when(sqlNodeList.get(anyInt())).thenReturn(sqlNode);
        when(sqlCase.getWhenOperands()).thenReturn(sqlNodeList);
        when(sqlCase.getThenOperands()).thenReturn(sqlNodeList);
        when(sqlCase.getElseOperand()).thenReturn(sqlNode);
        method.invoke(joinNodeDealer, sqlBasicCallLike,  Sets.newHashSet(), Sets.newHashSet(), Maps.newHashMap());
    }

    // @Test
    public void parseRightNode() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("parseRightNode"
                , SqlNode.class
                , Set.class
                , Queue.class
                , SqlNode.class
                , SqlNodeList.class
                , SqlNodeList.class
                , String.class
                , Set.class);
        method.setAccessible(true);

        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.IDENTIFIER);
        when(sqlNode.toString()).thenReturn("a");
        method.invoke(joinNodeDealer,  sqlNode, null, null, null, null, null, null, null);

        AliasInfo aliasInfo = mock(AliasInfo.class);
        when(aliasInfo.getName()).thenReturn("a");
        when(aliasInfo.getAlias()).thenReturn("b");
        when(sideSQLParser.parseSql(sqlNode, null, null, null, null, null, null, Sets.newHashSet())).thenReturn(aliasInfo);
        when(sqlNode.getKind()).thenReturn(SqlKind.SELECT);
        method.invoke(joinNodeDealer, sqlNode, null, null, null, null, null, null, null);

    }

    @Test
    public void buildCondition(){
        joinNodeDealer.buildCondition(Lists.newArrayList());
        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.toString()).thenReturn("a");
        joinNodeDealer.buildCondition(Lists.newArrayList(sqlBasicCall));
    }

    @Test
    public void buildSelectNode(){
        try {
            joinNodeDealer.buildSelectNode(null, null);
        } catch (Exception e){

        }
        joinNodeDealer.buildSelectNode(Sets.newHashSet("a.b"), Sets.newHashSet("c.d"));
    }

    @Test
    public void checkIsSideTable() throws  Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("checkIsSideTable", String.class, Set.class);
        method.setAccessible(true);

        method.invoke(joinNodeDealer, "a",  Sets.newHashSet());
        method.invoke(joinNodeDealer, "a",  Sets.newHashSet("a"));
    }

    @Test
    public void buildAsSqlNode() throws  Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("buildAsSqlNode", String.class, SqlNode.class);
        method.setAccessible(true);

        SqlNode sqlNode = mock(SqlNode.class);
        method.invoke(joinNodeDealer, "a",  sqlNode);
    }

    @Test
    public void extractWhereCondition() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("extractWhereCondition", Set.class, SqlBasicCall.class, Set.class);
        method.setAccessible(true);
        method.invoke(joinNodeDealer, null, null, null);

        SqlBasicCall parentWhere = mock(SqlBasicCall.class);
        when(parentWhere.getKind()).thenReturn(SqlKind.OTHER);
        method.invoke(joinNodeDealer, Sets.newHashSet(), parentWhere, Sets.newHashSet());


        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.AND);
        when(sqlBasicCall.getOperands()).thenReturn(Lists.newArrayList(parentWhere, parentWhere).toArray(new SqlBasicCall[2]));
        method.invoke(joinNodeDealer, Sets.newHashSet(), sqlBasicCall, Sets.newHashSet());
    }

    @Test
    public void checkAndRemoveWhereCondition() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("checkAndRemoveWhereCondition", Set.class, SqlBasicCall.class, List.class);
        method.setAccessible(true);

        method.invoke(joinNodeDealer, null, null, null);
        SqlBasicCall parentWhere = mock(SqlBasicCall.class);
        when(parentWhere.getKind()).thenReturn(SqlKind.OTHER);
        method.invoke(joinNodeDealer, Sets.newHashSet(), parentWhere, Lists.newArrayList());

        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.AND);
        when(sqlBasicCall.getOperands()).thenReturn(Lists.newArrayList(parentWhere, parentWhere).toArray(new SqlBasicCall[2]));
        method.invoke(joinNodeDealer, Sets.newHashSet(), sqlBasicCall, Lists.newArrayList());
    }

    @Test
    public void removeWhereConditionNode(){
        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getOperands()).thenReturn(Lists.newArrayList(sqlBasicCall).toArray(new SqlNode[1]));
        joinNodeDealer.removeWhereConditionNode(sqlBasicCall, 0);
    }

    @Test
    public void buildEmptyCondition(){
        joinNodeDealer.buildEmptyCondition();
    }

    @Test
    public  void checkAndReplaceJoinCondition() throws Exception {
        Method method = JoinNodeDealer.class.getDeclaredMethod("checkAndReplaceJoinCondition", SqlNode.class, Map.class);
        method.setAccessible(true);
        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.EQUALS);
        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(sqlIdentifier.getKind()).thenReturn(SqlKind.IDENTIFIER);
        sqlIdentifier.names = ImmutableList.copyOf(Lists.newArrayList("a", "b"));
        when(sqlBasicCall.getOperands()).thenReturn(Lists.newArrayList(sqlIdentifier).toArray(new SqlNode[1]));
        method.invoke(joinNodeDealer, sqlBasicCall, Maps.newHashMap());
    }

    @Test
    public void buildTmpTableFieldRefOriField(){
        joinNodeDealer.buildTmpTableFieldRefOriField(Sets.newHashSet("a.b"), "x");
    }
}
