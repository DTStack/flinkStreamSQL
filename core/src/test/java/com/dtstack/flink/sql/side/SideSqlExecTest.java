package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.exec.FlinkSQLExec;
import com.dtstack.flink.sql.parser.CreateTmpTableParser;
import com.dtstack.flink.sql.side.operator.SideWithAllCacheOperator;
import com.dtstack.flink.sql.util.ParseUtils;
import com.dtstack.flink.sql.util.TableUtils;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import jdk.nashorn.internal.runtime.regexp.joni.constants.StringType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SideSqlExec.class
        , FlinkSQLExec.class
        , TableUtils.class
        , ParseUtils.class
        , SideWithAllCacheOperator.class
        , SingleOutputStreamOperator.class})
public class SideSqlExecTest {

    private SideSqlExec sideSqlExec;

    @Before
    public void init() {
        sideSqlExec = new SideSqlExec();
        sideSqlExec.setLocalSqlPluginPath("");
        sideSqlExec.setPluginLoadMode("2");
    }

    // @Test
    public void exec() throws Exception {
        String sql = "";
        Map<String, AbstractSideTableInfo> sideTableMap = Maps.newHashMap();
        StreamTableEnvironment tableEnv = mock(StreamTableEnvironment.class);
        Map<String, Table> tableCache = Maps.newHashMap();
        StreamQueryConfig queryConfig = mock(StreamQueryConfig.class);
        CreateTmpTableParser.SqlParserResult createView = mock(CreateTmpTableParser.SqlParserResult.class);
        String scope = "1";

        Queue<Object> queue = Queues.newLinkedBlockingQueue();


        SideSQLParser sideSQLParser = mock(SideSQLParser.class);
        PowerMockito.whenNew(SideSQLParser.class).withAnyArguments().thenReturn(sideSQLParser);
        when(sideSQLParser.getExeQueue(anyString(), anySet(), anyString())).thenReturn(queue);

        //
        SqlNode sqlNodeInsert = mock(SqlNode.class);
        when(sqlNodeInsert.getKind()).thenReturn(SqlKind.INSERT);
        when(sqlNodeInsert.toString()).thenReturn("a");
        suppress(method(FlinkSQLExec.class, "sqlUpdate"));
        queue.add(sqlNodeInsert);

        SqlWithItem sqlNodeItem = mock(SqlWithItem.class);
        when(sqlNodeItem.getKind()).thenReturn(SqlKind.WITH_ITEM);

        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        sqlNodeItem.name = sqlIdentifier;
        when(sqlIdentifier.toString()).thenReturn("a");
        sqlNodeItem.query = sqlIdentifier;
        queue.add(sqlNodeItem);

        SqlNode sqlNodeSelect = mock(SqlNode.class);
        when(sqlNodeSelect.getKind()).thenReturn(SqlKind.SELECT);
        when(sqlNodeSelect.toString()).thenReturn("a");
        when(createView.getTableName()).thenReturn("a");
        queue.add(sqlNodeSelect);
        sideSqlExec.exec(sql, sideTableMap, tableEnv, tableCache, createView, scope);
    }


    @Test
    public void parseAsQuery() throws Exception {
        Method method = SideSqlExec.class.getDeclaredMethod("parseAsQuery", SqlBasicCall.class, Map.class);
        method.setAccessible(true);

        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        Map<String, Table> tableCache = Maps.newHashMap();

        SqlSelect sqlSelect = mock(SqlSelect.class);
        when(sqlBasicCall.getOperands()).thenReturn(Lists.newArrayList(sqlSelect, sqlSelect).toArray(new SqlNode[2]));
        method.invoke(sideSqlExec, sqlBasicCall, tableCache);

        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getFieldName()).thenReturn("a");
        when(fieldInfo.getTable()).thenReturn("t");

        when(sqlBasicCall.getKind()).thenReturn(SqlKind.SELECT);
        PowerMockito.mockStatic(TableUtils.class);
        when(TableUtils.parserSelectField(any(), anyMap())).thenReturn(Lists.newArrayList(fieldInfo));

        PowerMockito.mockStatic(ParseUtils.class);
        when(ParseUtils.dealDuplicateFieldName(anyMap(), anyString())).thenReturn("a");
        method.invoke(sideSqlExec, sqlBasicCall, tableCache);
    }

    @Test
    public void buildOutRowTypeInfo() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getTable()).thenReturn("t");
        when(fieldInfo.getFieldName()).thenReturn("a");
        when(fieldInfo.getTypeInformation()).thenReturn(Types.STRING);
        when(fieldInfo.getLogicalType()).thenReturn(new VarCharType());
        List<FieldInfo> sideJoinFieldInfo = Lists.newArrayList(fieldInfo);
        HashBasedTable<String, String, String> mappingTable = HashBasedTable.create();
        mappingTable.put("t", "a", "n");
        sideSqlExec.buildOutRowTypeInfo(sideJoinFieldInfo, mappingTable);
    }

    @Test
    public void getTableFromCache() throws Exception {
        Method method = SideSqlExec.class.getDeclaredMethod("getTableFromCache", Map.class, String.class, String.class);
        method.setAccessible(true);
        Map<String, Table> localTableCache = Maps.newHashMap();
        try {
            method.invoke(sideSqlExec, localTableCache, "a", "b");
        } catch (Exception e) {

        }
        Table table = mock(Table.class);
        localTableCache.put("a", table);
        method.invoke(sideSqlExec, localTableCache, "a", "b");
    }

    @Test
    public void getConditionFields() {
        SqlBasicCall conditionNode = mock(SqlBasicCall.class);
        String specifyTableName = "a";
        AbstractSideTableInfo sideTableInfo = mock(AbstractSideTableInfo.class);
        try {
            sideSqlExec.getConditionFields(conditionNode, specifyTableName, sideTableInfo);

        } catch (Exception e) {

        }
        when(conditionNode.getKind()).thenReturn(SqlKind.EQUALS);

        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(conditionNode.getOperands()).thenReturn(Lists.newArrayList(sqlIdentifier, sqlIdentifier).toArray(new SqlNode[2]));
        when(sqlIdentifier.getComponent(anyInt())).thenReturn(sqlIdentifier);
        when(sqlIdentifier.getSimple()).thenReturn("a");

        when(sideTableInfo.getPhysicalFields()).thenReturn(Maps.newHashMap());
        sideSqlExec.getConditionFields(conditionNode, specifyTableName, sideTableInfo);

        try {
            specifyTableName = "x";
            sideSqlExec.getConditionFields(conditionNode, specifyTableName, sideTableInfo);

        } catch (Exception e) {

        }
    }

    // @Test
    public void dealAsSourceTable() throws Exception {
        Method method = SideSqlExec.class.getDeclaredMethod("dealAsSourceTable", StreamTableEnvironment.class, SqlNode.class, Map.class);
        method.setAccessible(true);
        StreamTableEnvironment tableEnv = mock(StreamTableEnvironment.class);
        SqlBasicCall sqlNode = mock(SqlBasicCall.class);
        Map<String, Table> tableCache = Maps.newHashMap();

        Table table = mock(Table.class);
        when(tableEnv.sqlQuery(anyString())).thenReturn(table);

        when(sqlNode.getKind()).thenReturn(SqlKind.AS);
        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.toString()).thenReturn("a");

        when(sqlNode.getOperands()).thenReturn(Lists.newArrayList(sqlBasicCall, sqlBasicCall).toArray(new SqlNode[2]));

        method.invoke(sideSqlExec, tableEnv, sqlNode, tableCache);
    }

    // @Test
    public void joinFun() throws Exception {
        JoinInfo joinInfo = mock(JoinInfo.class);
        Map<String, Table> localTableCache = Maps.newHashMap();
        Map<String, AbstractSideTableInfo> sideTableMap = Maps.newHashMap();
        StreamTableEnvironment tableEnv = mock(StreamTableEnvironment.class);
        Method method = SideSqlExec.class.getDeclaredMethod("joinFun", Object.class, Map.class, Map.class, StreamTableEnvironment.class);
        method.setAccessible(true);


        SqlBasicCall conditionNode = mock(SqlBasicCall.class);
        when(conditionNode.getKind()).thenReturn(SqlKind.EQUALS);
        SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
        when(conditionNode.getOperands()).thenReturn(Lists.newArrayList(sqlIdentifier, sqlIdentifier).toArray(new SqlNode[2]));
        when(sqlIdentifier.getComponent(anyInt())).thenReturn(sqlIdentifier);
        when(sqlIdentifier.getSimple()).thenReturn("a");

        when(joinInfo.getLeftTableAlias()).thenReturn("a");
        when(joinInfo.getLeftTableName()).thenReturn("tablea");
        when(joinInfo.getRightTableAlias()).thenReturn("b");
        when(joinInfo.getRightTableName()).thenReturn("tableb");
        when(joinInfo.getTableFieldRef()).thenReturn(HashBasedTable.create());
        when(joinInfo.getCondition()).thenReturn(conditionNode);

        AbstractSideTableInfo sideTableInfo = mock(AbstractSideTableInfo.class);
        sideTableMap.put("b", sideTableInfo);
        when(sideTableInfo.getRowTypeInfo()).thenReturn(new RowTypeInfo(new TypeInformation[]{Types.STRING}, new String[]{"a"}));
        when(sideTableInfo.getBaseRowTypeInfo()).thenReturn(new BaseRowTypeInfo(new LogicalType[]{new VarCharType()}, new String[]{"a"}));
        when(sideTableInfo.isPartitionedJoin()).thenReturn(true);
        when(sideTableInfo.getCacheType()).thenReturn("ALL");
        when(sideTableInfo.getType()).thenReturn("kafka");


        Table table = mock(Table.class);
        localTableCache.put("a", table);


        DataStream dsOut = mock(DataStream.class);
        PowerMockito.mockStatic(SideWithAllCacheOperator.class);
        when(SideWithAllCacheOperator.getSideJoinDataStream(any(), anyString(), anyString(), any(), any(), anyList(), any(), anyString())).thenReturn(dsOut);
        Transformation streamTransformation = mock(Transformation.class);
        when(dsOut.getTransformation()).thenReturn(streamTransformation);

        when(table.getSchema()).thenReturn(new TableSchema(new String[]{"a"}, new TypeInformation[]{Types.STRING}));


        SingleOutputStreamOperator dataStream = mock(SingleOutputStreamOperator.class);
        when(tableEnv.toRetractStream(table, Row.class)).thenReturn(dataStream);
        when(dataStream.filter(any())).thenReturn(dataStream);
        when(dataStream.map(any())).thenReturn(dataStream);

        method.invoke(sideSqlExec, joinInfo, localTableCache, sideTableMap, tableEnv);
    }

    // @Test
    public void projectedTypeInfo() throws Exception {
        Method method = SideSqlExec.class.getDeclaredMethod("projectedTypeInfo", int[].class, TableSchema.class);
        method.setAccessible(true);
        TableSchema schema = mock(TableSchema.class);
        when(schema.getFieldNames()).thenReturn(new String[]{"a"});
        when(schema.getFieldTypes()).thenReturn(new TypeInformation[]{Types.STRING});
        method.invoke(sideSqlExec, new int[]{0}, schema);
    }

    @Test
    public void checkFieldsInfo() throws Exception {
        Method method = SideSqlExec.class.getDeclaredMethod("checkFieldsInfo", CreateTmpTableParser.SqlParserResult.class, Table.class);
        method.setAccessible(true);
        CreateTmpTableParser.SqlParserResult result = mock(CreateTmpTableParser.SqlParserResult.class);
        when(result.getFieldsInfoStr()).thenReturn("a varchar");

        Table table = mock(Table.class);
        TableSchema schema = mock(TableSchema.class);
        when(table.getSchema()).thenReturn(schema);
        when(schema.getFieldType(anyInt())).thenReturn(Optional.of(Types.STRING));

        method.invoke(sideSqlExec, result, table);
    }
}
