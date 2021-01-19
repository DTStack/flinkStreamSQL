/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.enums.EPluginLoadMode;
import com.dtstack.flink.sql.exec.FlinkSQLExec;
import com.dtstack.flink.sql.parser.CreateTmpTableParser;
import com.dtstack.flink.sql.side.operator.SideAsyncOperator;
import com.dtstack.flink.sql.side.operator.SideWithAllCacheOperator;
import com.dtstack.flink.sql.util.ClassUtil;
import com.dtstack.flink.sql.util.ParseUtils;
import com.dtstack.flink.sql.util.RowDataConvert;
import com.dtstack.flink.sql.util.TableUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyLocalDateTimeTypeInfo;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.AS;
import static org.apache.calcite.sql.SqlKind.INSERT;
import static org.apache.calcite.sql.SqlKind.SELECT;
import static org.apache.calcite.sql.SqlKind.WITH_ITEM;

/**
 * Reason:
 * Date: 2018/7/24
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class SideSqlExec {

    private static final Logger LOG = LoggerFactory.getLogger(SideSqlExec.class);

    private String localSqlPluginPath = null;

    private String tmpFields = null;

    private String pluginLoadMode = null;

    private SidePredicatesParser sidePredicatesParser = new SidePredicatesParser();

    private Map<String, Table> localTableCache = Maps.newHashMap();

    //维表重新注册之后的名字缓存
    private static Map<String, Table> dimTableNewTable = Maps.newHashMap();

    public void exec(String sql,
                     Map<String, AbstractSideTableInfo> sideTableMap,
                     StreamTableEnvironment tableEnv,
                     Map<String, Table> tableCache,
                     CreateTmpTableParser.SqlParserResult createView,
                     String scope) throws Exception {
        if (!pluginLoadMode.equalsIgnoreCase(EPluginLoadMode.LOCALTEST.name()) && localSqlPluginPath == null ) {
            throw new RuntimeException("need to set localSqlPluginPath");
        }

        localTableCache.putAll(tableCache);
        try {
            sidePredicatesParser.fillPredicatesForSideTable(sql, sideTableMap);
        } catch (Exception e) {
            LOG.error("fill predicates for sideTable fail ", e);
        }

        if (createView != null) {
            LOG.warn("create view info\n");
            LOG.warn(createView.getExecSql());
            LOG.warn("-----------------");
        }

        SideSQLParser sideSQLParser = new SideSQLParser();
        sideSQLParser.setLocalTableCache(localTableCache);
        Queue<Object> exeQueue = sideSQLParser.getExeQueue(sql, sideTableMap.keySet(), scope);
        Object pollObj = null;

        while ((pollObj = exeQueue.poll()) != null) {

            if (pollObj instanceof SqlNode) {
                SqlNode pollSqlNode = (SqlNode) pollObj;


                if (pollSqlNode.getKind() == INSERT) {
                    Collection<String> newRegisterTableList = dimTableNewTable.keySet();
                    FlinkSQLExec.sqlInsert(tableEnv, pollSqlNode, newRegisterTableList);
                    if (LOG.isInfoEnabled()) {
                        LOG.info("----------real exec sql-----------\n{}", pollSqlNode.toString());
                    }

                } else if (pollSqlNode.getKind() == AS) {
                    dealAsSourceTable(tableEnv, pollSqlNode, tableCache);

                } else if (pollSqlNode.getKind() == WITH_ITEM) {
                    SqlWithItem sqlWithItem = (SqlWithItem) pollSqlNode;
                    String TableAlias = sqlWithItem.name.toString();
                    Table table = tableEnv.sqlQuery(sqlWithItem.query.toString());
                    tableEnv.createTemporaryView(TableAlias, table);

                } else if (pollSqlNode.getKind() == SELECT) {
                    Preconditions.checkState(createView != null, "select sql must included by create view");
                    Collection<String> newRegisterTableList = dimTableNewTable.keySet();
                    Table table = FlinkSQLExec.sqlQuery(tableEnv, pollSqlNode, newRegisterTableList);

                    if (createView.getFieldsInfoStr() == null) {
                        tableEnv.registerTable(createView.getTableName(), table);
                    } else {
                        if (checkFieldsInfo(createView, table)) {
                            table = table.as(tmpFields);
                            tableEnv.createTemporaryView(createView.getTableName(), table);
                        } else {
                            throw new RuntimeException("Fields mismatch");
                        }
                    }

                    localTableCache.put(createView.getTableName(), table);
                }

            } else if (pollObj instanceof JoinInfo) {
                LOG.info("----------exec join info----------\n{}", pollObj.toString());
                joinFun(pollObj, localTableCache, dimTableNewTable,sideTableMap, tableEnv);
            }
        }

    }


    /**
     * 解析出as查询的表和字段的关系
     *
     * @param asSqlNode
     * @param tableCache
     * @return
     */
    private FieldReplaceInfo parseAsQuery(SqlBasicCall asSqlNode, Map<String, Table> tableCache) {
        SqlNode info = asSqlNode.getOperands()[0];
        SqlNode alias = asSqlNode.getOperands()[1];

        SqlKind infoKind = info.getKind();
        if (infoKind != SELECT) {
            return null;
        }

        List<FieldInfo> extractFieldList = TableUtils.parserSelectField((SqlSelect) info, tableCache);

        HashBasedTable<String, String, String> mappingTable = HashBasedTable.create();
        for (FieldInfo fieldInfo : extractFieldList) {
            String tableName = fieldInfo.getTable();
            String fieldName = fieldInfo.getFieldName();
            String mappingFieldName = ParseUtils.dealDuplicateFieldName(mappingTable, fieldName);
            mappingTable.put(tableName, fieldName, mappingFieldName);
        }

        FieldReplaceInfo replaceInfo = new FieldReplaceInfo();
        replaceInfo.setMappingTable(mappingTable);
        replaceInfo.setTargetTableName(alias.toString());
        replaceInfo.setTargetTableAlias(alias.toString());
        return replaceInfo;
    }


    public AliasInfo parseASNode(SqlNode sqlNode) throws SqlParseException {
        SqlKind sqlKind = sqlNode.getKind();
        if (sqlKind != AS) {
            throw new RuntimeException(sqlNode + " is not 'as' operator");
        }

        SqlNode info = ((SqlBasicCall) sqlNode).getOperands()[0];
        SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];

        AliasInfo aliasInfo = new AliasInfo();
        aliasInfo.setName(info.toString());
        aliasInfo.setAlias(alias.toString());

        return aliasInfo;
    }

    public BaseRowTypeInfo buildOutRowTypeInfo(List<FieldInfo> sideJoinFieldInfo,
                                               HashBasedTable<String, String, String> mappingTable) {
        // TypeInformation[] sideOutTypes = new TypeInformation[sideJoinFieldInfo.size()];
        LogicalType[] sideOutTypes = new LogicalType[sideJoinFieldInfo.size()];
        String[] sideOutNames = new String[sideJoinFieldInfo.size()];
        for (int i = 0; i < sideJoinFieldInfo.size(); i++) {
            FieldInfo fieldInfo = sideJoinFieldInfo.get(i);
            String tableName = fieldInfo.getTable();
            String fieldName = fieldInfo.getFieldName();

            String mappingFieldName = mappingTable.get(tableName, fieldName);
            Preconditions.checkNotNull(mappingFieldName, fieldInfo + " not mapping any field! it may be frame bug");

            sideOutTypes[i] = fieldInfo.getLogicalType();
            sideOutNames[i] = mappingFieldName;
        }
        return new BaseRowTypeInfo(sideOutTypes, sideOutNames);
    }


    /**
     * 对时间类型进行类型转换
     *
     * @param leftTypeInfo
     * @return
     */
    private RowTypeInfo buildLeftTableOutType(RowTypeInfo leftTypeInfo) {
        TypeInformation[] sideOutTypes = new TypeInformation[leftTypeInfo.getFieldNames().length];
        TypeInformation<?>[] fieldTypes = leftTypeInfo.getFieldTypes();
        for (int i = 0; i < sideOutTypes.length; i++) {
            sideOutTypes[i] = convertTimeAttributeType(fieldTypes[i]);
        }
        RowTypeInfo rowTypeInfo = new RowTypeInfo(sideOutTypes, leftTypeInfo.getFieldNames());
        return rowTypeInfo;
    }

    private TypeInformation convertTimeAttributeType(TypeInformation typeInformation) {
        if (typeInformation instanceof TimeIndicatorTypeInfo) {
            return TypeInformation.of(LocalDateTime.class);
        }
        return typeInformation;
    }


    public void setLocalSqlPluginPath(String localSqlPluginPath) {
        this.localSqlPluginPath = localSqlPluginPath;
    }

    public void setPluginLoadMode(String pluginLoadMode) {
        this.pluginLoadMode = pluginLoadMode;
    }

    private Table getTableFromCache(Map<String, Table> localTableCache, String tableAlias, String tableName) {
        Table table = localTableCache.get(tableAlias);
        if (table == null) {
            table = localTableCache.get(tableName);
        }

        if (table == null) {
            throw new RuntimeException("not register table " + tableAlias);
        }

        return table;
    }


    /**
     * Analyzing conditions are very join the dimension tables include all equivalent conditions (i.e., dimension table is the primary key definition
     *
     * @return
     */
    private boolean checkJoinCondition(SqlNode conditionNode, String sideTableAlias, AbstractSideTableInfo sideTableInfo) {
        List<String> conditionFields = getConditionFields(conditionNode, sideTableAlias, sideTableInfo);
        return CollectionUtils.isEqualCollection(conditionFields, convertPrimaryAlias(sideTableInfo));
    }

    private List<String> convertPrimaryAlias(AbstractSideTableInfo sideTableInfo) {
        List<String> res = Lists.newArrayList();
        sideTableInfo.getPrimaryKeys().forEach(field -> {
            res.add(sideTableInfo.getPhysicalFields().getOrDefault(field, field));
        });
        return res;
    }

    /**
     * check whether all table fields exist in join condition.
     * @param conditionNode
     * @param joinScope
     */
    public void checkConditionFieldsInTable(SqlNode conditionNode, JoinScope joinScope, AbstractSideTableInfo sideTableInfo) {
        List<SqlNode> sqlNodeList = Lists.newArrayList();
        ParseUtils.parseAnd(conditionNode, sqlNodeList);
        for (SqlNode sqlNode : sqlNodeList) {
            if (!SqlKind.COMPARISON.contains(sqlNode.getKind())) {
                throw new RuntimeException("It is not comparison operator.");
            }

            SqlNode leftNode = ((SqlBasicCall) sqlNode).getOperands()[0];
            SqlNode rightNode = ((SqlBasicCall) sqlNode).getOperands()[1];

            if (leftNode.getKind() == SqlKind.IDENTIFIER) {
                checkFieldInTable((SqlIdentifier) leftNode, joinScope, conditionNode, sideTableInfo);
            }

            if (rightNode.getKind() == SqlKind.IDENTIFIER) {
                checkFieldInTable((SqlIdentifier) rightNode, joinScope, conditionNode, sideTableInfo);
            }

        }
    }

    /**
     * check whether table exists and whether field is in table.
     * @param sqlNode
     * @param joinScope
     * @param conditionNode
     */
    private void checkFieldInTable(SqlIdentifier sqlNode, JoinScope joinScope, SqlNode conditionNode,  AbstractSideTableInfo sideTableInfo) {
        String tableName = sqlNode.getComponent(0).getSimple();
        String fieldName = sqlNode.getComponent(1).getSimple();
        JoinScope.ScopeChild scopeChild = joinScope.getScope(tableName);
        String tableErrorMsg = "Table [%s] is not exist. Error condition is [%s]. If you find [%s] is exist. Please check AS statement.";
        Preconditions.checkState(
            scopeChild != null,
            tableErrorMsg,
            tableName,
            conditionNode.toString(),
            tableName
        );

        String[] fieldNames = scopeChild.getRowTypeInfo().getFieldNames();
        ArrayList<String> allFieldNames = new ArrayList(
            Arrays.asList(fieldNames)
        );
        // HBase、Redis这种NoSQL Primary Key不在字段列表中，所以要加进去。
        if (sideTableInfo != null) {
            List<String> pks = sideTableInfo.getPrimaryKeys();
            if (pks != null) {
                pks.stream()
                    .filter(pk -> !allFieldNames.contains(pk))
                    .forEach(pk -> allFieldNames.add(pk));
            }
        }

        boolean hasField = allFieldNames.contains(fieldName);
        String fieldErrorMsg = "Table [%s] has not [%s] field. Error join condition is [%s]. If you find it is exist. Please check AS statement.";
        Preconditions.checkState(
            hasField,
            fieldErrorMsg,
            tableName,
            fieldName,
            conditionNode.toString()
        );
    }

    public List<String> getConditionFields(SqlNode conditionNode, String specifyTableName, AbstractSideTableInfo sideTableInfo) {
        List<SqlNode> sqlNodeList = Lists.newArrayList();
        ParseUtils.parseAnd(conditionNode, sqlNodeList);
        List<String> conditionFields = Lists.newArrayList();
        for (SqlNode sqlNode : sqlNodeList) {
            if (!SqlKind.COMPARISON.contains(sqlNode.getKind())) {
                throw new RuntimeException("not compare operator.");
            }

            SqlIdentifier left = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[0];
            SqlIdentifier right = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[1];

            String leftTableName = left.getComponent(0).getSimple();
            String rightTableName = right.getComponent(0).getSimple();

            String tableCol = "";
            if (leftTableName.equalsIgnoreCase(specifyTableName)) {
                tableCol = left.getComponent(1).getSimple();
            } else if (rightTableName.equalsIgnoreCase(specifyTableName)) {
                tableCol = right.getComponent(1).getSimple();
            } else {
                throw new RuntimeException(String.format("side table:%s join condition is wrong", specifyTableName));
            }
            tableCol = sideTableInfo.getPhysicalFields().getOrDefault(tableCol, tableCol);
            conditionFields.add(tableCol);
        }

        return conditionFields;
    }

    protected void dealAsSourceTable(StreamTableEnvironment tableEnv,
                                     SqlNode pollSqlNode,
                                     Map<String, Table> tableCache) throws SqlParseException {

        AliasInfo aliasInfo = parseASNode(pollSqlNode);
        if (localTableCache.containsKey(aliasInfo.getName())) {
            return;
        }

        Table table = tableEnv.sqlQuery(aliasInfo.getName());
        tableEnv.registerTable(aliasInfo.getAlias(), table);
        localTableCache.put(aliasInfo.getAlias(), table);

        LOG.info("Register Table {} by {}", aliasInfo.getAlias(), aliasInfo.getName());

        FieldReplaceInfo fieldReplaceInfo = parseAsQuery((SqlBasicCall) pollSqlNode, tableCache);
        if (fieldReplaceInfo == null) {
            return;
        }

        //as 的源表
        Set<String> fromTableNameSet = Sets.newHashSet();
        SqlNode fromNode = ((SqlBasicCall) pollSqlNode).getOperands()[0];
        TableUtils.getFromTableInfo(fromNode, fromTableNameSet);

    }

    private void joinFun(Object pollObj,
                         Map<String, Table> localTableCache,
                         Map<String, Table> dimTableNewTable,
                         Map<String, AbstractSideTableInfo> sideTableMap,
                         StreamTableEnvironment tableEnv) throws Exception {
        JoinInfo joinInfo = (JoinInfo) pollObj;

        JoinScope joinScope = new JoinScope();
        JoinScope.ScopeChild leftScopeChild = new JoinScope.ScopeChild();
        leftScopeChild.setAlias(joinInfo.getLeftTableAlias());
        leftScopeChild.setTableName(joinInfo.getLeftTableName());

        Table leftTable = getTableFromCache(localTableCache, joinInfo.getLeftTableAlias(), joinInfo.getLeftTableName());

        RowTypeInfo leftTypeInfo = new RowTypeInfo(leftTable.getSchema().getFieldTypes(), leftTable.getSchema().getFieldNames());

        int length = leftTable.getSchema().getFieldDataTypes().length;
        LogicalType[] logicalTypes = new LogicalType[length];
        for (int i = 0; i < length; i++) {
            logicalTypes[i] = leftTable.getSchema().getFieldDataTypes()[i].getLogicalType();
            if (logicalTypes[i] instanceof LegacyTypeInformationType &&
                    ((LegacyTypeInformationType<?>) logicalTypes[i]).getTypeInformation().getClass().equals(BigDecimalTypeInfo.class)) {
                logicalTypes[i] = new DecimalType(38, 18);
            }

            if (logicalTypes[i] instanceof LegacyTypeInformationType &&
                    (((LegacyTypeInformationType<?>) logicalTypes[i]).getTypeInformation().getClass().equals(LegacyLocalDateTimeTypeInfo.class))) {
                logicalTypes[i] = new TimestampType(TimestampType.MAX_PRECISION);
            }
        }

        BaseRowTypeInfo leftBaseTypeInfo = new BaseRowTypeInfo(logicalTypes, leftTable.getSchema().getFieldNames());

        leftScopeChild.setRowTypeInfo(leftTypeInfo);
        leftScopeChild.setBaseRowTypeInfo(leftBaseTypeInfo);

        JoinScope.ScopeChild rightScopeChild = new JoinScope.ScopeChild();
        rightScopeChild.setAlias(joinInfo.getRightTableAlias());
        rightScopeChild.setTableName(joinInfo.getRightTableName());
        AbstractSideTableInfo sideTableInfo = sideTableMap.get(joinInfo.getRightTableName());
        if (sideTableInfo == null) {
            sideTableInfo = sideTableMap.get(joinInfo.getRightTableAlias());
        }

        if (sideTableInfo == null) {
            throw new RuntimeException("can't not find side table:" + joinInfo.getRightTableName());
        }

        rightScopeChild.setRowTypeInfo(sideTableInfo.getRowTypeInfo());
        rightScopeChild.setBaseRowTypeInfo(sideTableInfo.getBaseRowTypeInfo());

        joinScope.addScope(leftScopeChild);
        joinScope.addScope(rightScopeChild);

        HashBasedTable<String, String, String> mappingTable = ((JoinInfo) pollObj).getTableFieldRef();
        // 检查JOIN等式字段是否在原表中
        checkConditionFieldsInTable(joinInfo.getCondition(), joinScope, sideTableInfo);

        //获取两个表的所有字段
        List<FieldInfo> sideJoinFieldInfo = ParserJoinField.getRowTypeInfo(joinInfo.getSelectNode(), joinScope, true);
        //通过join的查询字段信息过滤出需要的字段信息
        sideJoinFieldInfo.removeIf(tmpFieldInfo -> mappingTable.get(tmpFieldInfo.getTable(), tmpFieldInfo.getFieldName()) == null);

        String leftTableAlias = joinInfo.getLeftTableAlias();
        Table targetTable = localTableCache.get(leftTableAlias);
        if (targetTable == null) {
            targetTable = localTableCache.get(joinInfo.getLeftTableName());
        }

        TypeInformation<?>[] fieldDataTypes = targetTable.getSchema().getFieldTypes();
        for (int i = 0; i < fieldDataTypes.length; i++) {
            if (fieldDataTypes[i].getClass().equals(BigDecimalTypeInfo.class)) {
                fieldDataTypes[i] = BasicTypeInfo.BIG_DEC_TYPE_INFO;
            }

            if (fieldDataTypes[i].getClass().equals(LegacyLocalDateTimeTypeInfo.class)) {
                fieldDataTypes[i] = LocalTimeTypeInfo.LOCAL_DATE_TIME;
            }

            if (fieldDataTypes[i].getClass().equals(TimeIndicatorTypeInfo.class)) {
                fieldDataTypes[i] = TypeInformation.of(new TypeHint<Timestamp>() {});
            }
        }

        RowTypeInfo typeInfo = new RowTypeInfo(fieldDataTypes, targetTable.getSchema().getFieldNames());

        DataStream<BaseRow> adaptStream = tableEnv.toRetractStream(targetTable, typeInfo)
                .map(f -> RowDataConvert.convertToBaseRow(f));

        //join side table before keyby ===> Reducing the size of each dimension table cache of async
        if (sideTableInfo.isPartitionedJoin()) {
            // TODO need change
            // adaptStream = adaptStream.keyBy(new TupleKeySelector(leftBaseTypeInfo));
        }

        DataStream<BaseRow> dsOut = null;
        if(ECacheType.ALL.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            dsOut = SideWithAllCacheOperator.getSideJoinDataStream(adaptStream, sideTableInfo.getType(), localSqlPluginPath, typeInfo, joinInfo, sideJoinFieldInfo, sideTableInfo, pluginLoadMode);
        }else{
            dsOut = SideAsyncOperator.getSideJoinDataStream(adaptStream, sideTableInfo.getType(), localSqlPluginPath, typeInfo, joinInfo, sideJoinFieldInfo, sideTableInfo, pluginLoadMode);
        }

        BaseRowTypeInfo sideOutTypeInfo = buildOutRowTypeInfo(sideJoinFieldInfo, mappingTable);

        dsOut.getTransformation().setOutputType(sideOutTypeInfo);

        String targetTableName = joinInfo.getNewTableName();
        String targetTableAlias = joinInfo.getNewTableAlias();

        FieldReplaceInfo replaceInfo = new FieldReplaceInfo();
        replaceInfo.setMappingTable(mappingTable);
        replaceInfo.setTargetTableName(targetTableName);
        replaceInfo.setTargetTableAlias(targetTableAlias);

        boolean tableExists = false;
        for (String table : tableEnv.listTables()) {
            if (table.equals(targetTableName)) {
                tableExists = true;
                break;
            }
        }

        if (!tableExists) {
            Table joinTable = tableEnv.fromDataStream(dsOut);
            tableEnv.createTemporaryView(targetTableName, joinTable);
            localTableCache.put(joinInfo.getNewTableName(), joinTable);
            dimTableNewTable.put(joinInfo.getNewTableName(), joinTable);
        }
    }

    private boolean checkFieldsInfo(CreateTmpTableParser.SqlParserResult result, Table table) {
        List<String> fieldNames = new LinkedList<>();
        String fieldsInfo = result.getFieldsInfoStr();
        String[] fields = StringUtils.split(fieldsInfo, ",");
        for (int i = 0; i < fields.length; i++) {
            String[] filed = fields[i].split("\\s");
            if (filed.length < 2 || fields.length != table.getSchema().getFieldCount()) {
                return false;
            } else {
                String[] filedNameArr = new String[filed.length - 1];
                System.arraycopy(filed, 0, filedNameArr, 0, filed.length - 1);
                String fieldName = String.join(" ", filedNameArr);
                fieldNames.add(fieldName);
                String fieldType = filed[filed.length - 1].trim();
                Class fieldClass = ClassUtil.stringConvertClass(fieldType);
                Class tableField = table.getSchema().getFieldType(i).get().getTypeClass();
                if (fieldClass != tableField) {
                    return false;
                }
            }
        }
        tmpFields = String.join(",", fieldNames);
        return true;
    }

    public static Map<String, Table> getDimTableNewTable(){
        return dimTableNewTable;
    }

}
