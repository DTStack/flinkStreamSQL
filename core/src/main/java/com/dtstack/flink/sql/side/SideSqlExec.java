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
import com.dtstack.flink.sql.exec.FlinkSQLExec;
import com.dtstack.flink.sql.parser.CreateTmpTableParser;
import com.dtstack.flink.sql.side.operator.SideAsyncOperator;
import com.dtstack.flink.sql.side.operator.SideWithAllCacheOperator;
import com.dtstack.flink.sql.util.ClassUtil;
import com.dtstack.flink.sql.util.ParseUtils;
import com.dtstack.flink.sql.util.TableUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

import static org.apache.calcite.sql.SqlKind.*;

/**
 * Reason:
 * Date: 2018/7/24
 * Company: www.dtstack.com
 * @author xuchao
 */

public class SideSqlExec {

    private static final Logger LOG = LoggerFactory.getLogger(SideSqlExec.class);

    private String localSqlPluginPath = null;

    private String tmpFields = null;

    private SidePredicatesParser sidePredicatesParser = new SidePredicatesParser();

    private Map<String, Table> localTableCache = Maps.newHashMap();

    public void exec(String sql, Map<String, SideTableInfo> sideTableMap, StreamTableEnvironment tableEnv,
                     Map<String, Table> tableCache, StreamQueryConfig queryConfig, CreateTmpTableParser.SqlParserResult createView) throws Exception {
        if(localSqlPluginPath == null){
            throw new RuntimeException("need to set localSqlPluginPath");
        }

        localTableCache.putAll(tableCache);
        try {
            sidePredicatesParser.fillPredicatesForSideTable(sql, sideTableMap);
        } catch (Exception e) {
            LOG.error("fill predicates for sideTable fail ", e);
        }

        if(createView != null){
            LOG.warn("create view info\n");
            LOG.warn(createView.getExecSql());
            LOG.warn("-----------------");
        }

        SideSQLParser sideSQLParser = new SideSQLParser();
        sideSQLParser.setLocalTableCache(localTableCache);
        Queue<Object> exeQueue = sideSQLParser.getExeQueue(sql, sideTableMap.keySet());
        Object pollObj = null;

        while((pollObj = exeQueue.poll()) != null){

            if(pollObj instanceof SqlNode){
                SqlNode pollSqlNode = (SqlNode) pollObj;


                if(pollSqlNode.getKind() == INSERT){
                    System.out.println("----------real exec sql-----------" );
                    System.out.println(pollSqlNode.toString());
                    FlinkSQLExec.sqlUpdate(tableEnv, pollSqlNode.toString(), queryConfig);
                    if(LOG.isInfoEnabled()){
                        LOG.info("exec sql: " + pollSqlNode.toString());
                    }

                }else if(pollSqlNode.getKind() == AS){
                    dealAsSourceTable(tableEnv, pollSqlNode, tableCache);

                } else if (pollSqlNode.getKind() == WITH_ITEM) {
                    SqlWithItem sqlWithItem = (SqlWithItem) pollSqlNode;
                    String TableAlias = sqlWithItem.name.toString();
                    Table table = tableEnv.sqlQuery(sqlWithItem.query.toString());
                    tableEnv.registerTable(TableAlias, table);

                } else if (pollSqlNode.getKind() == SELECT){
                    Preconditions.checkState(createView != null, "select sql must included by create view");
                    Table table = tableEnv.sqlQuery(pollObj.toString());

                    if (createView.getFieldsInfoStr() == null){
                        tableEnv.registerTable(createView.getTableName(), table);
                    } else {
                        if (checkFieldsInfo(createView, table)){
                            table = table.as(tmpFields);
                            tableEnv.registerTable(createView.getTableName(), table);
                        } else {
                            throw new RuntimeException("Fields mismatch");
                        }
                    }

                    localTableCache.put(createView.getTableName(), table);
                }

            }else if (pollObj instanceof JoinInfo){
                System.out.println("----------exec join info----------");
                System.out.println(pollObj.toString());
                joinFun(pollObj, localTableCache, sideTableMap, tableEnv);
            }
        }

    }


    /**
     * 解析出as查询的表和字段的关系
     * @param asSqlNode
     * @param tableCache
     * @return
     */
    private FieldReplaceInfo parseAsQuery(SqlBasicCall asSqlNode, Map<String, Table> tableCache){
        SqlNode info = asSqlNode.getOperands()[0];
        SqlNode alias = asSqlNode.getOperands()[1];

        SqlKind infoKind = info.getKind();
        if(infoKind != SELECT){
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


    /**
     * 添加字段别名
     * @param pollSqlNode
     * @param fieldList
     * @param mappingTable
     */
    private void addAliasForFieldNode(SqlNode pollSqlNode, List<String> fieldList, HashBasedTable<String, String, String> mappingTable) {
        SqlKind sqlKind = pollSqlNode.getKind();
        switch (sqlKind) {
            case INSERT:
                SqlNode source = ((SqlInsert) pollSqlNode).getSource();
                addAliasForFieldNode(source, fieldList, mappingTable);
                break;
            case AS:
                addAliasForFieldNode(((SqlBasicCall) pollSqlNode).getOperands()[0], fieldList, mappingTable);
                break;
            case SELECT:
                SqlNodeList selectList = ((SqlSelect) pollSqlNode).getSelectList();
                selectList.getList().forEach(node -> {
                    if (node.getKind() == IDENTIFIER) {
                        SqlIdentifier sqlIdentifier = (SqlIdentifier) node;
                        if (sqlIdentifier.names.size() == 1) {
                            return;
                        }
                        // save real field
                        String fieldName = sqlIdentifier.names.get(1);
                        if (!fieldName.endsWith("0") || fieldName.endsWith("0") && mappingTable.columnMap().containsKey(fieldName)) {
                            fieldList.add(fieldName);
                        }

                    }
                });
                for (int i = 0; i < selectList.getList().size(); i++) {
                    SqlNode node = selectList.get(i);
                    if (node.getKind() == IDENTIFIER) {
                        SqlIdentifier sqlIdentifier = (SqlIdentifier) node;
                        if (sqlIdentifier.names.size() == 1) {
                            return;
                        }
                        String name = sqlIdentifier.names.get(1);
                        // avoid real field pv0 convert pv
                        if (name.endsWith("0") &&  !fieldList.contains(name) && !fieldList.contains(name.substring(0, name.length() - 1))) {
                            SqlOperator operator = new SqlAsOperator();
                            SqlParserPos sqlParserPos = new SqlParserPos(0, 0);

                            SqlIdentifier sqlIdentifierAlias = new SqlIdentifier(name.substring(0, name.length() - 1), null, sqlParserPos);
                            SqlNode[] sqlNodes = new SqlNode[2];
                            sqlNodes[0] = sqlIdentifier;
                            sqlNodes[1] = sqlIdentifierAlias;
                            SqlBasicCall sqlBasicCall = new SqlBasicCall(operator, sqlNodes, sqlParserPos);

                            selectList.set(i, sqlBasicCall);
                        }
                    }
                }
                break;
        }
    }


    public AliasInfo parseASNode(SqlNode sqlNode) throws SqlParseException {
        SqlKind sqlKind = sqlNode.getKind();
        if(sqlKind != AS){
            throw new RuntimeException(sqlNode + " is not 'as' operator");
        }

        SqlNode info = ((SqlBasicCall)sqlNode).getOperands()[0];
        SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];

        AliasInfo aliasInfo = new AliasInfo();
        aliasInfo.setName(info.toString());
        aliasInfo.setAlias(alias.toString());

        return aliasInfo;
    }

    public RowTypeInfo buildOutRowTypeInfo(List<FieldInfo> sideJoinFieldInfo,
                                           HashBasedTable<String, String, String> mappingTable) {
        TypeInformation[] sideOutTypes = new TypeInformation[sideJoinFieldInfo.size()];
        String[] sideOutNames = new String[sideJoinFieldInfo.size()];
        for (int i = 0; i < sideJoinFieldInfo.size(); i++) {
            FieldInfo fieldInfo = sideJoinFieldInfo.get(i);
            String tableName = fieldInfo.getTable();
            String fieldName = fieldInfo.getFieldName();

            String mappingFieldName = mappingTable.get(tableName, fieldName);
            Preconditions.checkNotNull(mappingFieldName, fieldInfo + " not mapping any field! it may be frame bug");

            sideOutTypes[i] = fieldInfo.getTypeInformation();
            sideOutNames[i] = mappingFieldName;
        }
        return new RowTypeInfo(sideOutTypes, sideOutNames);
    }



    /**
     *  对时间类型进行类型转换
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
            return TypeInformation.of(Timestamp.class);
        }
        return typeInformation;
    }






    public void setLocalSqlPluginPath(String localSqlPluginPath) {
        this.localSqlPluginPath = localSqlPluginPath;
    }

    private Table getTableFromCache(Map<String, Table> localTableCache, String tableAlias, String tableName){
        Table table = localTableCache.get(tableAlias);
        if(table == null){
            table = localTableCache.get(tableName);
        }

        if(table == null){
            throw new RuntimeException("not register table " + tableAlias);
        }

        return table;
    }


    /**
     * Analyzing conditions are very join the dimension tables include all equivalent conditions (i.e., dimension table is the primary key definition
     *
     * @return
     */
    private boolean checkJoinCondition(SqlNode conditionNode, String sideTableAlias, SideTableInfo sideTableInfo) {
        List<String> conditionFields = getConditionFields(conditionNode, sideTableAlias, sideTableInfo);
        if(CollectionUtils.isEqualCollection(conditionFields, convertPrimaryAlias(sideTableInfo))){
            return true;
        }
        return false;
    }

    private List<String> convertPrimaryAlias(SideTableInfo sideTableInfo) {
        List<String> res = Lists.newArrayList();
        sideTableInfo.getPrimaryKeys().forEach(field -> {
            res.add(sideTableInfo.getPhysicalFields().getOrDefault(field, field));
        });
        return res;
    }

    public List<String> getConditionFields(SqlNode conditionNode, String specifyTableName, SideTableInfo sideTableInfo){
        List<SqlNode> sqlNodeList = Lists.newArrayList();
        ParseUtils.parseAnd(conditionNode, sqlNodeList);
        List<String> conditionFields = Lists.newArrayList();
        for(SqlNode sqlNode : sqlNodeList){
            if (!SqlKind.COMPARISON.contains(sqlNode.getKind())) {
                throw new RuntimeException("not compare operator.");
            }

            SqlIdentifier left = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[0];
            SqlIdentifier right = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[1];

            String leftTableName = left.getComponent(0).getSimple();
            String rightTableName = right.getComponent(0).getSimple();

            String tableCol = "";
            if(leftTableName.equalsIgnoreCase(specifyTableName)){
                tableCol = left.getComponent(1).getSimple();
            }else if(rightTableName.equalsIgnoreCase(specifyTableName)){
                tableCol = right.getComponent(1).getSimple();
            }else{
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
        if(fieldReplaceInfo == null){
           return;
        }

        //as 的源表
        Set<String> fromTableNameSet = Sets.newHashSet();
        SqlNode fromNode = ((SqlBasicCall)pollSqlNode).getOperands()[0];
        TableUtils.getFromTableInfo(fromNode, fromTableNameSet);

    }

    private void joinFun(Object pollObj,
                         Map<String, Table> localTableCache,
                         Map<String, SideTableInfo> sideTableMap,
                         StreamTableEnvironment tableEnv) throws Exception{
        JoinInfo joinInfo = (JoinInfo) pollObj;

        JoinScope joinScope = new JoinScope();
        JoinScope.ScopeChild leftScopeChild = new JoinScope.ScopeChild();
        leftScopeChild.setAlias(joinInfo.getLeftTableAlias());
        leftScopeChild.setTableName(joinInfo.getLeftTableName());

        Table leftTable = getTableFromCache(localTableCache, joinInfo.getLeftTableAlias(), joinInfo.getLeftTableName());
        RowTypeInfo leftTypeInfo = new RowTypeInfo(leftTable.getSchema().getTypes(), leftTable.getSchema().getColumnNames());
        leftScopeChild.setRowTypeInfo(leftTypeInfo);

        JoinScope.ScopeChild rightScopeChild = new JoinScope.ScopeChild();
        rightScopeChild.setAlias(joinInfo.getRightTableAlias());
        rightScopeChild.setTableName(joinInfo.getRightTableName());
        SideTableInfo sideTableInfo = sideTableMap.get(joinInfo.getRightTableName());
        if(sideTableInfo == null){
            sideTableInfo = sideTableMap.get(joinInfo.getRightTableAlias());
        }

        if(sideTableInfo == null){
            throw new RuntimeException("can't not find side table:" + joinInfo.getRightTableName());
        }

        if(!checkJoinCondition(joinInfo.getCondition(), joinInfo.getRightTableAlias(), sideTableInfo)){
            throw new RuntimeException("ON condition must contain all equal fields!!!");
        }

        rightScopeChild.setRowTypeInfo(sideTableInfo.getRowTypeInfo());

        joinScope.addScope(leftScopeChild);
        joinScope.addScope(rightScopeChild);

        HashBasedTable<String, String, String> mappingTable = ((JoinInfo) pollObj).getTableFieldRef();

        //获取两个表的所有字段
        //TODO 抽取
        List<FieldInfo> sideJoinFieldInfo = ParserJoinField.getRowTypeInfo(joinInfo.getSelectNode(), joinScope, true);
        //通过join的查询字段信息过滤出需要的字段信息
        sideJoinFieldInfo.removeIf(tmpFieldInfo -> mappingTable.get(tmpFieldInfo.getTable(), tmpFieldInfo.getFieldName()) == null);

        String leftTableAlias = joinInfo.getLeftTableAlias();
        Table targetTable = localTableCache.get(leftTableAlias);
        if(targetTable == null){
            targetTable = localTableCache.get(joinInfo.getLeftTableName());
        }

        RowTypeInfo typeInfo = new RowTypeInfo(targetTable.getSchema().getTypes(), targetTable.getSchema().getColumnNames());

        DataStream adaptStream = tableEnv.toRetractStream(targetTable, org.apache.flink.types.Row.class)
                .filter((Tuple2<Boolean, Row> f0) -> f0.f0)
                .map((Tuple2<Boolean, Row> f0) -> f0.f1)
                .returns(Row.class);


        //join side table before keyby ===> Reducing the size of each dimension table cache of async
        if(sideTableInfo.isPartitionedJoin()){
            RowTypeInfo leftTableOutType = buildLeftTableOutType(leftTypeInfo);
            adaptStream.getTransformation().setOutputType(leftTableOutType);
            List<String> leftJoinColList = getConditionFields(joinInfo.getCondition(), joinInfo.getLeftTableAlias(), sideTableInfo);
            String[] leftJoinColArr = leftJoinColList.toArray(new String[leftJoinColList.size()]);
            adaptStream = adaptStream.keyBy(leftJoinColArr);
        }

        DataStream dsOut;
        if(ECacheType.ALL.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            dsOut = SideWithAllCacheOperator.getSideJoinDataStream(adaptStream, sideTableInfo.getType(), localSqlPluginPath, typeInfo, joinInfo, sideJoinFieldInfo, sideTableInfo);
        }else{
            dsOut = SideAsyncOperator.getSideJoinDataStream(adaptStream, sideTableInfo.getType(), localSqlPluginPath, typeInfo, joinInfo, sideJoinFieldInfo, sideTableInfo);
        }

        RowTypeInfo sideOutTypeInfo = buildOutRowTypeInfo(sideJoinFieldInfo, mappingTable);

        dsOut.getTransformation().setOutputType(sideOutTypeInfo);
        String targetTableName = joinInfo.getNewTableName();
        String targetTableAlias = joinInfo.getNewTableAlias();

        FieldReplaceInfo replaceInfo = new FieldReplaceInfo();
        replaceInfo.setMappingTable(mappingTable);
        replaceInfo.setTargetTableName(targetTableName);
        replaceInfo.setTargetTableAlias(targetTableAlias);

        if (!tableEnv.isRegistered(joinInfo.getNewTableName())){
            Table joinTable = tableEnv.fromDataStream(dsOut);
            tableEnv.registerTable(joinInfo.getNewTableName(), joinTable);
            localTableCache.put(joinInfo.getNewTableName(), joinTable);
        }
    }

    private boolean checkFieldsInfo(CreateTmpTableParser.SqlParserResult result, Table table) {
        List<String> fieldNames = new LinkedList<>();
        String fieldsInfo = result.getFieldsInfoStr();
        String[] fields = fieldsInfo.split(",");
        for (int i = 0; i < fields.length; i++) {
            String[] filed = fields[i].split("\\s");
            if (filed.length < 2 || fields.length != table.getSchema().getColumnNames().length){
                return false;
            } else {
                String[] filedNameArr = new String[filed.length - 1];
                System.arraycopy(filed, 0, filedNameArr, 0, filed.length - 1);
                String fieldName = String.join(" ", filedNameArr);
                fieldNames.add(fieldName);
                String fieldType = filed[filed.length - 1 ].trim();
                Class fieldClass = ClassUtil.stringConvertClass(fieldType);
                Class tableField = table.getSchema().getFieldType(i).get().getTypeClass();
                if (fieldClass == tableField){
                    continue;
                } else {
                    return false;
                }
            }
        }
        tmpFields = String.join(",", fieldNames);
        return true;
    }

}
