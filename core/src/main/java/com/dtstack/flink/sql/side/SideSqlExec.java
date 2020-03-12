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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.CRowKeySelector;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.exec.FlinkSQLExec;
import com.dtstack.flink.sql.parser.CreateTmpTableParser;
import com.dtstack.flink.sql.side.operator.SideAsyncOperator;
import com.dtstack.flink.sql.side.operator.SideWithAllCacheOperator;
import com.dtstack.flink.sql.util.ClassUtil;
import com.dtstack.flink.sql.util.ParseUtils;
import com.dtstack.flink.sql.util.TableUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

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

    public void exec(String sql, Map<String, AbstractSideTableInfo> sideTableMap, StreamTableEnvironment tableEnv,
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

        //need clean
        boolean preIsSideJoin = false;
        List<FieldReplaceInfo> replaceInfoList = Lists.newArrayList();

        while((pollObj = exeQueue.poll()) != null){

            if(pollObj instanceof SqlNode){
                SqlNode pollSqlNode = (SqlNode) pollObj;

                if(preIsSideJoin){
                    preIsSideJoin = false;
                    List<String> fieldNames = null;
                    for(FieldReplaceInfo replaceInfo : replaceInfoList){
                        fieldNames = Lists.newArrayList();
                        replaceFieldName(pollSqlNode, replaceInfo);
                        addAliasForFieldNode(pollSqlNode, fieldNames, replaceInfo.getMappingTable());
                    }
                }

                if(pollSqlNode.getKind() == INSERT){
                    System.out.println("----------real exec sql-----------" );
                    System.out.println(pollSqlNode.toString());
                    FlinkSQLExec.sqlUpdate(tableEnv, pollSqlNode.toString(), queryConfig);
                    if(LOG.isInfoEnabled()){
                        LOG.info("exec sql: " + pollSqlNode.toString());
                    }

                }else if(pollSqlNode.getKind() == AS){
                    dealAsSourceTable(tableEnv, pollSqlNode, tableCache, replaceInfoList);

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
                preIsSideJoin = true;
                joinFun(pollObj, localTableCache, sideTableMap, tableEnv, replaceInfoList);
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
            default:
                break;
        }
    }


    public AliasInfo parseAsNode(SqlNode sqlNode) throws SqlParseException {
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

    public RowTypeInfo buildOutRowTypeInfo(List<FieldInfo> sideJoinFieldInfo, HashBasedTable<String, String, String> mappingTable) {
        TypeInformation[] sideOutTypes = new TypeInformation[sideJoinFieldInfo.size()];
        String[] sideOutNames = new String[sideJoinFieldInfo.size()];
        for (int i = 0; i < sideJoinFieldInfo.size(); i++) {
            FieldInfo fieldInfo = sideJoinFieldInfo.get(i);
            String tableName = fieldInfo.getTable();
            String fieldName = fieldInfo.getFieldName();
            String mappingFieldName = ParseUtils.dealDuplicateFieldName(mappingTable, fieldName);
            mappingTable.put(tableName, fieldName, mappingFieldName);

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

    //需要考虑更多的情况
    private void replaceFieldName(SqlNode sqlNode, FieldReplaceInfo replaceInfo) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case INSERT:
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                replaceFieldName(sqlSource, replaceInfo);
                break;
            case AS:
                SqlNode asNode = ((SqlBasicCall) sqlNode).getOperands()[0];
                replaceFieldName(asNode, replaceInfo);
                break;
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) filterNodeWithTargetName(sqlNode, replaceInfo.getTargetTableName());
                if(sqlSelect == null){
                    return;
                }

                SqlNode sqlSource1 = sqlSelect.getFrom();
                if(sqlSource1.getKind() == AS){
                    String tableName = ((SqlBasicCall)sqlSource1).getOperands()[0].toString();
                    if(tableName.equalsIgnoreCase(replaceInfo.getTargetTableName())){
                        SqlNodeList sqlSelectList = sqlSelect.getSelectList();
                        SqlNode whereNode = sqlSelect.getWhere();
                        SqlNodeList sqlGroup = sqlSelect.getGroup();

                        //TODO 暂时不处理having
                        SqlNode sqlHaving = sqlSelect.getHaving();

                        List<SqlNode> newSelectNodeList = Lists.newArrayList();
                        for( int i=0; i<sqlSelectList.getList().size(); i++){
                            SqlNode selectNode = sqlSelectList.getList().get(i);
                            //特殊处理 isStar的标识
                            if(selectNode.getKind() == IDENTIFIER && ((SqlIdentifier) selectNode).isStar()){
                                List<SqlNode> replaceNodeList = replaceSelectStarFieldName(selectNode, replaceInfo);
                                newSelectNodeList.addAll(replaceNodeList);
                                continue;
                            }

                            SqlNode replaceNode = replaceSelectFieldName(selectNode, replaceInfo);
                            if(replaceNode == null){
                                continue;
                            }

                            //sqlSelectList.set(i, replaceNode);
                            newSelectNodeList.add(replaceNode);
                        }

                        SqlNodeList newSelectList = new SqlNodeList(newSelectNodeList, sqlSelectList.getParserPosition());
                        sqlSelect.setSelectList(newSelectList);

                        //where
                        if(whereNode != null){
                            SqlNode[] sqlNodeList = ((SqlBasicCall)whereNode).getOperands();
                            for(int i =0; i<sqlNodeList.length; i++) {
                                SqlNode whereSqlNode = sqlNodeList[i];
                                SqlNode replaceNode = replaceNodeInfo(whereSqlNode, replaceInfo);
                                sqlNodeList[i] = replaceNode;
                            }
                        }
                        if(sqlGroup != null && CollectionUtils.isNotEmpty(sqlGroup.getList())){
                            for( int i=0; i<sqlGroup.getList().size(); i++){
                                SqlNode selectNode = sqlGroup.getList().get(i);
                                SqlNode replaceNode = replaceNodeInfo(selectNode, replaceInfo);
                                sqlGroup.set(i, replaceNode);
                            }
                        }
                    }
                }else{
                    //TODO
                    System.out.println(sqlNode);
                    throw new RuntimeException("---not deal type:" + sqlNode);
                }

                break;
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];
                replaceFieldName(unionLeft, replaceInfo);
                replaceFieldName(unionRight, replaceInfo);

                break;
            case ORDER_BY:
                SqlOrderBy sqlOrderBy  = (SqlOrderBy) sqlNode;
                replaceFieldName(sqlOrderBy.query, replaceInfo);
                SqlNodeList orderFiledList = sqlOrderBy.orderList;
                for (int i=0 ;i<orderFiledList.size();i++) {
                    SqlNode replaceNode = replaceOrderByTableName(orderFiledList.get(i), replaceInfo.getTargetTableAlias());
                    orderFiledList.set(i, replaceNode);
                }

            default:
                break;
        }
    }

    private SqlNode replaceOrderByTableName(SqlNode orderNode, String tableAlias) {
        if(orderNode.getKind() == IDENTIFIER){
            SqlIdentifier sqlIdentifier = (SqlIdentifier) orderNode;
            if (sqlIdentifier.names.size() == 1) {
                return orderNode;
            }
            return sqlIdentifier.setName(0, tableAlias);
        } else if (orderNode instanceof  SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) orderNode;
            for(int i=0; i<sqlBasicCall.getOperandList().size(); i++){
                SqlNode sqlNode = sqlBasicCall.getOperandList().get(i);
                sqlBasicCall.getOperands()[i] = replaceOrderByTableName(sqlNode , tableAlias);
            }
            return sqlBasicCall;
        } else {
            return orderNode;
        }
    }

    private SqlNode replaceNodeInfo(SqlNode groupNode, FieldReplaceInfo replaceInfo){
        if(groupNode.getKind() == IDENTIFIER){
            SqlIdentifier sqlIdentifier = (SqlIdentifier) groupNode;
            if(sqlIdentifier.names.size() == 1){
                return sqlIdentifier;
            }

            String mappingFieldName = replaceInfo.getTargetFieldName(sqlIdentifier.getComponent(0).getSimple(), sqlIdentifier.getComponent(1).getSimple());
            if(mappingFieldName == null){
                throw new RuntimeException("can't find mapping fieldName:" + sqlIdentifier.toString() );
            }

            sqlIdentifier = sqlIdentifier.setName(0, replaceInfo.getTargetTableAlias());
            return sqlIdentifier.setName(1, mappingFieldName);
        }else if(groupNode instanceof  SqlBasicCall){
            SqlBasicCall sqlBasicCall = (SqlBasicCall) groupNode;
            for(int i=0; i<sqlBasicCall.getOperandList().size(); i++){
                SqlNode sqlNode = sqlBasicCall.getOperandList().get(i);
                SqlNode replaceNode = replaceSelectFieldName(sqlNode, replaceInfo);
                sqlBasicCall.getOperands()[i] = replaceNode;
            }

            return sqlBasicCall;
        }else{
            return groupNode;
        }
    }

    public SqlNode filterNodeWithTargetName(SqlNode sqlNode, String targetTableName) {

        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case SELECT:
                SqlNode fromNode = ((SqlSelect)sqlNode).getFrom();
                if(fromNode.getKind() == AS && ((SqlBasicCall)fromNode).getOperands()[0].getKind() == IDENTIFIER){
                    if(((SqlBasicCall)fromNode).getOperands()[0].toString().equalsIgnoreCase(targetTableName)){
                        return sqlNode;
                    }else{
                        return null;
                    }
                }else{
                    return filterNodeWithTargetName(fromNode, targetTableName);
                }
            case AS:
                SqlNode childNode = ((SqlBasicCall)sqlNode).getOperands()[0];
                return filterNodeWithTargetName(childNode, targetTableName);
            case JOIN:
                SqlNode leftNode = ((SqlJoin)sqlNode).getLeft();
                SqlNode rightNode =  ((SqlJoin)sqlNode).getRight();
                SqlNode leftReturnNode = filterNodeWithTargetName(leftNode, targetTableName);
                SqlNode rightReturnNode = filterNodeWithTargetName(rightNode, targetTableName);

                if(leftReturnNode != null) {
                    return leftReturnNode;
                }else if(rightReturnNode != null){
                    return rightReturnNode;
                }else{
                    return null;
                }
            default:
                break;
        }

        return null;
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
            throw new RuntimeException("not register table " + tableName);
        }

        return table;
    }

    private List<SqlNode> replaceSelectStarFieldName(SqlNode selectNode, FieldReplaceInfo replaceInfo){
        SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;
        List<SqlNode> sqlNodes = Lists.newArrayList();
        if(sqlIdentifier.isStar()){//处理 [* or table.*]
            int identifierSize = sqlIdentifier.names.size();
            Collection<String> columns = null;
            if(identifierSize == 1){
                columns = replaceInfo.getMappingTable().values();
            }else{
                columns = replaceInfo.getMappingTable().row(sqlIdentifier.names.get(0)).values();
            }

            for(String colAlias : columns){
                SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
                List<String> columnInfo = Lists.newArrayList();
                columnInfo.add(replaceInfo.getTargetTableAlias());
                columnInfo.add(colAlias);
                SqlIdentifier sqlIdentifierAlias = new SqlIdentifier(columnInfo, sqlParserPos);
                sqlNodes.add(sqlIdentifierAlias);
            }

            return sqlNodes;
        }else{
            throw new RuntimeException("is not a star select field." + selectNode);
        }
    }

    private SqlNode replaceSelectFieldName(SqlNode selectNode, FieldReplaceInfo replaceInfo) {
        if (selectNode.getKind() == AS) {
            SqlNode leftNode = ((SqlBasicCall) selectNode).getOperands()[0];
            SqlNode replaceNode = replaceSelectFieldName(leftNode, replaceInfo);
            if (replaceNode != null) {
                ((SqlBasicCall) selectNode).getOperands()[0] = replaceNode;
            }

            return selectNode;
        }else if(selectNode.getKind() == IDENTIFIER){
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            if(sqlIdentifier.names.size() == 1){
                return selectNode;
            }

            //Same level mappingTable
            String mappingFieldName = replaceInfo.getTargetFieldName(sqlIdentifier.getComponent(0).getSimple(), sqlIdentifier.getComponent(1).getSimple());
            if (mappingFieldName == null) {
                throw new RuntimeException("can't find mapping fieldName:" + selectNode.toString() );
            }

            sqlIdentifier = sqlIdentifier.setName(0, replaceInfo.getTargetTableAlias());
            sqlIdentifier = sqlIdentifier.setName(1, mappingFieldName);
            return sqlIdentifier;
        }else if(selectNode.getKind() == LITERAL || selectNode.getKind() == LITERAL_CHAIN){//字面含义
            return selectNode;
        }else if(  AGGREGATE.contains(selectNode.getKind())
                || AVG_AGG_FUNCTIONS.contains(selectNode.getKind())
                || COMPARISON.contains(selectNode.getKind())
                || selectNode.getKind() == OTHER_FUNCTION
                || selectNode.getKind() == DIVIDE
                || selectNode.getKind() == CAST
                || selectNode.getKind() == TRIM
                || selectNode.getKind() == TIMES
                || selectNode.getKind() == PLUS
                || selectNode.getKind() == NOT_IN
                || selectNode.getKind() == OR
                || selectNode.getKind() == AND
                || selectNode.getKind() == MINUS
                || selectNode.getKind() == TUMBLE
                || selectNode.getKind() == TUMBLE_START
                || selectNode.getKind() == TUMBLE_END
                || selectNode.getKind() == SESSION
                || selectNode.getKind() == SESSION_START
                || selectNode.getKind() == SESSION_END
                || selectNode.getKind() == HOP
                || selectNode.getKind() == HOP_START
                || selectNode.getKind() == HOP_END
                || selectNode.getKind() == BETWEEN
                || selectNode.getKind() == IS_NULL
                || selectNode.getKind() == IS_NOT_NULL
                || selectNode.getKind() == CONTAINS
                || selectNode.getKind() == TIMESTAMP_ADD
                || selectNode.getKind() == TIMESTAMP_DIFF
                || selectNode.getKind() == LIKE

                ){
            SqlBasicCall sqlBasicCall = (SqlBasicCall) selectNode;
            for(int i=0; i<sqlBasicCall.getOperands().length; i++){
                SqlNode sqlNode = sqlBasicCall.getOperands()[i];
                if(sqlNode instanceof SqlLiteral){
                    continue;
                }

                if(sqlNode instanceof SqlDataTypeSpec){
                    continue;
                }

                SqlNode replaceNode = replaceSelectFieldName(sqlNode, replaceInfo);
                if(replaceNode == null){
                    continue;
                }

                sqlBasicCall.getOperands()[i] = replaceNode;
            }

            return selectNode;
        }else if(selectNode.getKind() == CASE){
            System.out.println("selectNode");
            SqlCase sqlCase = (SqlCase) selectNode;
            SqlNodeList whenOperands = sqlCase.getWhenOperands();
            SqlNodeList thenOperands = sqlCase.getThenOperands();
            SqlNode elseNode = sqlCase.getElseOperand();

            for(int i=0; i<whenOperands.size(); i++){
                SqlNode oneOperand = whenOperands.get(i);
                SqlNode replaceNode = replaceSelectFieldName(oneOperand, replaceInfo);
                if (replaceNode != null) {
                    whenOperands.set(i, replaceNode);
                }
            }

            for(int i=0; i<thenOperands.size(); i++){
                SqlNode oneOperand = thenOperands.get(i);
                SqlNode replaceNode = replaceSelectFieldName(oneOperand, replaceInfo);
                if (replaceNode != null) {
                    thenOperands.set(i, replaceNode);
                }
            }

            ((SqlCase) selectNode).setOperand(3, replaceSelectFieldName(elseNode, replaceInfo));
            return selectNode;
        }else if(selectNode.getKind() == OTHER){
            //不处理
            return selectNode;
        }else{
            throw new RuntimeException(String.format("not support node kind of %s to replace name now.", selectNode.getKind()));
        }
    }

    /**
     * Analyzing conditions are very join the dimension tables include all equivalent conditions (i.e., dimension table is the primary key definition
     *
     * @return
     */
    private boolean checkJoinCondition(SqlNode conditionNode, String sideTableAlias, AbstractSideTableInfo sideTableInfo) {
        List<String> conditionFields = getConditionFields(conditionNode, sideTableAlias, sideTableInfo);
        if(CollectionUtils.isEqualCollection(conditionFields, convertPrimaryAlias(sideTableInfo))){
            return true;
        }
        return false;
    }

    private List<String> convertPrimaryAlias(AbstractSideTableInfo sideTableInfo) {
        List<String> res = Lists.newArrayList();
        sideTableInfo.getPrimaryKeys().forEach(field -> {
            res.add(sideTableInfo.getPhysicalFields().getOrDefault(field, field));
        });
        return res;
    }

    public List<String> getConditionFields(SqlNode conditionNode, String specifyTableName, AbstractSideTableInfo sideTableInfo){
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
                                     Map<String, Table> tableCache,
                                     List<FieldReplaceInfo> replaceInfoList) throws SqlParseException {

        AliasInfo aliasInfo = parseAsNode(pollSqlNode);
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
        for(FieldReplaceInfo tmp : replaceInfoList){
            if(fromTableNameSet.contains(tmp.getTargetTableName())
                    || fromTableNameSet.contains(tmp.getTargetTableAlias())){
                fieldReplaceInfo.setPreNode(tmp);
                break;
            }
        }
        replaceInfoList.add(fieldReplaceInfo);
    }

    private void joinFun(Object pollObj, Map<String, Table> localTableCache,
                         Map<String, AbstractSideTableInfo> sideTableMap, StreamTableEnvironment tableEnv,
                         List<FieldReplaceInfo> replaceInfoList) throws Exception{
        JoinInfo joinInfo = (JoinInfo) pollObj;

        JoinScope joinScope = new JoinScope();
        JoinScope.ScopeChild leftScopeChild = new JoinScope.ScopeChild();
        leftScopeChild.setAlias(joinInfo.getLeftTableAlias());
        leftScopeChild.setTableName(joinInfo.getLeftTableName());

        SqlKind sqlKind = joinInfo.getLeftNode().getKind();
        if(sqlKind == AS){
            dealAsSourceTable(tableEnv, joinInfo.getLeftNode(), localTableCache, replaceInfoList);
        }

        Table leftTable = getTableFromCache(localTableCache, joinInfo.getLeftTableAlias(), joinInfo.getLeftTableName());
        RowTypeInfo leftTypeInfo = new RowTypeInfo(leftTable.getSchema().getTypes(), leftTable.getSchema().getColumnNames());
        leftScopeChild.setRowTypeInfo(leftTypeInfo);

        JoinScope.ScopeChild rightScopeChild = new JoinScope.ScopeChild();
        rightScopeChild.setAlias(joinInfo.getRightTableAlias());
        rightScopeChild.setTableName(joinInfo.getRightTableName());
        AbstractSideTableInfo sideTableInfo = sideTableMap.get(joinInfo.getRightTableName());
        if(sideTableInfo == null){
            sideTableInfo = sideTableMap.get(joinInfo.getRightTableAlias());
        }

        if(sideTableInfo == null){
            throw new RuntimeException("can't not find side table:" + joinInfo.getRightTableName());
        }

//        if(!checkJoinCondition(joinInfo.getCondition(), joinInfo.getRightTableAlias(), sideTableInfo)){
//            throw new RuntimeException("ON condition must contain all equal fields!!!");
//        }

        rightScopeChild.setRowTypeInfo(sideTableInfo.getRowTypeInfo());

        joinScope.addScope(leftScopeChild);
        joinScope.addScope(rightScopeChild);

        //获取两个表的所有字段
        List<FieldInfo> sideJoinFieldInfo = ParserJoinField.getRowTypeInfo(joinInfo.getSelectNode(), joinScope, true);

        String leftTableAlias = joinInfo.getLeftTableAlias();
        Table targetTable = localTableCache.get(leftTableAlias);
        if(targetTable == null){
            targetTable = localTableCache.get(joinInfo.getLeftTableName());
        }

        RowTypeInfo typeInfo = new RowTypeInfo(targetTable.getSchema().getTypes(), targetTable.getSchema().getColumnNames());

        DataStream<CRow> adaptStream = tableEnv.toRetractStream(targetTable, org.apache.flink.types.Row.class)
                .map((Tuple2<Boolean, Row> tp2) -> {
                    return new CRow(tp2.f1, tp2.f0);
                }).returns(CRow.class);


        //join side table before keyby ===> Reducing the size of each dimension table cache of async
        if (sideTableInfo.isPartitionedJoin()) {
            List<String> leftJoinColList = getConditionFields(joinInfo.getCondition(), joinInfo.getLeftTableAlias(), sideTableInfo);
            List<String> fieldNames = Arrays.asList(targetTable.getSchema().getFieldNames());
            int[] keyIndex = leftJoinColList.stream().mapToInt(fieldNames::indexOf).toArray();
            adaptStream = adaptStream.keyBy(new CRowKeySelector(keyIndex, projectedTypeInfo(keyIndex, targetTable.getSchema())));
        }

        DataStream<CRow> dsOut = null;
        if(ECacheType.ALL.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            dsOut = SideWithAllCacheOperator.getSideJoinDataStream(adaptStream, sideTableInfo.getType(), localSqlPluginPath, typeInfo, joinInfo, sideJoinFieldInfo, sideTableInfo);
        }else{
            dsOut = SideAsyncOperator.getSideJoinDataStream(adaptStream, sideTableInfo.getType(), localSqlPluginPath, typeInfo, joinInfo, sideJoinFieldInfo, sideTableInfo);
        }

        // TODO  将嵌套表中的字段传递过去, 去除冗余的ROWtime
        HashBasedTable<String, String, String> mappingTable = HashBasedTable.create();
        RowTypeInfo sideOutTypeInfo = buildOutRowTypeInfo(sideJoinFieldInfo, mappingTable);

        CRowTypeInfo cRowTypeInfo = new CRowTypeInfo(sideOutTypeInfo);
        dsOut.getTransformation().setOutputType(cRowTypeInfo);

        String targetTableName = joinInfo.getNewTableName();
        String targetTableAlias = joinInfo.getNewTableAlias();

        FieldReplaceInfo replaceInfo = new FieldReplaceInfo();
        replaceInfo.setMappingTable(mappingTable);
        replaceInfo.setTargetTableName(targetTableName);
        replaceInfo.setTargetTableAlias(targetTableAlias);

        //判断之前是不是被替换过,被替换过则设置之前的替换信息作为上一个节点
        for(FieldReplaceInfo tmp : replaceInfoList){
            if(tmp.getTargetTableName().equalsIgnoreCase(joinInfo.getLeftTableName())
            ||tmp.getTargetTableName().equalsIgnoreCase(joinInfo.getLeftTableAlias())){
                replaceInfo.setPreNode(tmp);
                break;
            }
        }

        replaceInfoList.add(replaceInfo);

        if (!tableEnv.isRegistered(joinInfo.getNewTableName())){
            Table joinTable = tableEnv.fromDataStream(dsOut);
            tableEnv.registerTable(joinInfo.getNewTableName(), joinTable);
            localTableCache.put(joinInfo.getNewTableName(), joinTable);
        }
    }

    private TypeInformation<Row> projectedTypeInfo(int[] fields, TableSchema schema) {
        String[] fieldNames = schema.getFieldNames();
        TypeInformation<?>[] fieldTypes = schema.getFieldTypes();

        String[] projectedNames = Arrays.stream(fields).mapToObj(i -> fieldNames[i]).toArray(String[]::new);
        TypeInformation[] projectedTypes = Arrays.stream(fields).mapToObj(i -> fieldTypes[i]).toArray(TypeInformation[]::new);
        return new RowTypeInfo(projectedTypes, projectedNames);
    }


    private boolean checkFieldsInfo(CreateTmpTableParser.SqlParserResult result, Table table) {
        List<String> fieldNames = new LinkedList<>();
        String fieldsInfo = result.getFieldsInfoStr();
        String[] fields = StringUtils.split(fieldsInfo, ",");
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
