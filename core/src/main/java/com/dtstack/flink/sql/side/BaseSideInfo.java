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

import com.dtstack.flink.sql.side.cache.AbstractSideCache;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.*;
import org.apache.calcite.util.NlsString;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/9/18
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class BaseSideInfo implements Serializable{

    protected static final String QUOTE = "'";
    protected static final String ESCAPEQUOTE = "''";

    protected RowTypeInfo rowTypeInfo;

    protected List<FieldInfo> outFieldInfoList;

    protected List<String> equalFieldList = Lists.newArrayList();

    protected List<Integer> equalValIndex = Lists.newArrayList();

    protected String sqlCondition = "";

    protected String sideSelectFields = "";

    protected Map<Integer, String> sideSelectFieldsType = Maps.newHashMap();

    protected JoinType joinType;

    //key:Returns the value of the position, value: the ref field index​in the input table
    protected Map<Integer, Integer> inFieldIndex = Maps.newHashMap();

    //key:Returns the value of the position, value:  the ref field index​in the side table
    protected Map<Integer, Integer> sideFieldIndex = Maps.newHashMap();

    //key:Returns the value of the position, value:  the ref field name​in the side table
    protected Map<Integer, String> sideFieldNameIndex = Maps.newHashMap();

    protected AbstractSideTableInfo sideTableInfo;

    protected AbstractSideCache sideCache;

    protected JoinInfo joinInfo;

    public BaseSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList,
                        AbstractSideTableInfo sideTableInfo){
        this.rowTypeInfo = rowTypeInfo;
        this.outFieldInfoList = outFieldInfoList;
        this.joinType = joinInfo.getJoinType();
        this.sideTableInfo = sideTableInfo;
        this.joinInfo = joinInfo;
        parseSelectFields(joinInfo);
        buildEqualInfo(joinInfo, sideTableInfo);
    }

    public void parseSelectFields(JoinInfo joinInfo){
        String sideTableName = joinInfo.getSideTableName();
        String nonSideTableName = joinInfo.getNonSideTable();
        List<String> fields = Lists.newArrayList();
        int sideTableFieldIndex = 0;

        for( int i=0; i<outFieldInfoList.size(); i++){
            FieldInfo fieldInfo = outFieldInfoList.get(i);
            if(fieldInfo.getTable().equalsIgnoreCase(sideTableName)){
                String sideFieldName = sideTableInfo.getPhysicalFields().getOrDefault(fieldInfo.getFieldName(), fieldInfo.getFieldName());
                fields.add(sideFieldName);
                sideSelectFieldsType.put(sideTableFieldIndex, getTargetFieldType(fieldInfo.getFieldName()));
                sideFieldIndex.put(i, sideTableFieldIndex);
                sideFieldNameIndex.put(i, sideFieldName);
                sideTableFieldIndex++;
            }else if(fieldInfo.getTable().equalsIgnoreCase(nonSideTableName)){
                int nonSideIndex = rowTypeInfo.getFieldIndex(fieldInfo.getFieldName());
                inFieldIndex.put(i, nonSideIndex);
            }else{
                throw new RuntimeException("unknown table " + fieldInfo.getTable());
            }
        }

        if(fields.size() == 0){
            throw new RuntimeException("select non field from table " +  sideTableName);
        }

        sideSelectFields = String.join(",", fields);
    }

    public String getTargetFieldType(String fieldName){
        int fieldIndex = sideTableInfo.getFieldList().indexOf(fieldName);
        if(fieldIndex == -1){
            throw new RuntimeException(sideTableInfo.getName() + " can't find field: " + fieldName);
        }

        return sideTableInfo.getFieldTypes()[fieldIndex];
    }

    public void dealOneEqualCon(SqlNode sqlNode, String sideTableName) {
        if (!SqlKind.COMPARISON.contains(sqlNode.getKind())) {
            throw new RuntimeException("not compare operator.");
        }

        SqlNode leftNode = ((SqlBasicCall) sqlNode).getOperands()[0];
        SqlNode rightNode = ((SqlBasicCall) sqlNode).getOperands()[1];
        if (leftNode.getKind() == SqlKind.LITERAL) {
            evalConstantEquation(
                (SqlLiteral) leftNode,
                (SqlIdentifier) rightNode,
                sqlNode.getKind()
            );
        } else if (rightNode.getKind() == SqlKind.LITERAL) {
            evalConstantEquation(
                (SqlLiteral) rightNode,
                (SqlIdentifier) leftNode,
                sqlNode.getKind()
            );
        } else {
            SqlIdentifier left = (SqlIdentifier) leftNode;
            SqlIdentifier right = (SqlIdentifier) rightNode;
            evalEquation(left, right, sideTableName, sqlNode);
        }
    }

    /**
     * deal normal equation etc. foo.id = bar.id
     * @param left
     * @param right
     * @param sideTableName
     * @param sqlNode
     */
    private void evalEquation(SqlIdentifier left, SqlIdentifier right, String sideTableName, SqlNode sqlNode) {
        String leftTableName = left.getComponent(0).getSimple();
        String leftField = left.getComponent(1).getSimple();

        String rightTableName = right.getComponent(0).getSimple();
        String rightField = right.getComponent(1).getSimple();

        if (leftTableName.equalsIgnoreCase(sideTableName)) {
            associateField(rightField, leftField, sqlNode);
        } else if (rightTableName.equalsIgnoreCase(sideTableName)) {
            associateField(leftField, rightField, sqlNode);
        } else {
            throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString());
        }
    }

    /**
     * deal with equation with constant etc. foo.id = 1
     * @param literal
     * @param identifier
     */
    private void evalConstantEquation(SqlLiteral literal, SqlIdentifier identifier, SqlKind sqlKind) {
        String tableName = identifier.getComponent(0).getSimple();
        checkSupport(identifier);
        String fieldName = identifier.getComponent(1).getSimple();
        Object constant = literal.getValue();
        String condition;
        if(constant instanceof NlsString){
            condition = QUOTE + ((NlsString) constant).getValue().replace(QUOTE,ESCAPEQUOTE)+ QUOTE;
        }else {
            condition = constant.toString();
        }
        PredicateInfo predicate = PredicateInfo.builder()
            .setOperatorName(sqlKind.sql)
            .setOperatorKind(sqlKind.name())
            .setOwnerTable(tableName)
            .setFieldName(fieldName)
            .setCondition(condition)
            .build();
        sideTableInfo.addPredicateInfo(predicate);
        sideTableInfo.addFullPredicateInfoes(predicate);
    }

    private void checkSupport(SqlIdentifier identifier) {
        String tableName = identifier.getComponent(0).getSimple();
        String sideTableName;
        String sideTableAlias;
        if (joinInfo.isLeftIsSideTable()) {
            sideTableName = joinInfo.getLeftTableName();
            sideTableAlias = joinInfo.getLeftTableAlias();
        } else {
            sideTableName = joinInfo.getRightTableName();
            sideTableAlias = joinInfo.getRightTableAlias();
        }
        boolean isSide = tableName.equals(sideTableName) || tableName.equals(sideTableAlias);
        String errorMsg = "only support set side table constant field, error field " + identifier;
        Preconditions.checkState(isSide, errorMsg);
    }

    private void checkSupport(SqlIdentifier identifier) {
        String tableName = identifier.getComponent(0).getSimple();
        String sideTableName;
        String sideTableAlias;
        if (joinInfo.isLeftIsSideTable()) {
            sideTableName = joinInfo.getLeftTableName();
            sideTableAlias = joinInfo.getLeftTableAlias();
        } else {
            sideTableName = joinInfo.getRightTableName();
            sideTableAlias = joinInfo.getRightTableAlias();
        }
        boolean isSide = tableName.equals(sideTableName) || tableName.equals(sideTableAlias);
        String errorMsg = "only support set side table constant field, error field " + identifier;
        Preconditions.checkState(isSide, errorMsg);
    }

    private void associateField(String sourceTableField, String sideTableField, SqlNode sqlNode) {
        String errorMsg = "can't deal equal field: " + sqlNode;
        equalFieldList.add(sideTableField);
        int equalFieldIndex = -1;
        for (int i = 0; i < rowTypeInfo.getFieldNames().length; i++) {
            String fieldName = rowTypeInfo.getFieldNames()[i];
            if (fieldName.equalsIgnoreCase(sourceTableField)) {
                equalFieldIndex = i;
            }
        }
        Preconditions.checkState(equalFieldIndex != -1, errorMsg);
        equalValIndex.add(equalFieldIndex);
    }

    public abstract void buildEqualInfo(JoinInfo joinInfo, AbstractSideTableInfo sideTableInfo);

    public RowTypeInfo getRowTypeInfo() {
        return rowTypeInfo;
    }

    public void setRowTypeInfo(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    public List<FieldInfo> getOutFieldInfoList() {
        return outFieldInfoList;
    }

    public void setOutFieldInfoList(List<FieldInfo> outFieldInfoList) {
        this.outFieldInfoList = outFieldInfoList;
    }

    public List<String> getEqualFieldList() {
        return equalFieldList;
    }

    public void setEqualFieldList(List<String> equalFieldList) {
        this.equalFieldList = equalFieldList;
    }

    public List<Integer> getEqualValIndex() {
        return equalValIndex;
    }

    public void setEqualValIndex(List<Integer> equalValIndex) {
        this.equalValIndex = equalValIndex;
    }

    public String getSqlCondition() {
        return sqlCondition;
    }

    public void setSqlCondition(String sqlCondition) {
        this.sqlCondition = sqlCondition;
    }

    public String getSideSelectFields() {
        return sideSelectFields;
    }

    public void setSideSelectFields(String sideSelectFields) {
        this.sideSelectFields = sideSelectFields;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public Map<Integer, Integer> getInFieldIndex() {
        return inFieldIndex;
    }

    public void setInFieldIndex(Map<Integer, Integer> inFieldIndex) {
        this.inFieldIndex = inFieldIndex;
    }

    public Map<Integer, Integer> getSideFieldIndex() {
        return sideFieldIndex;
    }

    public void setSideFieldIndex(Map<Integer, Integer> sideFieldIndex) {
        this.sideFieldIndex = sideFieldIndex;
    }

    public AbstractSideTableInfo getSideTableInfo() {
        return sideTableInfo;
    }

    public void setSideTableInfo(AbstractSideTableInfo sideTableInfo) {
        this.sideTableInfo = sideTableInfo;
    }

    public AbstractSideCache getSideCache() {
        return sideCache;
    }

    public void setSideCache(AbstractSideCache sideCache) {
        this.sideCache = sideCache;
    }

    public Map<Integer, String> getSideFieldNameIndex() {
        return sideFieldNameIndex;
    }

    public void setSideFieldNameIndex(Map<Integer, String> sideFieldNameIndex) {
        this.sideFieldNameIndex = sideFieldNameIndex;
    }

    public Map<Integer, String> getSideSelectFieldsType() {
        return sideSelectFieldsType;
    }

    public void setSideSelectFieldsType(Map<Integer, String> sideSelectFieldsType) {
        this.sideSelectFieldsType = sideSelectFieldsType;
    }

    public String getSelectSideFieldType(int index){
        return sideSelectFieldsType.get(index);
    }

    public String[] getFieldNames(){

        int fieldTypeLength = rowTypeInfo.getFieldTypes().length;
        if( fieldTypeLength == 2
                && rowTypeInfo.getFieldTypes()[1].getClass().equals(BaseRowTypeInfo.class)){
            return ((BaseRowTypeInfo)rowTypeInfo.getFieldTypes()[1]).getFieldNames();
        } else if(fieldTypeLength ==1
                && rowTypeInfo.getFieldTypes()[0].getClass().equals(BaseRowTypeInfo.class)){
            return  ((BaseRowTypeInfo)rowTypeInfo.getFieldTypes()[0]).getFieldNames();
        }else {
            return  rowTypeInfo.getFieldNames();
        }
    }
}
