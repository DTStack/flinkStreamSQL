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

import com.dtstack.flink.sql.side.cache.AbsSideCache;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/9/18
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class SideInfo implements Serializable{

    protected RowTypeInfo rowTypeInfo;

    protected List<FieldInfo> outFieldInfoList;

    protected List<String> equalFieldList = Lists.newArrayList();

    protected List<Integer> equalValIndex = Lists.newArrayList();

    protected String sqlCondition = "";

    protected String sideSelectFields = "";

    protected JoinType joinType;

    //key:Returns the value of the position, value: the ref field index​in the input table
    protected Map<Integer, Integer> inFieldIndex = Maps.newHashMap();

    //key:Returns the value of the position, value:  the ref field index​in the side table
    protected Map<Integer, Integer> sideFieldIndex = Maps.newHashMap();

    //key:Returns the value of the position, value:  the ref field name​in the side table
    protected Map<Integer, String> sideFieldNameIndex = Maps.newHashMap();

    protected SideTableInfo sideTableInfo;

    protected AbsSideCache sideCache;

    public SideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList,
                    SideTableInfo sideTableInfo){
        this.rowTypeInfo = rowTypeInfo;
        this.outFieldInfoList = outFieldInfoList;
        this.joinType = joinInfo.getJoinType();
        this.sideTableInfo = sideTableInfo;
        parseSelectFields(joinInfo);
        buildEqualInfo(joinInfo, sideTableInfo);
    }

    public void parseSelectFields(JoinInfo joinInfo){
        String sideTableName = joinInfo.getSideTableName();
        String nonSideTableName = joinInfo.getNonSideTable();
        List<String> fields = Lists.newArrayList();

        int sideIndex = 0;
        for( int i=0; i<outFieldInfoList.size(); i++){
            FieldInfo fieldInfo = outFieldInfoList.get(i);
            if(fieldInfo.getTable().equalsIgnoreCase(sideTableName)){
                fields.add(fieldInfo.getFieldName());
                sideFieldIndex.put(i, sideIndex);
                sideFieldNameIndex.put(i, fieldInfo.getFieldName());
                sideIndex++;
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

    public void dealOneEqualCon(SqlNode sqlNode, String sideTableName){
        if(sqlNode.getKind() != SqlKind.EQUALS){
            throw new RuntimeException("not equal operator.");
        }

        SqlIdentifier left = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[0];
        SqlIdentifier right = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[1];

        String leftTableName = left.getComponent(0).getSimple();
        String leftField = left.getComponent(1).getSimple();

        String rightTableName = right.getComponent(0).getSimple();
        String rightField = right.getComponent(1).getSimple();

        if(leftTableName.equalsIgnoreCase(sideTableName)){
            equalFieldList.add(leftField);
            int equalFieldIndex = -1;
            for(int i=0; i<rowTypeInfo.getFieldNames().length; i++){
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if(fieldName.equalsIgnoreCase(rightField)){
                    equalFieldIndex = i;
                }
            }
            if(equalFieldIndex == -1){
                throw new RuntimeException("can't find equal field " + rightField);
            }

            equalValIndex.add(equalFieldIndex);

        }else if(rightTableName.equalsIgnoreCase(sideTableName)){

            equalFieldList.add(rightField);
            int equalFieldIndex = -1;
            for(int i=0; i<rowTypeInfo.getFieldNames().length; i++){
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if(fieldName.equalsIgnoreCase(leftField)){
                    equalFieldIndex = i;
                }
            }
            if(equalFieldIndex == -1){
                throw new RuntimeException("can't find equal field " + rightField);
            }

            equalValIndex.add(equalFieldIndex);

        }else{
            throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString());
        }
    }

    public abstract void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo);

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

    public SideTableInfo getSideTableInfo() {
        return sideTableInfo;
    }

    public void setSideTableInfo(SideTableInfo sideTableInfo) {
        this.sideTableInfo = sideTableInfo;
    }

    public AbsSideCache getSideCache() {
        return sideCache;
    }

    public void setSideCache(AbsSideCache sideCache) {
        this.sideCache = sideCache;
    }

    public Map<Integer, String> getSideFieldNameIndex() {
        return sideFieldNameIndex;
    }

    public void setSideFieldNameIndex(Map<Integer, String> sideFieldNameIndex) {
        this.sideFieldNameIndex = sideFieldNameIndex;
    }
}
