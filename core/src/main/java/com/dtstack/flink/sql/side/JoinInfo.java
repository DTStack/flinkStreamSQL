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

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlNode;
import com.google.common.base.Strings;

import java.io.Serializable;
import java.util.Map;

/**
 * Join信息
 * Date: 2018/7/24
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class JoinInfo implements Serializable {

    private static final long serialVersionUID = -1L;

    //左表是否是维表
    private boolean leftIsSideTable;

    private boolean leftIsTmpTable = false;

    //右表是否是维表
    private boolean rightIsSideTable;

    private String leftTableName;

    private String leftTableAlias;

    private String rightTableName;

    private String rightTableAlias;

    private SqlNode leftNode;

    private SqlNode rightNode;

    private SqlNode condition;

    private SqlNode selectFields;

    private SqlNode selectNode;

    private JoinType joinType;

    public String getSideTableName(){
        if(leftIsSideTable){
            return leftTableAlias;
        }

        return rightTableAlias;
    }

    public String getNonSideTable(){
        if(leftIsSideTable){
            return rightTableAlias;
        }

        return leftTableAlias;
    }

    public String getNewTableName(){
        //兼容左边表是as 的情况
        String leftStr = leftTableName;
        leftStr = Strings.isNullOrEmpty(leftStr) ? leftTableAlias : leftStr;
        return leftStr + "_" + rightTableName;
    }


    public String getNewTableAlias(){
        return leftTableAlias + "_" + rightTableAlias;
    }

    public boolean isLeftIsSideTable() {
        return leftIsSideTable;
    }

    public void setLeftIsSideTable(boolean leftIsSideTable) {
        this.leftIsSideTable = leftIsSideTable;
    }

    public boolean isRightIsSideTable() {
        return rightIsSideTable;
    }

    public void setRightIsSideTable(boolean rightIsSideTable) {
        this.rightIsSideTable = rightIsSideTable;
    }

    public String getLeftTableName() {
        return leftTableName;
    }

    public void setLeftTableName(String leftTableName) {
        this.leftTableName = leftTableName;
    }

    public String getRightTableName() {
        return rightTableName;
    }

    public void setRightTableName(String rightTableName) {
        this.rightTableName = rightTableName;
    }

    public SqlNode getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(SqlNode leftNode) {
        this.leftNode = leftNode;
    }

    public SqlNode getRightNode() {
        return rightNode;
    }

    public void setRightNode(SqlNode rightNode) {
        this.rightNode = rightNode;
    }

    public SqlNode getCondition() {
        return condition;
    }

    public void setCondition(SqlNode condition) {
        this.condition = condition;
    }

    public SqlNode getSelectFields() {
        return selectFields;
    }

    public void setSelectFields(SqlNode selectFields) {
        this.selectFields = selectFields;
    }

    public boolean checkIsSide(){
        return isLeftIsSideTable() || isRightIsSideTable();
    }

    public String getLeftTableAlias() {
        return leftTableAlias;
    }

    public void setLeftTableAlias(String leftTableAlias) {
        this.leftTableAlias = leftTableAlias;
    }

    public String getRightTableAlias() {
        return rightTableAlias;
    }

    public void setRightTableAlias(String rightTableAlias) {
        this.rightTableAlias = rightTableAlias;
    }

    public SqlNode getSelectNode() {
        return selectNode;
    }

    public void setSelectNode(SqlNode selectNode) {
        this.selectNode = selectNode;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public boolean isLeftIsTmpTable() {
        return leftIsTmpTable;
    }

    public void setLeftIsTmpTable(boolean leftIsTmpTable) {
        this.leftIsTmpTable = leftIsTmpTable;
    }
}
