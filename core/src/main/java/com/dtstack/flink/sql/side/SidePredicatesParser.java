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

import com.dtstack.flink.sql.config.CalciteConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.SqlKind.*;

/**
 *
 *  将同级谓词下推到维表
 * Date: 2019/12/11
 * Company: www.dtstack.com
 * @author maqi
 */
public class SidePredicatesParser {
    public void fillPredicatesForSideTable(String exeSql, Map<String, SideTableInfo> sideTableMap) throws SqlParseException {
        SqlParser sqlParser = SqlParser.create(exeSql, CalciteConfig.MYSQL_LEX_CONFIG);
        SqlNode sqlNode = sqlParser.parseStmt();
        parseSql(sqlNode, sideTableMap, Maps.newHashMap());
    }

    private void parseSql(SqlNode sqlNode, Map<String, SideTableInfo> sideTableMap, Map<String, String> tabMapping) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case INSERT:
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                parseSql(sqlSource, sideTableMap, tabMapping);
                break;
            case SELECT:
                SqlNode fromNode = ((SqlSelect) sqlNode).getFrom();
                SqlNode whereNode = ((SqlSelect) sqlNode).getWhere();

                if (fromNode.getKind() != IDENTIFIER) {
                    // 子查询或者AS
                    parseSql(fromNode, sideTableMap, tabMapping);
                }

                if (null != whereNode && whereNode.getKind() != OR) {
                    List<PredicateInfo> predicateInfos = Lists.newArrayList();
                    extractPredicateInfo(whereNode, predicateInfos);
                    // tabMapping:  <m,MyTable>,  <s.sideTable>
                    System.out.println(predicateInfos);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin) sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin) sqlNode).getRight();
                parseSql(leftNode, sideTableMap, tabMapping);
                parseSql(rightNode, sideTableMap, tabMapping);
                break;
            case AS:
                SqlNode info = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];
                if (info.getKind() == IDENTIFIER) {
                    tabMapping.put(alias.toString(), info.toString());
                } else {
                    // 为子查询创建一个同级map
                    parseSql(info, sideTableMap, Maps.newHashMap());
                }
                break;

            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];
                parseSql(unionLeft, sideTableMap, tabMapping);
                parseSql(unionRight, sideTableMap, tabMapping);
                break;
        }
    }



    private void extractPredicateInfo(SqlNode whereNode, List<PredicateInfo> predicatesInfo) {
        SqlKind sqlKind = whereNode.getKind();
        if (sqlKind == SqlKind.AND && ((SqlBasicCall) whereNode).getOperandList().size() == 2) {
            extractPredicateInfo(((SqlBasicCall) whereNode).getOperands()[0], predicatesInfo);
            extractPredicateInfo(((SqlBasicCall) whereNode).getOperands()[1], predicatesInfo);
        } else {
            SqlOperator operator = ((SqlBasicCall) whereNode).getOperator();
            String operatorName = operator.getName();
            SqlKind operatorKind = operator.getKind();

            if (operatorKind == SqlKind.BETWEEN) {
                SqlIdentifier fieldFullPath = (SqlIdentifier) ((SqlBasicCall) whereNode).getOperands()[0];
                if (fieldFullPath.names.size() == 2) {
                    String ownerTable = fieldFullPath.names.get(0);
                    String fieldName = fieldFullPath.names.get(1);
                    String content = ((SqlBasicCall) whereNode).getOperands()[1].toString() + " and " + ((SqlBasicCall) whereNode).getOperands()[2].toString();
                    PredicateInfo predicateInfo = PredicateInfo.builder().setOperatorName(operatorName).setOperatorKind(operatorKind.toString())
                            .setOwnerTable(ownerTable).setFieldName(fieldName).setCondition(content).build();
                    predicatesInfo.add(predicateInfo);
                }
            } else {
                SqlIdentifier fieldFullPath = (SqlIdentifier) ((SqlBasicCall) whereNode).getOperands()[0];
                // not table name not deal
                if (fieldFullPath.names.size() == 2) {
                    String ownerTable = fieldFullPath.names.get(0);
                    String fieldName = fieldFullPath.names.get(1);
                    String content = ((SqlBasicCall) whereNode).getOperands()[1].toString();
                    PredicateInfo predicateInfo = PredicateInfo.builder().setOperatorName(operatorName).setOperatorKind(operatorKind.toString())
                            .setOwnerTable(ownerTable).setFieldName(fieldName).setCondition(content).build();
                    predicatesInfo.add(predicateInfo);
                }
            }

        }
    }


}
