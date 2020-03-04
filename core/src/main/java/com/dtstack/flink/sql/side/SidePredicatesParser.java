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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.*;

/**
 *
 *  将同级谓词信息填充到维表
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

    /**
     *  将谓词信息填充到维表属性
     * @param sqlNode
     * @param sideTableMap
     * @param tabMapping  谓词属性中别名对应的真实维表名称
     */
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
                    parseSql(fromNode, sideTableMap, tabMapping);
                }
                //  带or的不解析
                if (null != whereNode && whereNode.getKind() != OR) {
                    List<PredicateInfo> predicateInfoList = Lists.newArrayList();
                    extractPredicateInfo(whereNode, predicateInfoList);
                    fillToSideTableInfo(sideTableMap, tabMapping, predicateInfoList);
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
            default:
                break;
        }
    }

    private void fillToSideTableInfo(Map<String, SideTableInfo> sideTableMap, Map<String, String> tabMapping, List<PredicateInfo> predicateInfoList) {
        predicateInfoList.stream().filter(info -> sideTableMap.containsKey(tabMapping.getOrDefault(info.getOwnerTable(), info.getOwnerTable())))
                .map(info -> sideTableMap.get(tabMapping.getOrDefault(info.getOwnerTable(), info.getOwnerTable())).getPredicateInfoes().add(info))
                .count();
    }


    private void extractPredicateInfo(SqlNode whereNode, List<PredicateInfo> predicatesInfoList) {
        SqlKind sqlKind = whereNode.getKind();
        if (sqlKind == SqlKind.AND && ((SqlBasicCall) whereNode).getOperandList().size() == 2) {
            extractPredicateInfo(((SqlBasicCall) whereNode).getOperands()[0], predicatesInfoList);
            extractPredicateInfo(((SqlBasicCall) whereNode).getOperands()[1], predicatesInfoList);
        } else {
            SqlOperator operator = ((SqlBasicCall) whereNode).getOperator();
            String operatorName = operator.getName();
            SqlKind operatorKind = operator.getKind();

            if (operatorKind == SqlKind.IS_NOT_NULL || operatorKind == SqlKind.IS_NULL) {
                fillPredicateInfoToList((SqlBasicCall) whereNode, predicatesInfoList, operatorName, operatorKind, 0, 0);
                return;
            }

            // 跳过函数
            if ((((SqlBasicCall) whereNode).getOperands()[0] instanceof SqlIdentifier)
                    && (((SqlBasicCall) whereNode).getOperands()[1].getKind() != SqlKind.OTHER_FUNCTION)) {
                fillPredicateInfoToList((SqlBasicCall) whereNode, predicatesInfoList, operatorName, operatorKind, 0, 1);
            } else if ((((SqlBasicCall) whereNode).getOperands()[1] instanceof SqlIdentifier)
                    && (((SqlBasicCall) whereNode).getOperands()[0].getKind() != SqlKind.OTHER_FUNCTION)) {
                fillPredicateInfoToList((SqlBasicCall) whereNode, predicatesInfoList, operatorName, operatorKind, 1, 0);
            }
        }
    }

    private void fillPredicateInfoToList(SqlBasicCall whereNode, List<PredicateInfo> predicatesInfoList, String operatorName, SqlKind operatorKind,
                                         int fieldIndex, int conditionIndex) {
        SqlIdentifier fieldFullPath = (SqlIdentifier) whereNode.getOperands()[fieldIndex];
        if (fieldFullPath.names.size() == 2) {
            String ownerTable = fieldFullPath.names.get(0);
            String fieldName = fieldFullPath.names.get(1);
            String content = (operatorKind == SqlKind.BETWEEN) ? whereNode.getOperands()[conditionIndex].toString() + " AND " +
                    whereNode.getOperands()[2].toString() : whereNode.getOperands()[conditionIndex].toString();

            PredicateInfo predicateInfo = PredicateInfo.builder().setOperatorName(operatorName).setOperatorKind(operatorKind.toString())
                    .setOwnerTable(ownerTable).setFieldName(fieldName).setCondition(content).build();
            predicatesInfoList.add(predicateInfo);
        }
    }

}
