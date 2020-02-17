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

package com.dtstack.flink.sql.side.elasticsearch6;

import org.apache.flink.api.java.typeutils.RowTypeInfo;

import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Arrays;
import java.util.List;

/**
 * @author yinxi
 * @date 2020/2/13 - 13:09
 */
public class Elasticsearch6AsyncSideInfo extends SideInfo {


    public Elasticsearch6AsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        Elasticsearch6SideTableInfo elasticsearch6SideTableInfo = (Elasticsearch6SideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();
        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        List<String> sqlJoinCompareOperate= Lists.newArrayList();

        ParseUtils.parseAnd(conditionNode, sqlNodeList);
        ParseUtils.parseJoinCompareOperate(conditionNode, sqlJoinCompareOperate);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }

        // set query condition
        elasticsearch6SideTableInfo.setSearchSourceBuilder(getSelectFromStatement(sideTableInfo.getPredicateInfoes()));

    }

    private SearchSourceBuilder getSelectFromStatement(List<PredicateInfo> predicateInfoes) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        if (predicateInfoes.size() != 0) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            for (PredicateInfo info : sideTableInfo.getPredicateInfoes()) {
                boolQueryBuilder = buildFilterCondition(boolQueryBuilder, info);
            }

            searchSourceBuilder.query(boolQueryBuilder);
        }

        return searchSourceBuilder;
    }

    public BoolQueryBuilder buildFilterCondition(BoolQueryBuilder boolQueryBuilder, PredicateInfo info){
        switch (info.getOperatorKind()) {
            case "GREATER_THAN_OR_EQUAL":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gte(info.getCondition()));
            case "GREATER_THAN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gt(info.getCondition()));
            case "LESS_THAN_OR_EQUAL":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).lte(info.getCondition()));
            case "LESS_THAN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).lt(info.getCondition()));
            case "EQUALS":
                return boolQueryBuilder.must(QueryBuilders.termQuery(info.getFieldName(), info.getCondition()));
            case "NOT_EQUALS":
                return boolQueryBuilder.mustNot(QueryBuilders.termQuery(info.getFieldName(), info.getCondition()));
            case "LIKE":
                return boolQueryBuilder.must(QueryBuilders.fuzzyQuery(info.getFieldName(), info.getCondition()));
            case "IN":
                return boolQueryBuilder.must(QueryBuilders.termsQuery(info.getFieldName(), Arrays.asList(StringUtils.split(info.getCondition().trim(), ","))));
            case "NOT_IN":
                return boolQueryBuilder.mustNot(QueryBuilders.termsQuery(info.getFieldName(), Arrays.asList(StringUtils.split(info.getCondition().trim(), ","))));
            default:
                try {
                    throw new Exception("Predicate does not match!");
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return boolQueryBuilder;
        }

    }


    @Override
    public void dealOneEqualCon(SqlNode sqlNode, String sideTableName) {
        if (!SqlKind.COMPARISON.contains(sqlNode.getKind())) {
            throw new RuntimeException("not compare operator.");
        }

        SqlIdentifier left = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[0];
        SqlIdentifier right = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[1];

        String leftTableName = left.getComponent(0).getSimple();
        String leftField = left.getComponent(1).getSimple();

        String rightTableName = right.getComponent(0).getSimple();
        String rightField = right.getComponent(1).getSimple();

        if (leftTableName.equalsIgnoreCase(sideTableName)) {
            equalFieldList.add(leftField);
            int equalFieldIndex = -1;
            for (int i = 0; i < rowTypeInfo.getFieldNames().length; i++) {
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if (fieldName.equalsIgnoreCase(rightField)) {
                    equalFieldIndex = i;
                }
            }
            if (equalFieldIndex == -1) {
                throw new RuntimeException("can't deal equal field: " + sqlNode);
            }

            equalValIndex.add(equalFieldIndex);

        } else if (rightTableName.equalsIgnoreCase(sideTableName)) {

            equalFieldList.add(rightField);
            int equalFieldIndex = -1;
            for (int i = 0; i < rowTypeInfo.getFieldNames().length; i++) {
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if (fieldName.equalsIgnoreCase(leftField)) {
                    equalFieldIndex = i;
                }
            }
            if (equalFieldIndex == -1) {
                throw new RuntimeException("can't deal equal field: " + sqlNode.toString());
            }

            equalValIndex.add(equalFieldIndex);

        } else {
            throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString());
        }

    }

}
