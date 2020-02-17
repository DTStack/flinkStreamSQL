/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author yinxi
 * @date 2020/1/13 - 1:01
 */
public class Elasticsearch6AllSideInfo extends SideInfo {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6AllSideInfo.class);

    public Elasticsearch6AllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {

        Elasticsearch6SideTableInfo elasticsearch6SideTableInfo = (Elasticsearch6SideTableInfo) sideTableInfo;

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

    public BoolQueryBuilder buildFilterCondition(BoolQueryBuilder boolQueryBuilder, PredicateInfo info) {
        switch (info.getOperatorKind()) {
            case "IN":
                return boolQueryBuilder.must(QueryBuilders.termsQuery(info.getFieldName(), StringUtils.split(info.getCondition().trim(), ",")));
            case "NOT_IN":
                return boolQueryBuilder.mustNot(QueryBuilders.termsQuery(info.getFieldName(), StringUtils.split(info.getCondition().trim(), ",")));
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
            default:
                try {
                    throw new Exception("elasticsearch6 does not support this operation: " + info.getOperatorName());
                } catch (Exception e) {

                    e.printStackTrace();
                    LOG.error(e.getMessage());
                }
                return boolQueryBuilder;
        }

    }

    @Override
    public void parseSelectFields(JoinInfo joinInfo) {
        String sideTableName = joinInfo.getSideTableName();
        String nonSideTableName = joinInfo.getNonSideTable();
        List<String> fields = Lists.newArrayList();

        int sideIndex = 0;
        for (int i = 0; i < outFieldInfoList.size(); i++) {
            FieldInfo fieldInfo = outFieldInfoList.get(i);
            if (fieldInfo.getTable().equalsIgnoreCase(sideTableName)) {
                fields.add(fieldInfo.getFieldName());
                sideFieldIndex.put(i, sideIndex);
                sideFieldNameIndex.put(i, fieldInfo.getFieldName());
                sideIndex++;
            } else if (fieldInfo.getTable().equalsIgnoreCase(nonSideTableName)) {
                int nonSideIndex = rowTypeInfo.getFieldIndex(fieldInfo.getFieldName());
                inFieldIndex.put(i, nonSideIndex);
            } else {
                throw new RuntimeException("unknown table " + fieldInfo.getTable());
            }
        }

        if (fields.size() == 0) {
            throw new RuntimeException("select non field from table " + sideTableName);
        }

        //add join on condition field to select fields
        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();

        ParseUtils.parseAnd(conditionNode, sqlNodeList);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }

        if (CollectionUtils.isEmpty(equalFieldList)) {
            throw new RuntimeException("no join condition found after table " + joinInfo.getLeftTableName());
        }

        for (String equalField : equalFieldList) {
            if (fields.contains(equalField)) {
                continue;
            }

            fields.add(equalField);
        }

        sideSelectFields = String.join(",", fields);
    }

}
