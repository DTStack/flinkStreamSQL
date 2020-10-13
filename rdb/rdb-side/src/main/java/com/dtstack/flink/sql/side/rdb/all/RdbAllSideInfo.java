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

package com.dtstack.flink.sql.side.rdb.all;

import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reason:
 * Date: 2018/11/26
 * Company: www.dtstack.com
 *
 * @author maqi
 */

public class RdbAllSideInfo extends BaseSideInfo {

    private static final long serialVersionUID = -5858335638589472159L;
    private static final Logger LOG = LoggerFactory.getLogger(RdbAllSideInfo.class.getSimpleName());

    public RdbAllSideInfo(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(sideTableInfo, lookupKeys);
    }

    public RdbAllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, AbstractSideTableInfo sideTableInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideTableInfo;
        sqlCondition = getSelectFromStatement(getTableName(rdbSideTableInfo), Arrays.asList(StringUtils.split(sideSelectFields, ",")), sideTableInfo.getPredicateInfoes());
        LOG.info(String.format("--------dimension sql query: %s-------\n", sqlCondition));
    }

    @Override
    public void buildEqualInfo(AbstractSideTableInfo sideTableInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideTableInfo;
        flinkPlannerSqlCondition = getSelectFromStatement(getTableName(rdbSideTableInfo), new ArrayList(sideTableInfo.getPhysicalFields().values()), sideTableInfo.getPredicateInfoes());
        LOG.info(String.format("--------dimension sql query: %s-------\n", flinkPlannerSqlCondition));
    }

    public String getAdditionalWhereClause() {
        return "";
    }

    private String getSelectFromStatement(String tableName, List<String> selectFields, List<PredicateInfo> predicateInfoes) {
        String fromClause = selectFields.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String predicateClause = predicateInfoes.stream().map(this::buildFilterCondition).collect(Collectors.joining(" AND "));
        String whereClause = buildWhereClause(predicateClause);
        return "SELECT " + fromClause + " FROM " + tableName + whereClause;
    }

    private String buildWhereClause(String predicateClause) {
        String additionalWhereClause = getAdditionalWhereClause();
        String whereClause = (!StringUtils.isEmpty(predicateClause) || !StringUtils.isEmpty(additionalWhereClause) ? " WHERE " + predicateClause : "");
        whereClause += (StringUtils.isEmpty(predicateClause)) ? additionalWhereClause.replaceFirst("AND", "") : additionalWhereClause;
        return whereClause;
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

    public String buildFilterCondition(PredicateInfo info) {
        switch (info.getOperatorKind()) {
            case "IN":
            case "NOT_IN":
                return quoteIdentifier(info.getFieldName()) + " " + info.getOperatorName() + " ( " + info.getCondition() + " )";
            case "NOT_EQUALS":
                return quoteIdentifier(info.getFieldName()) + " != " + info.getCondition();
            case "BETWEEN":
                return quoteIdentifier(info.getFieldName()) + " BETWEEN  " + info.getCondition();
            case "IS_NOT_NULL":
            case "IS_NULL":
                return quoteIdentifier(info.getFieldName()) + " " + info.getOperatorName();
            default:
                return quoteIdentifier(info.getFieldName()) + " " + info.getOperatorName() + " " + info.getCondition();
        }
    }

    public String getTableName(RdbSideTableInfo rdbSideTableInfo) {
        return rdbSideTableInfo.getTableName();
    }

    public String quoteIdentifier(String identifier) {
        return " " + identifier + " ";
    }
}
