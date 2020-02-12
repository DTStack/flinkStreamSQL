package com.dtstack.flink.sql.side.elasticsearch6;

import org.apache.flink.api.java.typeutils.RowTypeInfo;

import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yinxi
 * @date 2020/1/13 - 1:01
 */
public class Elasticsearch6AllSideInfo extends SideInfo {

    public Elasticsearch6AllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        Elasticsearch6SideTableInfo es6SideTableInfo = (Elasticsearch6SideTableInfo) sideTableInfo;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        sqlCondition = getSelectFromStatement(getEstype(es6SideTableInfo), Arrays.asList(sideSelectFields.split(",")), sideTableInfo.getPredicateInfoes());
        System.out.println("-------- all side sql query-------\n" + sqlCondition);
    }

    //基于rdb开发side,但是那些between,in,not in之类的不知道怎么处理

    public String getAdditionalWhereClause() {
        return "";
    }

    private String getSelectFromStatement(String tableName, List<String> selectFields, List<PredicateInfo> predicateInfoes) {
        String fromClause = selectFields.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String predicateClause = predicateInfoes.stream().map(this::buildFilterCondition).collect(Collectors.joining(" AND "));
        String whereClause = buildWhereClause(predicateClause);
        String sql = "SELECT " + fromClause + " FROM " + tableName + whereClause;
        return sql;
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

    public String getEstype(Elasticsearch6SideTableInfo es6SdideTableInfo) {
        return es6SdideTableInfo.getEsType();
    }

    public String quoteIdentifier(String identifier) {
        return " " + identifier + " ";
    }
}
