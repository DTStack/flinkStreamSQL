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

package com.dtstack.flink.sql.side.impala;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.impala.table.ImpalaSideTableInfo;
import com.dtstack.flink.sql.side.rdb.async.RdbAsyncSideInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;
import java.util.Map;

/**
 * Date: 2019/11/12
 * Company: www.dtstack.com
 *
 * @author xiuzhu
 */

public class ImpalaAsyncSideInfo extends RdbAsyncSideInfo {

    public ImpalaAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        ImpalaSideTableInfo impalaSideTableInfo = (ImpalaSideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();

        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();

        List<String> sqlJoinCompareOperate= Lists.newArrayList();

        ParseUtils.parseAnd(conditionNode, sqlNodeList);
        ParseUtils.parseJoinCompareOperate(conditionNode, sqlJoinCompareOperate);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }

        List<String> whereConditionList = Lists.newArrayList();;
        Map<String, String> physicalFields = impalaSideTableInfo.getPhysicalFields();
        SqlNode whereNode = ((SqlSelect) joinInfo.getSelectNode()).getWhere();
        if (whereNode != null) {
            // 解析维表中的过滤条件
            ParseUtils.parseSideWhere(whereNode, physicalFields, whereConditionList);
        }

        sqlCondition = "select ${selectField} from ${tableName} where ";
        for (int i = 0; i < equalFieldList.size(); i++) {
            String equalField = sideTableInfo.getPhysicalFields().getOrDefault(equalFieldList.get(i), equalFieldList.get(i));

            sqlCondition += equalField + " " + sqlJoinCompareOperate.get(i) + " ? ";
            if (i != equalFieldList.size() - 1) {
                sqlCondition += " and ";
            }
        }
        if (0 != whereConditionList.size()) {
            // 如果where条件中第一个符合条件的是维表中的条件
            String firstCondition = whereConditionList.get(0);
            if (!"and".equalsIgnoreCase(firstCondition) && !"or".equalsIgnoreCase(firstCondition)) {
                whereConditionList.add(0, "and (");
            } else {
                whereConditionList.add(1, "(");
            }
            whereConditionList.add(whereConditionList.size(), ")");
            sqlCondition += String.join(" ", whereConditionList);
        }

        sqlCondition = sqlCondition.replace("${tableName}", impalaSideTableInfo.getTableName()).replace("${selectField}", sideSelectFields);

        boolean enablePartiton = impalaSideTableInfo.isEnablePartition();
        if (enablePartiton){
            String whereTmp = " ";
            String[] partitionfields = impalaSideTableInfo.getPartitionfields();
            String[] partitionFieldTypes = impalaSideTableInfo.getPartitionFieldTypes();
            Map<String, List> partitionVaules = impalaSideTableInfo.getPartitionValues();
            int fieldsSize = partitionfields.length;
            for (int i=0; i < fieldsSize; i++) {
                String fieldName = partitionfields[i];
                String fieldType = partitionFieldTypes[i];
                List values = partitionVaules.get(fieldName);
                String vauleAppend = getVauleAppend(fieldType, values);
                whereTmp = whereTmp + String.format("and %s in (%s) ", fieldName, vauleAppend);

            }
            sqlCondition = sqlCondition + whereTmp;
        }

        System.out.println("--------side sql query:-------------------");
        System.out.println(sqlCondition);
    }

    public String getVauleAppend(String fieldType, List values) {
        String vauleAppend = "";
        for(int i=0; i < values.size(); i++) {
            if (fieldType.toLowerCase().equals("string") || fieldType.toLowerCase().equals("varchar")) {
                vauleAppend = vauleAppend + "," + "'" + values.get(i) + "'";
                continue;
            }
            vauleAppend = vauleAppend + "," + values.get(i).toString();
        }
        vauleAppend = vauleAppend.replaceFirst(",", "");
        return vauleAppend;
    }

}
