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

package com.dtstack.flink.sql.side.oracle;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.rdb.async.RdbAsyncSideInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.util.ParseUtils;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;


public class OracleAsyncSideInfo extends RdbAsyncSideInfo {

    public OracleAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();

        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        List<String> sqlJoinCompareOperate= Lists.newArrayList();

        ParseUtils.parseAnd(conditionNode, sqlNodeList);
        ParseUtils.parseJoinCompareOperate(conditionNode, sqlJoinCompareOperate);


        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }

        sqlCondition = "select ${selectField} from ${tableName} where ";
        for (int i = 0; i < equalFieldList.size(); i++) {
            String equalField = sideTableInfo.getPhysicalFields().getOrDefault(equalFieldList.get(i), equalFieldList.get(i));
            sqlCondition += dealLowerFiled(equalField) + " " + sqlJoinCompareOperate.get(i) + " " + " ?";
            if (i != equalFieldList.size() - 1) {
                sqlCondition += " and ";
            }
        }

        sqlCondition = sqlCondition.replace("${tableName}", DtStringUtil.getTableFullPath(rdbSideTableInfo.getSchema(), rdbSideTableInfo.getTableName())).replace("${selectField}", dealLowerSelectFiled(sideSelectFields));
        System.out.println("---------side_exe_sql-----\n" + sqlCondition);
    }



    private String dealLowerFiled(String field) {
        return   "\"" + field + "\"";
    }

    private String dealLowerSelectFiled(String fieldsStr) {
        StringBuilder sb = new StringBuilder();
        String[] fields = fieldsStr.split(",");

        for(String f : fields) {
            sb.append("\"").append(f).append("\"").append(",");
        }

        sb.deleteCharAt(sb.lastIndexOf(","));
        return  sb.toString();
    }

}
