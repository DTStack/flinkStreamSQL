/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import com.dtstack.flink.sql.side.rdb.all.RdbAllSideInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;

public class OracleAllSideInfo extends RdbAllSideInfo {

    public OracleAllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideTableInfo;

        sqlCondition = "select ${selectField} from ${tableName} ";


        sqlCondition = sqlCondition.replace("${tableName}", dealFiled(rdbSideTableInfo.getTableName())).replace("${selectField}", dealLowerSelectFiled(sideSelectFields));
        System.out.println("---------side_exe_sql-----\n" + sqlCondition);
    }


    private String dealFiled(String field) {
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
