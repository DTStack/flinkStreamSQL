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
import com.dtstack.flink.sql.side.rdb.all.RdbAllSideInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;
import java.util.Map;

public class ImpalaAllSideInfo extends RdbAllSideInfo {

    public ImpalaAllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        ImpalaSideTableInfo impalaSideTableInfo = (ImpalaSideTableInfo) sideTableInfo;

        boolean enablePartiton = impalaSideTableInfo.isEnablePartition();

        String sqlTmp = "select ${selectField} from ${tableName} ";
        sqlCondition = sqlTmp.replace("${tableName}", impalaSideTableInfo.getTableName()).replace("${selectField}", sideSelectFields);

        if (enablePartiton){
            String whereTmp = "where ";
            String[] partitionfields = impalaSideTableInfo.getPartitionfields();
            String[] partitionFieldTypes = impalaSideTableInfo.getPartitionFieldTypes();
            Map<String, List> partitionVaules = impalaSideTableInfo.getPartitionValues();
            int fieldsSize = partitionfields.length;
            for (int i=0; i < fieldsSize; i++) {
                String fieldName = partitionfields[i];
                String fieldType = partitionFieldTypes[i];
                List values = partitionVaules.get(fieldName);
                String vauleAppend = getVauleAppend(fieldType, values);
                if (fieldsSize - 1 == i) {
                    whereTmp = whereTmp + String.format("%s in (%s)", fieldName, vauleAppend);
                }else {
                    whereTmp = whereTmp + String.format("%s in (%s) and ", fieldName, vauleAppend);
                }

            }
            sqlCondition = sqlCondition + whereTmp;
        }
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
