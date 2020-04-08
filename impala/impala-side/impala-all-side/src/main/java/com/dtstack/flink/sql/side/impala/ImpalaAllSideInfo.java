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
import java.util.stream.Collectors;

public class ImpalaAllSideInfo extends RdbAllSideInfo {

    public ImpalaAllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public String getAdditionalWhereClause() {
        ImpalaSideTableInfo impalaSideTableInfo = (ImpalaSideTableInfo) sideTableInfo;
        return impalaSideTableInfo.isEnablePartition() ? buildPartitionCondition(impalaSideTableInfo) : "";
    }

    private String buildPartitionCondition(ImpalaSideTableInfo impalaSideTableInfo) {
        String partitionCondtion = " ";
        String[] partitionfields = impalaSideTableInfo.getPartitionfields();
        String[] partitionFieldTypes = impalaSideTableInfo.getPartitionFieldTypes();
        Map<String, List> partitionVaules = impalaSideTableInfo.getPartitionValues();

        int fieldsSize = partitionfields.length;
        for (int i = 0; i < fieldsSize; i++) {
            String fieldName = partitionfields[i];
            String fieldType = partitionFieldTypes[i];
            List values = partitionVaules.get(fieldName);
            String partitionVaule = getPartitionVaule(fieldType, values);
            partitionCondtion += String.format("AND %s IN (%s) ", fieldName, partitionVaule);
        }
        return partitionCondtion;
    }

    private String getPartitionVaule(String fieldType, List values) {
        String partitionVaule = values.stream().map(val -> {
            return (fieldType.toLowerCase().equals("string") || fieldType.toLowerCase().equals("varchar")) ? "'" + val + "'" : val.toString();
        }).collect(Collectors.joining(" , ")).toString();

        return partitionVaule;
    }

}
