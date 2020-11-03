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


package com.dtstack.flink.sql.side.hbase;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;
import java.util.Map;

public class HbaseAllSideInfo extends BaseSideInfo {

    private RowKeyBuilder rowKeyBuilder;

    private Map<String, String> colRefType;

    public HbaseAllSideInfo(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(sideTableInfo, lookupKeys);
    }

    public HbaseAllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, AbstractSideTableInfo sideTableInfo) {
        rowKeyBuilder = new RowKeyBuilder();
        if (sideTableInfo.getPrimaryKeys().size() < 1) {
            throw new RuntimeException("Primary key dimension table must be filled");
        }

        rowKeyBuilder.init(sideTableInfo.getPrimaryKeys().get(0), sideTableInfo);

        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        colRefType = Maps.newHashMap();
        for (int i = 0; i < hbaseSideTableInfo.getColumnRealNames().length; i++) {
            String realColName = hbaseSideTableInfo.getColumnRealNames()[i];
            String colType = hbaseSideTableInfo.getFieldTypes()[i];
            colRefType.put(realColName, colType);
        }

        String sideTableName = joinInfo.getSideTableName();
        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        ParseUtils.parseAnd(conditionNode, sqlNodeList);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }
    }

    @Override
    public void buildEqualInfo(AbstractSideTableInfo sideTableInfo) {
        rowKeyBuilder = new RowKeyBuilder();
        if (sideTableInfo.getPrimaryKeys().size() < 1) {
            throw new RuntimeException("Primary key dimension table must be filled");
        }

        rowKeyBuilder.init(sideTableInfo.getPrimaryKeys().get(0), sideTableInfo);

        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        colRefType = Maps.newHashMap();
        for (int i = 0; i < hbaseSideTableInfo.getColumnRealNames().length; i++) {
            String realColName = hbaseSideTableInfo.getColumnRealNames()[i];
            String colType = hbaseSideTableInfo.getFieldTypes()[i];
            colRefType.put(realColName, colType);
        }
    }

    public RowKeyBuilder getRowKeyBuilder() {
        return rowKeyBuilder;
    }

    public void setRowKeyBuilder(RowKeyBuilder rowKeyBuilder) {
        this.rowKeyBuilder = rowKeyBuilder;
    }

    public Map<String, String> getColRefType() {
        return colRefType;
    }

}