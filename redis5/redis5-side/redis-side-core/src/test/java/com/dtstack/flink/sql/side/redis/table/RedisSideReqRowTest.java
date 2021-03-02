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

package com.dtstack.flink.sql.side.redis.table;

import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: chuixue
 * @create: 2020-07-20 11:54
 * @description:
 **/
public class RedisSideReqRowTest {
    @Test
    public void teatFillData() {
        BaseSideInfo sideInfo = PowerMockito.mock(BaseSideInfo.class);
        RedisSideReqRow redisSideReqRow = new RedisSideReqRow(sideInfo, null);
        List<FieldInfo> outFieldInfoList = Lists.newArrayList();
        FieldInfo fieldInfo = new FieldInfo();
        fieldInfo.setTable("m");
        fieldInfo.setFieldName("id");
        fieldInfo.setTypeInformation(TypeInformation.of(Integer.class));
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);
        Map<Integer, Integer> inFieldIndex = Maps.newHashMap();
        inFieldIndex.put(0, 0);
        inFieldIndex.put(1, 1);
        inFieldIndex.put(2, 2);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Timestamp.class)}, new String[]{"id", "bb", "PROCTIME"});
        Map<Integer, Integer> sideFieldIndex = Maps.newHashMap();
        sideFieldIndex.put(3, 0);
        sideFieldIndex.put(4, 1);
        sideFieldIndex.put(5, 2);
        sideFieldIndex.put(6, 3);
        Map<Integer, String> sideFieldNameIndex = Maps.newHashMap();
        sideFieldNameIndex.put(3, "rowkey");
        sideFieldNameIndex.put(4, "channel");
        sideFieldNameIndex.put(5, "f");
        sideFieldNameIndex.put(6, "l");

        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, "bbbbbb");
        row.setField(2, "2020-07-14 01:27:43.969");

        Map<String, String> sideInput = new LinkedHashMap<>();
        sideInput.put("rowkey", "1");
        sideInput.put("channel", new Date(1594661263000L) + "");
        sideInput.put("f", 10.0f + "");
        sideInput.put("l", 10L + "");

        RedisSideTableInfo redisSideTableInfo = PowerMockito.mock(RedisSideTableInfo.class);
        List<Class> fieldClassList = Lists.newArrayList();
        fieldClassList.add(Integer.class);
        fieldClassList.add(Date.class);
        fieldClassList.add(Float.class);
        fieldClassList.add(Long.class);

        PowerMockito.when(sideInfo.getOutFieldInfoList()).thenReturn(outFieldInfoList);
        PowerMockito.when(sideInfo.getInFieldIndex()).thenReturn(inFieldIndex);
        PowerMockito.when(sideInfo.getRowTypeInfo()).thenReturn(rowTypeInfo);
        PowerMockito.when(sideInfo.getSideFieldIndex()).thenReturn(sideFieldIndex);
        PowerMockito.when(sideInfo.getSideFieldNameIndex()).thenReturn(sideFieldNameIndex);
        PowerMockito.when(sideInfo.getSideTableInfo()).thenReturn(redisSideTableInfo);
        PowerMockito.when(redisSideTableInfo.getFieldClassList()).thenReturn(fieldClassList);
        redisSideReqRow.fillData(row, sideInput);
    }
}
