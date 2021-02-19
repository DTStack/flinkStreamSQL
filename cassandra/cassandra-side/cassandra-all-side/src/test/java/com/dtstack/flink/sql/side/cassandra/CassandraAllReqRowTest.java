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

package com.dtstack.flink.sql.side.cassandra;

import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.cassandra.table.CassandraSideTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-28 10:58
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({CassandraAllReqRow.class,
        BaseAllReqRow.class,
        CassandraAllSideInfo.class})//要跳过的写在后面
@PowerMockIgnore({"javax.*"})
public class CassandraAllReqRowTest {

    private CassandraAllReqRow cassandraAllReqRow;
    private RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)}, new String[]{"id", "bb", "PROCTIME"});
    private JoinInfo joinInfo;
    private List<FieldInfo> outFieldInfoList = new ArrayList<>();
    private CassandraSideTableInfo sideTableInfo;
    private BaseSideInfo sideInfo;
    private AtomicReference<Map<String, List<Map<String, Object>>>> cacheRef = new AtomicReference<>();

    @Before
    public void setUp() {
        joinInfo = mock(JoinInfo.class);
        sideTableInfo = mock(CassandraSideTableInfo.class);
        sideInfo = PowerMockito.mock(CassandraAllSideInfo.class);

        Map<String, List<Map<String, Object>>> map = Maps.newHashMap();
        cacheRef.set(map);

        suppress(constructor(CassandraAllSideInfo.class));
        suppress(constructor(BaseAllReqRow.class));
        cassandraAllReqRow = new CassandraAllReqRow(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        Whitebox.setInternalState(cassandraAllReqRow, "sideInfo", sideInfo);
        Whitebox.setInternalState(cassandraAllReqRow, "cacheRef", cacheRef);
    }

    @Test
    public void testReloadCache() throws SQLException {
        when(sideInfo.getSideTableInfo()).thenReturn(sideTableInfo);
        when(sideTableInfo.getMaxRequestsPerConnection()).thenReturn(1);
        when(sideTableInfo.getCoreConnectionsPerHost()).thenReturn(1);
        when(sideTableInfo.getMaxConnectionsPerHost()).thenReturn(1);
        when(sideTableInfo.getMaxQueueSize()).thenReturn(1);
        when(sideTableInfo.getReadTimeoutMillis()).thenReturn(1);
        when(sideTableInfo.getConnectTimeoutMillis()).thenReturn(1);
        when(sideTableInfo.getPoolTimeoutMillis()).thenReturn(1);
        when(sideTableInfo.getAddress()).thenReturn("12.12.12.12:9042,10.10.10.10:9042");
        when(sideTableInfo.getUserName()).thenReturn("userName");
        when(sideTableInfo.getPassword()).thenReturn("password");
        when(sideTableInfo.getDatabase()).thenReturn("getDatabase");

        cassandraAllReqRow.initCache();
        cassandraAllReqRow.reloadCache();
    }

    @Test
    public void testFlatmap() throws Exception {
        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, "bbbbbb");
        row.setField(2, "2020-07-14 01:27:43.969");
        Collector<BaseRow> out = mock(Collector.class);

        List<String> equalFieldList = Lists.newArrayList();
        equalFieldList.add("rowkey");
        List<Integer> equalValIndex = Lists.newArrayList();
        equalValIndex.add(0);

        List<FieldInfo> outFieldInfoList = Lists.newArrayList();
        FieldInfo fieldInfo = new FieldInfo();
        fieldInfo.setTable("m");
        fieldInfo.setFieldName("id");
        fieldInfo.setTypeInformation(TypeInformation.of(Integer.class));
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);

        Map<Integer, Integer> inFieldIndex = Maps.newHashMap();
        inFieldIndex.put(0, 0);
        inFieldIndex.put(1, 1);


        Map<Integer, Integer> sideFieldIndex = Maps.newHashMap();
        sideFieldIndex.put(2, 0);
        sideFieldIndex.put(3, 1);

        Map<Integer, String> sideFieldNameIndex = Maps.newHashMap();
        sideFieldNameIndex.put(2, "rowkey");
        sideFieldNameIndex.put(3, "channel");

        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Timestamp.class)}, new String[]{"id", "bb", "PROCTIME"});

        when(sideInfo.getEqualValIndex()).thenReturn(equalValIndex);
        when(sideInfo.getJoinType()).thenReturn(JoinType.LEFT);
        when(sideInfo.getOutFieldInfoList()).thenReturn(outFieldInfoList);
        when(sideInfo.getInFieldIndex()).thenReturn(inFieldIndex);
        when(sideInfo.getRowTypeInfo()).thenReturn(rowTypeInfo);
        when(sideInfo.getSideFieldNameIndex()).thenReturn(sideFieldNameIndex);

        cassandraAllReqRow.flatMap(row, out);
    }

}
