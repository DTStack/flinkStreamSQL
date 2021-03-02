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

import com.google.common.util.concurrent.ListenableFuture;
import com.datastax.driver.core.Cluster;
import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.cassandra.table.CassandraSideTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-28 12:39
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({CassandraAsyncReqRow.class,
        BaseSideInfo.class,
        CassandraAsyncSideInfo.class,
        BaseAsyncReqRow.class})
@PowerMockIgnore({"javax.*"})
public class CassandraAsyncReqRowTest {

    private CassandraAsyncReqRow cassandraAsyncReqRow;
    private JoinInfo joinInfo;
    private AbstractSideTableInfo sideTableInfo;
    private RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)}, new String[]{"id", "bb", "PROCTIME"});
    private BaseSideInfo sideInfo;
    private Map<String, Object> inputParams = Maps.newHashMap();
    private GenericRow row = new GenericRow(4);

    @Before
    public void setUp() {
        joinInfo = mock(JoinInfo.class);
        sideTableInfo = mock(AbstractSideTableInfo.class);
        sideInfo = mock(BaseSideInfo.class);
        inputParams.put("a", "1");

        row.setField(0, "5f1d7fe5a38c8f074f2ba1e8");
        row.setField(1, "bbbbbbbb");
        row.setField(2, "ccxx");
        row.setField(3, "lisi");

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

        suppress(constructor(CassandraAsyncSideInfo.class));
        suppress(constructor(BaseSideInfo.class));
        cassandraAsyncReqRow = new CassandraAsyncReqRow(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        Whitebox.setInternalState(cassandraAsyncReqRow, "sideInfo", sideInfo);
        when(sideInfo.getOutFieldInfoList()).thenReturn(outFieldInfoList);
        when(sideInfo.getInFieldIndex()).thenReturn(inFieldIndex);
        when(sideInfo.getRowTypeInfo()).thenReturn(rowTypeInfo);
        when(sideInfo.getSideFieldIndex()).thenReturn(sideFieldIndex);
    }

    @Test
    public void testOpen() throws Exception {
        suppress(BaseAsyncReqRow.class.getMethod("open", Configuration.class));
        CassandraSideTableInfo sideTableInfo = mock(CassandraSideTableInfo.class);

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
        cassandraAsyncReqRow.open(new Configuration());
    }

    @Test
    public void testClose() throws Exception {
        Cluster cluster = mock(Cluster.class);
        Whitebox.setInternalState(cassandraAsyncReqRow, "cluster", cluster);
        cassandraAsyncReqRow.close();
    }

    @Test
    public void testHandleAsyncInvoke() throws Exception {
        ListenableFuture session = mock(ListenableFuture.class);
        Whitebox.setInternalState(cassandraAsyncReqRow, "session", session);
        ResultFuture resultFuture = mock(ResultFuture.class);

        cassandraAsyncReqRow.handleAsyncInvoke(inputParams, row, resultFuture);
    }

    @Test
    public void testFillData() {
        cassandraAsyncReqRow.fillData(row, null);
    }
}
