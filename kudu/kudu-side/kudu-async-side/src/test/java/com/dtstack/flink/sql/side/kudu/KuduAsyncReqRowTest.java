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

package com.dtstack.flink.sql.side.kudu;

import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.kudu.table.KuduSideTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-08-11 14:38
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({
        KuduAsyncReqRow.class,
        KuduAsyncSideInfo.class,
        BaseAsyncReqRow.class,
        AsyncKuduClient.AsyncKuduClientBuilder.class,
        AsyncKuduScanner.class,
        Deferred.class
})
public class KuduAsyncReqRowTest {

    private KuduAsyncReqRow kuduAsyncReqRow;
    private BaseSideInfo sideInfo;
    private KuduSideTableInfo kuduSideTableInfo;
    private AsyncKuduClient asyncClient;

    @Before
    public void setUp() {
        asyncClient = mock(AsyncKuduClient.class);
        sideInfo = mock(BaseSideInfo.class);
        suppress(constructor(KuduAsyncSideInfo.class));
        suppress(constructor(BaseAsyncReqRow.class));
        kuduAsyncReqRow = new KuduAsyncReqRow(null, null, null, null);
        Whitebox.setInternalState(kuduAsyncReqRow, "sideInfo", sideInfo);
    }

    @Test
    public void testHandleAsyncInvoke() throws Exception {
        ResultFuture resultFuture = mock(ResultFuture.class);

        GenericRow row = new GenericRow(4);
        row.setField(0, 1);
        row.setField(1, 23);
        row.setField(2, 10f);
        row.setField(3, 12L);

        kuduSideTableInfo = mock(KuduSideTableInfo.class);
        AsyncKuduClient.AsyncKuduClientBuilder asyncKuduClientBuilder = mock(AsyncKuduClient.AsyncKuduClientBuilder.class);
        KuduClient kuduClient = mock(KuduClient.class);
        KuduTable table = mock(KuduTable.class);
        AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder = mock(AsyncKuduScanner.AsyncKuduScannerBuilder.class);
        AsyncKuduScanner asyncKuduScanner = mock(AsyncKuduScanner.class);
        Deferred data = mock(Deferred.class);

        Whitebox.setInternalState(kuduAsyncReqRow, "kuduSideTableInfo", kuduSideTableInfo);

        suppress(BaseAsyncReqRow.class.getMethod("open", Configuration.class));
        when(sideInfo.getSideTableInfo()).thenReturn(kuduSideTableInfo);
        when(sideInfo.getSideSelectFields()).thenReturn("a,b");
        when(kuduSideTableInfo.getKuduMasters()).thenReturn("localhost");
        when(kuduSideTableInfo.getTableName()).thenReturn("tablename");
        when(kuduSideTableInfo.getWorkerCount()).thenReturn(3);
        when(kuduSideTableInfo.getDefaultOperationTimeoutMs()).thenReturn(1000);
        when(kuduSideTableInfo.getBatchSizeBytes()).thenReturn(1000);
        when(kuduSideTableInfo.getLimitNum()).thenReturn(1000L);
        when(kuduSideTableInfo.getFaultTolerant()).thenReturn(true);
        whenNew(AsyncKuduClient.AsyncKuduClientBuilder.class).withArguments("localhost").thenReturn(asyncKuduClientBuilder);
        when(asyncKuduClientBuilder.build()).thenReturn(asyncClient);
        when(asyncClient.syncClient()).thenReturn(kuduClient);
        when(kuduClient.tableExists("tablename")).thenReturn(true);
        when(kuduClient.openTable("tablename")).thenReturn(table);
        when(asyncClient.newScannerBuilder(table)).thenReturn(scannerBuilder);
        when(scannerBuilder.build()).thenReturn(asyncKuduScanner);
        when(asyncKuduScanner.nextRows()).thenReturn(data);

        kuduAsyncReqRow.handleAsyncInvoke(Maps.newHashMap(), row, resultFuture);
    }

    @Test
    public void testFillData() {
        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, "bbbbbb");
        row.setField(2, "2020-07-14 01:27:43.969");

        List<FieldInfo> outFieldInfoList = Lists.newArrayList();
        FieldInfo fieldInfo = new FieldInfo();
        fieldInfo.setTable("m");
        fieldInfo.setFieldName("id");
        FieldInfo fieldInfo2 = new FieldInfo();
        fieldInfo2.setTable("s");
        fieldInfo2.setFieldName("_id");
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo2);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo2);

        Map<Integer, Integer> inFieldIndex = Maps.newHashMap();
        inFieldIndex.put(0, 0);
        inFieldIndex.put(1, 1);

        Map<Integer, String> sideFieldIndex = Maps.newHashMap();
        sideFieldIndex.put(2, "1");
        sideFieldIndex.put(3, "1");
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(TimeIndicatorTypeInfo.class)}, new String[]{"id", "PROCTIME"});

        when(sideInfo.getOutFieldInfoList()).thenReturn(outFieldInfoList);
        when(sideInfo.getInFieldIndex()).thenReturn(inFieldIndex);
        when(sideInfo.getRowTypeInfo()).thenReturn(rowTypeInfo);
        when(sideInfo.getSideFieldNameIndex()).thenReturn(sideFieldIndex);

        kuduAsyncReqRow.fillData(row, null);
    }

    @Test
    public void testClose() throws Exception {
        Whitebox.setInternalState(kuduAsyncReqRow, "asyncClient", asyncClient);
        kuduAsyncReqRow.close();
    }
}
