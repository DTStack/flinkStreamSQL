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

import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.kudu.table.KuduSideTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-08-10 14:12
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({
        KuduAllReqRow.class,
        KuduAllSideInfo.class,
        BaseAllReqRow.class,
        KuduClient.KuduClientBuilder.class
})
public class KuduAllReqRowTest {

    private KuduAllReqRow kuduAllReqRow;
    private BaseSideInfo sideInfo = mock(BaseSideInfo.class);
    private KuduClient client = mock(KuduClient.class);

    @Before
    public void setUp() {
        suppress(constructor(KuduAllSideInfo.class));
        suppress(constructor(BaseAllReqRow.class));
        kuduAllReqRow = new KuduAllReqRow(null, null, null, null);
    }

    @Test
    public void testReloadCache() throws Exception {
        ColumnSchema columnSchema = mock(ColumnSchema.class);
        List<ColumnSchema> columnSchemas = Lists.newArrayList();
        columnSchemas.add(columnSchema);
        PartialRow partialRow = mock(PartialRow.class);
        Schema schema = mock(Schema.class);
        KuduTable table = mock(KuduTable.class);
        Whitebox.setInternalState(kuduAllReqRow, "sideInfo", sideInfo);
        KuduSideTableInfo tableInfo = mock(KuduSideTableInfo.class);
        KuduClient.KuduClientBuilder kuduClientBuilder = mock(KuduClient.KuduClientBuilder.class);
        KuduScanner.KuduScannerBuilder tokenBuilder = mock(KuduScanner.KuduScannerBuilder.class);
        KuduScanner scanner = mock(KuduScanner.class);

        when(columnSchema.getName()).thenReturn("mid");
        when(columnSchema.getType()).thenReturn(Type.STRING);
        whenNew(KuduClient.KuduClientBuilder.class).withArguments("localhost").thenReturn(kuduClientBuilder);
        when(kuduClientBuilder.build()).thenReturn(client);
        when(client.tableExists("tablename")).thenReturn(true);
        when(client.openTable("tablename")).thenReturn(table);
        when(tokenBuilder.setProjectedColumnNames(any())).thenReturn(tokenBuilder);
        when(tokenBuilder.build()).thenReturn(scanner);

        when(client.newScannerBuilder(table)).thenReturn(tokenBuilder);
        when(table.getSchema()).thenReturn(schema);
        when(schema.getPrimaryKeyColumns()).thenReturn(columnSchemas);
        when(schema.newPartialRow()).thenReturn(partialRow);
        when(sideInfo.getSideTableInfo()).thenReturn(tableInfo);
        when(sideInfo.getSideSelectFields()).thenReturn("a,b");
        when(tableInfo.getKuduMasters()).thenReturn("localhost");
        when(tableInfo.getTableName()).thenReturn("tablename");
        when(tableInfo.getWorkerCount()).thenReturn(3);
        when(tableInfo.getDefaultOperationTimeoutMs()).thenReturn(1000);
        when(tableInfo.getBatchSizeBytes()).thenReturn(1000);
        when(tableInfo.getLimitNum()).thenReturn(1000L);
        when(tableInfo.getFaultTolerant()).thenReturn(true);
        when(tableInfo.getLowerBoundPrimaryKey()).thenReturn("lower");
        when(tableInfo.getUpperBoundPrimaryKey()).thenReturn("upper");
        when(tableInfo.getPrimaryKey()).thenReturn("mid");

        kuduAllReqRow.reloadCache();
    }

    @Test
    public void testFlatMap() throws Exception {
        Collector out = mock(Collector.class);
        GenericRow row = new GenericRow(4);
        row.setField(0, 1);
        row.setField(1, 23);
        row.setField(2, 10f);
        row.setField(3, 12L);

        List<Integer> equalValIndex = Lists.newArrayList();
        equalValIndex.add(0);

        AtomicReference<Map<String, List<Map<String, Object>>>> cacheRef = new AtomicReference<>();
        Map keyValue = Maps.newConcurrentMap();
        keyValue.put("1", Lists.newArrayList());
        cacheRef.set(keyValue);

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
        outFieldInfoList.add(fieldInfo);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(Integer.class), TypeInformation.of(Integer.class), TypeInformation.of(Integer.class)}, new String[]{"id", "bb", "ddd", "PROCTIME"});

        Map<Integer, Integer> inFieldIndex = Maps.newHashMap();
        inFieldIndex.put(0, 0);
        inFieldIndex.put(1, 1);
        inFieldIndex.put(2, 2);
        inFieldIndex.put(3, 3);

        Map<Integer, String> sideFieldNameIndex = Maps.newHashMap();
        sideFieldNameIndex.put(3, "rowkey");
        sideFieldNameIndex.put(4, "channel");
        sideFieldNameIndex.put(5, "f");
        sideFieldNameIndex.put(6, "l");

        Whitebox.setInternalState(kuduAllReqRow, "cacheRef", cacheRef);
        Whitebox.setInternalState(kuduAllReqRow, "sideInfo", sideInfo);

        when(sideInfo.getOutFieldInfoList()).thenReturn(outFieldInfoList);
        when(sideInfo.getInFieldIndex()).thenReturn(inFieldIndex);
        when(sideInfo.getRowTypeInfo()).thenReturn(rowTypeInfo);
        when(sideInfo.getSideFieldNameIndex()).thenReturn(sideFieldNameIndex);
        when(sideInfo.getEqualValIndex()).thenReturn(equalValIndex);
        when(sideInfo.getJoinType()).thenReturn(JoinType.LEFT);

        kuduAllReqRow.flatMap(row, out);
    }

    @Test
    public void testClose() throws Exception {
        Whitebox.setInternalState(kuduAllReqRow, "client", client);
        kuduAllReqRow.close();
    }
}
