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

import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import com.dtstack.flink.sql.side.hbase.utils.HbaseConfigUtils;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-14 10:01
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({HbaseAllReqRow.class
        , BaseAllReqRow.class
        , RowTypeInfo.class
        , JoinInfo.class
        , HbaseSideTableInfo.class
        , LoggerFactory.class
        , ConnectionFactory.class
        , CellUtil.class
        , HbaseConfigUtils.class})
public class HbaseAllReqRowTest {

    private HbaseAllReqRow hbaseAllReqRow;
    private HbaseSideTableInfo hbaseSideTableInfo;
    private List<FieldInfo> outFieldInfoList = new ArrayList<>();
    private RowTypeInfo rowTypeInfo;
    private Map<Integer, Integer> inFieldIndex = Maps.newHashMap();
    private Map<Integer, Integer> sideFieldIndex = Maps.newHashMap();
    private HbaseAllSideInfo sideInfo;

    @Before
    public void setup() {
        Logger log = mock(Logger.class);
        mockStatic(LoggerFactory.class);
        JoinInfo joinInfo = mock(JoinInfo.class);
        FieldInfo fieldInfo1 = new FieldInfo();
        fieldInfo1.setTable("m");
        fieldInfo1.setFieldName("id");
        fieldInfo1.setTypeInformation(TypeInformation.of(Integer.class));
        FieldInfo fieldInfo2 = new FieldInfo();
        fieldInfo2.setTable("m");
        fieldInfo2.setFieldName("bb");
        fieldInfo2.setTypeInformation(TypeInformation.of(String.class));
        FieldInfo fieldInfo3 = new FieldInfo();
        fieldInfo3.setTable("s");
        fieldInfo3.setFieldName("channel");
        fieldInfo3.setTypeInformation(TypeInformation.of(String.class));
        FieldInfo fieldInfo4 = new FieldInfo();
        fieldInfo4.setTable("s");
        fieldInfo4.setFieldName("name");
        fieldInfo4.setTypeInformation(TypeInformation.of(String.class));
        rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)}, new String[]{"id", "bb", "PROCTIME"});
        outFieldInfoList.add(fieldInfo1);
        outFieldInfoList.add(fieldInfo2);
        outFieldInfoList.add(fieldInfo3);
        outFieldInfoList.add(fieldInfo4);
        inFieldIndex.put(0, 0);
        inFieldIndex.put(1, 1);
        sideFieldIndex.put(2, 0);
        sideFieldIndex.put(3, 1);
        sideInfo = mock(HbaseAllSideInfo.class);
        hbaseSideTableInfo = mock(HbaseSideTableInfo.class);

        String tableName = "bbb";
        Map<String, String> aliasNameRef = new HashMap<>();
        aliasNameRef.put("cf:channel", "channel");
        aliasNameRef.put("cf:name", "name");

        when(LoggerFactory.getLogger(BaseAllReqRow.class)).thenReturn(log);
        when(LoggerFactory.getLogger(HbaseAllReqRow.class)).thenReturn(log);
        suppress(constructor(HbaseAllSideInfo.class));
        suppress(constructor(BaseAsyncReqRow.class));
        when(hbaseSideTableInfo.getTableName()).thenReturn(tableName);
        when(hbaseSideTableInfo.getAliasNameRef()).thenReturn(aliasNameRef);
        when(sideInfo.getSideTableInfo()).thenReturn(hbaseSideTableInfo);

        hbaseAllReqRow = new HbaseAllReqRow(rowTypeInfo, joinInfo, outFieldInfoList, hbaseSideTableInfo);

        Whitebox.setInternalState(hbaseAllReqRow, "sideInfo", sideInfo);
    }

    @Test
    public void testFlatMap() throws Exception {
        AtomicReference<Map<String, Map<String, Object>>> cacheRef = new AtomicReference<>();
        Map keyValue = Maps.newConcurrentMap();
        Map value = Maps.newHashMap();
        value.put("name", "dtstack");
        value.put("channel", "bigdata");
        value.put(null, "daishuyun");
        keyValue.put("1", value);
        cacheRef.set(keyValue);

        Whitebox.setInternalState(hbaseAllReqRow, "cacheRef", cacheRef);

        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, "bbbbbb");
        row.setField(2, "2020-07-14 01:27:43.969");

        Collector<BaseRow> out = mock(Collector.class);
        RowKeyBuilder rowKeyBuilder = mock(RowKeyBuilder.class);
        Map<String, Object> rowkeyMap = Maps.newHashMap();
        rowkeyMap.put("rowkey", 1);

        when(sideInfo.getEqualValIndex()).thenReturn(Lists.newArrayList(0));
        when(sideInfo.getJoinType()).thenReturn(JoinType.LEFT);
        when(sideInfo.getEqualFieldList()).thenReturn(Lists.newArrayList("rowkey"));
        when(sideInfo.getRowKeyBuilder()).thenReturn(rowKeyBuilder);
        when(rowKeyBuilder.getRowKey(rowkeyMap)).thenReturn("1");
        when(hbaseSideTableInfo.isPreRowKey()).thenReturn(false);
        when(sideInfo.getOutFieldInfoList()).thenReturn(outFieldInfoList);
        when(sideInfo.getRowTypeInfo()).thenReturn(rowTypeInfo);
        when(sideInfo.getInFieldIndex()).thenReturn(inFieldIndex);
        when(sideInfo.getSideFieldIndex()).thenReturn(sideFieldIndex);

        hbaseAllReqRow.flatMap(row, out);
    }

    @Test
    public void testReloadCache() throws IOException {
        mockStatic(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        Configuration configuration = mock(Configuration.class);
        Table table = mock(Table.class);
        ResultScanner resultScanner = mock(ClientScanner.class);
        Iterator iterator = mock(Iterator.class);
        Result result = mock(Result.class);
        List<Cell> cells = Lists.newArrayList(new NoTagsKeyValue("001/cf:channel/1587606427805/Put/vlen=3/seqid=0".getBytes(), 0, 35));
        mockStatic(CellUtil.class);
        mockStatic(HbaseConfigUtils.class);

        when(HbaseConfigUtils.getConfig(new HashMap<>())).thenReturn(configuration);
        when(ConnectionFactory.createConnection(any(Configuration.class))).thenReturn(connection);
        when(hbaseSideTableInfo.getHost()).thenReturn("localhost:2181");
        when(connection.getTable(any(TableName.class))).thenReturn(table);
        when(table.getScanner(any(Scan.class))).thenReturn(resultScanner);
        when(resultScanner.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(result);
        when(result.listCells()).thenReturn(cells);
        when(result.getRow()).thenReturn("001".getBytes());
        when(CellUtil.cloneFamily(any(NoTagsKeyValue.class))).thenReturn("cf".getBytes());
        when(CellUtil.cloneQualifier(any(NoTagsKeyValue.class))).thenReturn("channel".getBytes());
        when(CellUtil.cloneValue(any(NoTagsKeyValue.class))).thenReturn("001/cf:channel/1587606427805/Put/vlen=3/seqid=0".getBytes());

        hbaseAllReqRow.reloadCache();
    }
}
