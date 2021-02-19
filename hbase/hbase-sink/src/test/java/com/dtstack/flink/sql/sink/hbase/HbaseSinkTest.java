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

package com.dtstack.flink.sql.sink.hbase;

import com.dtstack.flink.sql.sink.hbase.table.HbaseTableInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: chuixue
 * @create: 2020-07-08 09:17
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataStream.class,
        HbaseSink.class})
public class HbaseSinkTest {

    @InjectMocks
    HbaseSink hbaseSink;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    // @Test
    public void testEmitDataStream() throws IllegalAccessException {
        MemberModifier.field(HbaseSink.class, "zookeeperQuorum").set(hbaseSink, "localhost");
        MemberModifier.field(HbaseSink.class, "parent").set(hbaseSink, "/hbase");
        MemberModifier.field(HbaseSink.class, "tableName").set(hbaseSink, "tableName");
        MemberModifier.field(HbaseSink.class, "rowkey").set(hbaseSink, "rowkey");
        MemberModifier.field(HbaseSink.class, "updateMode").set(hbaseSink, "append");
        MemberModifier.field(HbaseSink.class, "fieldNames").set(hbaseSink, new String[]{"a", "b", "c"});
        MemberModifier.field(HbaseSink.class, "registerTabName").set(hbaseSink, "registerTabName");
        Map<String, String> columnNameFamily = new HashMap();
        columnNameFamily.put("f:a", "a");
        columnNameFamily.put("f:b", "b");
        columnNameFamily.put("f:c", "c");
        MemberModifier.field(HbaseSink.class, "columnNameFamily").set(hbaseSink, columnNameFamily);

        DataStream dataStream = PowerMockito.mock(DataStream.class);
        DataStreamSink dataStreamSink = PowerMockito.mock(DataStreamSink.class);
        when(dataStream.addSink(any())).thenReturn(dataStreamSink);
        when(dataStreamSink.name(any())).thenReturn(dataStreamSink);
//        hbaseSink.emitDataStream(dataStream);
    }

    @Test
    public void testGenStreamSink() {
        HbaseTableInfo hbaseTableInfo = new HbaseTableInfo();
        hbaseTableInfo.setHost("localhost");
        hbaseTableInfo.setPort("2181");
        hbaseTableInfo.setParent("/hbase");
        hbaseTableInfo.setTableName("tablename");
        hbaseTableInfo.setRowkey("rk");

        Map<String, String> columnNameFamily = new HashMap();
        columnNameFamily.put("f:a", "a");
        columnNameFamily.put("f:b", "b");
        columnNameFamily.put("f:c", "c");
        hbaseTableInfo.setColumnNameFamily(columnNameFamily);
        hbaseTableInfo.setUpdateMode("append");
//        hbaseSink.genStreamSink(hbaseTableInfo);
    }
}
