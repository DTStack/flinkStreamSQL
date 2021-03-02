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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Date;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
 * @author: chuixue
 * @create: 2020-07-07 19:29
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({HbaseOutputFormat.class,
        HBaseConfiguration.class,
        Configuration.class,
        ConnectionFactory.class,
        TableName.class,
        Table.class,
        Counter.class})
public class HbaseOutputFormatTest {
    @InjectMocks
    HbaseOutputFormat hbaseOutputFormat;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConfigure() throws IllegalAccessException {
        MemberModifier.field(HbaseOutputFormat.class, "host").set(hbaseOutputFormat, "localhost");
        MemberModifier.field(HbaseOutputFormat.class, "zkParent").set(hbaseOutputFormat, "/hbase");

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        PowerMockito.mockStatic(HBaseConfiguration.class);
        when(HBaseConfiguration.create()).thenReturn(conf);

        Configuration configuration = new Configuration();
        hbaseOutputFormat.configure(configuration);
    }

    public void testOpen() throws IOException {
        PowerMockito.mockStatic(ConnectionFactory.class);
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.mockStatic(TableName.class);
        TableName tableName = PowerMockito.mock(TableName.class);
        Table table = PowerMockito.mock(Table.class);

        when(ConnectionFactory.createConnection(any())).thenReturn(conn);
        when(conn.getTable(any())).thenReturn(table);
        when(TableName.valueOf(anyString())).thenReturn(tableName);
        doNothing().when(hbaseOutputFormat).initMetric();

        hbaseOutputFormat.open(1, 2);
    }

    // @Test
    public void testWriteRecord() throws IllegalAccessException {
        MemberModifier.field(HbaseOutputFormat.class, "columnNames").set(hbaseOutputFormat, new String[]{"f:a", "f:b", "f:c"});
        MemberModifier.field(HbaseOutputFormat.class, "rowkey").set(hbaseOutputFormat, "md5('1')");
        MemberModifier.field(HbaseOutputFormat.class, "families").set(hbaseOutputFormat, new String[]{"f", "f", "f"});
        MemberModifier.field(HbaseOutputFormat.class, "qualifiers").set(hbaseOutputFormat, new String[]{"a", "b", "c"});
        Table table = PowerMockito.mock(Table.class);
        MemberModifier.field(HbaseOutputFormat.class, "table").set(hbaseOutputFormat, table);
        Counter outRecords = PowerMockito.mock(Counter.class);
        MemberModifier.field(HbaseOutputFormat.class, "outRecords").set(hbaseOutputFormat, outRecords);

        Row row = new Row(3);
        row.setField(0, "aaaa");
        row.setField(1, "bbbb");
        row.setField(2, new Date());

        Tuple2<Boolean, Row> tuplea = new Tuple2<Boolean, Row>(true, row);
        hbaseOutputFormat.writeRecord(tuplea);

        Tuple2<Boolean, Row> tupleb = new Tuple2<Boolean, Row>(false, row);
        hbaseOutputFormat.writeRecord(tupleb);
    }
}
