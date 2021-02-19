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

package com.dtstack.flink.sql.sink.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-28 10:07
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({AbstractDtRichOutputFormat.class
        , Cluster.class})
@PowerMockIgnore({"javax.*"})
public class CassandraOutputFormatTest {

    private CassandraOutputFormat cassandraOutputFormat = new CassandraOutputFormat();
    private Session session;

    @Before
    public void setUp() {
        session = mock(Session.class);
        String[] fieldNames = new String[]{"mid", "mbb", "sid", "sbb"};
        Whitebox.setInternalState(cassandraOutputFormat, "session", session);
        Whitebox.setInternalState(cassandraOutputFormat, "database", "cx");
        Whitebox.setInternalState(cassandraOutputFormat, "tableName", "tableName");
        Whitebox.setInternalState(cassandraOutputFormat, "fieldNames", fieldNames);
    }

    @Test
    public void testOpen() throws NoSuchMethodException {
        suppress(AbstractDtRichOutputFormat.class.getMethod("initMetric"));
        Whitebox.setInternalState(cassandraOutputFormat, "address", "12.12.12.12:9042,10.10.10.10:9042");
        Whitebox.setInternalState(cassandraOutputFormat, "userName", "userName");
        Whitebox.setInternalState(cassandraOutputFormat, "password", "password");
        Whitebox.setInternalState(cassandraOutputFormat, "coreConnectionsPerHost", 1);
        Whitebox.setInternalState(cassandraOutputFormat, "maxConnectionsPerHost", 1);
        Whitebox.setInternalState(cassandraOutputFormat, "maxQueueSize", 1);
        Whitebox.setInternalState(cassandraOutputFormat, "readTimeoutMillis", 1);
        Whitebox.setInternalState(cassandraOutputFormat, "connectTimeoutMillis", 1);
        Whitebox.setInternalState(cassandraOutputFormat, "poolTimeoutMillis", 1);

        cassandraOutputFormat.open(1, 1);
    }

    @Test
    public void testClose() throws IOException {
        Cluster cluster = mock(Cluster.class);
        Whitebox.setInternalState(cassandraOutputFormat, "cluster", cluster);

        cassandraOutputFormat.close();
    }

    @Test
    public void testInsertWrite() throws IOException {
        Counter outRecords = mock(Counter.class);
        Counter outDirtyRecords = mock(Counter.class);
        Whitebox.setInternalState(cassandraOutputFormat, "outRecords", outRecords);
        Whitebox.setInternalState(cassandraOutputFormat, "outDirtyRecords", outDirtyRecords);

        Row row = new Row(4);
        row.setField(0, 1);
        row.setField(1, "ddd");
        row.setField(2, "ad");
        row.setField(3, "qwe");

        Tuple2 tuple2 = new Tuple2();
        tuple2.f0 = true;
        tuple2.f1 = row;

        ResultSet resultSet = mock(ResultSet.class);
        when(session.execute("")).thenReturn(resultSet);

        cassandraOutputFormat.writeRecord(tuple2);
    }
}
