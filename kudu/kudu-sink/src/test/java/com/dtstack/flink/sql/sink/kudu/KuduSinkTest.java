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

package com.dtstack.flink.sql.sink.kudu;

import com.dtstack.flink.sql.sink.kudu.table.KuduTableInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * @author: chuixue
 * @create: 2020-08-10 10:09
 * @description:
 **/
@RunWith(PowerMockRunner.class)
public class KuduSinkTest {
    KuduSink kuduSink;

    @Before
    public void setUp() {
        kuduSink = new KuduSink();
    }

    @Test
    public void testGenStreamSink() {
        KuduTableInfo kuduTableInfo = mock(KuduTableInfo.class);
        when(kuduTableInfo.getKuduMasters()).thenReturn("ip1,ip2,ip3");
        when(kuduTableInfo.getTableName()).thenReturn("impala::default.test");
        kuduSink.genStreamSink(kuduTableInfo);
    }

    @Test
    public void testeEmitDataStream() {
        DataStream dataStream = mock(DataStream.class);
        DataStreamSink dataStreamSink = mock(DataStreamSink.class);
        when(dataStream.addSink(any())).thenReturn(dataStreamSink);
        Whitebox.setInternalState(kuduSink, "tableName", "tableName");
        Whitebox.setInternalState(kuduSink, "kuduMasters", "kuduMasters");
        kuduSink.emitDataStream(dataStream);
    }
}
