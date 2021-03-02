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

package com.dtstack.flink.sql.sink.redis;

import com.dtstack.flink.sql.sink.redis.table.RedisTableInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

/**
 * @author: chuixue
 * @create: 2020-07-20 10:01
 * @description:
 **/
public class RedisSinkTest {
    private RedisSink redisSink;
    private RedisTableInfo targetTableInfo;

    @Before
    public void setUp() {
        targetTableInfo = new RedisTableInfo();
        List pks = new ArrayList<>();
        pks.add("mid");
        targetTableInfo.setUrl("localhost:6379");
        targetTableInfo.setDatabase("db0");
        targetTableInfo.setPassword("root");
        targetTableInfo.setTablename("sideTable");
        targetTableInfo.setPrimaryKeys(pks);
        targetTableInfo.setRedisType("1");
        targetTableInfo.setMaxTotal("1000");
        targetTableInfo.setMaxIdle("10");
        targetTableInfo.setMinIdle("1");
        targetTableInfo.setMasterName("redis");
        targetTableInfo.setTimeout(10000);
        redisSink = new RedisSink();
        redisSink.genStreamSink(targetTableInfo);
        assertEquals(redisSink, redisSink);
    }

    @Test
    public void testEmitDataStream() {
        DataStream<Tuple2<Boolean, Row>> dataStream = PowerMockito.mock(DataStream.class);

        DataStreamSink dataStreamSink = PowerMockito.mock(DataStreamSink.class);
        when(dataStream.addSink(any())).thenReturn(dataStreamSink);
        when(dataStreamSink.setParallelism(anyInt())).thenReturn(dataStreamSink);

        redisSink.emitDataStream(dataStream);
    }
}
