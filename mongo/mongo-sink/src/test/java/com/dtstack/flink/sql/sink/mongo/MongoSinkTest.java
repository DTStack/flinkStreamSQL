package com.dtstack.flink.sql.sink.mongo;/*
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

import com.dtstack.flink.sql.sink.mongo.table.MongoTableInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

/**
 * @author: chuixue
 * @create: 2020-07-24 16:52
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataStream.class,
        MongoSink.class})
public class MongoSinkTest {
    @InjectMocks
    MongoSink mongoSink;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void testEmitDataStream() throws IllegalAccessException {
        MemberModifier.field(MongoSink.class, "address").set(mongoSink, "localhost");
        MemberModifier.field(MongoSink.class, "database").set(mongoSink, "/hbase");
        MemberModifier.field(MongoSink.class, "tableName").set(mongoSink, "tableName");
        MemberModifier.field(MongoSink.class, "userName").set(mongoSink, "rowkey");
        MemberModifier.field(MongoSink.class, "password").set(mongoSink, "rowkey");

        DataStream dataStream = PowerMockito.mock(DataStream.class);

        DataStreamSink dataStreamSink = PowerMockito.mock(DataStreamSink.class);
        when(dataStream.addSink(any())).thenReturn(dataStreamSink);
        when(dataStreamSink.setParallelism(anyInt())).thenReturn(dataStreamSink);

        mongoSink.emitDataStream(dataStream);
    }

    @Test
    public void testGenStreamSink() {
        MongoTableInfo mongoTableInfo = new MongoTableInfo();
        mongoTableInfo.setAddress("172.16.8.193:27017");
        mongoTableInfo.setTableName("userInfo");
        mongoTableInfo.setUserName("name");
        mongoTableInfo.setPassword("pass");
        mongoTableInfo.setDatabase("dtstack");
        Assert.assertNotEquals(mongoTableInfo, mongoSink.genStreamSink(mongoTableInfo));
    }
}
