/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.source.kafka.deserializer;

import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class DtKafkaDeserializerTest {
    @InjectMocks
    DtKafkaDeserializer<byte[]> dtKafkaDeserializer;

    @Before
    public void setUp() {
        dtKafkaDeserializer = new DtKafkaDeserializer<>();
    }

    @Test
    public void testConfigure() {
        Map<String, String> map = new HashMap<>();
        String deserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
        map.put(KafkaSourceTableInfo.DT_KEY_DESERIALIZER, deserializer);
        map.put(KafkaSourceTableInfo.DT_VALUE_DESERIALIZER, deserializer);
        dtKafkaDeserializer.configure(map, true);
        dtKafkaDeserializer.configure(map, false);
        String nonexistentDeserializer = "xxx.xxx.xxx.serialization.XxxDeserializer";
        map.put(KafkaSourceTableInfo.DT_KEY_DESERIALIZER, nonexistentDeserializer);
        try {
            dtKafkaDeserializer.configure(map, true);
        }catch (Exception e){
            Assert.assertEquals("Can't create instance: " + nonexistentDeserializer, e.getMessage());
        }
    }

    @Test
    public void testDeserialize() {
        Map<String, String> map = new HashMap<>();
        String deserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
        map.put(KafkaSourceTableInfo.DT_KEY_DESERIALIZER, deserializer);
        dtKafkaDeserializer.configure(map, true);

        byte[] result = dtKafkaDeserializer.deserialize("topic", null, new byte[]{(byte) 0});
        Assert.assertArrayEquals(new byte[]{(byte) 0}, result);
    }

    @Test
    public void testDeserialize2() {
        Map<String, String> map = new HashMap<>();
        String deserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        map.put(KafkaSourceTableInfo.DT_KEY_DESERIALIZER, deserializer);
        dtKafkaDeserializer.configure(map, true);
        byte[] result = dtKafkaDeserializer.deserialize("topic", new byte[]{(byte) 0});
        Assert.assertArrayEquals(new byte[]{(byte) 0}, result);
    }

    @Test
    public void testClose() {
        Map<String, String> map = new HashMap<>();
        String deserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
        map.put(KafkaSourceTableInfo.DT_KEY_DESERIALIZER, deserializer);
        dtKafkaDeserializer.configure(map, true);
        dtKafkaDeserializer.close();
    }
}
