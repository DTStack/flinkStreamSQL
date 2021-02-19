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

import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-20 10:33
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({RedisOutputFormat.class})
public class RedisOutputFormatTest {
    @InjectMocks
    private RedisOutputFormat redisOutputFormat;
    private Jedis jedis;
    private Counter outRecords;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        jedis = mock(Jedis.class);
        outRecords = mock(Counter.class);
        Whitebox.setInternalState(redisOutputFormat, "outRecords", outRecords);
        MemberModifier.field(RedisOutputFormat.class, "maxTotal").set(redisOutputFormat, "10000");
        MemberModifier.field(RedisOutputFormat.class, "maxIdle").set(redisOutputFormat, "1000");
        MemberModifier.field(RedisOutputFormat.class, "minIdle").set(redisOutputFormat, "10");
        MemberModifier.field(RedisOutputFormat.class, "url").set(redisOutputFormat, "localhost:6379");
        MemberModifier.field(RedisOutputFormat.class, "redisType").set(redisOutputFormat, 1);
        MemberModifier.field(RedisOutputFormat.class, "jedis").set(redisOutputFormat, jedis);

        List pys = Lists.newArrayList();
        pys.add("mid");
        MemberModifier.field(RedisOutputFormat.class, "fieldNames").set(redisOutputFormat, new String[]{"mid", "mbb", "sid", "sbb"});
        MemberModifier.field(RedisOutputFormat.class, "primaryKeys").set(redisOutputFormat, pys);
        MemberModifier.field(RedisOutputFormat.class, "tableName").set(redisOutputFormat, "sinkTable");
        MemberModifier.field(RedisOutputFormat.class, "database").set(redisOutputFormat, "0");
    }

    @Test
    public void testOpen() throws Exception {
        JedisPool jedisPool = mock(JedisPool.class);

        PowerMockito.whenNew(JedisPool.class).withAnyArguments().thenReturn(jedisPool);
        when(jedisPool.getResource()).thenReturn(jedis);
        suppress(AbstractDtRichOutputFormat.class.getMethod("initMetric"));
        redisOutputFormat.open(1, 1);
    }

    @Test
    public void testWriteRecord() throws IOException {
        Row row = new Row(4);
        row.setField(0, "1");
        row.setField(1, "bbbbbb");
        row.setField(2, null);
        row.setField(3, null);
        Tuple2 records = new Tuple2(true, row);

        redisOutputFormat.writeRecord(records);
    }
}
