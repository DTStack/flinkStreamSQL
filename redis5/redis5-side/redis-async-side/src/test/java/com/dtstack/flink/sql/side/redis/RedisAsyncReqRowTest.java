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

package com.dtstack.flink.sql.side.redis;

import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.redis.table.RedisSideTableInfo;
import com.google.common.collect.Maps;
import io.lettuce.core.RedisAsyncCommandsImpl;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.StringCodec;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-22 09:12
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({BaseAsyncReqRow.class
        , RedisAsyncSideInfo.class
        , RedisClient.class
        , RedisAsyncReqRow.class
        , RedisHashAsyncCommands.class
        , RedisSideTableInfo.class
        , RedisClusterClient.class
        , StatefulRedisClusterConnection.class})
public class RedisAsyncReqRowTest {
    private RedisAsyncReqRow redisAsyncReqRow;
    private RowTypeInfo rowTypeInfo;
    private JoinInfo joinInfo;
    private List<FieldInfo> outFieldInfoList = new ArrayList<>();
    private BaseSideInfo sideInfo;
    private RedisSideTableInfo redisSideTableInfo;
    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;
    private StatefulRedisClusterConnection<String, String> clusterConnection;
    private RedisKeyAsyncCommands<String, String> async = new RedisAsyncCommandsImpl<>(null, new StringCodec());
    private RedisClusterClient clusterClient;

    @Captor
    private ArgumentCaptor<Consumer<Map<String, String>>> cb;

    @Before
    public void setUp() {
        rowTypeInfo = PowerMockito.mock(RowTypeInfo.class);
        joinInfo = PowerMockito.mock(JoinInfo.class);
        sideInfo = PowerMockito.mock(BaseSideInfo.class);
        redisSideTableInfo = PowerMockito.mock(RedisSideTableInfo.class);
        redisClient = mock(RedisClient.class);
        connection = mock(StatefulRedisConnection.class);
        clusterConnection = mock(StatefulRedisClusterConnection.class);
        clusterClient = mock(RedisClusterClient.class);

        List<String> primaryKeys = Lists.newArrayList();
        primaryKeys.add("rowkey");

        suppress(constructor(RedisAsyncSideInfo.class));
        suppress(constructor(BaseAsyncReqRow.class));

        redisAsyncReqRow = new RedisAsyncReqRow(rowTypeInfo, joinInfo, outFieldInfoList, redisSideTableInfo);
        Whitebox.setInternalState(redisAsyncReqRow, "sideInfo", sideInfo);
        Whitebox.setInternalState(redisAsyncReqRow, "redisSideTableInfo", redisSideTableInfo);
        Whitebox.setInternalState(redisAsyncReqRow, "connection", connection);
        Whitebox.setInternalState(redisAsyncReqRow, "redisClient", redisClient);
        Whitebox.setInternalState(redisAsyncReqRow, "clusterConnection", clusterConnection);
        Whitebox.setInternalState(redisAsyncReqRow, "clusterClient", clusterClient);
        Whitebox.setInternalState(redisAsyncReqRow, "async", async);
    }

    @Test
    public void testOpen() throws Exception {
        PowerMockito.mockStatic(RedisClient.class);

        when(RedisClient.create(RedisURI.create("redis://localhost:6379,localhost:6379"))).thenReturn(redisClient);
        when(redisClient.connect()).thenReturn(connection);
        suppress(BaseAsyncReqRow.class.getMethod("open", Configuration.class));
        when(sideInfo.getSideTableInfo()).thenReturn(redisSideTableInfo);
        when(redisSideTableInfo.getUrl()).thenReturn("localhost:6379,localhost:6379");
        when(redisSideTableInfo.getPassword()).thenReturn("pass");
        when(redisSideTableInfo.getMasterName()).thenReturn("master");

        when(redisSideTableInfo.getRedisType()).thenReturn(1);
        redisAsyncReqRow.open(new Configuration());

        PowerMockito.mockStatic(RedisClusterClient.class);
        when(RedisClusterClient.create(anyString())).thenReturn(clusterClient);
        when(clusterClient.connect()).thenReturn(clusterConnection);
        redisAsyncReqRow.open(new Configuration());
    }

    @Test
    public void testBuildCacheKey() throws Exception {
        List<String> primaryKeys = Lists.newArrayList();
        primaryKeys.add("rowkey");
        Map<String, Object> inputParams = Maps.newHashMap();
        inputParams.put("rowkey", 1);

        when(redisSideTableInfo.getTableName()).thenReturn("sideTable");
        when(redisSideTableInfo.getPrimaryKeys()).thenReturn(primaryKeys);

        Assert.assertEquals("sideTable_1", redisAsyncReqRow.buildCacheKey(inputParams));
    }

    @Test
    public void testClose() throws Exception {
        suppress(BaseAsyncReqRow.class.getMethod("close"));
        redisAsyncReqRow.close();
    }
}
