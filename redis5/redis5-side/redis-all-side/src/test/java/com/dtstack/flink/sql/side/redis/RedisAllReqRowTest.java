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

import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.redis.table.RedisSideReqRow;
import com.dtstack.flink.sql.side.redis.table.RedisSideTableInfo;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-20 14:43
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({BaseAllReqRow.class
        , RedisAllSideInfo.class
        , RedisAllReqRow.class
        , JedisPool.class
        , RedisSideReqRow.class})
@PowerMockIgnore({"javax.*"})
public class RedisAllReqRowTest {

    private RedisAllReqRow redisAllReqRow;
    private RowTypeInfo rowTypeInfo;
    private JoinInfo joinInfo;
    private List<FieldInfo> outFieldInfoList = new ArrayList<>();
    private RedisSideTableInfo tableInfo;
    private BaseSideInfo sideInfo;
    private AtomicReference<Map<String, Map<String, String>>> cacheRef = new AtomicReference<>();

    @Before
    public void setUp() {
        rowTypeInfo = PowerMockito.mock(RowTypeInfo.class);
        joinInfo = PowerMockito.mock(JoinInfo.class);
        tableInfo = PowerMockito.mock(RedisSideTableInfo.class);
        List<String> primaryKeys = Lists.newArrayList();
        primaryKeys.add("rowkey");

        suppress(constructor(BaseAllReqRow.class));
        suppress(constructor(RedisAllSideInfo.class));
        when(tableInfo.getTableName()).thenReturn("sideTable");
        when(tableInfo.getPrimaryKeys()).thenReturn(primaryKeys);

        redisAllReqRow = new RedisAllReqRow(rowTypeInfo, joinInfo, outFieldInfoList, tableInfo);
        Whitebox.setInternalState(redisAllReqRow, "tableInfo", tableInfo);
    }

    @Test
    public void testReloadCache() throws Exception {
        JedisPool jedisPool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);

        when(tableInfo.getUrl()).thenReturn("localhost:6379,localhost:6379");
        when(tableInfo.getPassword()).thenReturn("pass");
        when(tableInfo.getMaxTotal()).thenReturn("100");
        when(tableInfo.getMaxIdle()).thenReturn("100");
        when(tableInfo.getMinIdle()).thenReturn("100");
        when(tableInfo.getRedisType()).thenReturn(1);
        PowerMockito.whenNew(JedisPool.class).withAnyArguments().thenReturn(jedisPool);
        when(jedisPool.getResource()).thenReturn(jedis);

        redisAllReqRow.reloadCache();
    }

    //@Test
    public void testFlatMap() throws Exception {
        sideInfo = PowerMockito.mock(BaseSideInfo.class);
        cacheRef.set(Maps.newHashMap());

        List<Integer> equalValIndex = Lists.newArrayList();
        equalValIndex.add(0);

        List<String> equalFieldList = Lists.newArrayList();
        equalFieldList.add("rowkey");

        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, "bbbbbb");
        row.setField(2, "2020-07-14 01:27:43.969");
        Collector<BaseRow> out = mock(Collector.class);

        Whitebox.setInternalState(redisAllReqRow, "sideInfo", sideInfo);
        Whitebox.setInternalState(redisAllReqRow, "cacheRef", cacheRef);
        when(sideInfo.getEqualValIndex()).thenReturn(equalValIndex);
        when(sideInfo.getJoinType()).thenReturn(JoinType.INNER);
        when(sideInfo.getEqualFieldList()).thenReturn(equalFieldList);
        suppress(RedisSideReqRow.class.getMethod("fillData", Row.class, Object.class));

        redisAllReqRow.flatMap(row, out);
    }
}
