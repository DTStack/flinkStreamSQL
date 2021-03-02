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

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.redis.table.RedisSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-22 17:13
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({
        ParseUtils.class
        , RedisAsyncSideInfo.class
})
public class RedisAsyncSideInfoTest {
    private RowTypeInfo rowTypeInfo;
    private JoinInfo joinInfo;
    private List<FieldInfo> outFieldInfoList = new ArrayList<>();
    private AbstractSideTableInfo sideTableInfo;
    private RedisAsyncSideInfo redisAsyncSideInfo;

    @Before
    public void setUp() {
        rowTypeInfo = PowerMockito.mock(RowTypeInfo.class);
        joinInfo = PowerMockito.mock(JoinInfo.class);
        sideTableInfo = PowerMockito.mock(RedisSideTableInfo.class);
        suppress(constructor(BaseSideInfo.class));
        redisAsyncSideInfo = new RedisAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Test
    public void testBuildEqualInfo() {
        SqlNode conditionNode = PowerMockito.mock(SqlNode.class);
        PowerMockito.when(joinInfo.getSideTableName()).thenReturn("dd");
        PowerMockito.when(joinInfo.getCondition()).thenReturn(conditionNode);
        suppress(method(ParseUtils.class, "parseAnd", SqlNode.class, List.class));
        redisAsyncSideInfo.buildEqualInfo(joinInfo, sideTableInfo);
    }
}
