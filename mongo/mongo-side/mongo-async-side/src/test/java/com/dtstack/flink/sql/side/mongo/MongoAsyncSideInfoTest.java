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

package com.dtstack.flink.sql.side.mongo;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.mongo.table.MongoSideTableInfo;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-27 15:11
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({MongoAsyncSideInfo.class
        , BaseSideInfo.class})
public class MongoAsyncSideInfoTest {

    private MongoAsyncSideInfo mongoAsyncSideInfo;
    private RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)}, new String[]{"id", "bb", "PROCTIME"});
    private JoinInfo joinInfo;
    private List<FieldInfo> outFieldInfoList = new ArrayList<>();
    private MongoSideTableInfo sideTableInfo;
    private List<String> equalFieldList = Lists.newArrayList();
    private List<Integer> equalValIndex = Lists.newArrayList();

    @Before
    public void setUp() {
        joinInfo = mock(JoinInfo.class);
        sideTableInfo = mock(MongoSideTableInfo.class);

        suppress(constructor(BaseSideInfo.class));
        mongoAsyncSideInfo = new MongoAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        Whitebox.setInternalState(mongoAsyncSideInfo, "equalFieldList", equalFieldList);
        Whitebox.setInternalState(mongoAsyncSideInfo, "rowTypeInfo", rowTypeInfo);
        Whitebox.setInternalState(mongoAsyncSideInfo, "equalValIndex", equalValIndex);
    }

    @Test
    public void testBuildEqualInfo() {
        JoinInfo joinInfo = mock(JoinInfo.class);
        AbstractSideTableInfo sideTableInfo = mock(AbstractSideTableInfo.class);
        SqlBinaryOperator equalsOperators = SqlStdOperatorTable.EQUALS;
        SqlNode[] operands = new SqlNode[2];
        List<String> one = Lists.newArrayList();
        one.add("m");
        one.add("id");
        List<String> two = Lists.newArrayList();
        two.add("s");
        two.add("_id");
        operands[0] = new SqlIdentifier(one, new SqlParserPos(0, 0));
        operands[1] = new SqlIdentifier(two, new SqlParserPos(0, 0));
        SqlBasicCall sqlBasicCall = new SqlBasicCall(equalsOperators, operands, SqlParserPos.ZERO);
        when(joinInfo.getSideTableName()).thenReturn("s");
        when(joinInfo.getCondition()).thenReturn(sqlBasicCall);

        mongoAsyncSideInfo.buildEqualInfo(joinInfo, sideTableInfo);

        RowTypeInfo rowTypeInfo2 = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class)}, new String[]{"_id"});
        Whitebox.setInternalState(mongoAsyncSideInfo, "rowTypeInfo", rowTypeInfo2);
        when(joinInfo.getSideTableName()).thenReturn("m");
        mongoAsyncSideInfo.buildEqualInfo(joinInfo, sideTableInfo);
    }
}
