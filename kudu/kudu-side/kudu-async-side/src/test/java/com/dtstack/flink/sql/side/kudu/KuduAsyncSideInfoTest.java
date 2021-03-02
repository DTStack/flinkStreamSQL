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

package com.dtstack.flink.sql.side.kudu;

import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.kudu.table.KuduSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
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

import java.util.List;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-08-11 13:45
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({ParseUtils.class,
        KuduAsyncSideInfo.class,
        BaseSideInfo.class})
public class KuduAsyncSideInfoTest {

    private KuduAsyncSideInfo kuduAsyncSideInfo;

    @Before
    public void setUp() {
        suppress(constructor(BaseSideInfo.class));
        kuduAsyncSideInfo = new KuduAsyncSideInfo(null, null, null, null);
    }

    @Test
    public void testBuildEqualInfo() {
        JoinInfo joinInfo = mock(JoinInfo.class);
        KuduSideTableInfo kuduSideTableInfo = mock(KuduSideTableInfo.class);

        SqlBinaryOperator equalsOperators = SqlStdOperatorTable.EQUALS;
        SqlNode[] operands = new SqlNode[2];
        List<String> one = Lists.newArrayList();
        one.add("m");
        one.add("id");
        List<String> two = Lists.newArrayList();
        two.add("s");
        two.add("rowkey");
        operands[0] = new SqlIdentifier(one, new SqlParserPos(0, 0));
        operands[1] = new SqlIdentifier(two, new SqlParserPos(0, 0));
        SqlBasicCall sqlBasicCall = new SqlBasicCall(equalsOperators, operands, SqlParserPos.ZERO);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)}, new String[]{"id", "rowkey", "PROCTIME"});

        Whitebox.setInternalState(kuduAsyncSideInfo, "sideSelectFields", "sideSelectFields");
        Whitebox.setInternalState(kuduAsyncSideInfo, "equalFieldList", Lists.newArrayList());
        Whitebox.setInternalState(kuduAsyncSideInfo, "equalValIndex", Lists.newArrayList());
        Whitebox.setInternalState(kuduAsyncSideInfo, "rowTypeInfo", rowTypeInfo);
        when(kuduSideTableInfo.getTableName()).thenReturn("ddd");
        when(joinInfo.getSideTableName()).thenReturn("m");
        when(joinInfo.getNonSideTable()).thenReturn("s");
        when(joinInfo.getCondition()).thenReturn(sqlBasicCall);

        kuduAsyncSideInfo.buildEqualInfo(joinInfo, kuduSideTableInfo);
    }
}
