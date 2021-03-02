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
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;
import static org.mockito.Mockito.when;

/**
 * @author: chuixue
 * @create: 2020-07-27 11:08
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({MongoAllSideInfo.class
        , ParseUtils.class
        , JoinInfo.class
        , SqlBasicCall.class
        , BaseSideInfo.class})
public class MongoAllSideInfoTest {

    private MongoAllSideInfo mongoAllSideInfo;
    private JoinInfo joinInfo;
    private AbstractSideTableInfo sideTableInfo;

    @Before
    public void setUp() {
        joinInfo = mock(JoinInfo.class);
        sideTableInfo = mock(AbstractSideTableInfo.class);

        List<FieldInfo> outFieldInfoList = Lists.newArrayList();
        FieldInfo fieldInfo = new FieldInfo();
        fieldInfo.setTable("m");
        fieldInfo.setFieldName("_id");
        fieldInfo.setTypeInformation(TypeInformation.of(String.class));
        outFieldInfoList.add(fieldInfo);

        FieldInfo fieldInfo2 = new FieldInfo();
        fieldInfo2.setTable("s");
        fieldInfo2.setFieldName("name");
        fieldInfo2.setTypeInformation(TypeInformation.of(String.class));
        outFieldInfoList.add(fieldInfo2);

        Map<Integer, Integer> sideFieldIndex = Maps.newHashMap();
        Map<Integer, String> sideFieldNameIndex = Maps.newHashMap();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)}, new String[]{"id", "bb", "PROCTIME"});
        Map<Integer, Integer> inFieldIndex = Maps.newHashMap();
        List<String> equalFieldList = Lists.newArrayList();
        equalFieldList.add("_id");

        suppress(constructor(BaseSideInfo.class));
        mongoAllSideInfo = new MongoAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);

        Whitebox.setInternalState(mongoAllSideInfo, "outFieldInfoList", outFieldInfoList);
        Whitebox.setInternalState(mongoAllSideInfo, "sideFieldIndex", sideFieldIndex);
        Whitebox.setInternalState(mongoAllSideInfo, "sideFieldNameIndex", sideFieldNameIndex);
        Whitebox.setInternalState(mongoAllSideInfo, "rowTypeInfo", rowTypeInfo);
        Whitebox.setInternalState(mongoAllSideInfo, "inFieldIndex", inFieldIndex);
        Whitebox.setInternalState(mongoAllSideInfo, "equalFieldList", equalFieldList);
    }

    @Test
    public void testParseSelectFields() {
        mockStatic(ParseUtils.class);

        SqlBasicCall conditionNode = mock(SqlBasicCall.class);

        when(joinInfo.getSideTableName()).thenReturn("s");
        when(joinInfo.getNonSideTable()).thenReturn("m");
        when(joinInfo.getCondition()).thenReturn(conditionNode);
        suppress(method(ParseUtils.class, "parseAnd", SqlNode.class, List.class));
        mongoAllSideInfo.parseSelectFields(joinInfo);
    }

}
