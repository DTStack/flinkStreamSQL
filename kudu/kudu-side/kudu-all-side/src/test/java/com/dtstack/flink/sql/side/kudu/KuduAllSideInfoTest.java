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
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlNode;
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
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;


/**
 * @author: chuixue
 * @create: 2020-08-10 13:48
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({ParseUtils.class,
        KuduAllSideInfo.class,
        BaseSideInfo.class
})
public class KuduAllSideInfoTest {

    private KuduAllSideInfo kuduAllSideInfo;

    @Before
    public void setUp() {
        suppress(constructor(BaseSideInfo.class));
        kuduAllSideInfo = new KuduAllSideInfo(null, null, null, null);
    }

    @Test
    public void testParseSelectFields() {
        List<FieldInfo> outFieldInfoList = Lists.newArrayList();
        FieldInfo fieldInfo = new FieldInfo();
        fieldInfo.setTable("m");
        fieldInfo.setFieldName("id");
        fieldInfo.setTypeInformation(TypeInformation.of(Integer.class));
        FieldInfo fieldInfo2 = new FieldInfo();
        fieldInfo2.setTable("s");
        fieldInfo2.setFieldName("aa");
        fieldInfo2.setTypeInformation(TypeInformation.of(Integer.class));
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo2);
        outFieldInfoList.add(fieldInfo2);
        JoinInfo joinInfo = mock(JoinInfo.class);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)}, new String[]{"id", "bb", "PROCTIME"});
        List<String> equalFieldList = Lists.newArrayList();
        equalFieldList.add("a");
        equalFieldList.add("b");

        Whitebox.setInternalState(kuduAllSideInfo, "rowTypeInfo", rowTypeInfo);
        Whitebox.setInternalState(kuduAllSideInfo, "outFieldInfoList", outFieldInfoList);
        Whitebox.setInternalState(kuduAllSideInfo, "sideFieldIndex", Maps.newHashMap());
        Whitebox.setInternalState(kuduAllSideInfo, "sideFieldNameIndex", Maps.newHashMap());
        Whitebox.setInternalState(kuduAllSideInfo, "inFieldIndex", Maps.newHashMap());
        Whitebox.setInternalState(kuduAllSideInfo, "equalFieldList", equalFieldList);

        when(joinInfo.getSideTableName()).thenReturn("m");
        when(joinInfo.getNonSideTable()).thenReturn("s");
        suppress(method(ParseUtils.class, "parseAnd", SqlNode.class, List.class));

        kuduAllSideInfo.parseSelectFields(joinInfo);
    }
}
