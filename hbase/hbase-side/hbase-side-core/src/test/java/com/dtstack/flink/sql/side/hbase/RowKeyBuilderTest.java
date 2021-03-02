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

package com.dtstack.flink.sql.side.hbase;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.hbase.enums.EReplaceType;
import com.dtstack.flink.sql.util.TableUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-08 20:44
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({RowKeyBuilder.class, TableUtils.class})
public class RowKeyBuilderTest {
    @InjectMocks
    RowKeyBuilder rowKeyBuilder;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testInit() {
        rowKeyBuilder.init("md5(rowkey)");
    }

    @Test
    public void testGetRowKey() throws IllegalAccessException {
        List list = new ArrayList<ReplaceInfo>();
        ReplaceInfo replaceInfoa = new ReplaceInfo(EReplaceType.CONSTANT);
        replaceInfoa.setParam("f:a");

        ReplaceInfo replaceInfob = new ReplaceInfo(EReplaceType.FUNC);
        replaceInfob.setParam("f:b");

        ReplaceInfo replaceInfoc = new ReplaceInfo(EReplaceType.PARAM);
        replaceInfoc.setParam("f:c");
        list.add(replaceInfoa);
        list.add(replaceInfob);
        list.add(replaceInfoc);

        MemberModifier.field(RowKeyBuilder.class, "operatorChain").set(rowKeyBuilder, list);

        Map<String, Object> columnNameFamily = new HashMap();
        columnNameFamily.put("f:a", "a");
        columnNameFamily.put("f:b", "b");
        columnNameFamily.put("f:c", "c");

        suppress(method(TableUtils.class, "addConstant", Map.class, AbstractSideTableInfo.class));

        rowKeyBuilder.getRowKey(columnNameFamily);
    }
}
