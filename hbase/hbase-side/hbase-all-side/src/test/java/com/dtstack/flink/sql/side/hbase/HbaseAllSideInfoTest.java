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

import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-16 20:24
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({HbaseAllSideInfo.class
        , BaseSideInfo.class
        , ParseUtils.class
        , TypeExtractor.class
        , LoggerFactory.class})
public class HbaseAllSideInfoTest {

    private HbaseAllSideInfo hbaseAllSideInfo;

    @Test
    public void testBuildEqualInfo() {
        Logger log = mock(Logger.class);
        mockStatic(LoggerFactory.class);
        mockStatic(ParseUtils.class);
        JoinInfo joinInfo = mock(JoinInfo.class);
        HbaseSideTableInfo sideTableInfo = mock(HbaseSideTableInfo.class);
        when(sideTableInfo.getColumnRealNames()).thenReturn(new String[]{"cf:a"});
        when(sideTableInfo.getFieldTypes()).thenReturn(new String[]{"varchar"});
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)}, new String[]{"id", "bb", "PROCTIME"});
        List<FieldInfo> outFieldInfoList = new ArrayList<>();
        SqlNode sqlNode = mock(SqlNode.class);

        when(LoggerFactory.getLogger(TypeExtractor.class)).thenReturn(log);
        suppress(constructor(BaseSideInfo.class));
        when(sideTableInfo.getPrimaryKeys()).thenReturn(Lists.newArrayList("rowkey"));
        when(joinInfo.getSideTableName()).thenReturn("s");
        when(joinInfo.getCondition()).thenReturn(sqlNode);
        suppress(method(ParseUtils.class, "parseAnd", SqlNode.class, List.class));

        hbaseAllSideInfo = new HbaseAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        hbaseAllSideInfo.buildEqualInfo(joinInfo, sideTableInfo);

    }
}
