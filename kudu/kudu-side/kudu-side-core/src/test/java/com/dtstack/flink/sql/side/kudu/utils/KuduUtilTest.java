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

package com.dtstack.flink.sql.side.kudu.utils;

import com.google.common.collect.Maps;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * @author: chuixue
 * @create: 2020-08-10 11:44
 * @description:
 **/
@RunWith(PowerMockRunner.class)
public class KuduUtilTest {

    private Type[] types = new Type[]{Type.STRING, Type.FLOAT, Type.INT8, Type.INT16, Type.INT32, Type.INT64, Type.DOUBLE, Type.BOOL, Type.BINARY};


    @Test
    public void testPrimaryKeyRange() {
        PartialRow partialRow = mock(PartialRow.class);
        for (Type type : types) {
            KuduUtil.primaryKeyRange(partialRow, type, "primaryKey", "1");
        }
    }

    @Test
    public void testSetMapValue() {
        RowResult result = mock(RowResult.class);
        Map<String, Object> oneRow = Maps.newHashMap();
        for (Type type : types) {
            KuduUtil.setMapValue(type, oneRow, "1", result);
        }
    }

    @Test
    public void testGetValue() {
        for (Type type : types) {
            KuduUtil.getValue("1", type);
        }
    }
}
