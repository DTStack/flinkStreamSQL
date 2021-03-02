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

package com.dtstack.flink.sql.side.hbase.utils;

import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * @author: chuixue
 * @create: 2020-07-07 15:44
 * @description:
 **/
public class HbaseUtilTest {

    @Test
    public void testColumnTypeToTypeInformation() {
        Tuple2<byte[], String>[] types = new Tuple2[]{new Tuple2("1".getBytes(), "boolean"), new Tuple2("1111".getBytes(),"int"), new Tuple2("11111111".getBytes(),"bigint"), new Tuple2("11111111".getBytes(),"tinyint"), new Tuple2("11".getBytes(),"short"), new Tuple2("1".getBytes(),"char"), new Tuple2("1111".getBytes(),"float"), new Tuple2("11111111".getBytes(),"double"), new Tuple2("1111".getBytes(),"decimal")};
        for (Tuple2<byte[], String> type : types) {
            HbaseUtils.convertByte(type.f0, type.f1);

        }
    }
}
