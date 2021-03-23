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

package com.dtstack.flink.sql.side.kudu.table;

import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author: chuixue
 * @create: 2020-08-10 11:29
 * @description:
 **/
public class KuduSideParserTest {
    private KuduSideParser parser = new KuduSideParser();

    @Test
    public void testGetTableInfo() {
        Map<String, Object> props = Maps.newHashMap();
        props.put("parallelism", "12");
        props.put("kuduMasters", "localhost");
        props.put("tableName", "tableName");
        props.put("workerCount", "12");
        props.put("defaultOperationTimeoutMs", "111");
        props.put("defaultSocketReadTimeoutMs", "333");
        props.put("batchSizeBytes", "333");
        props.put("limitNum", "333");
        props.put("isFaultTolerant", "true");
        props.put("primaryKey", "mid");
        props.put("lowerBoundPrimaryKey", "lower");
        props.put("upperBoundPrimaryKey", "upper");

        parser.getTableInfo("Mykafka", "   mid  int ,   mbb varchar,   sid varchar,   sbb varchar, PRIMARY KEY(mid),  PERIOD FOR SYSTEM_TIME", props);

        assertEquals(parser, parser);
    }

    @Test
    public void testDbTypeConvertToJavaType() {
        String[] fieldTypes = new String[]{"bool", "int8", "int16", "int", "int64", "string", "float", "double", "date", "unixtime_micros", "decimal", "binary"};
        for (String fieldType : fieldTypes) {
            parser.dbTypeConvertToJavaType(fieldType);
        }
    }
}
