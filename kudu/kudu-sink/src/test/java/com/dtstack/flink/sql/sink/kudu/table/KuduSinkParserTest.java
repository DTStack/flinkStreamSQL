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

package com.dtstack.flink.sql.sink.kudu.table;

import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author: chuixue
 * @create: 2020-08-10 11:17
 * @description:
 **/
public class KuduSinkParserTest {
    private KuduSinkParser parser = new KuduSinkParser();

    @Test
    public void testGetTableInfo() {
        Map<String, Object> props = Maps.newHashMap();
        props.put("parallelism", "12");
        props.put("kuduMasters", "localhost");
        props.put("tableName", "tableName");
        props.put("writemode", "update");
        props.put("workerCount", "12");
        props.put("defaultOperationTimeoutMs", "111");
        props.put("defaultSocketReadTimeoutMs", "333");

        parser.getTableInfo("Mykafka", "   mid  int ,   mbb varchar,   sid varchar,   sbb varchar ", props);

        assertEquals(parser, parser);
    }

    @Test
    public void testDbTypeConvertToJavaType() {
        String[] types = new String[]{"boolean", "int8", "int16", "int32", "long", "string", "float", "double", "date", "unixtime_micros", "decimal", "binary"};
        for (String type : types) {
            parser.dbTypeConvertToJavaType(type);
        }
    }
}
