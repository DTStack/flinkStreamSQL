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

package com.dtstack.flink.sql.side.cassandra.table;

import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

/**
 * @author: chuixue
 * @create: 2020-07-28 10:55
 * @description:
 **/
public class CassandraSideParserTest {

//    @Test
    public void testGetTableInfo() {
        Map<String, Object> props = Maps.newHashMap();
        props.put("database", "cx");
        props.put("primarykeys", "mid");
        props.put("address", "localhost:9042");
        props.put("type", "cassandra");
        props.put("tablename", "sinkTable");

        CassandraSideParser cassandraSideParser = new CassandraSideParser();
        cassandraSideParser.getTableInfo("Mykafka", "   rowkey int,\n" +
                "  channel  varchar ,\n" +
                "  name varchar  ,\n" +
                "  PRIMARY  KEY  (rowkey),\n" +
                "  PERIOD  FOR  SYSTEM_TIME ", props);
    }
}
