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

import com.dtstack.flink.sql.side.hbase.table.HbaseSideParser;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: chuixue
 * @create: 2020-07-09 09:19
 * @description:
 **/
public class HbaseSideParserTest {
    @Test
    public void testGetTableInfo() {
        String fieldsInfo = "cf:name  varchar  as  f1," +
                "  cf:info  varchar  as  f2,  " +
                "  cf:other varchar  as  f3" +
                "  , PRIMARY  KEY  (f1)";

        Map<String, Object> prop = new HashMap();

        prop.put("type", "hbase");
        prop.put("zookeeperQuorum", "172.16.101.247:2181");
        prop.put("zookeeperParent", "/hbase");
        prop.put("tableName", "test");

        HbaseSideParser hbaseSideParser = new HbaseSideParser();
        hbaseSideParser.getTableInfo("tableName", fieldsInfo, prop);
    }
}
