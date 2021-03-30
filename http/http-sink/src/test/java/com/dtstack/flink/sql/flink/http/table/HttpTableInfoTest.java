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

package com.dtstack.flink.sql.flink.http.table;

import com.dtstack.flink.sql.sink.http.table.HttpSinkParser;
import com.dtstack.flink.sql.sink.http.table.HttpTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

/**
 * @author tiezhu
 * @date 2021/3/18 星期四
 * Company dtstack
 */
public class HttpTableInfoTest {
    private static final HttpSinkParser parser = new HttpSinkParser();
    private HashMap<String, Object> map;

    @Before
    public void setUp() {
        map = new HashMap<>();
        map.put("type", "http");
        map.put("url", "localhost:9999");
        map.put("delay", "20");
        map.put("flag", "7f8717b2-110c-4012-8e3c-c1965e84ee75");
    }

    @Test
    public void parsePropertiesAndGetTableInfo() throws Exception {
        AbstractTableInfo tableInfo = parser.getTableInfo(
            "Test",
            "id int, name string",
            map
        );
        HttpTableInfo httpTableInfo = (HttpTableInfo) tableInfo;
        Assert.assertEquals("http", httpTableInfo.getType());
        Assert.assertEquals(20, httpTableInfo.getDelay());
    }
}
