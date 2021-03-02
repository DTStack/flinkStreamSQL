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

package com.dtstack.flink.sql.sink.redis.table;

import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author: chuixue
 * @create: 2020-07-20 10:24
 * @description:
 **/
public class RedisSinkParserTest {

    @Test
    public void testGetTableInfo() {
        Map<String, Object> props = Maps.newHashMap();
        props.put("primaryKeys", "mid");
        props.put("redistype", "1");
        props.put("type", "redis");
        props.put("tablename", "sinkTable");
        props.put("url", "localhost:6379");
        RedisSinkParser parser = new RedisSinkParser();
        parser.getTableInfo("Mykafka", "   mid  int ,   mbb varchar,   sid varchar,   sbb varchar ", props);

        assertEquals(parser, parser);
    }
}
