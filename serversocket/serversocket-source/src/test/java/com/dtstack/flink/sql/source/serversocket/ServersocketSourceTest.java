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

package com.dtstack.flink.sql.source.serversocket;

import com.dtstack.flink.sql.source.serversocket.table.ServersocketSourceTableInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * @author: chuixue
 * @create: 2020-08-03 09:54
 * @description:
 **/
@RunWith(PowerMockRunner.class)
public class ServersocketSourceTest {

    @Test
    public void testGenStreamSource() {
        ServersocketSourceTableInfo serversocketSourceTableInfo = mock(ServersocketSourceTableInfo.class);
        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);
        StreamTableEnvironment tableEnv = mock(StreamTableEnvironment.class);
        ServersocketSource serversocketSource = new ServersocketSource();

        when(serversocketSourceTableInfo.getName()).thenReturn("test");
        when(serversocketSourceTableInfo.getFields()).thenReturn(new String[]{"id", "name"});
        when(serversocketSourceTableInfo.getFieldClasses()).thenReturn(new Class[]{Integer.class, String.class});

        serversocketSource.genStreamSource(serversocketSourceTableInfo, env, tableEnv);
    }
}
