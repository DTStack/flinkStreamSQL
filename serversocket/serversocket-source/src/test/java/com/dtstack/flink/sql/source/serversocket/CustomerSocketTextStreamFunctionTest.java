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
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.BufferedReader;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;


/**
 * @author: chuixue
 * @create: 2020-08-03 10:14
 * @description:
 **/
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({BufferedReader.class,
//        CustomerSocketTextStreamFunction.class})
public class CustomerSocketTextStreamFunctionTest {

    private ServersocketSourceTableInfo tableInfo = mock(ServersocketSourceTableInfo.class);
    private CustomerSocketTextStreamFunction customerSocketTextStreamFunction;

//    @Before
    public void setUp() {
        TypeInformation typeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class)}, new String[]{"id", "name"});
        Map<String, String> rowAndFieldMapping = Maps.newHashMap();
        List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfos = Lists.newArrayList();

//        customerSocketTextStreamFunction
//                = new CustomerSocketTextStreamFunction(tableInfo, typeInfo, rowAndFieldMapping, fieldExtraInfos);
    }

//    @Test
    public void testRun() throws Exception {
        BufferedReader reader = mock(BufferedReader.class);
        char[] cbuf = new char[8192];
        Socket socket = mock(Socket.class);
        InputStream inputStream = mock(InputStream.class);

        whenNew(Socket.class).withNoArguments().thenReturn(socket);
        suppress(Socket.class.getMethod("connect", SocketAddress.class, int.class));
        when(socket.getInputStream()).thenReturn(inputStream);

        whenNew(BufferedReader.class).withAnyArguments().thenReturn(reader);
        when(tableInfo.getHostname()).thenReturn("localhost");
        when(tableInfo.getPort()).thenReturn(9090);
        when(tableInfo.getDelimiter()).thenReturn("|");
        when(reader.read(cbuf)).thenReturn(-1);

        SourceFunction.SourceContext<Row> ctx = mock(SourceFunction.SourceContext.class);
        customerSocketTextStreamFunction.run(ctx);
    }

//    @Test
    public void testCancel() {
        Socket currentSocket = mock(Socket.class);
        Whitebox.setInternalState(customerSocketTextStreamFunction, "currentSocket", currentSocket);
        customerSocketTextStreamFunction.cancel();
    }
}
