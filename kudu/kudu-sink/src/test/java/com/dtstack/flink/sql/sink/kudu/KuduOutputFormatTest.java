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

package com.dtstack.flink.sql.sink.kudu;

import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-08-10 10:24
 * @description:
 **/
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({
//        KuduOutputFormat.class
//        , AsyncKuduClient.AsyncKuduClientBuilder.class
//        , AsyncKuduClient.class
//        , KuduClient.class
//        , AbstractDtRichOutputFormat.class})
public class KuduOutputFormatTest {

    @InjectMocks
    private KuduOutputFormat kuduOutputFormat;
    private AsyncKuduClient client = mock(AsyncKuduClient.class);
    private Counter outRecords = mock(Counter.class);

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Whitebox.setInternalState(kuduOutputFormat, "client", client);
        Whitebox.setInternalState(kuduOutputFormat, "outRecords", outRecords);
    }

//    @Test
    public void testOpen() throws Exception {
        AsyncKuduClient.AsyncKuduClientBuilder asyncKuduClientBuilder = mock(AsyncKuduClient.AsyncKuduClientBuilder.class);
        KuduClient syncClient = mock(KuduClient.class);

        whenNew(AsyncKuduClient.AsyncKuduClientBuilder.class).withArguments("ip1,ip2,ip3").thenReturn(asyncKuduClientBuilder);
        suppress(AbstractDtRichOutputFormat.class.getMethod("initMetric"));
        when(asyncKuduClientBuilder.build()).thenReturn(client);
        when(client.syncClient()).thenReturn(syncClient);
        when(syncClient.tableExists("tableName")).thenReturn(true);

        Whitebox.setInternalState(kuduOutputFormat, "tableName", "tableName");
        Whitebox.setInternalState(kuduOutputFormat, "kuduMasters", "ip1,ip2,ip3");
        Whitebox.setInternalState(kuduOutputFormat, "workerCount", 2);
        Whitebox.setInternalState(kuduOutputFormat, "defaultSocketReadTimeoutMs", 600000);
        Whitebox.setInternalState(kuduOutputFormat, "defaultOperationTimeoutMs", 600000);
        kuduOutputFormat.open(10, 10);
    }

//    @Test
    public void testWriteRecord() throws IOException {
        KuduTable table = mock(KuduTable.class);
        Upsert operation = mock(Upsert.class);
        PartialRow partialRow = mock(PartialRow.class);
        AsyncKuduSession session = mock(AsyncKuduSession.class);
        String[] fieldNames = new String[]{"a", "b", "C", "d"};
        TypeInformation[] fieldTypes = new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Float.class), TypeInformation.of(Long.class)};

        Row row = new Row(4);
        row.setField(0, 1);
        row.setField(1, "ddd");
        row.setField(2, 10f);
        row.setField(3, 12L);
        Tuple2 record = new Tuple2();
        record.f0 = true;
        record.f1 = row;

        Whitebox.setInternalState(kuduOutputFormat, "writeMode", KuduOutputFormat.WriteMode.UPSERT);
        Whitebox.setInternalState(kuduOutputFormat, "fieldNames", fieldNames);
        Whitebox.setInternalState(kuduOutputFormat, "table", table);
        Whitebox.setInternalState(kuduOutputFormat, "fieldTypes", fieldTypes);

        when(table.newUpsert()).thenReturn(operation);
        when(operation.getRow()).thenReturn(partialRow);
        when(client.newSession()).thenReturn(session);

        kuduOutputFormat.writeRecord(record);
    }

//    @Test
    public void testClose(){
        kuduOutputFormat.close();
    }
}
