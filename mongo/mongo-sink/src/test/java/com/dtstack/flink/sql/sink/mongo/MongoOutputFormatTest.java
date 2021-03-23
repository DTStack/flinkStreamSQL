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

package com.dtstack.flink.sql.sink.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;


/**
 * @author: chuixue
 * @create: 2020-07-27 06:33
 * @description:
 **/
@RunWith(PowerMockRunner.class)
public class MongoOutputFormatTest {
    @InjectMocks
    private MongoOutputFormat mongoOutputFormat;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testWriteRecord() throws IOException {
        Counter outRecords = mock(Counter.class);
        MongoDatabase db = mock(MongoDatabase.class);
        MongoCollection dbCollection = mock(MongoCollection.class);
        UpdateResult updateResult = mock(UpdateResult.class);
        String[] fieldNames = new String[]{"_ID", "mbb", "sid", "sbb"};

        Whitebox.setInternalState(mongoOutputFormat, "fieldNames", fieldNames);
        Whitebox.setInternalState(mongoOutputFormat, "db", db);
        Whitebox.setInternalState(mongoOutputFormat, "tableName", "tableName");
        Whitebox.setInternalState(mongoOutputFormat, "outRecords", outRecords);

        when(db.getCollection(anyString(), any())).thenReturn(dbCollection);
        when(dbCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

        Row row = new Row(4);
        row.setField(0, "5f1d7fe5a38c8f074f2ba1e8");
        row.setField(1, "bbbbbbbb");
        row.setField(2, "ccxx");
        row.setField(3, "lisi");
        Tuple2<Boolean, Row> tuple = new Tuple2<>(true, row);

        mongoOutputFormat.writeRecord(tuple);
    }
}
