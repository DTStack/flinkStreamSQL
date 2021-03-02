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

package com.dtstack.flink.sql.side.mongo;

import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.mongo.table.MongoSideTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-07-27 13:45
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({MongoAsyncReqRow.class
        , MongoCollection.class
        , FindIterable.class})
public class MongoAsyncReqRowTest {

    private MongoAsyncReqRow mongoAsyncReqRow;
    private RowTypeInfo rowTypeInfo;
    private JoinInfo joinInfo;
    private List<FieldInfo> outFieldInfoList = new ArrayList<>();
    private MongoSideTableInfo sideTableInfo;
    private BaseSideInfo sideInfo;
    private MongoClient mongoClient;
    private MongoDatabase db;
    @Captor
    private ArgumentCaptor<Block<Document>> cb;
    @Captor
    private ArgumentCaptor<SingleResultCallback<Void>> eb;

    @Before
    public void setUp() {
        rowTypeInfo = mock(RowTypeInfo.class);
        joinInfo = mock(JoinInfo.class);
        sideTableInfo = mock(MongoSideTableInfo.class);
        sideInfo = mock(MongoAsyncSideInfo.class);
        mongoClient = mock(MongoClient.class);
        db = mock(MongoDatabase.class);

        suppress(constructor(MongoAsyncSideInfo.class));
        suppress(constructor(BaseAllReqRow.class));

        mongoAsyncReqRow = new MongoAsyncReqRow(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);

        Whitebox.setInternalState(mongoAsyncReqRow, "sideInfo", sideInfo);
        Whitebox.setInternalState(mongoAsyncReqRow, "mongoSideTableInfo", sideTableInfo);
        Whitebox.setInternalState(mongoAsyncReqRow, "mongoClient", mongoClient);
        Whitebox.setInternalState(mongoAsyncReqRow, "db", db);
    }

    @Test
    public void testOpen() throws Exception {
        suppress(BaseAsyncReqRow.class.getMethod("open", Configuration.class));
        when(sideInfo.getSideTableInfo()).thenReturn(sideTableInfo);
        when(sideTableInfo.getAddress()).thenReturn("localhost");
        when(sideTableInfo.getUserName()).thenReturn("username");
        when(sideTableInfo.getPassword()).thenReturn("password");
        when(sideTableInfo.getDatabase()).thenReturn("getDatabase");
        mongoAsyncReqRow.open(new Configuration());
    }

    @Test
    public void testClose() throws Exception {
        suppress(BaseAsyncReqRow.class.getMethod("close"));
        mongoAsyncReqRow.close();
    }

    @Test
    public void testBuildCacheKey() {
        Map<String, Object> inputParams = Maps.newHashMap();
        inputParams.put("_id", 1);
        mongoAsyncReqRow.buildCacheKey(inputParams);
    }

    @Test
    public void testFillData() {
        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, "bbbbbb");
        row.setField(2, "2020-07-14 01:27:43.969");

        List<FieldInfo> outFieldInfoList = Lists.newArrayList();
        FieldInfo fieldInfo = new FieldInfo();
        fieldInfo.setTable("m");
        fieldInfo.setFieldName("id");
        FieldInfo fieldInfo2 = new FieldInfo();
        fieldInfo2.setTable("s");
        fieldInfo2.setFieldName("_id");
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo2);
        outFieldInfoList.add(fieldInfo);
        outFieldInfoList.add(fieldInfo2);

        Map<Integer, Integer> inFieldIndex = Maps.newHashMap();
        inFieldIndex.put(0, 0);
        inFieldIndex.put(1, 1);

        Map<Integer, Integer> sideFieldIndex = Maps.newHashMap();
        sideFieldIndex.put(2, 0);
        sideFieldIndex.put(3, 1);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(TimeIndicatorTypeInfo.class)}, new String[]{"id", "PROCTIME"});

        when(sideInfo.getOutFieldInfoList()).thenReturn(outFieldInfoList);
        when(sideInfo.getInFieldIndex()).thenReturn(inFieldIndex);
        when(sideInfo.getRowTypeInfo()).thenReturn(rowTypeInfo);
        when(sideInfo.getSideFieldIndex()).thenReturn(sideFieldIndex);

        mongoAsyncReqRow.fillData(row, null);
    }

    //  @Test
    public void testHandleAsyncInvoke() throws Exception {
        Map<String, Object> inputParams = Maps.newHashMap();
        inputParams.put("_id", 1);
        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, "bbbbbb");
        row.setField(2, "2020-07-14 01:27:43.969");
        ResultFuture resultFuture = mock(ResultFuture.class);
        MongoCollection dbCollection = mock(MongoCollection.class);
        BasicDBObject basicDbObject = mock(BasicDBObject.class);
        Class<?> classFindIterable = Class.forName("com.mongodb.async.client.FindIterableImpl");
        FindIterable findIterable = (FindIterable) classFindIterable.newInstance();

        Document document = new Document();
        document.put("_id", "1");
        document.put("channel", "asdfad");
        document.put("name", "asdfasf");

        when(sideTableInfo.getTableName()).thenReturn("cx");
        when(db.getCollection("cx", Document.class)).thenReturn(dbCollection);
        when(dbCollection.find(basicDbObject)).thenReturn(findIterable);

        mongoAsyncReqRow.handleAsyncInvoke(inputParams, row, resultFuture);

        verify(findIterable).forEach(cb.capture(), eb.capture());
        cb.getValue().apply(document);
    }
}
