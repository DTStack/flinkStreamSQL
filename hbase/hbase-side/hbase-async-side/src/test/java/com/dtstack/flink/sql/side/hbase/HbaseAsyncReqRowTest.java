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

import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * @author: chuixue
 * @create: 2020-07-10 19:00
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({HbaseAsyncReqRow.class
        , HBaseClient.class
        , Deferred.class
        , HbaseAsyncReqRow.CheckResult.class
        , LoggerFactory.class
        , Scanner.class})
public class HbaseAsyncReqRowTest {

    private HbaseAsyncReqRow hbaseAsyncReqRow;
    private RowTypeInfo rowTypeInfo = getRowTypeInfo();
    private HbaseSideTableInfo sideTableInfo = getSideTableInfo();
    private JoinInfo joinInfo = getJoinInfo();
    private List<FieldInfo> outFieldInfoList = getOutFieldInfoList();
    private HBaseClient hBaseClient;
    private Deferred deferred;
    private Configuration parameters = new Configuration();
    private Map<String, Object> inputParams = Maps.newHashMap();
    private GenericRow cRow = new GenericRow(3);
    private ArrayList<KeyValue> keyValues = Lists.newArrayList();
    private ArrayList<ArrayList<KeyValue>> preKeyValues = Lists.newArrayList();
    private Deferred deferredDealer;
    private ResultFuture<BaseRow> resultFuture;

    @Captor
    private ArgumentCaptor<Callback<String, ArrayList<KeyValue>>> rowkeycb;
    @Captor
    private ArgumentCaptor<Callback<String, ArrayList<ArrayList<KeyValue>>>> preRowkeycb;
    @Captor
    private ArgumentCaptor<Callback<String, Object>> eb;

    @Before
    public void setup() throws Exception {
        inputParams.put("rowkey", 1);
        cRow.setField(0, 1);
        cRow.setField(1, "bbbbbb");
        cRow.setField(2, "2020-07-14 01:27:43.969");

        keyValues.add(new KeyValue("1".getBytes(), "cf".getBytes(), "channel".getBytes(), 1591917901129L, "bigtdata".getBytes()));
        preKeyValues.add(keyValues);

        deferredDealer = mock(Deferred.class);
        hBaseClient = mock(HBaseClient.class);
        deferred = mock(Deferred.class);
        HbaseAsyncReqRow.CheckResult result = mock(HbaseAsyncReqRow.CheckResult.class);
        resultFuture = mock(ResultFuture.class);

        when(hBaseClient.get(any(GetRequest.class))).thenReturn(deferredDealer);
        PowerMockito.suppress(BaseAsyncReqRow.class.getMethod("open", Configuration.class));
        whenNew(HBaseClient.class).withAnyArguments().thenReturn(hBaseClient);
        when(hBaseClient.ensureTableExists(any(String.class))).thenReturn(deferred);
        when(deferred.addCallbacks(any(), any())).thenReturn(deferred);
        when(deferred.join()).thenReturn(result);
        when(result.isConnect()).thenReturn(true);
        sideTableInfo.setPreRowKey(false);

        hbaseAsyncReqRow = new HbaseAsyncReqRow(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        hbaseAsyncReqRow.open(parameters);
    }

    @Test
    public void testHandleAsyncInvokeRowkey() throws Exception {
        hbaseAsyncReqRow.handleAsyncInvoke(inputParams, cRow, resultFuture);
        verify(deferredDealer).addCallbacks(rowkeycb.capture(), eb.capture());
        assertNotEquals(hbaseAsyncReqRow.new CheckResult(true, "msg"), rowkeycb.getValue().call(keyValues));
    }

    @Test
    public void testHandleAsyncInvokePreRowkey() throws Exception {
        Scanner prefixScanner = mock(Scanner.class);

        sideTableInfo.setPreRowKey(true);
        hbaseAsyncReqRow = new HbaseAsyncReqRow(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        hbaseAsyncReqRow.open(parameters);

        when(hBaseClient.newScanner("bbb")).thenReturn(prefixScanner);
        when(prefixScanner.nextRows()).thenReturn(deferredDealer);
        hbaseAsyncReqRow.handleAsyncInvoke(inputParams, cRow, resultFuture);

        verify(deferredDealer).addCallbacks(preRowkeycb.capture(), eb.capture());

        HbaseAsyncReqRow.CheckResult checkResult = hbaseAsyncReqRow.new CheckResult(false, "msg");
        checkResult.setConnect(true);
        checkResult.setExceptionMsg("error");
        assertNotEquals(checkResult, preRowkeycb.getValue().call(preKeyValues));
    }


    public List<FieldInfo> getOutFieldInfoList() {
        List<FieldInfo> outFieldInfoList = new ArrayList<>();
        FieldInfo fieldInfoa = new FieldInfo();
        fieldInfoa.setTable("m");
        fieldInfoa.setFieldName("id");
        fieldInfoa.setTypeInformation(TypeInformation.of(Integer.class));
        outFieldInfoList.add(fieldInfoa);
        FieldInfo fieldInfob = new FieldInfo();
        fieldInfob.setTable("m");
        fieldInfob.setFieldName("bb");
        fieldInfob.setTypeInformation(TypeInformation.of(String.class));
        outFieldInfoList.add(fieldInfob);
        FieldInfo fieldInfoc = new FieldInfo();
        fieldInfoc.setTable("s");
        fieldInfoc.setFieldName("channel");
        fieldInfoc.setTypeInformation(TypeInformation.of(String.class));
        outFieldInfoList.add(fieldInfoc);
        FieldInfo fieldInfod = new FieldInfo();
        fieldInfod.setTable("s");
        fieldInfod.setFieldName("name");
        fieldInfod.setTypeInformation(TypeInformation.of(String.class));
        outFieldInfoList.add(fieldInfod);
        return outFieldInfoList;
    }

    public JoinInfo getJoinInfo() {
        String sql = "select \n" +
                "   m.id as mid \n" +
                "   ,m.bb mbb \n" +
                "   ,s.channel as sid\n" +
                "   ,s.name as sbb \n" +
                "  from MyTable m left join hbaseSide s\n" +
                "  on m.id = s.rowkey";
        SqlParser.Config config = SqlParser
                .configBuilder()
                .setLex(Lex.MYSQL)
                .build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException("", e);
        }

        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setLeftIsSideTable(false);
        joinInfo.setRightIsSideTable(true);
        joinInfo.setLeftTableName("MyTable");
        joinInfo.setLeftTableAlias("m");
        joinInfo.setRightTableName("hbaseSide");
        joinInfo.setRightTableAlias("s");
        joinInfo.setLeftNode(((SqlJoin) ((SqlSelect) sqlNode).getFrom()).getLeft());
        joinInfo.setRightNode(((SqlJoin) ((SqlSelect) sqlNode).getFrom()).getRight());
        joinInfo.setCondition(((SqlJoin) ((SqlSelect) sqlNode).getFrom()).getCondition());
        joinInfo.setSelectFields(((SqlSelect) sqlNode).getSelectList());
        joinInfo.setSelectNode(((SqlSelect) sqlNode).getSelectList());
        joinInfo.setJoinType(((SqlJoin) ((SqlSelect) sqlNode).getFrom()).getJoinType());
        joinInfo.setScope("0");
        Map leftSelectField = new HashMap<String, String>();
        leftSelectField.put("bb", "bb");
        leftSelectField.put("id", "id");
        joinInfo.setLeftSelectFieldInfo(leftSelectField);
        Map rightSelectField = new HashMap<String, String>();
        rightSelectField.put("channel", "channel");
        rightSelectField.put("name", "name");
        rightSelectField.put("rowkey", "rowkey");
        joinInfo.setRightSelectFieldInfo(rightSelectField);
        return joinInfo;
    }

    public HbaseSideTableInfo getSideTableInfo() {
        HbaseSideTableInfo sideTableInfo = new HbaseSideTableInfo();
        sideTableInfo.setHost("localhost:2181");
        sideTableInfo.setParent("/hbase");
        sideTableInfo.setTableName("bbb");
        sideTableInfo.setColumnRealNames(new String[]{"cf:channel", "cf:name"});
        sideTableInfo.addColumnRealName("cf:channel");
        sideTableInfo.addColumnRealName("cf:name");
        sideTableInfo.putAliasNameRef("channel", "cf:channel");
        sideTableInfo.putAliasNameRef("name", "cf:name");
        sideTableInfo.setName("hbaseSide");
        sideTableInfo.setType("hbase");
        sideTableInfo.setFields(new String[]{"name", "channel"});
        sideTableInfo.setFieldTypes(new String[]{"varchar", "varchar"});
        sideTableInfo.setFieldClasses(new Class[]{String.class, String.class});

        sideTableInfo.addField("channel");
        sideTableInfo.addField("name");
        sideTableInfo.addPhysicalMappings("channel", "cf:channel");
        sideTableInfo.addPhysicalMappings("name", "cf:name");
        sideTableInfo.addFieldType("channel");
        sideTableInfo.addFieldType("name");
        sideTableInfo.addFieldClass(String.class);
        sideTableInfo.addFieldClass(String.class);
        sideTableInfo.setPrimaryKeys(Arrays.asList("rowkey"));
        return sideTableInfo;
    }

    public RowTypeInfo getRowTypeInfo() {
        return new RowTypeInfo(new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class)}, new String[]{"id", "bb"});
    }

    public BaseSideInfo getSideInfo() {
        return new HbaseAsyncSideInfo(getRowTypeInfo(), getJoinInfo(), getOutFieldInfoList(), getSideTableInfo());
    }
}
