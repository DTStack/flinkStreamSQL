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

 

package com.dtstack.flink.sql.sink.wtable;

import com.bj58.spat.wtable.SetArg;
import com.bj58.spat.wtable.WtableClient;
import com.bj58.spat.wtable.exception.WtableException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WtableOutputFormat extends RichOutputFormat<Tuple2> {

    private String nameCenter;
    private String bid;
    private String password;
    private String[] rowkey;
    private int tableId;
    private String[] columnNames;
    private String[] columnTypes;
    private int cachettlms;
    private List<String> primaryKeys;

    private boolean ignoreRowKeyColumn = false;
    // WTable客户端，对象是线程安全的，可以在多线程中使用
    private WtableClient client;

    private final SimpleDateFormat ROWKEY_DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
    private final SimpleDateFormat FIELD_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private String tmpPath = "/tmp/";

    private String configFile ;
    @Override
    public void configure(Configuration parameters) {
        StringBuffer sb = new StringBuffer();
        sb.append("namecenter=").append(nameCenter).append("\n");
        sb.append("bid=").append(bid).append("\n");
        sb.append("password=").append(password);

        configFile = tmpPath+ UUID.randomUUID();
        File file = null;
        FileWriter fw;
        BufferedWriter bw = null;
        try {
            file = new File(configFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            bw.write(sb.toString());

            if (bw != null) {
                bw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            file.deleteOnExit();
        }

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            // 根据配置文件初始化WTable客户端
            client = WtableClient.getInstance(configFile);
        } catch (WtableException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeRecord(Tuple2 tuple2) throws IOException {

        Tuple2<Boolean, Row> tupleTrans = tuple2;
        Boolean retract = tupleTrans.getField(0);
        if(!retract){
            //FIXME 暂时不处理wtable删除操作--->wtable要求有key,所有认为都是可以执行update查找
            return;
        }
        Row record = tupleTrans.getField(1);
        final Set<Integer>  keyIndexSet = new HashSet<>();
        List<String> primaryKeysVal = new ArrayList<>();
        if(CollectionUtils.isEmpty(primaryKeys)) {
            for(int i = 0; i < rowkey.length; ++i) {
                String colName = rowkey[i];
                int j = 0;
                for(; j < columnNames.length; ++j) {
                    if(columnNames[j].equals(colName)) {
                        break;
                    }
                }
                if(j != columnNames.length && record.getField(i) != null) {
                    Object field = record.getField(j);
                    if(field == null ) {
                        primaryKeysVal.add("null");
                    } else if (field instanceof java.util.Date){
                        java.util.Date d = (java.util.Date)field;
                        primaryKeysVal.add(ROWKEY_DATE_FORMAT.format(d));
                    } else {
                        primaryKeysVal.add(field.toString());
                    }
                }
            }
        }else{
            for (String primaryKey:  primaryKeys) {
                int primaryKeyIndex = ArrayUtils.indexOf(columnNames, primaryKey);
                if(primaryKeyIndex < 0)
                    throw new IllegalStateException("Primary key " + primaryKey + " not set up correctly.");
                keyIndexSet.add(primaryKeyIndex);
                Object field = record.getField(primaryKeyIndex);
                if(field == null ) {
                    primaryKeysVal.add("null");
                } else if (field instanceof java.util.Date){
                    java.util.Date d = (java.util.Date)field;
                    primaryKeysVal.add(ROWKEY_DATE_FORMAT.format(d));
                } else {
                    primaryKeysVal.add(field.toString());
                }
            }
        }
        //最终的rowkey是这样的
        String key = StringUtils.join(primaryKeysVal, "-");
        //开始写入wtable
        ArrayList<SetArg> setArgs = new ArrayList<SetArg>();
        for(int i = 0; i < record.getArity(); ++i) {
            if(CollectionUtils.isNotEmpty(primaryKeys) && keyIndexSet.contains(i))
                continue;
            Object field = record.getField(i);
            byte[] val = null;
            if (field != null) {
               val = field.toString().getBytes();
            }
            byte[] column = columnNames[i].getBytes();
            if (cachettlms == 0) {
                setArgs.add(new SetArg(tableId, key.getBytes(), column, val, 10));
            } else {
                setArgs.add(new SetArg(tableId, key.getBytes(), column, val, 10, cachettlms));
            }
        }
        try {
            client.mSet(setArgs);
        } catch (WtableException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if(client != null) {
            client = null;
        }
    }

    private WtableOutputFormat() {}

    public static WtableOutputFormatBuilder buildWtableOutputFormat() {
        return new WtableOutputFormatBuilder();
    }

    public static class WtableOutputFormatBuilder {

        private WtableOutputFormat format;

        private WtableOutputFormatBuilder() {
            format = new WtableOutputFormat();
        }

        public WtableOutputFormatBuilder setNameCenter(String nameCenter) {
            format.nameCenter = nameCenter;
            return this;
        }

        public WtableOutputFormatBuilder setBid(String bid){
            format.bid = bid;
            return this;
        }


        public WtableOutputFormatBuilder setPassword(String password){
            format.password = password;
            return this;
        }

        public WtableOutputFormatBuilder setTableId(int tableId) {
            format.tableId = tableId;
            return this;
        }

        public WtableOutputFormatBuilder setCachettlms(int cachettlms){
            format.cachettlms = cachettlms;
            return this;
        }

        public WtableOutputFormatBuilder setRowkey(String[] rowkey) {
            format.rowkey = rowkey;
            return this;
        }

        public WtableOutputFormatBuilder setColumnNames(String[] columnNames) {
            format.columnNames = columnNames;
            return this;
        }

        public WtableOutputFormatBuilder setColumnTypes(String[] columnTypes) {
            format.columnTypes = columnTypes;
            return this;
        }

        public WtableOutputFormatBuilder ignoreRowKeyColumn(boolean ignoreRowKeyColumn) {
            format.ignoreRowKeyColumn = ignoreRowKeyColumn;
            return this;
        }
        public WtableOutputFormatBuilder setPrimaryKeys(List<String > primaryKeys){
            format.primaryKeys = primaryKeys;
            return this;
        }
        public WtableOutputFormat finish() {
            Preconditions.checkNotNull(format.nameCenter, "nameCenter should be specified");
            Preconditions.checkNotNull(format.bid, "bid should be specified");
            Preconditions.checkNotNull(format.password, "password should be specified");
            Preconditions.checkNotNull(format.columnNames, "columnNames should be specified");
            Preconditions.checkArgument(format.columnNames.length != 0, "columnNames length should not be zero");

            return format;
        }

    }
}
