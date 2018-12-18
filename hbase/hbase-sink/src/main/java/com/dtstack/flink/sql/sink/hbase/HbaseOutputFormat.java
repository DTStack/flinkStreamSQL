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

 

package com.dtstack.flink.sql.sink.hbase;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * author: jingzhen@dtstack.com
 * date: 2017-6-29
 */
public class HbaseOutputFormat extends RichOutputFormat<Tuple2> {

    private String host;
    private String zkParent;
    private String[] rowkey;
    private String tableName;
    private String[] columnNames;
    private String[] columnTypes;

    private String[] families;
    private String[] qualifiers;
    protected List<String> primaryKeys;

    private transient org.apache.hadoop.conf.Configuration conf;
    private transient Connection conn;
    private transient Table table;

    private boolean ignoreRowKeyColumn = true;

    public final SimpleDateFormat ROWKEY_DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
    public final SimpleDateFormat FIELD_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void configure(Configuration parameters) {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        if(zkParent != null && !"".equals(zkParent)){
            conf.set("zookeeper.znode.parent", zkParent);
        }
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf(tableName));
    }

    @Override
    public void writeRecord(Tuple2 tuple2) throws IOException {

        Tuple2<Boolean, Row> tupleTrans = tuple2;
        Boolean retract = tupleTrans.getField(0);
        if(!retract){
            //FIXME 暂时不处理hbase删除操作--->hbase要求有key,所有认为都是可以执行update查找
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
                String[] quaAndCol = primaryKey.split(":");
                int primaryKeyIndex = ArrayUtils.indexOf(qualifiers, quaAndCol[1]);
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
        String key = StringUtils.join(primaryKeysVal, "-");
        Put put = new Put(key.getBytes());
        for(int i = 0; i < record.getArity(); ++i) {
            if(CollectionUtils.isNotEmpty(primaryKeys) && keyIndexSet.contains(i))
                continue;
            Object field = record.getField(i);
            byte[] val = null;
            if (field != null) {
               val = field.toString().getBytes();
            }
            byte[] cf = families[i].getBytes();
            byte[] qualifier = qualifiers[i].getBytes();
            put.addColumn(cf, qualifier, val);
        }
        table.put(put);
    }

    @Override
    public void close() throws IOException {
        if(conn != null) {
            conn.close();
            conn = null;
        }
    }

    private HbaseOutputFormat() {}

    public static HbaseOutputFormatBuilder buildHbaseOutputFormat() {
        return new HbaseOutputFormatBuilder();
    }

    public static class HbaseOutputFormatBuilder {

        private HbaseOutputFormat format;

        private HbaseOutputFormatBuilder() {
            format = new HbaseOutputFormat();
        }

        public HbaseOutputFormatBuilder setHost(String host) {
            format.host = host;
            return this;
        }

        public HbaseOutputFormatBuilder setZkParent(String parent){
            format.zkParent = parent;
            return this;
        }


        public HbaseOutputFormatBuilder setTable(String tableName) {
            format.tableName = tableName;
            return this;
        }

        public HbaseOutputFormatBuilder setRowkey(String[] rowkey) {
            format.rowkey = rowkey;
            return this;
        }

        public HbaseOutputFormatBuilder setColumnNames(String[] columnNames) {
            format.columnNames = columnNames;
            return this;
        }

        public HbaseOutputFormatBuilder setColumnTypes(String[] columnTypes) {
            format.columnTypes = columnTypes;
            return this;
        }

        public HbaseOutputFormatBuilder ignoreRowKeyColumn(boolean ignoreRowKeyColumn) {
            format.ignoreRowKeyColumn = ignoreRowKeyColumn;
            return this;
        }
        public HbaseOutputFormatBuilder setPrimaryKeys(List<String > primaryKeys){
            format.primaryKeys = primaryKeys;
            return this;
        }
        public HbaseOutputFormat finish() {
            Preconditions.checkNotNull(format.host, "zookeeperQuorum should be specified");
            Preconditions.checkNotNull(format.tableName, "tableName should be specified");
            Preconditions.checkNotNull(format.columnNames, "columnNames should be specified");
            Preconditions.checkArgument(format.columnNames.length != 0, "columnNames length should not be zero");

            String[] families = new String[format.columnNames.length];
            String[] qualifiers = new String[format.columnNames.length];

            for(int i = 0; i < format.columnNames.length; ++i) {
                String col = format.columnNames[i];
                String[] part = col.split(":");
                families[i] = part[0];
                qualifiers[i] = part[1];
            }

            format.families = families;
            format.qualifiers = qualifiers;

            return format;
        }

    }


}
