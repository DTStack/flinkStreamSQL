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

import com.dtstack.flink.sql.metric.MetricConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * author: jingzhen@dtstack.com
 * date: 2017-6-29
 */
public class HbaseOutputFormat extends RichOutputFormat<Tuple2> {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseOutputFormat.class);

    private String host;
    private String zkParent;
    private String[] rowkey;
    private String tableName;
    private String[] columnNames;
    private String[] columnTypes;

    private String[] families;
    private String[] qualifiers;

    private transient org.apache.hadoop.conf.Configuration conf;
    private transient Connection conn;
    private transient Table table;

    private transient Counter outRecords;

    private transient Meter outRecordsRate;

    public final SimpleDateFormat ROWKEY_DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
    public final SimpleDateFormat FIELD_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void configure(Configuration parameters) {
        LOG.warn("---configure---");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        if(zkParent != null && !"".equals(zkParent)){
            conf.set("zookeeper.znode.parent", zkParent);
        }
        LOG.warn("---configure end ---");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.warn("---open---");
        conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf(tableName));
        LOG.warn("---open end(get table from hbase) ---");
        initMetric();
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

        List<String> list = new ArrayList<>();
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
                    list.add("null");
                } else if (field instanceof java.util.Date){
                    java.util.Date d = (java.util.Date)field;
                    list.add(ROWKEY_DATE_FORMAT.format(d));
                } else {
                    list.add(field.toString());
                }
            }
        }

        String key = StringUtils.join(list, "-");
        Put put = new Put(key.getBytes());
        for(int i = 0; i < record.getArity(); ++i) {
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
        outRecords.inc();

    }

    private void initMetric() {
        outRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        outRecordsRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(outRecords, 20));
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
