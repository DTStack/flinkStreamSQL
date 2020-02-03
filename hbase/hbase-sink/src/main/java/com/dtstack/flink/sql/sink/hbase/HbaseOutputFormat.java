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

import com.dtstack.flink.sql.enums.EUpdateMode;
import com.dtstack.flink.sql.sink.MetricOutputFormat;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * author: jingzhen@dtstack.com
 * date: 2017-6-29
 */
public class HbaseOutputFormat extends MetricOutputFormat<Tuple2> {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseOutputFormat.class);

    private String host;
    private String zkParent;
    private String[] rowkey;
    private String tableName;
    private String[] columnNames;
    private String updateMode;
    private String[] columnTypes;
    private Map<String, String> columnNameFamily;

    private String[] families;
    private String[] qualifiers;

    private transient org.apache.hadoop.conf.Configuration conf;
    private transient Connection conn;
    private transient Table table;

    public final SimpleDateFormat ROWKEY_DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

    private static int rowLenth = 1000;
    private static int dirtyDataPrintFrequency = 1000;


    @Override
    public void configure(Configuration parameters) {
        LOG.warn("---configure---");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        if (zkParent != null && !"".equals(zkParent)) {
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
    public void writeRecord(Tuple2 tuple2) {
        Tuple2<Boolean, Row> tupleTrans = tuple2;
        Boolean retract = tupleTrans.getField(0);
        if (!retract && StringUtils.equalsIgnoreCase(updateMode, EUpdateMode.UPSERT.name())) {
            dealDelete(tupleTrans);
        } else {
            dealInsert(tupleTrans);
        }
    }

    protected void dealInsert(Tuple2<Boolean, Row> tupleTrans) {
        Row record = tupleTrans.getField(1);
        Put put = getPutByRow(record);
        if (put == null) {
            return;
        }

        try {
            table.put(put);
        } catch (IOException e) {
            outDirtyRecords.inc();
            if (outDirtyRecords.getCount() % dirtyDataPrintFrequency == 0 || LOG.isDebugEnabled()) {
                LOG.error("record insert failed ..", record.toString());
                LOG.error("", e);
            }
        }

        if (outRecords.getCount() % rowLenth == 0) {
            LOG.info(record.toString());
        }
        outRecords.inc();
    }

    protected void dealDelete(Tuple2<Boolean, Row> tupleTrans) {
        Row record = tupleTrans.getField(1);
        String rowKey = buildRowKey(record);
        if (!StringUtils.isEmpty(rowKey)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            try {
                table.delete(delete);
            } catch (IOException e) {
                outDirtyRecords.inc();
                if (outDirtyRecords.getCount() % dirtyDataPrintFrequency == 0 || LOG.isDebugEnabled()) {
                    LOG.error("record insert failed ..", record.toString());
                    LOG.error("", e);
                }
            }
            if (outRecords.getCount() % rowLenth == 0) {
                LOG.info(record.toString());
            }
            outRecords.inc();
        }
    }

    private Put getPutByRow(Row record) {
        String rowKey = buildRowKey(record);
        if (StringUtils.isEmpty(rowKey)) {
            return null;
        }
        Put put = new Put(rowKey.getBytes());
        for (int i = 0; i < record.getArity(); ++i) {
            Object fieldVal = record.getField(i);
            if (fieldVal == null) {
                continue;
            }
            byte[] val = fieldVal.toString().getBytes();
            byte[] cf = families[i].getBytes();
            byte[] qualifier = qualifiers[i].getBytes();

            put.addColumn(cf, qualifier, val);
        }
        return put;
    }

    private String buildRowKey(Row record) {
        List<String> rowKeyValues = getRowKeyValues(record);
        // all rowkey not null
        if (rowKeyValues.size() != rowkey.length) {
            LOG.error("row key value must not null,record is ..", record);
            outDirtyRecords.inc();
            return "";
        }
        return StringUtils.join(rowKeyValues, "-");
    }

    private List<String> getRowKeyValues(Row record) {
        List<String> rowKeyValues = Lists.newArrayList();
        for (int i = 0; i < rowkey.length; ++i) {
            String colName = rowkey[i];
            int rowKeyIndex = 0;
            for (; rowKeyIndex < columnNames.length; ++rowKeyIndex) {
                if (columnNames[rowKeyIndex].equals(colName)) {
                    break;
                }
            }

            if (rowKeyIndex != columnNames.length && record.getField(rowKeyIndex) != null) {
                Object field = record.getField(rowKeyIndex);
                if (field == null) {
                    continue;
                } else if (field instanceof java.util.Date) {
                    java.util.Date d = (java.util.Date) field;
                    rowKeyValues.add(ROWKEY_DATE_FORMAT.format(d));
                } else {
                    rowKeyValues.add(field.toString());
                }
            }
        }
        return rowKeyValues;
    }

    @Override
    public void close() throws IOException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    private HbaseOutputFormat() {
    }

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

        public HbaseOutputFormatBuilder setZkParent(String parent) {
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

        public HbaseOutputFormatBuilder setUpdateMode(String updateMode) {
            format.updateMode = updateMode;
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

        public HbaseOutputFormatBuilder setColumnNameFamily(Map<String, String> columnNameFamily) {
            format.columnNameFamily = columnNameFamily;
            return this;
        }

        public HbaseOutputFormat finish() {
            Preconditions.checkNotNull(format.host, "zookeeperQuorum should be specified");
            Preconditions.checkNotNull(format.tableName, "tableName should be specified");
            Preconditions.checkNotNull(format.columnNames, "columnNames should be specified");
            Preconditions.checkArgument(format.columnNames.length != 0, "columnNames length should not be zero");

            String[] families = new String[format.columnNames.length];
            String[] qualifiers = new String[format.columnNames.length];

            if (format.columnNameFamily != null) {
                Set<String> keySet = format.columnNameFamily.keySet();
                String[] columns = keySet.toArray(new String[keySet.size()]);
                for (int i = 0; i < columns.length; ++i) {
                    String col = columns[i];
                    String[] part = col.split(":");
                    families[i] = part[0];
                    qualifiers[i] = part[1];
                }
            }
            format.families = families;
            format.qualifiers = qualifiers;

            return format;
        }

    }


}
