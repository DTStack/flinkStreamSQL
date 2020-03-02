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

import com.dtstack.flink.sql.outputformat.DtRichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 *  @author  gituser
 *  @modify  xiuzhu
 */
public class KuduOutputFormat extends DtRichOutputFormat<Tuple2> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);

    public enum WriteMode {
        // insert
        INSERT,
        // update
        UPDATE,
        // update or insert
        UPSERT
    }

    private String kuduMasters;

    private String tableName;

    private WriteMode writeMode;

    protected String[] fieldNames;

    TypeInformation<?>[] fieldTypes;

    private AsyncKuduClient client;

    private KuduTable table;

    private Integer workerCount;

    private Integer defaultOperationTimeoutMs;

    private Integer defaultSocketReadTimeoutMs;


    private KuduOutputFormat() {
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        establishConnection();
        initMetric();
    }


    private void establishConnection() throws KuduException {
        AsyncKuduClient.AsyncKuduClientBuilder asyncKuduClientBuilder = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasters);
        if (null != workerCount) {
            asyncKuduClientBuilder.workerCount(workerCount);
        }
        if (null != defaultSocketReadTimeoutMs) {
            asyncKuduClientBuilder.workerCount(defaultSocketReadTimeoutMs);
        }

        if (null != defaultOperationTimeoutMs) {
            asyncKuduClientBuilder.workerCount(defaultOperationTimeoutMs);
        }
        client = asyncKuduClientBuilder.build();
        KuduClient syncClient = client.syncClient();

        if (syncClient.tableExists(tableName)) {
            table = syncClient.openTable(tableName);
        }
    }

    @Override
    public void writeRecord(Tuple2 record) throws IOException {
        Tuple2<Boolean, Row> tupleTrans = record;
        Boolean retract = tupleTrans.getField(0);
        if (!retract) {
            return;
        }
        Row row = tupleTrans.getField(1);
        if (row.getArity() != fieldNames.length) {
            if(outDirtyRecords.getCount() % DIRTY_PRINT_FREQUENCY == 0) {
                LOG.error("record insert failed ..{}", row.toString());
                LOG.error("cause by row.getArity() != fieldNames.length");
            }
            outDirtyRecords.inc();
            return;
        }
        Operation operation = toOperation(writeMode, row);
        AsyncKuduSession session = client.newSession();

        try {
            if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0) {
                LOG.info("Receive data : {}", row);
            }

            session.apply(operation);
            session.close();
            outRecords.inc();
        } catch (KuduException e) {
            if(outDirtyRecords.getCount() % DIRTY_PRINT_FREQUENCY == 0){
                LOG.error("record insert failed, total dirty record:{} current row:{}", outDirtyRecords.getCount(), row.toString());
                LOG.error("", e);
            }
            outDirtyRecords.inc();
        }
    }

    @Override
    public void close() {
        if (null != client) {
            try {
                client.close();
            } catch (Exception e) {
                throw new IllegalArgumentException("[closeKudu]:" + e.getMessage());
            }
        }
    }

    public static KuduOutputFormatBuilder buildKuduOutputFormat() {
        return new KuduOutputFormatBuilder();
    }

    public static class KuduOutputFormatBuilder {
        private final KuduOutputFormat kuduOutputFormat;

        protected KuduOutputFormatBuilder() {
            this.kuduOutputFormat = new KuduOutputFormat();
        }

        public KuduOutputFormatBuilder setKuduMasters(String kuduMasters) {
            kuduOutputFormat.kuduMasters = kuduMasters;
            return this;
        }

        public KuduOutputFormatBuilder setTableName(String tableName) {
            kuduOutputFormat.tableName = tableName;
            return this;
        }


        public KuduOutputFormatBuilder setFieldNames(String[] fieldNames) {
            kuduOutputFormat.fieldNames = fieldNames;
            return this;
        }

        public KuduOutputFormatBuilder setFieldTypes(TypeInformation<?>[] fieldTypes) {
            kuduOutputFormat.fieldTypes = fieldTypes;
            return this;
        }

        public KuduOutputFormatBuilder setWriteMode(WriteMode writeMode) {
            if (null == writeMode) {
                kuduOutputFormat.writeMode = WriteMode.UPSERT;
            }
            kuduOutputFormat.writeMode = writeMode;
            return this;
        }

        public KuduOutputFormatBuilder setWorkerCount(Integer workerCount) {
            kuduOutputFormat.workerCount = workerCount;
            return this;
        }

        public KuduOutputFormatBuilder setDefaultOperationTimeoutMs(Integer defaultOperationTimeoutMs) {
            kuduOutputFormat.defaultOperationTimeoutMs = defaultOperationTimeoutMs;
            return this;
        }

        public KuduOutputFormatBuilder setDefaultSocketReadTimeoutMs(Integer defaultSocketReadTimeoutMs) {
            kuduOutputFormat.defaultSocketReadTimeoutMs = defaultSocketReadTimeoutMs;
            return this;
        }


        public KuduOutputFormat finish() {
            if (kuduOutputFormat.kuduMasters == null) {
                throw new IllegalArgumentException("No kuduMasters supplied.");
            }

            if (kuduOutputFormat.tableName == null) {
                throw new IllegalArgumentException("No tablename supplied.");
            }

            return kuduOutputFormat;
        }
    }

    private Operation toOperation(WriteMode writeMode, Row row) {
        if (null == table) {
            throw new IllegalArgumentException("Table Open Failed , please check table exists");
        }
        Operation operation = toOperation(writeMode);
        PartialRow partialRow = operation.getRow();

        for (int index = 0; index < row.getArity(); index++) {
            //解决kudu中全小写字段找不到的bug
            String fieldName = fieldNames[index].toLowerCase();
            if (row.getField(index) == null) {
                partialRow.setNull(fieldName);
            } else {
                if (fieldTypes[index].getTypeClass() == String.class) {
                    partialRow.addString(fieldName, (String) row.getField(index));
                    continue;
                }
                if (fieldTypes[index].getTypeClass() == Float.class) {
                    partialRow.addFloat(fieldName, (Float) row.getField(index));
                    continue;
                }
                if (fieldTypes[index].getTypeClass() == Byte.class) {
                    partialRow.addByte(fieldName, (Byte) row.getField(index));
                    continue;
                }

                if (fieldTypes[index].getTypeClass() == Short.class) {
                    partialRow.addShort(fieldName, (Short) row.getField(index));
                    continue;
                }

                if (fieldTypes[index].getTypeClass() == Integer.class) {
                    partialRow.addInt(fieldName, (Integer) row.getField(index));
                    continue;
                }

                if (fieldTypes[index].getTypeClass() == Long.class) {
                    partialRow.addLong(fieldName, (Long) row.getField(index));
                    continue;
                }

                if (fieldTypes[index].getTypeClass() == Double.class) {
                    partialRow.addDouble(fieldName, (Double) row.getField(index));
                    continue;
                }

                if (fieldTypes[index].getTypeClass() == BigDecimal.class) {
                    partialRow.addDecimal(fieldName, (BigDecimal) row.getField(index));
                    continue;
                }
                if (fieldTypes[index].getTypeClass() == Boolean.class) {
                    partialRow.addBoolean(fieldName, (Boolean) row.getField(index));
                    continue;
                }

                if (fieldTypes[index].getTypeClass() == Date.class) {
                    partialRow.addTimestamp(fieldName, new Timestamp(((Date) row.getField(index)).getTime()));
                    continue;
                }

                if (fieldTypes[index].getTypeClass() == Timestamp.class) {
                    partialRow.addTimestamp(fieldName, (Timestamp) row.getField(index));
                    continue;
                }

                if (fieldTypes[index].getTypeClass() == byte[].class) {
                    partialRow.addBinary(fieldName, (byte[]) row.getField(index));
                    continue;
                }
                throw new IllegalArgumentException("Illegal var type: " + fieldTypes[index]);
            }
        }
        return operation;

    }

    private Operation toOperation(WriteMode writeMode) {
        switch (writeMode) {
            case INSERT:
                return table.newInsert();
            case UPDATE:
                return table.newUpdate();
            case UPSERT:
                return table.newUpsert();
            default:
                return table.newUpsert();
        }
    }

}