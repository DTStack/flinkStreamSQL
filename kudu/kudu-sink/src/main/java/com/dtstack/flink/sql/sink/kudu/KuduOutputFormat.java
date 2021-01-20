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
import com.dtstack.flink.sql.util.KrbUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Objects;

/**
 * @author gituser
 * @modify xiuzhu
 */
public class KuduOutputFormat extends AbstractDtRichOutputFormat<Tuple2> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);
    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;
    boolean enableKrb;
    private String kuduMasters;
    private String tableName;
    private WriteMode writeMode;
    private KuduClient client;

    private KuduTable table;

    private volatile KuduSession session;

    private Integer workerCount;

    private Integer defaultOperationTimeoutMs;

    private Integer defaultSocketReadTimeoutMs;

    /**
     * kerberos
     */
    private String principal;
    private String keytab;
    private String krb5conf;

    private KuduOutputFormat() {
    }

    public static KuduOutputFormatBuilder buildKuduOutputFormat() {
        return new KuduOutputFormatBuilder();
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        establishConnection();
        initMetric();
    }

    private void establishConnection() throws IOException {
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMasters);
        if (null != workerCount) {
            kuduClientBuilder.workerCount(workerCount);
        }
        if (null != defaultSocketReadTimeoutMs) {
            kuduClientBuilder.defaultSocketReadTimeoutMs(defaultSocketReadTimeoutMs);
        }

        if (null != defaultOperationTimeoutMs) {
            kuduClientBuilder.defaultOperationTimeoutMs(defaultOperationTimeoutMs);
        }

        if (enableKrb) {
            UserGroupInformation ugi = KrbUtils.loginAndReturnUgi(
                    principal,
                    keytab,
                    krb5conf
            );
            client = ugi.doAs(
                    (PrivilegedAction<KuduClient>) kuduClientBuilder::build);
        } else {
            client = kuduClientBuilder.build();
        }

        if (client.tableExists(tableName)) {
            table = client.openTable(tableName);
        }
        if (Objects.isNull(table)) {
            throw new IllegalArgumentException(
                    String.format("Table [%s] Open Failed , please check table exists", tableName));
        }
        LOG.info("connect kudu is succeed!");

        session = client.newSession();
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
            if (outDirtyRecords.getCount() % DIRTY_PRINT_FREQUENCY == 0) {
                LOG.error("record insert failed ..{}", row.toString());
                LOG.error("cause by row.getArity() != fieldNames.length");
            }
            outDirtyRecords.inc();
            return;
        }

        try {
            if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0) {
                LOG.info("Receive data : {}", row);
            }
            session.apply(toOperation(writeMode, row));
            outRecords.inc();
        } catch (KuduException e) {
            if (outDirtyRecords.getCount() % DIRTY_PRINT_FREQUENCY == 0) {
                LOG.error("record insert failed, total dirty record:{} current row:{}", outDirtyRecords.getCount(), row.toString());
                LOG.error("", e);
            }
            outDirtyRecords.inc();
        }
    }

    @Override
    public void close() {
        if (Objects.nonNull(session) && !session.isClosed()) {
            try {
                session.close();
            } catch (Exception e) {
                throw new IllegalArgumentException("[closeKuduSession]: " + e.getMessage());
            }
        }

        if (null != client) {
            try {
                client.shutdown();
            } catch (Exception e) {
                throw new IllegalArgumentException("[closeKuduClient]:" + e.getMessage());
            }
        }
    }

    private Operation toOperation(WriteMode writeMode, Row row) {
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
            default:
                return table.newUpsert();
        }
    }

    public enum WriteMode {
        // insert
        INSERT,
        // update
        UPDATE,
        // update or insert
        UPSERT
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

        public KuduOutputFormatBuilder setPrincipal(String principal) {
            kuduOutputFormat.principal = principal;
            return this;
        }

        public KuduOutputFormatBuilder setKeytab(String keytab) {
            kuduOutputFormat.keytab = keytab;
            return this;
        }

        public KuduOutputFormatBuilder setKrb5conf(String krb5conf) {
            kuduOutputFormat.krb5conf = krb5conf;
            return this;
        }

        public KuduOutputFormatBuilder setEnableKrb(boolean enableKrb) {
            kuduOutputFormat.enableKrb = enableKrb;
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

}