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

import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.dtstack.flink.sql.sink.kudu.table.KuduTableInfo;
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
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gituser
 * @modify xiuzhu
 */
public class KuduOutputFormat extends AbstractDtRichOutputFormat<Tuple2<Boolean, Row>> {

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

    /**
     * kerberos
     */
    private String principal;
    private String keytab;
    private String krb5conf;

    /**
     * batch size
     */
    private Integer batchSize;
    private Integer batchWaitInterval;
    /**
     * kudu session flush mode
     */
    private String flushMode;

    private transient AtomicInteger rowCount;

    /**
     * 定时任务
     */
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;

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
        initSchedulerTask();
        rowCount = new AtomicInteger(0);
    }

    /**
     * init the scheduler task of {@link KuduOutputFormat#flush()}
     */
    private void initSchedulerTask() {
        try {
            if (batchWaitInterval > 0) {
                this.scheduler = new ScheduledThreadPoolExecutor(
                        1,
                        new DTThreadFactory("kudu-batch-flusher")
                );

                this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(
                        this::flush, batchWaitInterval, batchWaitInterval, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            LOG.error("init schedule task failed !");
            throw new RuntimeException(e);
        }
    }

    private void establishConnection() throws IOException {
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMasters);
        if (null != workerCount) {
            kuduClientBuilder.workerCount(workerCount);
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

        session = buildSessionWithFlushMode(flushMode, client);
    }

    /**
     * According to the different flush mode, build different session. Detail see {@link SessionConfiguration.FlushMode}
     *
     * @param flushMode  flush mode
     * @param kuduClient kudu client
     * @return KuduSession with flush mode
     */
    private KuduSession buildSessionWithFlushMode(String flushMode, KuduClient kuduClient) {
        KuduSession kuduSession = kuduClient.newSession();
        if (flushMode.equalsIgnoreCase(KuduTableInfo.KuduFlushMode.MANUAL_FLUSH.name())) {
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            kuduSession.setMutationBufferSpace(
                    Integer.parseInt(String.valueOf(Math.round(batchSize * 1.2)))
            );
        }

        if (flushMode.equalsIgnoreCase(KuduTableInfo.KuduFlushMode.AUTO_FLUSH_SYNC.name())) {
            LOG.warn("Parameter [batchSize] will not take effect at AUTO_FLUSH_SYNC mode.");
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        }

        if (flushMode.equalsIgnoreCase(KuduTableInfo.KuduFlushMode.AUTO_FLUSH_BACKGROUND.name())) {
            LOG.warn("Unable to determine the order of data at AUTO_FLUSH_BACKGROUND mode.");
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        }

        return kuduSession;
    }

    @Override
    public void writeRecord(Tuple2<Boolean, Row> record) throws IOException {
        Boolean retract = record.getField(0);
        if (!retract) {
            return;
        }
        Row row = record.getField(1);

        try {
            if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0) {
                LOG.info("Receive data : {}", row);
            }
            if (rowCount.getAndIncrement() >= batchSize) {
                flush();
            }
            // At AUTO_FLUSH_SYNC mode, kudu automatically flush once session apply operation, then get the response from kudu server.
            if (flushMode.equalsIgnoreCase(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC.name())) {
                dealResponse(session.apply(toOperation(writeMode, row)));
            }

            session.apply(toOperation(writeMode, row));
            outRecords.inc();
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Flush data with session, then deal the responses of operations and reset rowCount.
     * Detail of flush see {@link KuduSession#flush()}
     */
    private synchronized void flush() {
        try {
            if (session.isClosed()) {
                throw new IllegalStateException("Session is closed! Flush data error!");
            }

            // At AUTO_FLUSH_SYNC mode, kudu automatically flush once session apply operation
            if (flushMode.equalsIgnoreCase(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC.name())) {
                return;
            }
            session.flush().forEach(this::dealResponse);
            // clear
            rowCount.set(0);
        } catch (KuduException kuduException) {
            LOG.error("flush data error!", kuduException);
            throw new RuntimeException(kuduException);
        }
    }

    /**
     * Deal response when operation apply.
     * At MANUAL_FLUSH mode, response returns after {@link KuduSession#flush()}.
     * But at AUTO_FLUSH_SYNC mode, response returns after {@link KuduSession#apply(Operation)}
     *
     * @param response {@link OperationResponse} response after operation done.
     */
    private void dealResponse(OperationResponse response) {
        if (response.hasRowError()) {
            RowError error = response.getRowError();
            String errorMsg = error.getErrorStatus().toString();
            if (outDirtyRecords.getCount() % DIRTY_PRINT_FREQUENCY == 0) {
                LOG.error(errorMsg);
                LOG.error(String.format("Dirty data count: [%s]. Row data: [%s]",
                        outDirtyRecords.getCount() + 1, error.getOperation().getRow().toString()));
            }
            outDirtyRecords.inc();

            if (error.getErrorStatus().isNotFound()
                    || error.getErrorStatus().isIOError()
                    || error.getErrorStatus().isRuntimeError()
                    || error.getErrorStatus().isServiceUnavailable()
                    || error.getErrorStatus().isIllegalState()) {
                throw new RuntimeException(errorMsg);
            }
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

        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }

        if (scheduler != null) {
            scheduler.shutdownNow();
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

        public KuduOutputFormatBuilder setBatchSize(Integer batchSize) {
            kuduOutputFormat.batchSize = batchSize;
            return this;
        }

        public KuduOutputFormatBuilder setBatchWaitInterval(Integer batchWaitInterval) {
            kuduOutputFormat.batchWaitInterval = batchWaitInterval;
            return this;
        }

        public KuduOutputFormatBuilder setFlushMode(String flushMode) {
            kuduOutputFormat.flushMode = flushMode;
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