package com.dtstack.flink.sql.sink.kudu;


import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.kudu.table.KuduTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Objects;

public class KuduSink implements RetractStreamTableSink<Row>, Serializable, IStreamSinkGener<KuduSink> {

    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;
    boolean enableKrb;
    private String kuduMasters;
    private String tableName;
    private KuduOutputFormat.WriteMode writeMode;
    private Integer workerCount;
    private Integer defaultOperationTimeoutMs;
    private int parallelism = 1;
    private String principal;
    private String keytab;
    private String krb5conf;
    private Integer batchSize;
    private Integer batchWaitInterval;
    private String flushMode;
    private Integer mutationBufferMaxOps;

    @Override
    public KuduSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        KuduTableInfo kuduTableInfo = (KuduTableInfo) targetTableInfo;
        this.kuduMasters = kuduTableInfo.getKuduMasters();
        this.tableName = kuduTableInfo.getTableName();
        this.defaultOperationTimeoutMs = kuduTableInfo.getDefaultOperationTimeoutMs();
        this.workerCount = kuduTableInfo.getWorkerCount();
        this.writeMode = kuduTableInfo.getWriteMode();
        this.principal = kuduTableInfo.getPrincipal();
        this.keytab = kuduTableInfo.getKeytab();
        this.krb5conf = kuduTableInfo.getKrb5conf();
        this.enableKrb = kuduTableInfo.isEnableKrb();
        this.parallelism = Objects.isNull(kuduTableInfo.getParallelism()) ?
                parallelism : kuduTableInfo.getParallelism();

        this.batchSize = kuduTableInfo.getBatchSize();
        this.batchWaitInterval = kuduTableInfo.getBatchWaitInterval();
        this.flushMode = kuduTableInfo.getFlushMode();
        this.mutationBufferMaxOps = Objects.isNull(kuduTableInfo.getMutationBufferMaxOps()) ?
            Integer.parseInt(String.valueOf(Math.round(batchSize * 1.2))) :
            kuduTableInfo.getMutationBufferMaxOps();
        return this;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        KuduOutputFormat.KuduOutputFormatBuilder builder = KuduOutputFormat.buildKuduOutputFormat();
        KuduOutputFormat kuduOutputFormat = builder.setKuduMasters(this.kuduMasters)
                .setTableName(this.tableName)
                .setWriteMode(writeMode)
                .setWorkerCount(this.workerCount)
                .setDefaultOperationTimeoutMs(this.defaultOperationTimeoutMs)
                .setFieldNames(this.fieldNames)
                .setFieldTypes(this.fieldTypes)
                .setPrincipal(this.principal)
                .setKeytab(this.keytab)
                .setKrb5conf(this.krb5conf)
                .setEnableKrb(this.enableKrb)
                .setBatchSize(this.batchSize)
                .setBatchWaitInterval(this.batchWaitInterval)
                .setFlushMode(this.flushMode)
                .setMutationBufferMaxOps(this.mutationBufferMaxOps)
                .finish();
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(kuduOutputFormat);
        DataStreamSink dataStreamSink = dataStream.addSink(richSinkFunction);
        dataStreamSink.name(tableName);
        if (parallelism > 0) {
            dataStreamSink.setParallelism(parallelism);
        }
        return dataStreamSink;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }


    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }


}
