package com.dtstack.flink.sql.sink.kudu;


import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.kudu.table.KuduTableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class KuduSink implements RetractStreamTableSink<Row>, Serializable, IStreamSinkGener<KuduSink> {

    private String kuduMasters;

    private String tableName;

    private KuduOutputFormat.WriteMode writeMode;

    protected String[] fieldNames;

    TypeInformation<?>[] fieldTypes;

//    protected List<String> primaryKeys;

//    private KuduOutputFormat.Consistency consistency = KuduOutputFormat.Consistency.STRONG;


    private Integer workerCount;

    private Integer defaultOperationTimeoutMs;

    private Integer defaultSocketReadTimeoutMs;

    private int parallelism = -1;

    @Override
    public KuduSink genStreamSink(TargetTableInfo targetTableInfo) {
        KuduTableInfo kuduTableInfo = (KuduTableInfo) targetTableInfo;
        this.kuduMasters = kuduTableInfo.getKuduMasters();
        this.tableName = kuduTableInfo.getTableName();
        this.defaultOperationTimeoutMs = kuduTableInfo.getDefaultOperationTimeoutMs();
        this.defaultSocketReadTimeoutMs = kuduTableInfo.getDefaultSocketReadTimeoutMs();
        this.workerCount = kuduTableInfo.getWorkerCount();
        this.writeMode = kuduTableInfo.getWriteMode();

        return this;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        KuduOutputFormat.KuduOutputFormatBuilder builder = KuduOutputFormat.buildKuduOutputFormat();
        builder.setKuduMasters(this.kuduMasters)
                .setTableName(this.tableName)
                .setWriteMode(writeMode)
                .setWorkerCount(this.workerCount)
                .setDefaultOperationTimeoutMs(this.defaultOperationTimeoutMs)
                .setDefaultSocketReadTimeoutMs(this.defaultSocketReadTimeoutMs)
                .setFieldNames(this.fieldNames)
                .setFieldTypes(this.fieldTypes);
        KuduOutputFormat kuduOutputFormat = builder.finish();
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(kuduOutputFormat);
        dataStream.addSink(richSinkFunction);
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
