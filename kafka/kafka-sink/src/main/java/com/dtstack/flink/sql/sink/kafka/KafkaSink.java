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

package com.dtstack.flink.sql.sink.kafka;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

/**
 * @author: chuixue
 * @create: 2019-11-05 11:45
 * @description:
 **/
public class KafkaSink implements RetractStreamTableSink<Row>, IStreamSinkGener<KafkaSink> {

    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    protected String topic;

    protected int parallelism;

    protected Properties properties;

    protected FlinkKafkaProducer<Row> flinkKafkaProducer;

    /** The schema of the table. */
    private TableSchema schema;

    /** Partitioner to select Kafka partition for each item. */
    protected Optional<FlinkKafkaPartitioner<Row>> partitioner;

    private String[] partitionKeys;

    @Override
    public KafkaSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        KafkaSinkTableInfo kafkaSinkTableInfo = (KafkaSinkTableInfo) targetTableInfo;
        this.topic = kafkaSinkTableInfo.getTopic();

        properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaSinkTableInfo.getBootstrapServers());

        for (String key : kafkaSinkTableInfo.getKafkaParamKeys()) {
            properties.setProperty(key, kafkaSinkTableInfo.getKafkaParam(key));
        }
        this.partitioner = Optional.of(new CustomerFlinkPartition<>());
        this.partitionKeys = getPartitionKeys(kafkaSinkTableInfo);
        this.fieldNames = kafkaSinkTableInfo.getFields();
        TypeInformation[] types = new TypeInformation[kafkaSinkTableInfo.getFields().length];
        for (int i = 0; i < kafkaSinkTableInfo.getFieldClasses().length; i++) {
            types[i] = TypeInformation.of(kafkaSinkTableInfo.getFieldClasses()[i]);
        }
        this.fieldTypes = types;

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i=0;i<fieldNames.length;i++) {
            schemaBuilder.field(fieldNames[i], fieldTypes[i]);
        }
        this.schema = schemaBuilder.build();

        Integer parallelism = kafkaSinkTableInfo.getParallelism();
        if (parallelism != null) {
            this.parallelism = parallelism;
        }

        this.flinkKafkaProducer = (FlinkKafkaProducer<Row>) new KafkaProducerFactory().createKafkaProducer(kafkaSinkTableInfo, getOutputType().getTypeAt(1), properties, partitioner, partitionKeys);
        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        DataStream<Row> mapDataStream = dataStream.filter((Tuple2<Boolean, Row> record) -> record.f0)
                .map((Tuple2<Boolean, Row> record) -> record.f1)
                .returns(getOutputType().getTypeAt(1))
                .setParallelism(parallelism);

        mapDataStream.addSink(flinkKafkaProducer).name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(fieldTypes, fieldNames));
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    private String[] getPartitionKeys(KafkaSinkTableInfo kafkaSinkTableInfo){
        if(StringUtils.isNotBlank(kafkaSinkTableInfo.getPartitionKeys())){
            return StringUtils.split(kafkaSinkTableInfo.getPartitionKeys(), ',');
        }
        return null;
    }
}
