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
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

/**
 * kafka result table
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author DocLi
 *
 * @modifyer maqi
 *
 */
public class KafkaSink implements RetractStreamTableSink<Row>, IStreamSinkGener<KafkaSink> {
    private static final String SINK_OPERATOR_NAME_TPL = "${topic}_${table}";

    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    protected String topic;

    protected int parallelism;

    protected Properties properties;

    protected SinkFunction<Tuple2<Boolean,Row>> kafkaProducer011;

    /** The schema of the table. */
    private TableSchema schema;

    /** Partitioner to select Kafka partition for each item. */
    protected Optional<FlinkKafkaPartitioner<Tuple2<Boolean,Row>>> partitioner;

    protected String[] partitionKeys;

    protected String sinkOperatorName;


    //TODO build KafkaSinkBase
    @Override
    public KafkaSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        KafkaSinkTableInfo kafka11SinkTableInfo = (KafkaSinkTableInfo) targetTableInfo;
        this.topic = kafka11SinkTableInfo.getTopic();

        properties = new Properties();
        properties.setProperty("bootstrap.servers", kafka11SinkTableInfo.getBootstrapServers());

        for (String key : kafka11SinkTableInfo.getKafkaParamKeys()) {
            properties.setProperty(key, kafka11SinkTableInfo.getKafkaParam(key));
        }
        this.partitioner = Optional.of(new CustomerFlinkPartition<>());
        this.partitionKeys = getPartitionKeys(kafka11SinkTableInfo);
        this.fieldNames = kafka11SinkTableInfo.getFields();
        TypeInformation[] types = new TypeInformation[kafka11SinkTableInfo.getFields().length];
        for (int i = 0; i < kafka11SinkTableInfo.getFieldClasses().length; i++) {
            types[i] = TypeInformation.of(kafka11SinkTableInfo.getFieldClasses()[i]);
        }
        this.fieldTypes = types;

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < fieldNames.length; i++) {
            schemaBuilder.field(fieldNames[i], fieldTypes[i]);
        }
        this.schema = schemaBuilder.build();

        Integer parallelism = kafka11SinkTableInfo.getParallelism();
        if (parallelism != null) {
            this.parallelism = parallelism;
        }

        this.kafkaProducer011 =  new KafkaProducer011Factory()
                .createKafkaProducer(kafka11SinkTableInfo, getOutputType().getTypeAt(1), properties, partitioner, partitionKeys);

        sinkOperatorName = SINK_OPERATOR_NAME_TPL.replace("${topic}", topic).replace("${table}", kafka11SinkTableInfo.getName());
        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        DataStreamSink<Tuple2<Boolean, Row>> dataStreamSink = dataStream.addSink(kafkaProducer011).name(sinkOperatorName);
        if (parallelism > 0) {
            dataStreamSink.setParallelism(parallelism);
        }
        return dataStreamSink;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(fieldTypes, fieldNames));
    }


    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    private String[] getPartitionKeys(KafkaSinkTableInfo kafkaSinkTableInfo) {
        if (StringUtils.isNotBlank(kafkaSinkTableInfo.getPartitionKeys())) {
            return StringUtils.split(kafkaSinkTableInfo.getPartitionKeys(), ',');
        }
        return null;
    }

}
