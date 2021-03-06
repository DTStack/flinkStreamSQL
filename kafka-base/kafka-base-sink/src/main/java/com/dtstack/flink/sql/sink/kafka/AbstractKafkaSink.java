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

import com.dtstack.flink.sql.enums.EUpdateMode;
import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.dtstack.flink.sql.util.DataTypeUtils;
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
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Date: 2020/3/30
 * Company: www.dtstack.com
 * @author maqi
 */
public abstract class AbstractKafkaSink implements RetractStreamTableSink<Row>, IStreamSinkGener {
    public static final String SINK_OPERATOR_NAME_TPL = "${topic}_${table}";

    protected String[] fieldNames;
    protected TypeInformation<?>[] fieldTypes;

    protected String[] partitionKeys;
    protected String sinkOperatorName;
    protected Properties properties;
    protected int parallelism = -1;
    protected String topic;
    protected String tableName;
    protected String updateMode;

    protected TableSchema schema;
    protected SinkFunction<Tuple2<Boolean,Row>> kafkaProducer011;

    protected Optional<FlinkKafkaPartitioner<Tuple2<Boolean,Row>>> partitioner;

    protected Properties getKafkaProperties(KafkaSinkTableInfo KafkaSinkTableInfo) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSinkTableInfo.getBootstrapServers());

        for (String key : KafkaSinkTableInfo.getKafkaParamKeys()) {
            props.setProperty(key, KafkaSinkTableInfo.getKafkaParam(key));
        }
        return props;
    }
    // TODO Source有相同的方法日后可以合并
    protected TypeInformation[] getTypeInformations(KafkaSinkTableInfo kafka11SinkTableInfo) {
        String[] fieldTypes = kafka11SinkTableInfo.getFieldTypes();
        Class<?>[] fieldClasses = kafka11SinkTableInfo.getFieldClasses();
        TypeInformation[] types = IntStream.range(0, fieldClasses.length)
                .mapToObj(
                    i -> {
                        if (fieldClasses[i].isArray()) {
                            return DataTypeUtils.convertToArray(fieldTypes[i]);
                        }
                        if (fieldClasses[i] == new HashMap().getClass()) {
                            return DataTypeUtils.convertToMap(fieldTypes[i]);
                        }
                        return TypeInformation.of(fieldClasses[i]);
                    })
                .toArray(TypeInformation[]::new);
        return types;
    }


    protected TableSchema buildTableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        Preconditions.checkArgument(fieldNames.length == fieldTypes.length, "fieldNames length must equals fieldTypes length !");

        DataType[] dataTypes = IntStream.range(0, fieldTypes.length)
                .mapToObj(i -> fromLegacyInfoToDataType(fieldTypes[i]))
                .toArray(DataType[]::new);

        TableSchema tableSchema = TableSchema.builder()
                .fields(fieldNames, dataTypes)
                .build();
        return tableSchema;
    }

    protected String[] getPartitionKeys(KafkaSinkTableInfo kafkaSinkTableInfo) {
        String keysStr = kafkaSinkTableInfo.getPartitionKeys();
        if (StringUtils.isNotBlank(keysStr)) {
            String[] keys = StringUtils.split(keysStr, ",");
            String[] cleanedKeys = Arrays.stream(keys)
                .map(x -> x.trim())
                .toArray(String[]::new);
            return cleanedKeys;
        }
        return null;
    }

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        if (updateMode.equalsIgnoreCase(EUpdateMode.APPEND.name())) {
            dataStream = dataStream.filter((Tuple2<Boolean, Row> record) -> record.f0);
        }
        DataStreamSink<Tuple2<Boolean, Row>> dataStreamSink = dataStream.addSink(kafkaProducer011).name(sinkOperatorName);
        if (parallelism > 0) {
            dataStreamSink.setParallelism(parallelism);
        }
        return dataStreamSink;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
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
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }
}
