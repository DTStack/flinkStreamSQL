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

package com.dtstack.flink.sql.source.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializedValue;

import com.dtstack.flink.sql.format.DeserializationMetricWrapper;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;


/**
 * Reason:
 * Date: 2018/10/19
 * Company: www.dtstack.com
 *
 * @author xuchao
 */
public class KafkaConsumer010 extends FlinkKafkaConsumer010<Row> {

    private DeserializationMetricWrapper deserializationMetricWrapper;

    public KafkaConsumer010(String topic, DeserializationMetricWrapper deserializationMetricWrapper, Properties props) {
        super(Arrays.asList(StringUtils.split(topic, ",")), deserializationMetricWrapper, props);
        this.deserializationMetricWrapper = deserializationMetricWrapper;
    }

    public KafkaConsumer010(String topic,
                            DeserializationMetricWrapper deserializationMetricWrapper,
                            Map<KafkaTopicPartition, Long> specificEndOffsets,
                            Properties props) {
        super(Arrays.asList(StringUtils.split(topic, ",")),
                new DtKafkaDeserializationSchemaWrapper<>(deserializationMetricWrapper, specificEndOffsets),
                props);
        this.deserializationMetricWrapper = deserializationMetricWrapper;
    }

    public KafkaConsumer010(Pattern subscriptionPattern, DeserializationMetricWrapper deserializationMetricWrapper, Properties props) {
        super(subscriptionPattern, deserializationMetricWrapper, props);
        this.deserializationMetricWrapper = deserializationMetricWrapper;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        deserializationMetricWrapper.setRuntimeContext(getRuntimeContext());
        deserializationMetricWrapper.initMetric();
        super.run(sourceContext);
    }

    @Override
    protected AbstractFetcher<Row, ?> createFetcher(SourceContext<Row> sourceContext,
                                                    Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
                                                    SerializedValue<AssignerWithPeriodicWatermarks<Row>> watermarksPeriodic,
                                                    SerializedValue<AssignerWithPunctuatedWatermarks<Row>> watermarksPunctuated,
                                                    StreamingRuntimeContext runtimeContext,
                                                    OffsetCommitMode offsetCommitMode,
                                                    MetricGroup consumerMetricGroup,
                                                    boolean useMetrics) throws Exception {

        AbstractFetcher<Row, ?> fetcher = super.createFetcher(sourceContext,
                assignedPartitionsWithInitialOffsets,
                watermarksPeriodic, watermarksPunctuated,
                runtimeContext, offsetCommitMode,
                consumerMetricGroup,
                useMetrics);

        ((KafkaDeserializationMetricWrapper) deserializationMetricWrapper).setFetcher(fetcher);
        return fetcher;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        DeserializationSchema<Row> deserializationSchema = deserializationMetricWrapper.getDeserializationSchema();
        return deserializationSchema.getProducedType();
    }

}