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

import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.source.kafka.enums.EKafkaOffset;
import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.dtstack.flink.sql.util.DataTypeUtils;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.lang.reflect.Array;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Date: 2020/3/20
 * Company: www.dtstack.com
 * @author maqi
 */
public abstract class AbstractKafkaSource implements IStreamSourceGener<Table> {

    private static final String SOURCE_OPERATOR_NAME_TPL = "${topic}_${table}";

    protected Properties getKafkaProperties(KafkaSourceTableInfo kafkaSourceTableInfo) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSourceTableInfo.getBootstrapServers());

        if (DtStringUtil.isJson(kafkaSourceTableInfo.getOffsetReset())) {
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EKafkaOffset.NONE.name().toLowerCase());
        } else {
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaSourceTableInfo.getOffsetReset());
        }

        if (StringUtils.isNotBlank(kafkaSourceTableInfo.getGroupId())) {
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaSourceTableInfo.getGroupId());
        }

        for (String key : kafkaSourceTableInfo.getKafkaParamKeys()) {
            props.setProperty(key, kafkaSourceTableInfo.getKafkaParam(key));
        }
        return props;
    }

    protected String generateOperatorName(String tabName, String topicName) {
        return SOURCE_OPERATOR_NAME_TPL.replace("${topic}", topicName).replace("${table}", tabName);
    }

    protected TypeInformation<Row> getRowTypeInformation(KafkaSourceTableInfo kafkaSourceTableInfo) {
        String[] fieldTypes = kafkaSourceTableInfo.getFieldTypes();
        Class<?>[] fieldClasses = kafkaSourceTableInfo.getFieldClasses();
        TypeInformation[] types =
                IntStream.range(0, fieldClasses.length)
                .mapToObj(i -> {
                    if (fieldClasses[i].isArray()) {
                        return DataTypeUtils.convertToArray(fieldTypes[i]);
                    }
                    return TypeInformation.of(fieldClasses[i]);
                })
                .toArray(TypeInformation[]::new);

        return new RowTypeInfo(types, kafkaSourceTableInfo.getFields());
    }

    protected void setParallelism(Integer parallelism, DataStreamSource kafkaSource) {
        if (parallelism > 0) {
            kafkaSource.setParallelism(parallelism);
        }
    }

    protected void setStartPosition(String offset, String topicName, FlinkKafkaConsumerBase<Row> kafkaSrc) {
        if (StringUtils.equalsIgnoreCase(offset, EKafkaOffset.EARLIEST.name())) {
            kafkaSrc.setStartFromEarliest();
        } else if (DtStringUtil.isJson(offset)) {
            Map<KafkaTopicPartition, Long> specificStartupOffsets = buildOffsetMap(offset, topicName);
            kafkaSrc.setStartFromSpecificOffsets(specificStartupOffsets);
        } else {
            kafkaSrc.setStartFromLatest();
        }
    }

    /**
     *    kafka offset,eg.. {"0":12312,"1":12321,"2":12312}
     * @param offsetJson
     * @param topicName
     * @return
     */
    protected Map<KafkaTopicPartition, Long> buildOffsetMap(String offsetJson, String topicName) {
        try {
            Properties properties = PluginUtil.jsonStrToObject(offsetJson, Properties.class);
            Map<String, Object> offsetMap = PluginUtil.objectToMap(properties);
            Map<KafkaTopicPartition, Long> specificStartupOffsets = offsetMap
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            (Map.Entry<String, Object> entry) -> new KafkaTopicPartition(topicName, Integer.valueOf(entry.getKey())),
                            (Map.Entry<String, Object> entry) -> Long.valueOf(entry.getValue().toString()))
                    );

            return specificStartupOffsets;
        } catch (Exception e) {
            throw new RuntimeException("not support offsetReset type:" + offsetJson);
        }
    }

}
