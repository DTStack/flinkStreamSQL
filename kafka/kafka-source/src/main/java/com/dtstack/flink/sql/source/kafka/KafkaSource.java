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
import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.dtstack.flink.sql.table.SourceTableInfo;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author: chuixue
 * @create: 2019-11-05 10:55
 * @description:
 **/
public class KafkaSource implements IStreamSourceGener<Table> {

    private static final String SOURCE_OPERATOR_NAME_TPL = "${topic}_${table}";

    /**
     * Get kafka data source, you need to provide the data field names, data types
     * If you do not specify auto.offset.reset, the default use groupoffset
     *
     * @param sourceTableInfo
     * @return
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Table genStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        KafkaSourceTableInfo kafkaSourceTableInfo = (KafkaSourceTableInfo) sourceTableInfo;
        String topicName = kafkaSourceTableInfo.getTopic();

        Properties props = new Properties();
        for (String key : kafkaSourceTableInfo.getKafkaParamKeys()) {
            props.setProperty(key, kafkaSourceTableInfo.getKafkaParam(key));
        }
        props.setProperty("bootstrap.servers", kafkaSourceTableInfo.getBootstrapServers());
        if (DtStringUtil.isJosn(kafkaSourceTableInfo.getOffsetReset())) {
            props.setProperty("auto.offset.reset", "none");
        } else {
            props.setProperty("auto.offset.reset", kafkaSourceTableInfo.getOffsetReset());
        }
        if (StringUtils.isNotBlank(kafkaSourceTableInfo.getGroupId())) {
            props.setProperty("group.id", kafkaSourceTableInfo.getGroupId());
        }

        TypeInformation[] types = new TypeInformation[kafkaSourceTableInfo.getFields().length];
        for (int i = 0; i < kafkaSourceTableInfo.getFieldClasses().length; i++) {
            types[i] = TypeInformation.of(kafkaSourceTableInfo.getFieldClasses()[i]);
        }

        TypeInformation<Row> typeInformation = new RowTypeInfo(types, kafkaSourceTableInfo.getFields());

        FlinkKafkaConsumer<Row> kafkaSrc;
        CustomerJsonDeserialization customerJsonDeserialization = new CustomerJsonDeserialization(typeInformation, kafkaSourceTableInfo.getPhysicalFields(),
                kafkaSourceTableInfo.getFieldExtraInfoList());
        customerJsonDeserialization.setDirtyConfig(kafkaSourceTableInfo.getDirtyConfig());

        if (BooleanUtils.isTrue(kafkaSourceTableInfo.getTopicIsPattern())) {
            kafkaSrc = new CustomerKafkaConsumer(Pattern.compile(topicName), customerJsonDeserialization, props);
        } else {
            kafkaSrc = new CustomerKafkaConsumer(topicName, customerJsonDeserialization, props);
        }

        //earliest,latest
        if ("earliest".equalsIgnoreCase(kafkaSourceTableInfo.getOffsetReset())) {
            kafkaSrc.setStartFromEarliest();
        } else if (DtStringUtil.isJosn(kafkaSourceTableInfo.getOffsetReset())) {// {"0":12312,"1":12321,"2":12312}
            try {
                Properties properties = PluginUtil.jsonStrToObject(kafkaSourceTableInfo.getOffsetReset(), Properties.class);
                Map<String, Object> offsetMap = PluginUtil.ObjectToMap(properties);
                Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
                for (Map.Entry<String, Object> entry : offsetMap.entrySet()) {
                    specificStartupOffsets.put(new KafkaTopicPartition(topicName, Integer.valueOf(entry.getKey())), Long.valueOf(entry.getValue().toString()));
                }
                kafkaSrc.setStartFromSpecificOffsets(specificStartupOffsets);
            } catch (Exception e) {
                throw new RuntimeException("not support offsetReset type:" + kafkaSourceTableInfo.getOffsetReset());
            }
        } else {
            kafkaSrc.setStartFromLatest();
        }

        String fields = StringUtils.join(kafkaSourceTableInfo.getFields(), ",");
        String sourceOperatorName = SOURCE_OPERATOR_NAME_TPL.replace("${topic}", topicName).replace("${table}", sourceTableInfo.getName());

        DataStreamSource kafkaSource = env.addSource(kafkaSrc, sourceOperatorName, typeInformation);
        Integer parallelism = kafkaSourceTableInfo.getParallelism();
        if (parallelism != null) {
            kafkaSource.setParallelism(parallelism);
        }
        return tableEnv.fromDataStream(kafkaSource, fields);
    }
}
