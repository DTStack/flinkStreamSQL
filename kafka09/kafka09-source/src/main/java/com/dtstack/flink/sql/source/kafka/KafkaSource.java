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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * If eventtime field is specified, the default time field rowtime
 * Date: 2017/2/20
 * Company: www.dtstack.com
 * @author xuchao
 */

public class KafkaSource implements IStreamSourceGener<Table> {

    private static final String SOURCE_OPERATOR_NAME_TPL = "${topic}_${table}";

    /**
     * Get kafka data source, you need to provide the data field names, data types
     * If you do not specify auto.offset.reset, the default use groupoffset
     * @param sourceTableInfo
     * @return
     */
    @SuppressWarnings("rawtypes")
    @Override
	public Table genStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        KafkaSourceTableInfo kafka09SourceTableInfo = (KafkaSourceTableInfo) sourceTableInfo;
        String topicName = kafka09SourceTableInfo.getTopic();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafka09SourceTableInfo.getBootstrapServers());
        if (DtStringUtil.isJosn(kafka09SourceTableInfo.getOffsetReset())){
            props.setProperty("auto.offset.reset", "none");
        } else {
            props.setProperty("auto.offset.reset", kafka09SourceTableInfo.getOffsetReset());
        }
        if (StringUtils.isNotBlank(kafka09SourceTableInfo.getGroupId())){
            props.setProperty("group.id", kafka09SourceTableInfo.getGroupId());
        }
        // only required for Kafka 0.8
        //TODO props.setProperty("zookeeper.connect", kafka09SourceTableInfo.)

        TypeInformation[] types = new TypeInformation[kafka09SourceTableInfo.getFields().length];
        for(int i = 0; i< kafka09SourceTableInfo.getFieldClasses().length; i++){
            types[i] = TypeInformation.of(kafka09SourceTableInfo.getFieldClasses()[i]);
        }

        TypeInformation<Row> typeInformation = new RowTypeInfo(types, kafka09SourceTableInfo.getFields());
        FlinkKafkaConsumer09<Row> kafkaSrc;
        if (BooleanUtils.isTrue(kafka09SourceTableInfo.getTopicIsPattern())) {
            kafkaSrc = new CustomerKafka09Consumer(Pattern.compile(topicName),
                    new CustomerJsonDeserialization(typeInformation, kafka09SourceTableInfo.getPhysicalFields()), props);
        } else {
            kafkaSrc = new CustomerKafka09Consumer(topicName,
                    new CustomerJsonDeserialization(typeInformation, kafka09SourceTableInfo.getPhysicalFields()), props);
        }

        //earliest,latest
        if("earliest".equalsIgnoreCase(kafka09SourceTableInfo.getOffsetReset())){
            kafkaSrc.setStartFromEarliest();
        }else if(DtStringUtil.isJosn(kafka09SourceTableInfo.getOffsetReset())){// {"0":12312,"1":12321,"2":12312}
            try {
                Properties properties = PluginUtil.jsonStrToObject(kafka09SourceTableInfo.getOffsetReset(), Properties.class);
                Map<String, Object> offsetMap = PluginUtil.ObjectToMap(properties);
                Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
                for(Map.Entry<String,Object> entry:offsetMap.entrySet()){
                    specificStartupOffsets.put(new KafkaTopicPartition(topicName,Integer.valueOf(entry.getKey())),Long.valueOf(entry.getValue().toString()));
                }
                kafkaSrc.setStartFromSpecificOffsets(specificStartupOffsets);
            } catch (Exception e) {
                throw new RuntimeException("not support offsetReset type:" + kafka09SourceTableInfo.getOffsetReset());
            }
        }else {
            kafkaSrc.setStartFromLatest();
        }

        String fields = StringUtils.join(kafka09SourceTableInfo.getFields(), ",");
        String sourceOperatorName = SOURCE_OPERATOR_NAME_TPL.replace("${topic}", topicName).replace("${table}", sourceTableInfo.getName());
        return tableEnv.fromDataStream(env.addSource(kafkaSrc, sourceOperatorName, typeInformation), fields);
    }
}
