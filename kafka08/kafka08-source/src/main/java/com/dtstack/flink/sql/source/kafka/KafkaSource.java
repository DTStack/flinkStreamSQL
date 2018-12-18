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
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

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

        KafkaSourceTableInfo kafka08SourceTableInfo = (KafkaSourceTableInfo) sourceTableInfo;
        String topicName = kafka08SourceTableInfo.getKafkaParam("topic");
        String offsetReset = kafka08SourceTableInfo.getKafkaParam("auto.offset.reset");

        Properties props = new Properties();
        for (String key:kafka08SourceTableInfo.getKafkaParamKeys()) {
            props.setProperty(key, kafka08SourceTableInfo.getKafkaParam(key));
        }

        TypeInformation[] types = new TypeInformation[kafka08SourceTableInfo.getFields().length];
        for(int i = 0; i< kafka08SourceTableInfo.getFieldClasses().length; i++){
            types[i] = TypeInformation.of(kafka08SourceTableInfo.getFieldClasses()[i]);
        }

        TypeInformation<Row> typeInformation = new RowTypeInfo(types, kafka08SourceTableInfo.getFields());

        FlinkKafkaConsumer08<Row> kafkaSrc;
        String fields;
        if("json".equalsIgnoreCase(kafka08SourceTableInfo.getSourceDataType())){
            kafkaSrc = new FlinkKafkaConsumer08(topicName,
                    new CustomerJsonDeserialization(typeInformation), props);
            fields = StringUtils.join(kafka08SourceTableInfo.getFields(), ",");
        }else if ("csv".equalsIgnoreCase(kafka08SourceTableInfo.getSourceDataType())){
            kafkaSrc = new FlinkKafkaConsumer08(topicName,
                    new CustomerCsvDeserialization(typeInformation,
                            kafka08SourceTableInfo.getFieldDelimiter(),kafka08SourceTableInfo.getLengthCheckPolicy()),props);
            fields = StringUtils.join(kafka08SourceTableInfo.getFields(), ",");
        }else{
            kafkaSrc = new FlinkKafkaConsumer08(topicName,
                    new CustomerCommonDeserialization(),props);
            fields = StringUtils.join(kafka08SourceTableInfo.getFields(), ",");
        }
        kafkaSrc.setCommitOffsetsOnCheckpoints(true);
        //earliest,latest
        if("earliest".equalsIgnoreCase(offsetReset)){
            kafkaSrc.setStartFromEarliest();
        }else{
            kafkaSrc.setStartFromLatest();
        }

        DataStreamSource kafkaSource = env.addSource(kafkaSrc, typeInformation);
        Integer parallelism = kafka08SourceTableInfo.getParallelism();
        if(parallelism != null){
            kafkaSource.setParallelism(parallelism);
        }
        return tableEnv.fromDataStream(kafkaSource, fields);
    }
}
