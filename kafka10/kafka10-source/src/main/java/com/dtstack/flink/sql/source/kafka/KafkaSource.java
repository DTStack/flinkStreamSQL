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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * If eventtime field is specified, the default time field rowtime
 * Date: 2018/09/18
 * Company: www.dtstack.com
 * @author sishu.yss
 */

public class KafkaSource implements IStreamSourceGener<Table> {

    /**
     * Get kafka data source, you need to provide the data field names, data types
     * If you do not specify auto.offset.reset, the default use groupoffset
     * @param sourceTableInfo
     * @return
     */
    @SuppressWarnings("rawtypes")
    @Override
	public Table genStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        KafkaSourceTableInfo kafka010SourceTableInfo = (KafkaSourceTableInfo) sourceTableInfo;
        String topicName = kafka010SourceTableInfo.getTopic();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafka010SourceTableInfo.getBootstrapServers());
        props.setProperty("auto.offset.reset", kafka010SourceTableInfo.getOffsetReset());
        //TODO props.setProperty("zookeeper.connect", kafka09SourceTableInfo.)

        TypeInformation[] types = new TypeInformation[kafka010SourceTableInfo.getFields().length];
        for(int i = 0; i< kafka010SourceTableInfo.getFieldClasses().length; i++){
            types[i] = TypeInformation.of(kafka010SourceTableInfo.getFieldClasses()[i]);
        }

        TypeInformation<Row> typeInformation = new RowTypeInfo(types, kafka010SourceTableInfo.getFields());
        FlinkKafkaConsumer010<Row> kafkaSrc = new CustomerKafka010Consumer(topicName,
                new CustomerJsonDeserialization(typeInformation), props);

        //earliest,latest
        if("earliest".equalsIgnoreCase(kafka010SourceTableInfo.getOffsetReset())){
            kafkaSrc.setStartFromEarliest();
        }else{
            kafkaSrc.setStartFromLatest();
        }

        String fields = StringUtils.join(kafka010SourceTableInfo.getFields(), ",");
        return tableEnv.fromDataStream(env.addSource(kafkaSrc, typeInformation), fields);
    }
}
