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

package com.dtstack.flink.sql.sink.kafka.table;

import com.dtstack.flink.sql.format.FormatType;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author DocLi
 * @modifyer maqi
 */
public class KafkaSinkParser extends AbstractTableParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        KafkaSinkTableInfo kafkaSinkTableInfo = new KafkaSinkTableInfo();
        kafkaSinkTableInfo.setName(tableName);
        kafkaSinkTableInfo.setType(MathUtil.getString(props.get(KafkaSinkTableInfo.TYPE_KEY.toLowerCase())));
        parseFieldsInfo(fieldsInfo, kafkaSinkTableInfo);

        if (props.get(KafkaSinkTableInfo.SINK_DATA_TYPE) != null) {
            kafkaSinkTableInfo.setSinkDataType(props.get(KafkaSinkTableInfo.SINK_DATA_TYPE).toString());
        } else {
            kafkaSinkTableInfo.setSinkDataType(FormatType.JSON.name());
        }

        kafkaSinkTableInfo.setBootstrapServers(MathUtil.getString(props.get(KafkaSinkTableInfo.BOOTSTRAPSERVERS_KEY.toLowerCase())));
        kafkaSinkTableInfo.setTopic(MathUtil.getString(props.get(KafkaSinkTableInfo.TOPIC_KEY.toLowerCase())));

        Integer parallelism = MathUtil.getIntegerVal(props.get(KafkaSinkTableInfo.PARALLELISM_KEY.toLowerCase()));
        kafkaSinkTableInfo.setParallelism(parallelism);

        for (String key : props.keySet()) {
            if (!key.isEmpty() && key.startsWith("kafka.")) {
                kafkaSinkTableInfo.addKafkaParam(key.substring(6), props.get(key).toString());
            }
        }
        kafkaSinkTableInfo.check();

        return kafkaSinkTableInfo;
    }
}