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

 

package com.dtstack.flink.sql.source.kafka.table;

import com.dtstack.flink.sql.table.AbsSourceParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.ClassUtil;
import com.dtstack.flink.sql.util.MathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/09/18
 * Company: www.dtstack.com
 * @author sishu.yss
 */

public class KafkaSourceParser extends AbsSourceParser {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceParser.class);

    private static final String KAFKA_NEST_FIELD_KEY = "nestFieldKey";

    private static Pattern kafkaNestFieldKeyPattern = Pattern.compile("(?i)((\\S+\\.)*\\S+)\\s+(\\w+)\\s+AS\\s+(\\w+)$");

    static {
        keyPatternMap.put(KAFKA_NEST_FIELD_KEY, kafkaNestFieldKeyPattern);

        keyHandlerMap.put(KAFKA_NEST_FIELD_KEY, KafkaSourceParser::dealNestField);
    }

    /**
     * add parser for alias field
     * @param matcher
     * @param tableInfo
     */
    static void dealNestField(Matcher matcher, TableInfo tableInfo) {
        String physicalField = matcher.group(1);
        String fieldType = matcher.group(3);
        String mappingField = matcher.group(4);
        Class fieldClass= ClassUtil.stringConvertClass(fieldType);

        tableInfo.addPhysicalMappings(mappingField, physicalField);
        tableInfo.addField(mappingField);
        tableInfo.addFieldClass(fieldClass);
        tableInfo.addFieldType(fieldType);
        if(LOG.isInfoEnabled()){
            LOG.info(physicalField + "--->" + mappingField + " Class: " + fieldClass.toString());
        }
    }

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {

        KafkaSourceTableInfo kafka11SourceTableInfo = new KafkaSourceTableInfo();
        kafka11SourceTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, kafka11SourceTableInfo);

        kafka11SourceTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(KafkaSourceTableInfo.PARALLELISM_KEY.toLowerCase())));
        String bootstrapServer = MathUtil.getString(props.get(KafkaSourceTableInfo.BOOTSTRAPSERVERS_KEY.toLowerCase()));
        if (bootstrapServer == null || bootstrapServer.trim().equals("")){
            throw new Exception("BootstrapServers can not be empty!");
        } else {
            kafka11SourceTableInfo.setBootstrapServers(bootstrapServer);
        }
        kafka11SourceTableInfo.setGroupId(MathUtil.getString(props.get(KafkaSourceTableInfo.GROUPID_KEY.toLowerCase())));
        kafka11SourceTableInfo.setTopic(MathUtil.getString(props.get(KafkaSourceTableInfo.TOPIC_KEY.toLowerCase())));
        kafka11SourceTableInfo.setOffsetReset(MathUtil.getString(props.get(KafkaSourceTableInfo.OFFSETRESET_KEY.toLowerCase())));
        kafka11SourceTableInfo.setTopicIsPattern(MathUtil.getBoolean(props.get(KafkaSourceTableInfo.TOPICISPATTERN_KEY.toLowerCase())));
        kafka11SourceTableInfo.setTimeZone(MathUtil.getString(props.get(KafkaSourceTableInfo.TIME_ZONE_KEY.toLowerCase())));
        kafka11SourceTableInfo.check();
        return kafka11SourceTableInfo;
    }
}
