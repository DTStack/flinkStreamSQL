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
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * Reason:
 * Date: 2018/7/4
 * Company: www.dtstack.com
 * @author xuchao
 */

public class KafkaSourceParser extends AbsSourceParser {

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {

        KafkaSourceTableInfo kafka09SourceTableInfo = new KafkaSourceTableInfo();
        kafka09SourceTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, kafka09SourceTableInfo);

        kafka09SourceTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(KafkaSourceTableInfo.PARALLELISM_KEY.toLowerCase())));
        String bootstrapServer = MathUtil.getString(props.get(KafkaSourceTableInfo.BOOTSTRAPSERVERS_KEY.toLowerCase()));
        if (bootstrapServer == null || bootstrapServer.trim().equals("")){
            throw new Exception("BootstrapServers can not be empty!");
        } else {
            kafka09SourceTableInfo.setBootstrapServers(bootstrapServer);
        }
        kafka09SourceTableInfo.setGroupId(MathUtil.getString(props.get(KafkaSourceTableInfo.GROUPID_KEY.toLowerCase())));
        kafka09SourceTableInfo.setTopic(MathUtil.getString(props.get(KafkaSourceTableInfo.TOPIC_KEY.toLowerCase())));
        kafka09SourceTableInfo.setOffsetReset(MathUtil.getString(props.get(KafkaSourceTableInfo.OFFSETRESET_KEY.toLowerCase())));
        kafka09SourceTableInfo.setTopicIsPattern(MathUtil.getBoolean(props.get(KafkaSourceTableInfo.TOPICISPATTERN_KEY.toLowerCase())));
        kafka09SourceTableInfo.setTimeZone(MathUtil.getString(props.get(KafkaSourceTableInfo.TIME_ZONE_KEY.toLowerCase())));
        for (String key : props.keySet()) {
            if (!key.isEmpty() && key.startsWith("kafka.")) {
                kafka09SourceTableInfo.addKafkaParam(key.substring(6), props.get(key).toString());
            }
        }
        kafka09SourceTableInfo.check();
        return kafka09SourceTableInfo;
    }
}
