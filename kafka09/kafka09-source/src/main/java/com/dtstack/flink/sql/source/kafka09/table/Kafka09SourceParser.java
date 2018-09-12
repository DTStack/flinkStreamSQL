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

 

package com.dtstack.flink.sql.source.kafka09.table;

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

public class Kafka09SourceParser extends AbsSourceParser {

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {

        Kafka09SourceTableInfo kafka09SourceTableInfo = new Kafka09SourceTableInfo();
        kafka09SourceTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, kafka09SourceTableInfo);

        kafka09SourceTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(Kafka09SourceTableInfo.PARALLELISM_KEY.toLowerCase())));
        kafka09SourceTableInfo.setBootstrapServers(MathUtil.getString(props.get(Kafka09SourceTableInfo.BOOTSTRAPSERVERS_KEY.toLowerCase())));
        kafka09SourceTableInfo.setGroupId(MathUtil.getString(props.get(Kafka09SourceTableInfo.GROUPID_KEY.toLowerCase())));
        kafka09SourceTableInfo.setTopic(MathUtil.getString(props.get(Kafka09SourceTableInfo.TOPIC_KEY.toLowerCase())));
        return kafka09SourceTableInfo;
    }
}
