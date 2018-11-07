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


package com.dtstack.flink.sql.sink.mongo.table;

import com.dtstack.flink.sql.sink.mongo.table.MongoTableInfo;
import com.dtstack.flink.sql.table.AbsTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * Reason:
 * Date: 2018/11/6
 *
 * @author xuqianjin
 */

public class MongoSinkParser extends AbsTableParser {

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        MongoTableInfo MongoTableInfo = new MongoTableInfo();
        MongoTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, MongoTableInfo);

        MongoTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(MongoTableInfo.PARALLELISM_KEY.toLowerCase())));
        MongoTableInfo.setAddress(MathUtil.getString(props.get(MongoTableInfo.ADDRESS_KEY.toLowerCase())));
        MongoTableInfo.setTableName(MathUtil.getString(props.get(MongoTableInfo.TABLE_NAME_KEY.toLowerCase())));
        MongoTableInfo.setDatabase(MathUtil.getString(props.get(MongoTableInfo.DATABASE_KEY.toLowerCase())));
        MongoTableInfo.setUserName(MathUtil.getString(props.get(MongoTableInfo.USER_NAME_KEY.toLowerCase())));
        MongoTableInfo.setPassword(MathUtil.getString(props.get(MongoTableInfo.PASSWORD_KEY.toLowerCase())));

        return MongoTableInfo;
    }
}
