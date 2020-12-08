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

package com.dtstack.flink.sql.sink.aws.table;

import com.dtstack.flink.sql.sink.aws.AwsConstantKey;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * @author tiezhu
 * date 2020/12/1
 * company dtstack
 */
public class AwsSinkParser extends AbstractTableParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        AwsTableInfo tableInfo = new AwsTableInfo();
        tableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, tableInfo);

        tableInfo.setParallelism(MathUtil.getIntegerVal(props.get(AwsConstantKey.PARALLELISM_KEY.toLowerCase())));
        tableInfo.setAccessKey(MathUtil.getString(props.get(AwsConstantKey.ACCESS_KEY.toLowerCase())));
        tableInfo.setSecretKey(MathUtil.getString(props.get(AwsConstantKey.SECRET_KEY.toLowerCase())));
        tableInfo.setStorageType(MathUtil.getString(props.get(AwsConstantKey.STORAGE_TYPE.toLowerCase())));
        tableInfo.setHostname(MathUtil.getString(props.get(AwsConstantKey.HOST_NAME.toLowerCase())));
        tableInfo.setBucketAcl(MathUtil.getString(props.get(AwsConstantKey.BUCKET_ACL.toLowerCase())));
        tableInfo.setBucketName(MathUtil.getString(props.get(AwsConstantKey.BUCKET_KEY.toLowerCase())));
        tableInfo.setObjectName(MathUtil.getString(props.get(AwsConstantKey.OBJECT_NAME.toLowerCase())));

        tableInfo.check();

        return tableInfo;
    }
}
