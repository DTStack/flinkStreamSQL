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

package com.dtstack.flink.sql.sink.http.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * @author: chuixue
 * @create: 2021-03-03 10:40
 * @description:
 **/
public class HttpSinkParser extends AbstractTableParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        HttpTableInfo httpTableInfo = new HttpTableInfo();
        httpTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, httpTableInfo);
        httpTableInfo.setUrl(MathUtil.getString(props.get(HttpTableInfo.URL_KEY)));
        httpTableInfo.setDelay(MathUtil.getIntegerVal(props.getOrDefault(HttpTableInfo.DELAY_KEY, 50)));
        httpTableInfo.setFlag(MathUtil.getString(props.getOrDefault(HttpTableInfo.FLAG_KEY, "")));
        httpTableInfo.setMaxNumRetries(MathUtil.getIntegerVal(props.getOrDefault(HttpTableInfo.MAXNUMRETRIES_KEY, 3)));

        httpTableInfo.check();
        return httpTableInfo;
    }

}
