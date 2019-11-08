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

package com.dtstack.flink.sql.sink.redis.table;

import com.dtstack.flink.sql.table.AbsTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class RedisSinkParser extends AbsTableParser {
    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        RedisTableInfo redisTableInfo = new RedisTableInfo();
        redisTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, redisTableInfo);
        redisTableInfo.setUrl(MathUtil.getString(props.get(RedisTableInfo.URL_KEY)));
        redisTableInfo.setDatabase(MathUtil.getString(props.get(RedisTableInfo.DATABASE_KEY)));
        redisTableInfo.setPassword(MathUtil.getString(props.get(RedisTableInfo.PASSWORD_KEY)));
        redisTableInfo.setTablename(MathUtil.getString(props.get(RedisTableInfo.TABLENAME_KEY.toLowerCase())));
        if (props.get(RedisTableInfo.TIMEOUT) != null){
            redisTableInfo.setTimeout(Integer.parseInt(MathUtil.getString(props.get(RedisTableInfo.TIMEOUT))));
        }
        redisTableInfo.setMaxTotal(MathUtil.getString(props.get(RedisTableInfo.MAXTOTAL.toLowerCase())));
        redisTableInfo.setMaxIdle(MathUtil.getString(props.get(RedisTableInfo.MAXIDLE.toLowerCase())));
        redisTableInfo.setMinIdle(MathUtil.getString(props.get(RedisTableInfo.MINIDLE.toLowerCase())));
        redisTableInfo.setRedisType(MathUtil.getString(props.get(RedisTableInfo.REDIS_TYPE.toLowerCase())));
        redisTableInfo.setMasterName(MathUtil.getString(props.get(RedisTableInfo.MASTER_NAME.toLowerCase())));

        String primaryKeysStr = MathUtil.getString(props.get("primarykeys"));
        ArrayList<String> primaryKeysList = null;
        if (!StringUtils.isEmpty(primaryKeysStr)) {
            String[] primaryKeysArray = primaryKeysStr.split(",");
            primaryKeysList = new ArrayList<String>(Arrays.asList(primaryKeysArray));
        } else {
            primaryKeysList = new ArrayList<>();
        }
        redisTableInfo.setPrimaryKeys(primaryKeysList);

        return redisTableInfo;
    }
}
