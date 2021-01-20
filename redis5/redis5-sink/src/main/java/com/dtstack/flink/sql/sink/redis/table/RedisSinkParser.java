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

import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
/**
 * @author yanxi
 */
public class RedisSinkParser extends AbstractTableParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
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

        if (props.get(RedisTableInfo.KEY_EXPIRED_TIME.toLowerCase()) != null) {
            redisTableInfo.setKeyExpiredTime(Integer.parseInt(MathUtil.getString(props.get(RedisTableInfo.KEY_EXPIRED_TIME.toLowerCase()))));
        }

        String primaryKeysStr = MathUtil.getString(props.get(RedisTableInfo.PRIMARY_KEYS_NAME));
        List<String> primaryKeysList = Lists.newArrayList();
        if (!StringUtils.isEmpty(primaryKeysStr)) {
            primaryKeysList = Arrays.asList(StringUtils.split(primaryKeysStr, ","));
        }
        redisTableInfo.setPrimaryKeys(primaryKeysList);
        redisTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(RedisTableInfo.PARALLELISM_KEY.toLowerCase())));

        return redisTableInfo;
    }
}
