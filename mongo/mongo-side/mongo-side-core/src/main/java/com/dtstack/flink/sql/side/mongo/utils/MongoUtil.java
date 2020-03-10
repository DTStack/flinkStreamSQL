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

package com.dtstack.flink.sql.side.mongo.utils;

import com.dtstack.flink.sql.side.PredicateInfo;
import com.mongodb.BasicDBObject;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Date: 2019/12/17
 * Company: www.dtstack.com
 * @author maqi
 */
public class MongoUtil {
    public static BasicDBObject buildFilterObject(PredicateInfo info) {
        switch (info.getOperatorName()) {
            case "=":
                return new BasicDBObject("$eq", info.getCondition());
            case ">":
                return new BasicDBObject("$gt", info.getCondition());
            case ">=":
                return new BasicDBObject("$gte", info.getCondition());
            case "<":
                return new BasicDBObject("$lt", info.getCondition());
            case "<=":
                return new BasicDBObject("$lte", info.getCondition());
            case "<>":
                return new BasicDBObject("$ne", info.getCondition());
            case "IN":
                Object[] values = Arrays.stream(StringUtils.split(info.getCondition(), ",")).map(String::trim)
                        .collect(Collectors.toList()).toArray();
                return new BasicDBObject("$in", values);
            case "NOT IN":
                return new BasicDBObject("$nin", StringUtils.split(info.getCondition(), ","));
            case "IS NOT NULL":
                return new BasicDBObject("$exists", true);
            case "IS NULL":
                return new BasicDBObject("$exists", false);
            default:
        }
        return null;
    }
}
