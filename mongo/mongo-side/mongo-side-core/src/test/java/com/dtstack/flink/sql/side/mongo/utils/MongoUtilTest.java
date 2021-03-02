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
import org.junit.Test;

/**
 * @author: chuixue
 * @create: 2020-07-27 09:57
 * @description:
 **/
public class MongoUtilTest {

    @Test
    public void testBuildFilterObject() {
        PredicateInfo[] predicateInfos = new PredicateInfo[]{
                new PredicateInfo("=", null, null, null, "a,b,c")
                , new PredicateInfo(">", null, null, null, "a,b,c")
                , new PredicateInfo(">=", null, null, null, "a,b,c")
                , new PredicateInfo("<", null, null, null, "a,b,c")
                , new PredicateInfo("<=", null, null, null, "a,b,c")
                , new PredicateInfo("<>", null, null, null, "a,b,c")
                , new PredicateInfo("IN", null, null, null, "a,b,c")
                , new PredicateInfo("NOT IN", null, null, null, "a,b,c")
                , new PredicateInfo("IS NOT NULL", null, null, null, "a,b,c")
                , new PredicateInfo("IS NULL", null, null, null, "a,b,c")
        };
        for (PredicateInfo predicateInfo : predicateInfos) {
            MongoUtil.buildFilterObject(predicateInfo);
        }
    }
}
