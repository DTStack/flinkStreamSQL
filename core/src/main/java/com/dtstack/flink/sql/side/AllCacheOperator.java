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


package com.dtstack.flink.sql.side;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/**
 * get plugin which implement from RichFlatMapFunction
 * Date: 2018/9/18
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class AllCacheOperator {

    private static void loadFlatMap(){

    }

    public static DataStream getSideJoinDataStream(DataStream inputStream, String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo, JoinInfo joinInfo,
                                                   List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) throws Exception {
        return null;
    }
}
