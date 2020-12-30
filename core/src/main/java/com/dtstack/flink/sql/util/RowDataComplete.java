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

package com.dtstack.flink.sql.util;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2020-05-20
 */
public class RowDataComplete {

    public static void completeBaseRow(ResultFuture<BaseRow> resultFuture, BaseRow row) {
        resultFuture.complete(Collections.singleton(RowDataConvert.convertToBaseRow(row)));
    }

    public static void completeBaseRow(ResultFuture<BaseRow> resultFuture, List<BaseRow> rowList) {
        List<BaseRow> baseRowList = Lists.newArrayList();
        for (BaseRow baseRow : rowList) {
            baseRowList.add(RowDataConvert.convertToBaseRow(baseRow));
        }
        resultFuture.complete(baseRowList);
    }

    public static void collectBaseRow(Collector<BaseRow> out, BaseRow row) {
        out.collect(RowDataConvert.convertToBaseRow(row));
    }
}