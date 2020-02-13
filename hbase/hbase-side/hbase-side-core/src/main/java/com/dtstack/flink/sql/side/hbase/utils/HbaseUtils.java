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



package com.dtstack.flink.sql.side.hbase.utils;

import com.dtstack.flink.sql.util.MathUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Reason:
 * Date: 2018/9/10
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class HbaseUtils {

    public static Object convertByte(byte[] hbaseData, String type){
        String data = Bytes.toString(hbaseData);
        if(type == null){
            return data;
        }

        switch (type.toLowerCase()) {
            case "boolean":
                return MathUtil.getBoolean(data);

            case "int":
                return MathUtil.getIntegerVal(data);

            case "bigint":
                return MathUtil.getLongVal(data);

            case "tinyint":
            case "byte":
                return MathUtil.getByte(data);

            case "short":
            case "smallint":
                return MathUtil.getShort(data);

            case "char":
            case "varchar":
            case "string":
                return data;

            case "float":
                return MathUtil.getFloatVal(data);

            case "double":
                return MathUtil.getDoubleVal(data);

            default:
                throw new RuntimeException("not support type of " + type);
        }
    }
}
