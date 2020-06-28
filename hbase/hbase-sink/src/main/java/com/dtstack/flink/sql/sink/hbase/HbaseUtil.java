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

 

package com.dtstack.flink.sql.sink.hbase;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import scala.tools.nsc.Global;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Created by softfly on 17/6/30.
 */
public class HbaseUtil {

    private HbaseUtil() {}

    public static TypeInformation columnTypeToTypeInformation(String type) {
        type = type.toUpperCase();

        switch(type) {
            case "TINYINT":
                return BasicTypeInfo.getInfoFor(ByteWritable.class);
            case "SMALLINT":
                return BasicTypeInfo.SHORT_TYPE_INFO;
            case "INT":
                return BasicTypeInfo.getInfoFor(IntWritable.class);
            case "BIGINT":
                return BasicTypeInfo.LONG_TYPE_INFO;
            case "FLOAT":
                return BasicTypeInfo.FLOAT_TYPE_INFO;
            case "DOUBLE":
                return BasicTypeInfo.DOUBLE_TYPE_INFO;
            case "TIMESTAMP":
            case "DATE":
                return BasicTypeInfo.DATE_TYPE_INFO;
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return BasicTypeInfo.getInfoFor(Text.class);
            case "BOOLEAN":
                return BasicTypeInfo.BOOLEAN_TYPE_INFO;
            default:
                throw new IllegalArgumentException("Unsupported type");
        }

    }

    public static byte[] toByte(Object value) {
        if (Objects.isNull(value)) {
            return new byte[]{};
        }
        if (value instanceof Integer) {
            return Bytes.toBytes((Integer) value);
        } else if (value instanceof Boolean) {
            return Bytes.toBytes((Boolean) value);
        } else if (value instanceof ByteBuffer) {
            return Bytes.toBytes((ByteBuffer) value);
        } else if (value instanceof Double) {
            return Bytes.toBytes((Double) value);
        } else if (value instanceof Float) {
            return Bytes.toBytes((Float) value);
        } else if (value instanceof Long) {
            return Bytes.toBytes((Long) value);
        } else if (value instanceof Short) {
            return Bytes.toBytes((Short) value);
        } else if (value instanceof String) {
            return Bytes.toBytes(String.valueOf(value));
        } else if (value instanceof BigDecimal) {
            return Bytes.toBytes((BigDecimal) value);
        }
        throw new RuntimeException("unkown dateType[" + value.toString() + "]");
    }

}
