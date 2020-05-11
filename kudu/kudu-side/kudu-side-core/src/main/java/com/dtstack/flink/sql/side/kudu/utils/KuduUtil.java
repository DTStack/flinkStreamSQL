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

package com.dtstack.flink.sql.side.kudu.utils;

import com.dtstack.flink.sql.side.PredicateInfo;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * util for kudu use
 * Date: 2019/12/16
 * Company: www.dtstack.com
 * @author maqi
 */
public class KuduUtil {

    public static void primaryKeyRange(PartialRow partialRow, Type type, String primaryKey, String value) {
        switch (type) {
            case STRING:
                partialRow.addString(primaryKey, value);
                break;
            case FLOAT:
                partialRow.addFloat(primaryKey, Float.valueOf(value));
                break;
            case INT8:
                partialRow.addByte(primaryKey, Byte.valueOf(value));
                break;
            case INT16:
                partialRow.addShort(primaryKey, Short.valueOf(value));
                break;
            case INT32:
                partialRow.addInt(primaryKey, Integer.valueOf(value));
                break;
            case INT64:
                partialRow.addLong(primaryKey, Long.valueOf(value));
                break;
            case DOUBLE:
                partialRow.addDouble(primaryKey, Double.valueOf(value));
                break;
            case BOOL:
                partialRow.addBoolean(primaryKey, Boolean.valueOf(value));
                break;
            case UNIXTIME_MICROS:
                partialRow.addTimestamp(primaryKey, Timestamp.valueOf(value));
                break;
            case BINARY:
                partialRow.addBinary(primaryKey, value.getBytes());
                break;
            default:
                throw new IllegalArgumentException("Illegal var type: " + type);
        }
    }

    public static void setMapValue(Type type, Map<String, Object> oneRow, String sideFieldName, RowResult result) {
        switch (type) {
            case STRING:
                oneRow.put(sideFieldName, result.getString(sideFieldName));
                break;
            case FLOAT:
                oneRow.put(sideFieldName, result.getFloat(sideFieldName));
                break;
            case INT8:
                oneRow.put(sideFieldName, (int) result.getByte(sideFieldName));
                break;
            case INT16:
                oneRow.put(sideFieldName, (int) result.getShort(sideFieldName));
                break;
            case INT32:
                oneRow.put(sideFieldName, result.getInt(sideFieldName));
                break;
            case INT64:
                oneRow.put(sideFieldName, result.getLong(sideFieldName));
                break;
            case DOUBLE:
                oneRow.put(sideFieldName, result.getDouble(sideFieldName));
                break;
            case BOOL:
                oneRow.put(sideFieldName, result.getBoolean(sideFieldName));
                break;
            case UNIXTIME_MICROS:
                oneRow.put(sideFieldName, result.getTimestamp(sideFieldName));
                break;
            case BINARY:
                oneRow.put(sideFieldName, result.getBinary(sideFieldName));
                break;
            default:
                throw new IllegalArgumentException("Illegal var type: " + type);
        }
    }

    public static Object getValue(String value, Type type){
        if(value == null){
            return null;
        }

        if(value.startsWith("\"") || value.endsWith("'")){
            value = value.substring(1, value.length() - 1);
        }

        Object objValue;
        if (Type.BOOL.equals(type)){
            objValue = Boolean.valueOf(value);
        } else if(Type.INT8.equals(type)){
            objValue = Byte.valueOf(value);
        } else if(Type.INT16.equals(type)){
            objValue = Short.valueOf(value);
        } else if(Type.INT32.equals(type)){
            objValue = Integer.valueOf(value);
        } else if(Type.INT64.equals(type)){
            objValue = Long.valueOf(value);
        } else if(Type.FLOAT.equals(type)){
            objValue = Float.valueOf(value);
        } else if(Type.DOUBLE.equals(type)){
            objValue = Double.valueOf(value);
        } else if(Type.DECIMAL.equals(type)){
            objValue = new BigDecimal(value);
        } else if(Type.UNIXTIME_MICROS.equals(type)){
            if(NumberUtils.isNumber(value)){
                objValue = Long.valueOf(value);
            } else {
                objValue = Timestamp.valueOf(value);
            }
        } else {
            objValue = value;
        }

        return objValue;
    }

    public static KuduPredicate buildKuduPredicate(Schema schema, PredicateInfo info) {
        ColumnSchema column = schema.getColumn(info.getFieldName());
        Object value = "";
        switch (info.getOperatorKind()) {
            case "IN":
            case "NOT_IN":
            case "BETWEEN":
                value = Arrays.asList(StringUtils.split(info.getCondition(), ",")).stream()
                        .map(val -> KuduUtil.getValue(val.trim(), column.getType())).collect(Collectors.toList());
                break;
            case "IS_NOT_NULL":
            case "IS_NULL":
                break;
            default:
                value = KuduUtil.getValue(info.getCondition(), column.getType());
        }

        switch (info.getOperatorName()) {
            case "=":
                return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.EQUAL, value);
            case ">":
                return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.GREATER, value);
            case ">=":
                return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.GREATER_EQUAL, value);
            case "<":
                return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.LESS, value);
            case "<=":
                return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.LESS_EQUAL, value);
            case "IN":
                return KuduPredicate.newInListPredicate(column, (List) value);
            case "IS NOT NULL":
                return KuduPredicate.newIsNotNullPredicate(column);
            case "IS NULL":
                return KuduPredicate.newIsNullPredicate(column);
            default:
        }
        return null;

    }
}
