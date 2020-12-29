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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2017年03月10日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 */
public class ClassUtil {

    public static Class<?> stringConvertClass(String str) {

        // 这部分主要是告诉Class转TypeInfomation的方法，字段是Array类型
        String lowerStr = str.toLowerCase().trim();
        if (lowerStr.startsWith("array")) {
            return Array.newInstance(Integer.class, 0).getClass();
        }
        if (lowerStr.startsWith("map")) {
            Map m = new HashMap();
            return m.getClass();
        }

        switch (lowerStr) {
            case "boolean":
            case "bit":
                return Boolean.class;

            case "smallint":
            case "smallintunsigned":
            case "tinyint":
            case "tinyintunsigned":
            case "mediumint":
            case "mediumintunsigned":
            case "integer":
            case "int":
                return Integer.class;

            case "blob":
                return Byte.class;

            case "bigint":
            case "intunsigned":
            case "integerunsigned":
            case "bigintunsigned":
                return Long.class;

            case "varchar":
            case "char":
            case "string":
                return String.class;
            case "text":
                throw new IllegalArgumentException(str + " type is not support, please use STRING. ");

            case "real":
            case "float":
            case "realunsigned":
            case "floatunsigned":
                return Float.class;

            case "double":
            case "doubleunsigned":
                return Double.class;

            case "date":
                return Date.class;

            case "datetime":
            case "timestamp":
                return Timestamp.class;

            case "time":
                return Time.class;

            case "decimal":
            case "decimalunsigned":
                return BigDecimal.class;
            default:
                break;
        }

        throw new RuntimeException("不支持 " + str + " 类型");
    }

    public static Object convertType(Object field, String fromType, String toType) {
        fromType = fromType.toUpperCase();
        toType = toType.toUpperCase();
        String rowData = field.toString();

        switch(toType) {
            case "TINYINT":
                return Byte.valueOf(rowData);
            case "SMALLINT":
                return Short.valueOf(rowData);
            case "INT":
                return Integer.valueOf(rowData);
            case "BIGINT":
                return Long.valueOf(rowData);
            case "FLOAT":
                return Float.valueOf(rowData);
            case "DOUBLE":
                return Double.valueOf(rowData);
            case "STRING":
                return rowData;
            case "BOOLEAN":
                return Boolean.valueOf(rowData);
            case "DATE":
                return DateUtil.columnToDate(field);
            case "TIMESTAMP":
                Date d = DateUtil.columnToDate(field);
                return new Timestamp(d.getTime());
            default:
                throw new RuntimeException("Can't convert from " + fromType + " to " + toType);
        }

    }

    public static String getTypeFromClass(Class<?> clz) {

        if(clz == Byte.class){
            return "TINYINT";
        }
        else if(clz == Short.class){
            return "SMALLINT";
        }
        else if(clz == Integer.class){
            return "INT";
        }
        else if(clz == Long.class){
            return "BIGINT";
        }
        else if(clz == String.class){
            return "STRING";
        }
        else if(clz == Float.class){
            return "FLOAT";
        }
        else if(clz == Double.class){
            return "DOUBLE";
        }
        else if(clz == Date.class){
            return "DATE";
        }
        else if(clz == Timestamp.class){
            return "TIMESTAMP";
        }
        else if(clz == Boolean.class){
            return "BOOLEAN";
        }
        throw new IllegalArgumentException("Unsupported data type: " + clz.getName());

    }

    public static String getTypeFromClassName(String clzName) {

        if(clzName.equals(Byte.class.getName())){
            return "TINYINT";
        }
        else if(clzName.equals(Short.class.getName())){
            return "SMALLINT";
        }
        else if(clzName.equals(Integer.class.getName())){
            return "INT";
        }
        else if(clzName.equals(Long.class.getName())){
            return "BIGINT";
        }
        else if(clzName.equals(String.class.getName())){
            return "STRING";
        }
        else if(clzName.equals(Float.class.getName())){
            return "FLOAT";
        }
        else if(clzName.equals(Double.class.getName())){
            return "DOUBLE";
        }
        else if(clzName.equals(Date.class.getName())){
            return "DATE";
        }
        else if(clzName.equals(Timestamp.class.getName())){
            return "TIMESTAMP";
        }
        else if(clzName.equals(Boolean.class.getName())){
            return "BOOLEAN";
        }
        throw new IllegalArgumentException("Unsupported data type: " + clzName);
    }

}
