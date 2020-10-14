/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.side.rdb.util;

import com.dtstack.flink.sql.util.MathUtil;

/**
 * Reason:
 * Date: 2018/12/3
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class SwitchUtil {
    public static Object getTarget(Object obj, String targetType) {
        targetType = targetType.toLowerCase();
        switch (targetType) {

            case "smallint":
            case "smallintunsigned":
            case "tinyint":
            case "tinyintunsigned":
            case "mediumint":
            case "mediumintunsigned":
            case "integer":
            case "int":
                return MathUtil.getIntegerVal(obj);

            case "bigint":
            case "bigintunsigned":
            case "intunsigned":
            case "integerunsigned":
                return MathUtil.getLongVal(obj);

            case "boolean":
                return MathUtil.getBoolean(obj);

            case "blob":
                return MathUtil.getByte(obj);

            case "varchar":
            case "char":
            case "text":
                return MathUtil.getString(obj);

            case "real":
            case "float":
            case "realunsigned":
            case "floatunsigned":
                return MathUtil.getFloatVal(obj);

            case "double":
            case "doubleunsigned":
                return MathUtil.getDoubleVal(obj);

            case "decimal":
            case "decimalunsigned":
                return MathUtil.getBigDecimal(obj);

            case "date":
                return MathUtil.getDate(obj);

            case "timestamp":
            case "datetime":
                return MathUtil.getTimestamp(obj);
            case "time":
                return MathUtil.getTime(obj);
            default:
        }
        return obj;
    }
}
