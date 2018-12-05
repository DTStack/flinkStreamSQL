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

import java.text.ParseException;

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
            case "int":
            case "integer":
                return MathUtil.getIntegerVal(obj);
            case "bigint":
                return MathUtil.getLongVal(obj);
            case "boolean":
                return MathUtil.getBoolean(obj);
            case "tinyint":
                return MathUtil.getByte(obj);
            case "smallint":
                return MathUtil.getShort(obj);
            case "varchar":
                return MathUtil.getString(obj);
            case "real":
            case "float":
                return MathUtil.getFloatVal(obj);
            case "double":
                return MathUtil.getDoubleVal(obj);
            case "decimal":
                return MathUtil.getBigDecimal(obj);
            case "date":
                return MathUtil.getDate(obj);
            case "timestamp":
                return MathUtil.getTimestamp(obj);
        }
        return obj;
    }
}
