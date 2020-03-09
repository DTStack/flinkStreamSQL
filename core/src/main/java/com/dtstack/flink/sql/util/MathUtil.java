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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;


import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

/**
 * Convert val to specified numeric type
 * Date: 2017/4/21
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MathUtil {

    private static final Pattern DATETIME = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3,9})?Z$");
    private static final Pattern DATE = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}$");
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private static final int MILLIS_PER_SECOND = 1000;

    public static Long getLongVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Long.valueOf((String) obj);
        } else if (obj instanceof Long) {
            return (Long) obj;
        } else if (obj instanceof Integer) {
            return Long.valueOf(obj.toString());
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).longValue();
        } else if (obj instanceof BigInteger) {
            return ((BigInteger) obj).longValue();
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Long.");
    }

    public static Long getLongVal(Object obj, long defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getLongVal(obj);
    }

    public static Integer getIntegerVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Integer.valueOf((String) obj);
        } else if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else if (obj instanceof Double) {
            return ((Double) obj).intValue();
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).intValue();
        } else if (obj instanceof BigInteger) {
            return ((BigInteger) obj).intValue();
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Integer.");
    }

    public static Integer getIntegerVal(Object obj, int defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getIntegerVal(obj);
    }

    public static Float getFloatVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Float.valueOf((String) obj);
        } else if (obj instanceof Float) {
            return (Float) obj;
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).floatValue();
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Float.");
    }

    public static Float getFloatVal(Object obj, float defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getFloatVal(obj);
    }

    public static Double getDoubleVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Double.valueOf((String) obj);
        } else if (obj instanceof Float) {
            return ((Float) obj).doubleValue();
        } else if (obj instanceof Double) {
            return (Double) obj;
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).doubleValue();
        } else if (obj instanceof Integer) {
            return ((Integer) obj).doubleValue();
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Double.");
    }

    public static Double getDoubleVal(Object obj, double defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getDoubleVal(obj);
    }


    public static Boolean getBoolean(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Boolean.valueOf((String) obj);
        } else if (obj instanceof Boolean) {
            return (Boolean) obj;
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Boolean.");
    }

    public static Boolean getBoolean(Object obj, boolean defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getBoolean(obj);
    }

    public static String getString(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return (String) obj;
        }

        return obj.toString();
    }

    public static Byte getByte(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Byte.valueOf((String) obj);
        } else if (obj instanceof Byte) {
            return (Byte) obj;
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Byte.");
    }

    public static Short getShort(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Short.valueOf((String) obj);
        } else if (obj instanceof Short) {
            return (Short) obj;
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Short.");
    }

    public static BigDecimal getBigDecimal(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            return new BigDecimal((String) obj);
        } else if (obj instanceof BigDecimal) {
            return (BigDecimal) obj;
        } else if (obj instanceof BigInteger) {
            return new BigDecimal((BigInteger) obj);
        } else if (obj instanceof Number) {
            return new BigDecimal(((Number) obj).doubleValue());
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to BigDecimal.");
    }

    public static Date getDate(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            return getDateFromStr((String) obj);
        } else if (obj instanceof Timestamp) {
            return new Date(((Timestamp) obj).getTime());
        } else if (obj instanceof Date) {
            return (Date) obj;
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Date.");
    }


    public static Date getDateFromStr(String dateStr) {
        // 2020-01-01 format
        if (DATE.matcher(dateStr).matches()) {
            // convert from local date to instant
            Instant instant = LocalDate.parse(dateStr).atTime(LocalTime.of(0, 0, 0, 0)).toInstant(ZoneOffset.UTC);
            // calculate the timezone offset in millis
            int offset = TimeZone.getDefault().getOffset(instant.toEpochMilli());
            // need to remove the offset since time has no TZ component
            return new Date(instant.toEpochMilli() - offset);
        } else if (DATETIME.matcher(dateStr).matches()) {
            // 2020-01-01T12:12:12Z format
            Instant instant = Instant.from(ISO_INSTANT.parse(dateStr));
            return new Date(instant.toEpochMilli());
        } else {
            try {
                // 2020-01-01 12:12:12.0 format
                return new Date(TIMESTAMP_FORMAT.parse(dateStr).getTime());
            } catch (ParseException e) {
                throw new RuntimeException("String convert to Date fail.");
            }
        }
    }

    public static Timestamp getTimestamp(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Timestamp) {
            return (Timestamp) obj;
        } else if (obj instanceof Date) {
            return new Timestamp(((Date) obj).getTime());
        } else if (obj instanceof String) {
            return getTimestampFromStr(obj.toString());
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Date.");
    }

    public static Timestamp getTimestampFromStr(String timeStr) {
        if (DATETIME.matcher(timeStr).matches()) {
            Instant instant = Instant.from(ISO_INSTANT.parse(timeStr));
            return new Timestamp(instant.getEpochSecond() * MILLIS_PER_SECOND);
        } else {
            Date date = null;
            try {
                date = new Date(TIMESTAMP_FORMAT.parse(timeStr).getTime());
            } catch (ParseException e) {
                throw new RuntimeException("getTimestampFromStr error data is " + timeStr);
            }
            return new Timestamp(date.getTime());
        }
    }

    public static String getStringFromTimestamp(Timestamp timestamp) {
        return TIMESTAMP_FORMAT.format(timestamp);
    }

    public static String getStringFromDate(Date date) {
        return DATE_FORMAT.format(date);
    }

}
