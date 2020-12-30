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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.DataFormatConverters;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2020-05-20
 */
public class RowDataConvert {

    public static BaseRow convertToBaseRow(Tuple2<Boolean, Row> input) {
        Row row = input.f1;
        int length = row.getArity();
        GenericRow genericRow = new GenericRow(length);
        for (int i = 0; i < length; i++) {
            if (row.getField(i) == null) {
                genericRow.setField(i, row.getField(i));
            } else if (row.getField(i) instanceof String) {
                genericRow.setField(i, BinaryString.fromString((String) row.getField(i)));
            } else if (row.getField(i) instanceof Timestamp) {
                SqlTimestamp newTimestamp = SqlTimestamp.fromTimestamp(((Timestamp) row.getField(i)));
                genericRow.setField(i, newTimestamp);
            } else if (row.getField(i) instanceof LocalDateTime) {
                genericRow.setField(i, SqlTimestamp.fromLocalDateTime((LocalDateTime) row.getField(i)));
            } else if (row.getField(i) instanceof Time) {
                genericRow.setField(i, DataFormatConverters.TimeConverter.INSTANCE.toInternal((Time) row.getField(i)));
            } else if (row.getField(i) instanceof Double || row.getField(i).getClass().equals(double.class)) {
                genericRow.setField(i, DataFormatConverters.DoubleConverter.INSTANCE.toInternal((Double) row.getField(i)));
            } else if (row.getField(i) instanceof Float || row.getField(i).getClass().equals(float.class)) {
                genericRow.setField(i, DataFormatConverters.FloatConverter.INSTANCE.toInternal((Float) row.getField(i)));
            } else if (row.getField(i) instanceof Long || row.getField(i).getClass().equals(long.class)) {
                genericRow.setField(i, DataFormatConverters.LongConverter.INSTANCE.toInternal((Long) row.getField(i)));
            } else if (row.getField(i) instanceof Boolean || row.getField(i).getClass().equals(boolean.class)) {
                genericRow.setField(i, DataFormatConverters.BooleanConverter.INSTANCE.toInternal((Boolean) row.getField(i)));
            } else if (row.getField(i) instanceof Integer || row.getField(i).getClass().equals(int.class)) {
                genericRow.setField(i, DataFormatConverters.IntConverter.INSTANCE.toInternal((Integer) row.getField(i)));
            } else if (row.getField(i) instanceof Short || row.getField(i).getClass().equals(short.class)) {
                genericRow.setField(i, DataFormatConverters.ShortConverter.INSTANCE.toInternal((Short) row.getField(i)));
            } else if (row.getField(i) instanceof Byte || row.getField(i).getClass().equals(byte.class)) {
                genericRow.setField(i, DataFormatConverters.ByteConverter.INSTANCE.toInternal((Byte) row.getField(i)));
            } else if (row.getField(i) instanceof Date) {
                genericRow.setField(i, DataFormatConverters.DateConverter.INSTANCE.toInternal((Date) row.getField(i)));
            } else if (row.getField(i) instanceof LocalDate) {
                genericRow.setField(i, DataFormatConverters.LocalDateConverter.INSTANCE.toInternal((LocalDate) row.getField(i)));
            } else if (row.getField(i) instanceof LocalTime) {
                genericRow.setField(i, DataFormatConverters.LocalTimeConverter.INSTANCE.toInternal((LocalTime) row.getField(i)));
            } else if (row.getField(i) instanceof BigDecimal) {
                BigDecimal tempDecimal = (BigDecimal) row.getField(i);
                int precision = ((BigDecimal) row.getField(i)).precision();
                int scale = ((BigDecimal) row.getField(i)).scale();
                DataFormatConverters.DecimalConverter decimalConverter = new DataFormatConverters.DecimalConverter(precision, scale);
                genericRow.setField(i, decimalConverter.toExternal(Decimal.fromBigDecimal(tempDecimal, precision, scale)));
            } else {
                genericRow.setField(i, row.getField(i));
            }
        }

        if (input.f0) {
            BaseRowUtil.setAccumulate(genericRow);
        } else {
            BaseRowUtil.setRetract(genericRow);
        }

        return genericRow;
    }


    public static BaseRow convertToBaseRow(BaseRow input) {
        GenericRow row = (GenericRow) input;
        int length = row.getArity();
        GenericRow genericRow = new GenericRow(length);
        genericRow.setHeader(row.getHeader());
        for (int i = 0; i < length; i++) {
            if (row.getField(i) == null) {
                genericRow.setField(i, row.getField(i));
            } else if (row.getField(i) instanceof String) {
                genericRow.setField(i, BinaryString.fromString((String) row.getField(i)));
            } else if (row.getField(i) instanceof Timestamp) {
                SqlTimestamp newTimestamp = SqlTimestamp.fromTimestamp(((Timestamp) row.getField(i)));
                genericRow.setField(i, newTimestamp);
            } else if (row.getField(i) instanceof LocalDateTime) {
                genericRow.setField(i, SqlTimestamp.fromLocalDateTime((LocalDateTime) row.getField(i)));
            } else if (row.getField(i) instanceof Time) {
                genericRow.setField(i, DataFormatConverters.TimeConverter.INSTANCE.toInternal((Time) row.getField(i)));
            } else if (row.getField(i) instanceof Double || row.getField(i).getClass().equals(double.class)) {
                genericRow.setField(i, DataFormatConverters.DoubleConverter.INSTANCE.toInternal((Double) row.getField(i)));
            } else if (row.getField(i) instanceof Float || row.getField(i).getClass().equals(float.class)) {
                genericRow.setField(i, DataFormatConverters.FloatConverter.INSTANCE.toInternal((Float) row.getField(i)));
            } else if (row.getField(i) instanceof Long || row.getField(i).getClass().equals(long.class)) {
                genericRow.setField(i, DataFormatConverters.LongConverter.INSTANCE.toInternal((Long) row.getField(i)));
            } else if (row.getField(i) instanceof Boolean || row.getField(i).getClass().equals(boolean.class)) {
                genericRow.setField(i, DataFormatConverters.BooleanConverter.INSTANCE.toInternal((Boolean) row.getField(i)));
            } else if (row.getField(i) instanceof Integer || row.getField(i).getClass().equals(int.class)) {
                genericRow.setField(i, DataFormatConverters.IntConverter.INSTANCE.toInternal((Integer) row.getField(i)));
            } else if (row.getField(i) instanceof Short || row.getField(i).getClass().equals(short.class)) {
                genericRow.setField(i, DataFormatConverters.ShortConverter.INSTANCE.toInternal((Short) row.getField(i)));
            } else if (row.getField(i) instanceof Byte || row.getField(i).getClass().equals(byte.class)) {
                genericRow.setField(i, DataFormatConverters.ByteConverter.INSTANCE.toInternal((Byte) row.getField(i)));
            } else if (row.getField(i) instanceof Date) {
                genericRow.setField(i, DataFormatConverters.DateConverter.INSTANCE.toInternal((Date) row.getField(i)));
            } else if (row.getField(i) instanceof LocalDate) {
                genericRow.setField(i, DataFormatConverters.LocalDateConverter.INSTANCE.toInternal((LocalDate) row.getField(i)));
            } else if (row.getField(i) instanceof LocalTime) {
                genericRow.setField(i, DataFormatConverters.LocalTimeConverter.INSTANCE.toInternal((LocalTime) row.getField(i)));
            } else if (row.getField(i) instanceof BigDecimal) {
                BigDecimal tempDecimal = (BigDecimal) row.getField(i);
                int precision = ((BigDecimal) row.getField(i)).precision();
                int scale = ((BigDecimal) row.getField(i)).scale();
                DataFormatConverters.DecimalConverter decimalConverter = new DataFormatConverters.DecimalConverter(precision, scale);
                genericRow.setField(i, decimalConverter.toExternal(Decimal.fromBigDecimal(tempDecimal, precision, scale)));
            } else {
                genericRow.setField(i, row.getField(i));
            }
        }
        return genericRow;
    }
}