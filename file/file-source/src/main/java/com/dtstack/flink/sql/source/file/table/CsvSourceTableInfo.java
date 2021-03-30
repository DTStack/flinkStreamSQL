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

package com.dtstack.flink.sql.source.file.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.types.Row;

/**
 * @author tiezhu
 * @date 2021/3/22 星期一
 * Company dtstack
 */
public class CsvSourceTableInfo extends FileSourceTableInfo {

    /**
     * 是否忽略解析错误，默认为true
     */
    private Boolean ignoreParseErrors;

    /**
     * 字段值的分割符，默认为','
     */
    private char fieldDelimiter;

    /**
     * 针对null的替换值，默认为"null"字符
     */
    private String nullLiteral;

    /**
     * 默认为true
     */
    private Boolean allowComments;

    /**
     * 数组元素分割符，默认为','
     */
    private String arrayElementDelimiter;

    private char quoteCharacter;

    private char escapeCharacter;

    private CsvSourceTableInfo() {
    }

    public Boolean getIgnoreParseErrors() {
        return ignoreParseErrors;
    }

    public void setIgnoreParseErrors(Boolean ignoreParseErrors) {
        this.ignoreParseErrors = ignoreParseErrors;
    }

    public char getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(char fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getNullLiteral() {
        return nullLiteral;
    }

    public void setNullLiteral(String nullLiteral) {
        this.nullLiteral = nullLiteral;
    }

    public Boolean getAllowComments() {
        return allowComments;
    }

    public void setAllowComments(Boolean allowComments) {
        this.allowComments = allowComments;
    }

    public String getArrayElementDelimiter() {
        return arrayElementDelimiter;
    }

    public void setArrayElementDelimiter(String arrayElementDelimiter) {
        this.arrayElementDelimiter = arrayElementDelimiter;
    }

    public char getQuoteCharacter() {
        return quoteCharacter;
    }

    public void setQuoteCharacter(char quoteCharacter) {
        this.quoteCharacter = quoteCharacter;
    }

    public char getEscapeCharacter() {
        return escapeCharacter;
    }

    public void setEscapeCharacter(char escapeCharacter) {
        this.escapeCharacter = escapeCharacter;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final CsvSourceTableInfo tableInfo;

        public Builder() {
            tableInfo = new CsvSourceTableInfo();
        }

        public Builder setFieldDelimiter(Character fieldDelimiter) {
            tableInfo.setFieldDelimiter(fieldDelimiter);
            return this;
        }

        public Builder setIgnoreParseErrors(Boolean ignoreParseErrors) {
            tableInfo.setIgnoreParseErrors(ignoreParseErrors);
            return this;
        }

        public Builder setNullLiteral(String nullLiteral) {
            tableInfo.setNullLiteral(nullLiteral);
            return this;
        }

        public Builder setAllowComment(Boolean allowComment) {
            tableInfo.setAllowComments(allowComment);
            return this;
        }

        public Builder setArrayElementDelimiter(String arrayElementDelimiter) {
            tableInfo.setArrayElementDelimiter(arrayElementDelimiter);
            return this;
        }

        public Builder setQuoterCharacter(Character quoterCharacter) {
            tableInfo.setQuoteCharacter(quoterCharacter);
            return this;
        }

        public Builder setEscapeCharacter(Character escapeCharacter) {
            tableInfo.setEscapeCharacter(escapeCharacter);
            return this;
        }

        public Builder setTypeInformation(TypeInformation<Row> typeInformation) {
            tableInfo.setTypeInformation(typeInformation);
            return this;
        }

        public DeserializationSchema<Row> buildCsvDeserializationSchema() {
            return new CsvRowDeserializationSchema
                .Builder(tableInfo.getTypeInformation())
                .setIgnoreParseErrors(tableInfo.getIgnoreParseErrors())
                .setFieldDelimiter(tableInfo.getFieldDelimiter())
                .setNullLiteral(tableInfo.getNullLiteral())
                .setAllowComments(tableInfo.getAllowComments())
                .setArrayElementDelimiter(tableInfo.getArrayElementDelimiter())
                .setEscapeCharacter(tableInfo.getEscapeCharacter())
                .setQuoteCharacter(tableInfo.getQuoteCharacter())
                .build();
        }
    }
}
