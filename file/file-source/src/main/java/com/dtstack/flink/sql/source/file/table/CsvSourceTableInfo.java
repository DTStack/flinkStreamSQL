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

import com.dtstack.flink.sql.source.file.DTCsvRowDeserializationSchema;

/**
 * @author tiezhu
 * @date 2021/3/22 星期一
 * Company dtstack
 */
public class CsvSourceTableInfo extends FileSourceTableInfo {

    /**
     * 字段值的分割符，默认为','
     */
    private Character fieldDelimiter;

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

    private Character quoteCharacter;

    private Character escapeCharacter;

    public Character getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(Character fieldDelimiter) {
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

    public Character getQuoteCharacter() {
        return quoteCharacter;
    }

    public void setQuoteCharacter(Character quoteCharacter) {
        this.quoteCharacter = quoteCharacter;
    }

    public Character getEscapeCharacter() {
        return escapeCharacter;
    }

    public void setEscapeCharacter(Character escapeCharacter) {
        this.escapeCharacter = escapeCharacter;
    }

    public void buildDeserializationSchema() {
        DTCsvRowDeserializationSchema dtCsvRowDeserializationSchema = new DTCsvRowDeserializationSchema
            .Builder()
            .setTypeInfo(this.buildRowTypeInfo())
            .setFieldDelimiter(this.getFieldDelimiter())
            .setNullLiteral(this.getNullLiteral())
            .setAllowComments(this.getAllowComments())
            .setArrayElementDelimiter(this.getArrayElementDelimiter())
            .setEscapeCharacter(this.getEscapeCharacter())
            .setQuoteCharacter(this.getQuoteCharacter())
            .build();
        this.setDeserializationSchema(dtCsvRowDeserializationSchema);
    }
}