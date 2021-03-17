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

import com.dtstack.flink.sql.source.file.FileSourceConstant;
import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.types.Row;

import java.util.Locale;
import java.util.Map;

/**
 * @author tiezhu
 * @date 2021/3/9 星期二
 * Company dtstack
 */
public class FileSourceParser extends AbstractSourceParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName,
                                          String fieldsInfo,
                                          Map<String, Object> props) throws Exception {
        FileSourceTableInfo tableInfo = new FileSourceTableInfo();

        tableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, tableInfo);

        tableInfo.setType(MathUtil.getString(props.get(FileSourceConstant.TYPE_KEY.toLowerCase())));
        tableInfo.setFormat(MathUtil.getString(props.getOrDefault(FileSourceConstant.FORMAT_KEY.toLowerCase(), FileSourceConstant.DEFAULT_FILE_FORMAT)));
        tableInfo.setFileName(MathUtil.getString(props.get(FileSourceConstant.FILE_NAME_KEY.toLowerCase())));
        tableInfo.setFilePath(MathUtil.getString(props.getOrDefault(FileSourceConstant.FILE_PATH_KEY.toLowerCase(), FileSourceConstant.DEFAULT_PATH)));
        tableInfo.setCharsetName(MathUtil.getString(props.getOrDefault(FileSourceConstant.CHARSET_NAME_KEY.toLowerCase(), FileSourceConstant.DEFAULT_CHARSET)));

        DeserializationSchema<Row> rowDeserializationSchema = deserializationSchemaFactory(tableInfo, props);
        tableInfo.setDeserializationSchema(rowDeserializationSchema);

        tableInfo.check();

        return tableInfo;
    }

    private DeserializationSchema<Row> deserializationSchemaFactory(FileSourceTableInfo tableInfo,
                                                                    Map<String, Object> props) {
        String format = tableInfo.getFormat();
        switch (format.toLowerCase(Locale.ROOT)) {
            case "csv":
                return CsvSourceTableInfo
                    .newBuilder()
                    .setTypeInformation(tableInfo.buildRowTypeInfo())
                    .setIgnoreParseErrors(MathUtil.getBoolean(props.getOrDefault(FileSourceConstant.IGNORE_PARSER_ERROR.toLowerCase(), FileSourceTableInfo.DEFAULT_TRUE)))
                    .setFieldDelimiter(MathUtil.getChar(props.getOrDefault(FileSourceConstant.FIELD_DELIMITER_KEY.toLowerCase(), FileSourceConstant.DEFAULT_DELIMITER)))
                    .setNullLiteral(MathUtil.getString(props.getOrDefault(FileSourceConstant.NULL_LITERAL_KEY.toLowerCase(), FileSourceConstant.DEFAULT_NULL_LITERAL)))
                    .setAllowComment(MathUtil.getBoolean(props.getOrDefault(FileSourceConstant.ALLOW_COMMENT_KEY.toLowerCase(), FileSourceTableInfo.DEFAULT_TRUE)))
                    .setArrayElementDelimiter(MathUtil.getString(props.getOrDefault(FileSourceConstant.ARRAY_ELEMENT_DELIMITER_KEY.toLowerCase(), FileSourceConstant.DEFAULT_DELIMITER)))
                    .setQuoterCharacter(MathUtil.getChar(props.getOrDefault(FileSourceConstant.QUOTA_CHARACTER_KEY.toLowerCase(), FileSourceConstant.DEFAULT_QUOTE_CHAR)))
                    .setEscapeCharacter(MathUtil.getChar(props.getOrDefault(FileSourceConstant.ESCAPE_CHARACTER_KEY.toLowerCase(), FileSourceConstant.DEFAULT_ESCAPE_CHAR)))
                    .buildCsvDeserializationSchema();

            case "json":
                return JsonSourceTableInfo
                    .newBuilder()
                    .setTypeInformation(tableInfo.buildRowTypeInfo())
                    .buildCsvDeserializationSchema();
            case "arvo":
                return ArvoSourceTableInfo
                    .newBuilder()
                    .setTypeInformation(tableInfo.buildRowTypeInfo())
                    .setArvoFormat(MathUtil.getString(props.get(FileSourceConstant.AVRO_FORMAT_KEY.toLowerCase())))
                    .buildCsvDeserializationSchema();
            default:
                throw new IllegalStateException("Unexpected value: " + format);
        }
    }
}
