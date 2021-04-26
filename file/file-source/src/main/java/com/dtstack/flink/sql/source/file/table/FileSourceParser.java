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
                                          Map<String, Object> props) {
        FileSourceTableInfo tableInfo = switchTableType(tableName, fieldsInfo, props);

        tableInfo.setType(MathUtil.getString(props.get(FileSourceConstant.TYPE_KEY.toLowerCase())));
        tableInfo.setFileName(MathUtil.getString(props.get(FileSourceConstant.FILE_NAME_KEY.toLowerCase())));
        tableInfo.setFilePath(MathUtil.getString(props.getOrDefault(FileSourceConstant.FILE_PATH_KEY.toLowerCase(), FileSourceConstant.DEFAULT_PATH)));
        tableInfo.setCharsetName(MathUtil.getString(props.getOrDefault(FileSourceConstant.CHARSET_NAME_KEY.toLowerCase(), FileSourceConstant.DEFAULT_CHARSET)));

        tableInfo.check();

        return tableInfo;
    }

    private FileSourceTableInfo switchTableType(String tableName,
                                                String fieldsInfo,
                                                Map<String, Object> props) {
        String format = MathUtil.getString(props.getOrDefault(FileSourceConstant.FORMAT_KEY.toLowerCase(), FileSourceConstant.DEFAULT_FILE_FORMAT)).toLowerCase(Locale.ROOT);
        switch (format) {
            case "csv": {
                CsvSourceTableInfo tableInfo = new CsvSourceTableInfo();
                tableInfo.setName(tableName);
                parseFieldsInfo(fieldsInfo, tableInfo);

                tableInfo.setTypeInformation(tableInfo.buildRowTypeInfo());
                tableInfo.setFieldDelimiter(MathUtil.getChar(props.getOrDefault(FileSourceConstant.FIELD_DELIMITER_KEY.toLowerCase(), FileSourceConstant.DEFAULT_DELIMITER)));
                tableInfo.setNullLiteral(MathUtil.getString(props.get(FileSourceConstant.NULL_LITERAL_KEY.toLowerCase())));
                tableInfo.setAllowComments(MathUtil.getBoolean(props.getOrDefault(FileSourceConstant.ALLOW_COMMENT_KEY.toLowerCase(), FileSourceTableInfo.DEFAULT_TRUE)));
                tableInfo.setArrayElementDelimiter(MathUtil.getString(props.get(FileSourceConstant.ARRAY_ELEMENT_DELIMITER_KEY.toLowerCase())));
                Character quoteChar = MathUtil.getChar(props.get(FileSourceConstant.QUOTA_CHARACTER_KEY.toLowerCase()));
                if (quoteChar != null && !Character.isSpaceChar(quoteChar)) {
                    tableInfo.setQuoteCharacter(quoteChar);
                }

                Character escapeChar = MathUtil.getChar(props.get(FileSourceConstant.ESCAPE_CHARACTER_KEY.toLowerCase()));
                if (escapeChar != null && !Character.isSpaceChar(escapeChar)) {
                    tableInfo.setEscapeCharacter(escapeChar);
                }

                tableInfo.setFromLine(MathUtil.getIntegerVal(props.getOrDefault(FileSourceConstant.FROM_LINE_KEY, 1)));
                tableInfo.buildDeserializationSchema();
                return tableInfo;
            }
            case "json": {
                JsonSourceTableInfo tableInfo = new JsonSourceTableInfo();
                tableInfo.setName(tableName);
                parseFieldsInfo(fieldsInfo, tableInfo);
                return JsonSourceTableInfo
                    .newBuilder(tableInfo)
                    .setTypeInformation(tableInfo.buildRowTypeInfo())
                    .buildTableInfo();
            }
            case "avro": {
                ArvoSourceTableInfo tableInfo = new ArvoSourceTableInfo();
                tableInfo.setName(tableName);
                parseFieldsInfo(fieldsInfo, tableInfo);

                return ArvoSourceTableInfo
                    .newBuilder(tableInfo)
                    .setTypeInformation(tableInfo.buildRowTypeInfo())
                    .setArvoFormat(MathUtil.getString(props.get(FileSourceConstant.AVRO_FORMAT_KEY.toLowerCase())))
                    .buildTableInfo();
            }
            default:
                throw new IllegalStateException("Unexpected value: " + format);
        }
    }
}
