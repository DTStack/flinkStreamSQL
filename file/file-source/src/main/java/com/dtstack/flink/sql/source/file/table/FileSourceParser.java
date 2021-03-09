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

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

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
        FileSourceTableInfo fileSourceTableInfo = new FileSourceTableInfo();

        fileSourceTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, fileSourceTableInfo);

        fileSourceTableInfo.setType(
            MathUtil.getString(
                props.get(FileSourceTableInfo.TYPE_KEY.toLowerCase())
            )
        );

        fileSourceTableInfo.setFilePath(
            MathUtil.getString(
                props.get(FileSourceTableInfo.FILE_PATH_KEY.toLowerCase())
            )
        );

        fileSourceTableInfo.setFileName(
            MathUtil.getString(
                props.get(FileSourceTableInfo.FILE_NAME_KEY.toLowerCase())
            )
        );

        fileSourceTableInfo.setFieldDelimiter(
            MathUtil.getChar(
                props.getOrDefault(
                    FileSourceTableInfo.FIELD_DELIMITER_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_DELIMITER)
            )
        );

        fileSourceTableInfo.setFormat(
            MathUtil.getString(
                props.getOrDefault(
                    FileSourceTableInfo.FORMAT_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_FILE_FORMAT
                )
            )
        );

        fileSourceTableInfo.setAvroFormat(
            MathUtil.getString(
                props.get(FileSourceTableInfo.AVRO_FORMAT_KEY.toLowerCase()
                )
            )
        );

        fileSourceTableInfo.setCharsetName(
            MathUtil.getString(
                props.getOrDefault(
                    FileSourceTableInfo.CHARSET_NAME_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_CHARSET
                )
            )
        );

        fileSourceTableInfo.setIgnoreParseErrors(
            MathUtil.getBoolean(
                props.getOrDefault(
                    FileSourceTableInfo.IGNORE_PARSER_ERROR.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_TRUE
                )
            )
        );

        fileSourceTableInfo.setNullLiteral(
            MathUtil.getString(
                props.getOrDefault(
                    FileSourceTableInfo.NULL_LITERAL_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_NULL_LITERAL
                )
            )
        );

        fileSourceTableInfo.setAllowComments(
            MathUtil.getBoolean(
                props.getOrDefault(
                    FileSourceTableInfo.ALLOW_COMMENT_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_TRUE
                )
            )
        );

        fileSourceTableInfo.setArrayElementDelimiter(
            MathUtil.getString(
                props.getOrDefault(
                    FileSourceTableInfo.ARRAY_ELEMENT_DELIMITER_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_ARRAY_ELEMENT_SEPARATOR
                )
            )
        );

        fileSourceTableInfo.setQuoteCharacter(
            MathUtil.getChar(
                props.getOrDefault(
                    FileSourceTableInfo.QUOTA_CHARACTER_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_QUOTE_CHAR
                )
            )
        );

        fileSourceTableInfo.setEscapeCharacter(
            MathUtil.getChar(
                props.getOrDefault(
                    FileSourceTableInfo.ESCAPE_CHARACTER_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_ESCAPE_CHAR
                )
            )
        );

        fileSourceTableInfo.setLocation(
            MathUtil.getString(
                props.getOrDefault(
                    FileSourceTableInfo.LOCATION_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_LOCATION
                )
            )
        );

        fileSourceTableInfo.setHdfsSite(
            MathUtil.getString(
                props.getOrDefault(
                    FileSourceTableInfo.HDFS_SITE_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_HDFS_SITE
                )
            )
        );

        fileSourceTableInfo.setCoreSite(
            MathUtil.getString(
                props.getOrDefault(
                    FileSourceTableInfo.CORE_SITE_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_CORE_SITE
                )
            )
        );

        fileSourceTableInfo.setHdfsUser(
            MathUtil.getString(
                props.getOrDefault(
                    FileSourceTableInfo.HDFS_USER_KEY.toLowerCase(),
                    FileSourceTableInfo.DEFAULT_HDFS_USER
                )
            )
        );

        fileSourceTableInfo.check();

        return fileSourceTableInfo;
    }
}
