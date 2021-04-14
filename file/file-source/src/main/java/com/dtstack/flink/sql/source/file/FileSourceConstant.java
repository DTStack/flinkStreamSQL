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

package com.dtstack.flink.sql.source.file;

/**
 * @author tiezhu
 * @date 2021/3/22 星期一
 * Company dtstack
 */
public class FileSourceConstant {
    public static final String TYPE_KEY = "type";

    public static final String FORMAT_KEY = "format";

    public static final String FIELD_DELIMITER_KEY = "fieldDelimiter";

    public static final String FILE_NAME_KEY = "fileName";

    public static final String FILE_PATH_KEY = "filePath";

    public static final String CHARSET_NAME_KEY = "charsetName";

    public static final String AVRO_FORMAT_KEY = "avroFormat";

    public static final String NULL_LITERAL_KEY = "nullLiteral";

    public static final String ALLOW_COMMENT_KEY = "allowComment";

    public static final String ARRAY_ELEMENT_DELIMITER_KEY = "arrayElementDelimiter";

    public static final String QUOTA_CHARACTER_KEY = "quoteCharacter";

    public static final String ESCAPE_CHARACTER_KEY = "escapeCharacter";

    public static final String FROM_LINE_KEY = "fromLine";

    public static final Character DEFAULT_DELIMITER = ',';

    public static final String DEFAULT_FILE_FORMAT = "csv";

    public static final String DEFAULT_CHARSET = "UTF-8";

    public static final String DEFAULT_PATH = ".";

    public static final String FILE_LOCAL = "local";

    public static final String FILE_HDFS = "hdfs";

}
