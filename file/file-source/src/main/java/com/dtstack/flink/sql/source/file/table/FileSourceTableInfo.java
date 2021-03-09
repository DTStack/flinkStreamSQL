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

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.dtstack.flink.sql.util.DataTypeUtils;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * @author tiezhu
 * @date 2021/3/9 星期二
 * Company dtstack
 */
public class FileSourceTableInfo extends AbstractSourceTableInfo {

    private static final Logger LOG = LoggerFactory.getLogger(FileSourceTableInfo.class);

    private static final String SOURCE_OPERATOR_NAME_TEMPLATE = "${fileName}_${tableName}";

    public static final String TYPE_KEY = "type";

    public static final String FORMAT_KEY = "format";

    public static final String FIELD_DELIMITER_KEY = "fieldDelimiter";

    public static final String FILE_NAME_KEY = "fileName";

    public static final String FILE_PATH_KEY = "filePath";

    public static final String IGNORE_PARSER_ERROR = "ignoreParserError";

    public static final String CHARSET_NAME_KEY = "charsetName";

    public static final String AVRO_FORMAT_KEY = "avroFormat";

    public static final String NULL_LITERAL_KEY = "nullLiteral";

    public static final String ALLOW_COMMENT_KEY = "allowComment";

    public static final String ARRAY_ELEMENT_DELIMITER_KEY = "arrayElementDelimiter";

    public static final String QUOTA_CHARACTER_KEY = "quoteCharacter";

    public static final String ESCAPE_CHARACTER_KEY = "escapeCharacter";

    public static final String LOCATION_KEY = "location";

    public static final String HDFS_SITE_KEY = "hdfsSite";

    public static final String CORE_SITE_KEY = "coreSite";

    public static final String HDFS_USER_KEY = "hdfsUser";

    public static final Character DEFAULT_DELIMITER = ',';

    public static final String DEFAULT_FILE_FORMAT = "csv";

    public static final String DEFAULT_CHARSET = "UTF-8";

    public static final String DEFAULT_LOCATION = "local";

    public static final String DEFAULT_ARRAY_ELEMENT_SEPARATOR = ",";

    public static final char DEFAULT_QUOTE_CHAR = '"';

    public static final char DEFAULT_ESCAPE_CHAR = '\\';

    public static final String DEFAULT_NULL_LITERAL = "null";

    public static final String DEFAULT_HDFS_USER = "root";

    public static final String DEFAULT_HDFS_SITE;

    public static final String DEFAULT_CORE_SITE;

    public static final String HADOOP_CONF_HOME;

    static {
        HADOOP_CONF_HOME = System.getProperty("HADOOP_CONF_HOME");
        if (Objects.isNull(HADOOP_CONF_HOME)) {
            LOG.warn("HADOOP_CONF_HOME is null..");
            DEFAULT_CORE_SITE = "./core-site.xml";
            DEFAULT_HDFS_SITE = "./hdfs-site.xml";
        } else {
            DEFAULT_CORE_SITE = HADOOP_CONF_HOME + File.separator + "core-site.xml";
            DEFAULT_HDFS_SITE = HADOOP_CONF_HOME + File.separator + "hdfs-site.xml";
        }
        LOG.info("Default [core-site] path: " + DEFAULT_CORE_SITE);
        LOG.info("Default [hdfs-site] path: " + DEFAULT_HDFS_SITE);
    }


    /*-------------------------------------------------------------*/

    /**
     * 默认为csv，可选json
     * TODO orc, parquet等也需要支持
     */
    private String format;

    /**
     * 文件名
     */
    private String fileName;

    /**
     * 文件存放的绝对路径
     * TODO 需要支持HDFS,OSS,S3等
     */
    private String filePath;

    /**
     * 文件编码格式，默认 UTF-8
     */
    private String charsetName;

    /**
     * 文件存储位置，local表示在本地, hdfs表示在HDFS，默认为local
     * TODO 目前只支持Local、HDFS，其他如S3、OSS等后续拓展
     */
    private String location;

    /*------------------------------------
                       csv
      -------------------------------------*/

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

    /*------------------------------------
                      arvo
     -------------------------------------*/

    /**
     * 针对arvo特定参数
     */
    private String avroFormat;

    /*------------------------------------
                     hdfs
     -------------------------------------*/

    /**
     * hdfs-site.xml文件存放位置
     */
    private String hdfsSite;

    /**
     * core-site.xml文件存放位置
     */
    private String coreSite;

    /**
     * 登陆hdfs的用户，默认为root
     */
    private String hdfsUser;

    /*-------------------------------------------------------------*/

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getAvroFormat() {
        return avroFormat;
    }

    public void setAvroFormat(String avroFormat) {
        this.avroFormat = avroFormat;
    }

    public char getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(char fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Boolean getIgnoreParseErrors() {
        return ignoreParseErrors;
    }

    public void setIgnoreParseErrors(Boolean ignoreParseErrors) {
        this.ignoreParseErrors = ignoreParseErrors;
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    public String getNullLiteral() {
        return nullLiteral;
    }

    public void setNullLiteral(String nullLiteral) {
        this.nullLiteral = nullLiteral;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
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

    public String getHdfsSite() {
        return hdfsSite;
    }

    public void setHdfsSite(String hdfsSite) {
        this.hdfsSite = hdfsSite;
    }

    public String getCoreSite() {
        return coreSite;
    }

    public void setCoreSite(String coreSite) {
        this.coreSite = coreSite;
    }

    public String getHdfsUser() {
        return hdfsUser;
    }

    public void setHdfsUser(String hdfsUser) {
        this.hdfsUser = hdfsUser;
    }

    @SuppressWarnings("unchecked")
    public TypeInformation<Row> getRowTypeInformation() {
        String[] types = getFieldTypes();
        Class<?>[] classes = getFieldClasses();

        TypeInformation<Row>[] rowTypes =
            IntStream.range(0, classes.length)
                .mapToObj(
                    i -> {
                        if (classes[i].isArray()) {
                            return DataTypeUtils.convertToArray(types[i]);
                        } else {
                            return TypeInformation.of(classes[i]);
                        }
                    }
                )
                .toArray(TypeInformation[]::new);
        return new RowTypeInfo(rowTypes, getFields());
    }

    public String getOperatorName() {
        return SOURCE_OPERATOR_NAME_TEMPLATE
            .replace("${fileName}", getFileName())
            .replace("${tableName}", getName());
    }

    public DeserializationSchema<Row> buildDeserializationSchema() {
        switch (format) {
            case "json":
                return new JsonRowDeserializationSchema
                    .Builder(getRowTypeInformation())
                    .build();
            case "csv":
                return new CsvRowDeserializationSchema
                    .Builder(getRowTypeInformation())
                    .setIgnoreParseErrors(ignoreParseErrors)
                    .setFieldDelimiter(fieldDelimiter)
                    .setNullLiteral(nullLiteral)
                    .setAllowComments(allowComments)
                    .setArrayElementDelimiter(arrayElementDelimiter)
                    .setEscapeCharacter(escapeCharacter)
                    .setQuoteCharacter(quoteCharacter)
                    .build();
            case "arvo":
                Preconditions.checkNotNull(avroFormat, "format [arvo] must set arvoFormat");
                return new AvroRowDeserializationSchema(avroFormat);
            default:
                throw new IllegalStateException("Unexpected value: " + format);
        }
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(getType(), "Type is required!");
        Preconditions.checkNotNull(fileName, "File name is required!");
        Preconditions.checkNotNull(filePath, "File path is required!");
        Preconditions.checkNotNull(format, "File format is required!");

        return false;
    }
}
