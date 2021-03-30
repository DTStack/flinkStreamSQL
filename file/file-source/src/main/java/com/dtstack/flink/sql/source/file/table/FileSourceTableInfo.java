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
import org.apache.flink.types.Row;

import java.util.stream.IntStream;

/**
 * @author tiezhu
 * @date 2021/3/9 星期二
 * Company dtstack
 */
public class FileSourceTableInfo extends AbstractSourceTableInfo {

    private static final String SOURCE_OPERATOR_NAME_TEMPLATE = "${fileName}_${tableName}";

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
     * 文件存放路径
     */
    private String filePath;

    /**
     * 文件编码格式，默认 UTF-8
     */
    private String charsetName;

    private DeserializationSchema<Row> deserializationSchema;

    private TypeInformation<Row> typeInformation;

    /*-------------------------------------------------------------*/

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
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

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    public TypeInformation<Row> getTypeInformation() {
        return typeInformation;
    }

    public void setTypeInformation(TypeInformation<Row> typeInformation) {
        this.typeInformation = typeInformation;
    }

    public DeserializationSchema<Row> getDeserializationSchema() {
        return deserializationSchema;
    }

    public void setDeserializationSchema(DeserializationSchema<Row> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @SuppressWarnings("unchecked")
    public TypeInformation<Row> buildRowTypeInfo() {
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

    @Override
    public boolean check() {
        Preconditions.checkNotNull(getType(), "Type is required!");
        Preconditions.checkNotNull(fileName, "File name is required!");

        return false;
    }
}
