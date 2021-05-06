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

import com.dtstack.flink.sql.source.file.throwable.LengthMismatchException;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.csv.CsvRowSchemaConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author tiezhu
 * @date 2021/4/14 星期三
 * Company dtstack
 */
@PublicEvolving
public class DTCsvRowDeserializationSchema implements DeserializationSchema<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(DTCsvRowDeserializationSchema.class);

    /**
     * Type information describing the result type.
     */
    private TypeInformation<Row> typeInfo;

    /**
     * Runtime instance that performs the actual work.
     */
    private RuntimeConverter runtimeConverter;

    /**
     * Object reader used to read rows. It is configured by {@link CsvSchema}.
     */
    private ObjectReader objectReader;

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

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            final JsonNode root = objectReader.readValue(message);
            return (Row) runtimeConverter.convert(root);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }

    private void init() {
        runtimeConverter = createRowRuntimeConverter((RowTypeInfo) typeInfo, true);
        CsvSchema csvSchema = initCsvSchema(typeInfo);
        objectReader = new CsvMapper().readerFor(JsonNode.class).with(csvSchema);
    }

    private CsvSchema initCsvSchema(TypeInformation<Row> typeInfo) {

        CsvSchema.Builder rebuild = CsvRowSchemaConverter
            .convert((RowTypeInfo) typeInfo)
            .rebuild()
            .setAllowComments(allowComments);

        if (arrayElementDelimiter != null) {
            rebuild.setArrayElementSeparator(arrayElementDelimiter);
        }

        if (fieldDelimiter != null) {
            rebuild.setColumnSeparator(fieldDelimiter);
        }

        if (quoteCharacter != null) {
            rebuild.setQuoteChar(quoteCharacter);
        }

        if (nullLiteral != null) {
            rebuild.setNullValue(nullLiteral);
        }

        if (escapeCharacter != null) {
            rebuild.setEscapeChar(escapeCharacter);
        }

        return rebuild.build();
    }

    // --------------------------------------------------------------------------------------------
    // Setter
    // --------------------------------------------------------------------------------------------

    public void setFieldDelimiter(Character fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public void setNullLiteral(String nullLiteral) {
        this.nullLiteral = nullLiteral;
    }

    public void setAllowComments(Boolean allowComments) {
        this.allowComments = allowComments;
    }

    public void setArrayElementDelimiter(String arrayElementDelimiter) {
        this.arrayElementDelimiter = arrayElementDelimiter;
    }

    public void setQuoteCharacter(Character quoteCharacter) {
        this.quoteCharacter = quoteCharacter;
    }

    public void setEscapeCharacter(Character escapeCharacter) {
        this.escapeCharacter = escapeCharacter;
    }

    public void setTypeInfo(TypeInformation<Row> typeInfo) {
        this.typeInfo = typeInfo;
    }

    // --------------------------------------------------------------------------------------------
    // Builder
    // --------------------------------------------------------------------------------------------

    public static class Builder {

        private final DTCsvRowDeserializationSchema deserializationSchema;

        public Builder() {
            deserializationSchema = new DTCsvRowDeserializationSchema();
        }

        public Builder setFieldDelimiter(Character fieldDelimiter) {
            deserializationSchema.setFieldDelimiter(fieldDelimiter);
            return this;
        }

        public Builder setNullLiteral(String nullLiteral) {
            deserializationSchema.setNullLiteral(nullLiteral);
            return this;
        }

        public Builder setAllowComments(Boolean allowComments) {
            deserializationSchema.setAllowComments(allowComments);
            return this;
        }

        public Builder setArrayElementDelimiter(String arrayElementDelimiter) {
            deserializationSchema.setArrayElementDelimiter(arrayElementDelimiter);
            return this;
        }

        public Builder setQuoteCharacter(Character quoteCharacter) {
            deserializationSchema.setQuoteCharacter(quoteCharacter);
            return this;
        }

        public Builder setEscapeCharacter(Character escapeCharacter) {
            deserializationSchema.setEscapeCharacter(escapeCharacter);
            return this;
        }

        public Builder setTypeInfo(TypeInformation<Row> typeInfo) {
            deserializationSchema.setTypeInfo(typeInfo);
            return this;
        }

        public DTCsvRowDeserializationSchema build() {
            deserializationSchema.init();
            return deserializationSchema;
        }
    }

    // --------------------------------------------------------------------------------------------
    // RuntimeConverter
    // --------------------------------------------------------------------------------------------

    private interface RuntimeConverter extends Serializable {
        Object convert(JsonNode node) throws IOException;
    }

    private static RuntimeConverter createRowRuntimeConverter(
        RowTypeInfo rowTypeInfo,
        boolean isTopLevel) {
        final TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        final String[] fieldNames = rowTypeInfo.getFieldNames();

        final RuntimeConverter[] fieldConverters =
            createFieldRuntimeConverters(fieldTypes);

        return assembleRowRuntimeConverter(isTopLevel, fieldNames, fieldConverters);
    }

    private static RuntimeConverter[] createFieldRuntimeConverters(TypeInformation<?>[] fieldTypes) {
        final RuntimeConverter[] fieldConverters = new RuntimeConverter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldConverters[i] = createNullableRuntimeConverter(fieldTypes[i]);
        }
        return fieldConverters;
    }

    private static RuntimeConverter assembleRowRuntimeConverter(
        boolean isTopLevel,
        String[] fieldNames,
        RuntimeConverter[] fieldConverters) {
        final int rowArity = fieldNames.length;

        return (node) -> {
            final int nodeSize = node.size();

            validateArity(rowArity, nodeSize);

            final Row row = new Row(rowArity);
            for (int i = 0; i < Math.min(rowArity, nodeSize); i++) {
                // Jackson only supports mapping by name in the first level
                if (isTopLevel) {
                    row.setField(i, fieldConverters[i].convert(node.get(fieldNames[i])));
                } else {
                    row.setField(i, fieldConverters[i].convert(node.get(i)));
                }
            }
            return row;
        };
    }

    private static RuntimeConverter createNullableRuntimeConverter(
        TypeInformation<?> info) {
        final RuntimeConverter valueConverter = createRuntimeConverter(info);
        return (node) -> {
            if (node.isNull()) {
                return null;
            }
            return valueConverter.convert(node);
        };
    }

    private static RuntimeConverter createRuntimeConverter(TypeInformation<?> info) {
        if (info.equals(Types.VOID)) {
            return (node) -> null;
        } else if (info.equals(Types.STRING)) {
            return JsonNode::asText;
        } else if (info.equals(Types.BOOLEAN)) {
            return (node) -> Boolean.valueOf(node.asText().trim());
        } else if (info.equals(Types.BYTE)) {
            return (node) -> Byte.valueOf(node.asText().trim());
        } else if (info.equals(Types.SHORT)) {
            return (node) -> Short.valueOf(node.asText().trim());
        } else if (info.equals(Types.INT)) {
            return (node) -> Integer.valueOf(node.asText().trim());
        } else if (info.equals(Types.LONG)) {
            return (node) -> Long.valueOf(node.asText().trim());
        } else if (info.equals(Types.FLOAT)) {
            return (node) -> Float.valueOf(node.asText().trim());
        } else if (info.equals(Types.DOUBLE)) {
            return (node) -> Double.valueOf(node.asText().trim());
        } else if (info.equals(Types.BIG_DEC)) {
            return (node) -> new BigDecimal(node.asText().trim());
        } else if (info.equals(Types.BIG_INT)) {
            return (node) -> new BigInteger(node.asText().trim());
        } else if (info.equals(Types.SQL_DATE)) {
            return (node) -> Date.valueOf(node.asText());
        } else if (info.equals(Types.SQL_TIME)) {
            return (node) -> Time.valueOf(node.asText());
        } else if (info.equals(Types.SQL_TIMESTAMP)) {
            return (node) -> Timestamp.valueOf(node.asText());
        } else if (info.equals(Types.LOCAL_DATE)) {
            return (node) -> Date.valueOf(node.asText()).toLocalDate();
        } else if (info.equals(Types.LOCAL_TIME)) {
            return (node) -> Time.valueOf(node.asText()).toLocalTime();
        } else if (info.equals(Types.LOCAL_DATE_TIME)) {
            return (node) -> Timestamp.valueOf(node.asText()).toLocalDateTime();
        } else if (info instanceof RowTypeInfo) {
            final RowTypeInfo rowTypeInfo = (RowTypeInfo) info;
            return createRowRuntimeConverter(rowTypeInfo, false);
        } else if (info instanceof BasicArrayTypeInfo) {
            return createObjectArrayRuntimeConverter(
                ((BasicArrayTypeInfo<?, ?>) info).getComponentInfo()
            );
        } else if (info instanceof ObjectArrayTypeInfo) {
            return createObjectArrayRuntimeConverter(
                ((ObjectArrayTypeInfo<?, ?>) info).getComponentInfo()
            );
        } else if (info instanceof PrimitiveArrayTypeInfo &&
            ((PrimitiveArrayTypeInfo<?>) info).getComponentType() == Types.BYTE) {
            return createByteArrayRuntimeConverter();
        } else {
            throw new RuntimeException("Unsupported type information '" + info + "'.");
        }
    }

    private static RuntimeConverter createObjectArrayRuntimeConverter(
        TypeInformation<?> elementType) {
        final Class<?> elementClass = elementType.getTypeClass();
        final RuntimeConverter elementConverter = createNullableRuntimeConverter(elementType);

        return (node) -> {
            final int nodeSize = node.size();
            final Object[] array = (Object[]) Array.newInstance(elementClass, nodeSize);
            for (int i = 0; i < nodeSize; i++) {
                array[i] = elementConverter.convert(node.get(i));
            }
            return array;
        };
    }

    private static RuntimeConverter createByteArrayRuntimeConverter() {
        return JsonNode::binaryValue;
    }

    private static void validateArity(int expected, int actual) {
        if (expected != actual) {
            throw new LengthMismatchException(
                    "Row length mismatch. " + expected +
                            " fields expected but was " + actual + ".");
        }
    }
}
