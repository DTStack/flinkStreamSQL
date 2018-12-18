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
package com.dtstack.flink.sql.sink.kafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.shaded.org.apache.commons.lang.StringEscapeUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.StringValue;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.writeNullMask;
/**
 *
 * Date: 2018/12/18
 * Company: www.dtstack.com
 * @author DocLi
 *
 * @modifyer maqi
 *
 */
@Internal
public final class CustomerCsvSerialization extends TypeSerializerSingleton<Row> {

    private static final long serialVersionUID = 1L;

    private String fieldDelimiter = "\u0001";
    private TypeInformation<?>[] fieldTypes;
    private TypeSerializer<Object>[] fieldSerializers;
    private static final Row EMPTY = null;

    public CustomerCsvSerialization(String fielddelimiter,TypeInformation<?>[] fieldTypes) {
        this.fieldDelimiter = fielddelimiter;
        this.fieldTypes = fieldTypes;
        this.fieldSerializers = (TypeSerializer<Object>[])createSerializer(new ExecutionConfig());
    }

    public TypeSerializer<?>[] createSerializer(ExecutionConfig config) {
        int len = fieldTypes.length;
        TypeSerializer<?>[] fieldSerializers = new TypeSerializer[len];
        for (int i = 0; i < len; i++) {
            fieldSerializers[i] = fieldTypes[i].createSerializer(config);
        }
        return fieldSerializers;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public Row createInstance() {
        return EMPTY;
    }

    @Override
    public Row copy(Row from) {
        return null;
    }

    @Override
    public Row copy(Row from, Row reuse) {
        return null;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Row record, DataOutputView target) throws IOException {
        int len = fieldSerializers.length;

        if (record.getArity() != len) {
            throw new RuntimeException("Row arity of from does not match serializers.");
        }

        // write a null mask
        writeNullMask(len, record, target);

        // serialize non-null fields
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < len; i++) {
            Object o = record.getField(i);
            if (o != null) {
                //fieldSerializers[i].serialize(o, target);
                stringBuffer.append(o);
            }
            if(i != len-1){
                stringBuffer.append(StringEscapeUtils.unescapeJava(fieldDelimiter));
                //fieldSerializers[i].serialize(fieldDelimiter, target);
            }
        }
        StringValue.writeString(stringBuffer.toString(), target);
    }

    @Override
    public Row deserialize(DataInputView source) throws IOException {
        return null;
    }

    @Override
    public Row deserialize(Row reuse, DataInputView source) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        StringValue.copyString(source, target);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof CustomerCsvSerialization;
    }

    @Override
    protected boolean isCompatibleSerializationFormatIdentifier(String identifier) {
        return super.isCompatibleSerializationFormatIdentifier(identifier)
                || identifier.equals(StringValue.class.getCanonicalName());
    }
}
