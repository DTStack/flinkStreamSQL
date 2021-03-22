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

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.types.Row;

/**
 * @author tiezhu
 * @date 2021/3/22 星期一
 * Company dtstack
 */
public class ArvoSourceTableInfo extends FileSourceTableInfo {
    /**
     * 针对arvo特定参数
     */
    private String arvoFormat;

    private ArvoSourceTableInfo() {
    }

    public String getArvoFormat() {
        return arvoFormat;
    }

    public void setArvoFormat(String arvoFormat) {
        this.arvoFormat = arvoFormat;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final ArvoSourceTableInfo tableInfo;

        public Builder() {
            tableInfo = new ArvoSourceTableInfo();
        }

        public Builder setArvoFormat(String arvoFormat) {
            tableInfo.setArvoFormat(arvoFormat);
            return this;
        }

        public Builder setTypeInformation(TypeInformation<Row> typeInformation) {
            tableInfo.setTypeInformation(typeInformation);
            return this;
        }

        public DeserializationSchema<Row> buildCsvDeserializationSchema() {
            String arvoFormat = tableInfo.getArvoFormat();
            Preconditions.checkNotNull(arvoFormat, "format [arvo] must set arvoFormat");
            return new AvroRowDeserializationSchema(arvoFormat);
        }
    }

}
