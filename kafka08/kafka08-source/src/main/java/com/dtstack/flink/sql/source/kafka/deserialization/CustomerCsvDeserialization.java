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

 

package com.dtstack.flink.sql.source.kafka.deserialization;


import com.dtstack.flink.sql.source.AbsDeserialization;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * json string parsing custom
 * Date: 2017/5/28
 * Company: www.dtstack.com
 * @author DocLi
 */

public class CustomerCsvDeserialization extends AbsDeserialization<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerCsvDeserialization.class);

    private static final long serialVersionUID = -2706012724306826506L;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Type information describing the result type. */
    private final TypeInformation<Row> typeInfo;

    /** Field names to parse. Indices match fieldTypes indices. */
    private final String[] fieldNames;

    /** Types to parse fields as. Indices match fieldNames indices. */
    private final TypeInformation<?>[] fieldTypes;

    /** Flag indicating whether to fail on a missing field. */
    private boolean failOnMissingField;

    private String fieldDelimiter;

    private String lengthCheckPolicy;

    public CustomerCsvDeserialization(TypeInformation<Row> typeInfo, String fieldDelimiter, String lengthCheckPolicy){
        this.typeInfo = typeInfo;

        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();

        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();

        this.fieldDelimiter = fieldDelimiter;

        this.lengthCheckPolicy = lengthCheckPolicy;
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {

        try {
            numInRecord.inc();
            numInBytes.inc(message.length);
            String[] fieldsList = null;
            if (message != null && message.length > 0){
                fieldsList = new String(message).split(fieldDelimiter);
            }
            if (fieldsList == null || fieldsList.length != fieldNames.length){//exception condition
                if (lengthCheckPolicy.equalsIgnoreCase("SKIP")) {
                    return null;
                }else if (lengthCheckPolicy.equalsIgnoreCase("EXCEPTION")) {
                    throw new RuntimeException("lengthCheckPolicy Error,message have "+fieldsList.length+" fields,sql have "+fieldNames.length);
                }
            }

            Row row = new Row(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                if (i<fieldsList.length) {
                    row.setField(i, DtStringUtil.parse(fieldsList[i],fieldTypes[i].getTypeClass()));
                } else {
                    break;
                }
            }

            numInResolveRecord.inc();
            return row;
        } catch (Throwable t) {
            //add metric of dirty data
            dirtyDataCounter.inc();
            throw new RuntimeException(t);
        }
    }


    public void setFailOnMissingField(boolean failOnMissingField) {
        this.failOnMissingField = failOnMissingField;
    }

}
