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

 

package com.dtstack.flink.sql.side;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.Objects;

/**
 * Reason:
 * Date: 2018/7/23
 * Company: www.dtstack.com
 * @author xuchao
 */

public class FieldInfo implements Serializable {

    private static final long serialVersionUID = -1L;

    private String table;

    private String fieldName;

    private TypeInformation typeInformation;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public TypeInformation getTypeInformation() {
        return typeInformation;
    }

    public void setTypeInformation(TypeInformation typeInformation) {
        this.typeInformation = typeInformation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldInfo fieldInfo = (FieldInfo) o;
        return Objects.equals(table, fieldInfo.table) &&
                Objects.equals(fieldName, fieldInfo.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, fieldName);
    }

    @Override
    public String toString() {
        return "FieldInfo{" +
                "table='" + table + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", typeInformation=" + typeInformation +
                '}';
    }
}
