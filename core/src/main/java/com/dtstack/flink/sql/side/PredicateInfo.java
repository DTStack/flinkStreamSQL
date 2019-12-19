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

import java.io.Serializable;

/**
 *   Predicate base info
 *
 * Date: 2019/12/11
 * Company: www.dtstack.com
 * @author maqi
 */
public class PredicateInfo implements Serializable {

    private String operatorName;
    private String operatorKind;
    private String ownerTable;
    private String fieldName;
    private String condition;

    public PredicateInfo(String operatorName, String operatorKind, String ownerTable, String fieldName, String condition) {
        this.operatorName = operatorName;
        this.operatorKind = operatorKind;
        this.ownerTable = ownerTable;
        this.fieldName = fieldName;
        this.condition = condition;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public String getOperatorKind() {
        return operatorKind;
    }

    public void setOperatorKind(String operatorKind) {
        this.operatorKind = operatorKind;
    }

    public String getOwnerTable() {
        return ownerTable;
    }

    public void setOwnerTable(String ownerTable) {
        this.ownerTable = ownerTable;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    @Override
    public String toString() {
        return "PredicateInfo{" +
                "operatorName='" + operatorName + '\'' +
                ", operatorKind='" + operatorKind + '\'' +
                ", ownerTable='" + ownerTable + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", condition='" + condition + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {

        private String operatorName;
        private String operatorKind;
        private String ownerTable;
        private String fieldName;
        private String condition;

        public Builder setOperatorName(String operatorName) {
            this.operatorName = operatorName;
            return this;
        }

        public Builder setOperatorKind(String operatorKind) {
            this.operatorKind = operatorKind;
            return this;
        }

        public Builder setOwnerTable(String ownerTable) {
            this.ownerTable = ownerTable;
            return this;
        }

        public Builder setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder setCondition(String condition) {
            this.condition = condition;
            return this;
        }

        public PredicateInfo build() {
            return new PredicateInfo(
                    operatorName, operatorKind, ownerTable, fieldName, condition);
        }
    }


}
