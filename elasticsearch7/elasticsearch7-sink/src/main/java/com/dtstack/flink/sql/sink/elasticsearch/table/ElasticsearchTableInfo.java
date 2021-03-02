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


package com.dtstack.flink.sql.sink.elasticsearch.table;

import com.dtstack.flink.sql.sink.elasticsearch.Es7Util;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Arrays;

/**
 * @description:
 * @program: flink.sql
 * @author: lany
 * @create: 2021/01/04 17:19
 */
public class ElasticsearchTableInfo  extends AbstractTargetTableInfo {

    private static final String CURR_TYPE = "elasticsearch7";

    private String address;

    private String index;

    private String id;

    private boolean authMesh = false;

    private String userName;

    private String password;

    private String index_definition;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String getType() {
        //return super.getType().toLowerCase() + TARGET_SUFFIX;
        return super.getType().toLowerCase();
    }

    public boolean isAuthMesh() {
        return authMesh;
    }

    public void setAuthMesh(boolean authMesh) {
        this.authMesh = authMesh;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setIndex_definition(String index_definition) {
        this.index_definition = index_definition;
    }

    public String getIndex_definition() {
        return index_definition;
    }

    public ElasticsearchTableInfo() {
        setType(CURR_TYPE);
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(address, "elasticsearch7 type of address is required");
        Preconditions.checkNotNull(index, "elasticsearch7 type of index is required");

        if (index_definition != null && !Es7Util.isJson(index_definition)) {
            throw new IllegalArgumentException("elasticsearch7 type of index_definition must be json format!");
        }
        if (isAuthMesh()) {
            Preconditions.checkNotNull(userName, "elasticsearch7 type of userName is required");
            Preconditions.checkNotNull(password, "elasticsearch7 type of password is required");
        }

        return true;
    }

}
