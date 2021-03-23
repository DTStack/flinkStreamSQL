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

package com.dtstack.flink.sql.side.elasticsearch6.table;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.google.common.base.Preconditions;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * @author yinxi
 * @date 2020/1/13 - 15:00
 */
public class Elasticsearch6SideTableInfo extends AbstractSideTableInfo {

    private static final String CURR_TYPE = "elasticsearch6";

    private String address;

    private String clusterName;

    private String index;

    private String esType;

    private boolean authMesh = false;

    private String userName;

    private String password;

    private transient SearchSourceBuilder searchSourceBuilder;

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
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

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
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


    public SearchSourceBuilder getSearchSourceBuilder() {
        return searchSourceBuilder;
    }

    public void setSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        this.searchSourceBuilder = searchSourceBuilder;
    }

    public Elasticsearch6SideTableInfo() {
        setType(CURR_TYPE);
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(address, "elasticsearch6 type of address is required");
        Preconditions.checkNotNull(clusterName, "elasticsearch6 type of clusterName is required");

        if (isAuthMesh()) {
            Preconditions.checkNotNull(userName, "elasticsearch6 type of userName is required");
            Preconditions.checkNotNull(password, "elasticsearch6 type of password is required");
        }

        return true;
    }
}
