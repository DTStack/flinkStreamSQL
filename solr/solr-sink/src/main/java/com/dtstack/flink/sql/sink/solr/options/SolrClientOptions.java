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

package com.dtstack.flink.sql.sink.solr.options;

import java.io.Serializable;
import java.util.List;

/**
 * @author wuren
 * @program flinkStreamSQL
 * @create 2021/05/17
 */
public class SolrClientOptions implements Serializable {

    private List<String> zkHosts;
    private String zkChroot;
    private String collection;

    public SolrClientOptions(List<String> zkHosts, String zkChroot, String collection) {
        this.zkHosts = zkHosts;
        this.zkChroot = zkChroot;
        this.collection = collection;
    }

    public List<String> getZkHosts() {
        return zkHosts;
    }

    public void setZkHosts(List<String> zkHosts) {
        this.zkHosts = zkHosts;
    }

    public String getZkChroot() {
        return zkChroot;
    }

    public void setZkChroot(String zkChroot) {
        this.zkChroot = zkChroot;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }
}
