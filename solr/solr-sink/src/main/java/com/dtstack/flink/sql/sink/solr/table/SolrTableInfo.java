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

package com.dtstack.flink.sql.sink.solr.table;

import com.dtstack.flink.sql.sink.solr.options.KerberosOptions;
import com.dtstack.flink.sql.sink.solr.options.SolrClientOptions;
import com.dtstack.flink.sql.sink.solr.options.SolrWriteOptions;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;

import java.util.List;

/**
 * @author wuren
 * @program flinkStreamSQL
 * @create 2021/05/17
 */
public class SolrTableInfo extends AbstractTargetTableInfo {

    private static final String TYPE = "solr";

    private SolrClientOptions solrClientOptions;
    private SolrWriteOptions solrWriteOptions;
    private KerberosOptions kerberosOptions;

    public SolrClientOptions getSolrClientOptions() {
        return solrClientOptions;
    }

    public void setSolrClientOptions(SolrClientOptions solrClientOptions) {
        this.solrClientOptions = solrClientOptions;
    }

    public SolrWriteOptions getSolrWriteOptions() {
        return solrWriteOptions;
    }

    public void setSolrWriteOptions(SolrWriteOptions solrWriteOptions) {
        this.solrWriteOptions = solrWriteOptions;
    }

    public KerberosOptions getKerberosOptions() {
        return kerberosOptions;
    }

    public void setKerberosOptions(KerberosOptions kerberosOptions) {
        this.kerberosOptions = kerberosOptions;
    }

    @Override
    public boolean check() {
        List<String> zkHosts = solrClientOptions.getZkHosts();
        Preconditions.checkNotNull(zkHosts, "zk-hosts is required");
        Preconditions.checkState(!zkHosts.isEmpty(), "zk-hosts is required");
        String collection = solrClientOptions.getCollection();
        Preconditions.checkNotNull(collection, "collection is required");
        Preconditions.checkState(!collection.isEmpty(), "collection is required");
        return true;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
