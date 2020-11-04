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

package com.dtstack.flink.sql.sink.elasticsearch;

import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.util.Preconditions;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author yinxi
 * @date 2020/1/9 - 15:09
 */
public class ExtendEs6ApiCallBridge implements ElasticsearchApiCallBridge<RestHighLevelClient> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtendEs6ApiCallBridge.class);

    private final List<HttpHost> HttpAddresses;

    protected ElasticsearchTableInfo es6TableInfo;

    public ExtendEs6ApiCallBridge(List<HttpHost> httpAddresses, ElasticsearchTableInfo es6TableInfo) {
        Preconditions.checkArgument(httpAddresses != null && !httpAddresses.isEmpty());
        this.HttpAddresses = httpAddresses;
        this.es6TableInfo = es6TableInfo;
    }

    @Override
    public RestHighLevelClient createClient(Map<String, String> clientConfig) {

        RestClientBuilder restClientBuilder = RestClient.builder(HttpAddresses.toArray(new HttpHost[HttpAddresses.size()]));
        if (es6TableInfo.isAuthMesh()) {
            // 进行用户和密码认证
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(es6TableInfo.getUserName(), es6TableInfo.getPassword()));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        RestHighLevelClient rhlClient = new RestHighLevelClient(restClientBuilder);

        if (LOG.isInfoEnabled()) {
            LOG.info("Pinging Elasticsearch cluster via hosts {} ...", HttpAddresses);
        }

        try{
            if (!rhlClient.ping(RequestOptions.DEFAULT)) {
                throw new RuntimeException("There are no reachable Elasticsearch nodes!");
            }
        } catch (IOException e){
            LOG.warn("", e);
        }


        if (LOG.isInfoEnabled()) {
            LOG.info("Created Elasticsearch RestHighLevelClient connected to {}", HttpAddresses.toString());
        }

        return rhlClient;
    }

    @Override
    public BulkProcessor.Builder createBulkProcessorBuilder(RestHighLevelClient client, BulkProcessor.Listener listener) {
        return BulkProcessor.builder(client::bulkAsync, listener);
    }

    @Override
    public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
        if (!bulkItemResponse.isFailed()) {
            return null;
        } else {
            return bulkItemResponse.getFailure().getCause();
        }
    }

    @Override
    public void configureBulkProcessorBackoff(
            BulkProcessor.Builder builder,
            @Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {

        BackoffPolicy backoffPolicy;
        if (flushBackoffPolicy != null) {
            switch (flushBackoffPolicy.getBackoffType()) {
                case CONSTANT:
                    backoffPolicy = BackoffPolicy.constantBackoff(
                            new TimeValue(flushBackoffPolicy.getDelayMillis()),
                            flushBackoffPolicy.getMaxRetryCount());
                    break;
                case EXPONENTIAL:
                default:
                    backoffPolicy = BackoffPolicy.exponentialBackoff(
                            new TimeValue(flushBackoffPolicy.getDelayMillis()),
                            flushBackoffPolicy.getMaxRetryCount());
            }
        } else {
            backoffPolicy = BackoffPolicy.noBackoff();
        }

        builder.setBackoffPolicy(backoffPolicy);
    }

    @Override
    public void verifyClientConnection(RestHighLevelClient client) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("Pinging Elasticsearch cluster via hosts {} ...", this.HttpAddresses);
        }

        if (!client.ping(new Header[0])) {
            throw new RuntimeException("There are no reachable Elasticsearch nodes!");
        } else {
            if (LOG.isInfoEnabled()) {
                LOG.info("Elasticsearch RestHighLevelClient is connected to {}", this.HttpAddresses);
            }

        }
    }

}
