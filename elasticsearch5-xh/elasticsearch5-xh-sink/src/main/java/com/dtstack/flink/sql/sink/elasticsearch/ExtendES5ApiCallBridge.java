/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.sink.elasticsearch;

import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import com.dtstack.flink.sql.util.KrbUtils;
import io.transwarp.plugin.doorkeeper.client.DoorKeeperClientPlugin;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.util.ElasticsearchUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transwarp.org.elasticsearch.action.bulk.BackoffPolicy;
import transwarp.org.elasticsearch.action.bulk.BulkItemResponse;
import transwarp.org.elasticsearch.action.bulk.BulkProcessor;
import transwarp.org.elasticsearch.client.transport.TransportClient;
import transwarp.org.elasticsearch.common.network.NetworkModule;
import transwarp.org.elasticsearch.common.settings.Settings;
import transwarp.org.elasticsearch.common.transport.TransportAddress;
import transwarp.org.elasticsearch.common.unit.TimeValue;
import transwarp.org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @date 2019/11/16
 * @author xiuzhu
 * @Company: www.dtstack.com
 */

public class ExtendES5ApiCallBridge implements ElasticsearchApiCallBridge<TransportClient> {
    private static final long serialVersionUID = -5222683870097809633L;

    private static final Logger LOG = LoggerFactory.getLogger(ExtendES5ApiCallBridge.class);

    private final List<InetSocketAddress> transportAddresses;

    protected ElasticsearchTableInfo esTableInfo;

    public ExtendES5ApiCallBridge(List<InetSocketAddress> transportAddresses, ElasticsearchTableInfo esTableInfo) {
        Preconditions.checkArgument(transportAddresses != null && !transportAddresses.isEmpty());
        this.transportAddresses = transportAddresses;
        this.esTableInfo = esTableInfo;
    }

    @Override
    public TransportClient createClient(Map<String, String> clientConfig) throws IOException{

        //1. login kdc with keytab and krb5 conf
        UserGroupInformation ugi  = KrbUtils.loginAndReturnUgi(
                esTableInfo.getPrincipal(),
                esTableInfo.getKeytab(),
                esTableInfo.getKrb5conf());

        //2. set transwarp attributes
        Settings settings = Settings.builder().put(clientConfig)
                .put("security.enable", true)
                .put(NetworkModule.TRANSPORT_TYPE_KEY, "security-netty3")
                .build();

        //3. build transport client with transwarp plugins
        TransportClient transportClient = ugi.doAs((PrivilegedAction<TransportClient>) () -> {
            TransportClient tmpClient = new PreBuiltTransportClient(settings,
                    Collections.singletonList(DoorKeeperClientPlugin.class));
            for (TransportAddress transport : ElasticsearchUtils.convertInetSocketAddresses(transportAddresses)) {
                tmpClient.addTransportAddress(transport);
            }
            return tmpClient;
        });

        // verify that we actually are connected to a cluster
        if (transportClient.connectedNodes().isEmpty()) {
            // close the transportClient here
            IOUtils.closeQuietly(transportClient);
            throw new RuntimeException("Elasticsearch client is not connected to any Elasticsearch nodes!");
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Created Elasticsearch TransportClient with connected nodes {}", transportClient.connectedNodes());
        }

        return transportClient;
    }

    @Override
    public BulkProcessor.Builder createBulkProcessorBuilder(TransportClient client, BulkProcessor.Listener listener) {
        return BulkProcessor.builder(client, listener);
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
    public void verifyClientConnection(TransportClient client) throws IOException {

    }
}
