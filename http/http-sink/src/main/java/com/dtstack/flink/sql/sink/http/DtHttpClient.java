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

package com.dtstack.flink.sql.sink.http;

import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


/**
 * @author tiezhu
 * @date 2021/3/22 星期一
 * Company dtstack
 */
public class DtHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(DtHttpClient.class);

    private static final int DEFAULT_MAX_TOTAL = 20;

    private static final int DEFAULT_MAX_PER_ROUTE = 20;

    private static final int DEFAULT_SOCKET_TIMEOUT = 5000;// 5秒

    private static final int DEFAULT_CONNECT_TIMEOUT = 5000;// 5秒

    private static final int DEFAULT_TIME_OUT = 10000;// 10秒

    private static final String DEFAULT_CONTENT_TYPE = "application/json";

    private static final String DEFAULT_CONTENT_ENCODING = "UTF-8";

    private static final CloseableHttpClient httpClient;

    private static final RequestConfig requestConfig;

    static {
        httpClient = createHttpClient();
        requestConfig = RequestConfig.custom()
            .setSocketTimeout(DEFAULT_TIME_OUT)
            .setConnectTimeout(DEFAULT_TIME_OUT)
            .setConnectionRequestTimeout(DEFAULT_TIME_OUT)
            .build();
    }

    private static CloseableHttpClient createHttpClient() {
        LOG.info(" init http pool ...");
        PlainConnectionSocketFactory plainConnectionSocketFactory = PlainConnectionSocketFactory.getSocketFactory();
        SSLConnectionSocketFactory sslConnectionSocketFactory = SSLConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> registry = RegistryBuilder
            .<ConnectionSocketFactory>create()
            .register("http", plainConnectionSocketFactory)
            .register("https", sslConnectionSocketFactory)
            .build();
        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager(registry);
        manager.setMaxTotal(DEFAULT_MAX_TOTAL);
        manager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);

        //设置请求和传输超时时间
        RequestConfig requestConfig = RequestConfig.custom()
            //设置从connect Manager获取Connection 超时时间，单位毫秒。这个属性是新加的属性，因为目前版本是可以共享连接池的。
            .setConnectionRequestTimeout(DEFAULT_CONNECT_TIMEOUT)
            //请求获取数据的超时时间，单位毫秒。 如果访问一个接口，多少时间内无法返回数据，就直接放弃此次调用。
            .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT)
            //设置连接超时时间，单位毫秒。
            .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT)
            .build();

        return HttpClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .setConnectionManager(manager)
            .setRetryHandler(new SqlHttpRequestRetryHandler())
            .build();
    }

    public static void post(String httpUrl, String message) throws IOException {
        post(httpUrl, message, DEFAULT_CONTENT_TYPE, DEFAULT_CONTENT_ENCODING);
    }

    public static void post(String httpUrl, Map<String, Object> message) throws IOException {
        post(httpUrl, PluginUtil.objectToString(message), DEFAULT_CONTENT_TYPE, DEFAULT_CONTENT_ENCODING);
    }

    public static void post(String httpUrl,
                            String message,
                            String contentType,
                            String contentEncoding) throws IOException {
        HttpPost httpPost = new HttpPost(httpUrl);
        httpPost.setConfig(requestConfig);
        StringEntity entity = new StringEntity(message, contentEncoding);
        entity.setContentType(contentType);
        entity.setContentEncoding(contentEncoding);
        httpPost.setEntity(entity);
        try (final CloseableHttpResponse response = httpClient.execute(httpPost)) {
            LOG.debug("Get response: {}", response.getStatusLine().getStatusCode());
        }
    }

    public static void close() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.error("DtHttpClient close error!", e);
            }
        }
    }
}
