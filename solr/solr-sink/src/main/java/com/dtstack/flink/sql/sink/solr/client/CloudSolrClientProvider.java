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

package com.dtstack.flink.sql.sink.solr.client;

import com.dtstack.flink.sql.sink.solr.options.KerberosOptions;
import com.dtstack.flink.sql.sink.solr.options.SolrClientOptions;
import com.dtstack.flink.sql.util.DtFileUtils;
import org.apache.flink.runtime.security.DynamicConfiguration;
import org.apache.flink.runtime.security.KerberosUtils;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Optional;

import static com.dtstack.flink.sql.sink.solr.client.FlinkxKrb5HttpClientBuilder.SOLR_KERBEROS_JAAS_APPNAME;

/**
 * 目前只实现CloudSolrClient一种，以后有其他需求可以实现其他Provider。 例如HttpSolrClient，ConcurrentUpdateSolrClient。
 * Inspired by flink connector jdbc.
 *
 * @author wuren
 * @program flinkStreamSQL
 * @create 2021/05/18
 */
public class CloudSolrClientProvider implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    private static final String JAAS_APP_NAME = "SolrJClient";
    private final SolrClientOptions solrClientOptions;
    private final KerberosOptions kerberosOptions;
    private CloudSolrClient cloudSolrClient;
    private Subject subject;

    public CloudSolrClientProvider(
            SolrClientOptions solrClientOptions, KerberosOptions kerberosOptions) {
        this.solrClientOptions = solrClientOptions;
        this.kerberosOptions = kerberosOptions;
    }

    public SolrClient getClient() throws KrbException, LoginException {
        if (kerberosOptions.isEnableKrb()) {
            String principal = kerberosOptions.getPrincipal();
            String krb5conf = kerberosOptions.getKrb5conf();
            String keytab = kerberosOptions.getKeytab();

            System.setProperty(SOLR_KERBEROS_JAAS_APPNAME, JAAS_APP_NAME);
            setKrb5Conf(krb5conf);
            subject = genSubject(principal, keytab);
            setKrb5HttpClient(principal, keytab);

            LOG.info("Kerberos login principal: {}, keytab: {}", principal, keytab);
            Subject.doAs(
                    subject,
                    (PrivilegedAction<Object>)
                            () -> {
                                connect();
                                return null;
                            });
        } else {
            connect();
        }
        return cloudSolrClient;
    }

    private void connect() {
        cloudSolrClient =
                new CloudSolrClient.Builder(
                                solrClientOptions.getZkHosts(),
                                Optional.of(solrClientOptions.getZkChroot()))
                        .build();
        String collectionName = solrClientOptions.getCollection();
        cloudSolrClient.setDefaultCollection(collectionName);
        cloudSolrClient.connect();
    }

    private void refreshConfig() throws KrbException {
        sun.security.krb5.Config.refresh();
        KerberosName.resetDefaultRealm();
    }

    public void add(SolrInputDocument solrDocument)
            throws IOException, SolrServerException, PrivilegedActionException {
        if (kerberosOptions.isEnableKrb()) {
            Subject.doAs(
                    subject,
                    new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws IOException, SolrServerException {
                            cloudSolrClient.add(solrDocument);
                            return null;
                        }
                    });
        } else {
            cloudSolrClient.add(solrDocument);
        }
    }

    public void commit() throws IOException, SolrServerException, PrivilegedActionException {
        if (kerberosOptions.isEnableKrb()) {
            Subject.doAs(
                    subject,
                    new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws IOException, SolrServerException {
                            cloudSolrClient.commit();
                            return null;
                        }
                    });
        } else {
            cloudSolrClient.commit();
        }
    }

    public void close() throws IOException, PrivilegedActionException {
        if (kerberosOptions.isEnableKrb()) {
            Subject.doAs(
                    subject,
                    new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws IOException {
                            cloudSolrClient.close();
                            return null;
                        }
                    });
        } else {
            cloudSolrClient.close();
        }
    }

    private void setKrb5Conf(String krb5conf) throws KrbException {
        DtFileUtils.checkExists(krb5conf);
        System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5conf);
        LOG.info("{} is set to {}", JAVA_SECURITY_KRB5_CONF, krb5conf);
        refreshConfig();
    }

    private Subject genSubject(String principal, String keytab) throws LoginException {
        // construct a dynamic JAAS configuration
        DynamicConfiguration currentConfig = new DynamicConfiguration(null);
        // wire up the configured JAAS login contexts to use the krb5 entries
        AppConfigurationEntry krb5Entry = KerberosUtils.keytabEntry(keytab, principal);
        currentConfig.addAppConfigurationEntry(JAAS_APP_NAME, krb5Entry);
        LoginContext context = new LoginContext(JAAS_APP_NAME, null, null, currentConfig);

        context.login();
        return context.getSubject();
    }

    private void setKrb5HttpClient(String principal, String keytab) {
        Krb5HttpClientBuilder krbBuild = new FlinkxKrb5HttpClientBuilder(principal, keytab);
        SolrHttpClientBuilder kb = krbBuild.getBuilder();
        HttpClientUtil.setHttpClientBuilder(kb);
    }
}
