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

package com.dtstack.flink.sql.sink.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.io.File;
import java.io.IOException;

/**
 *
 *  The utility class of HBase connection
 *
 * Date: 2019/12/24
 * Company: www.dtstack.com
 * @author maqi
 */
public class HbaseConfigUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseConfigUtils.class);
    // sync side kerberos
    public final static String KEY_HBASE_SECURITY_AUTHENTICATION = "hbase.security.authentication";
    public final static String KEY_HBASE_SECURITY_AUTHORIZATION = "hbase.security.authorization";
    public final static String KEY_HBASE_MASTER_KEYTAB_FILE = "hbase.master.keytab.file";
    public final static String KEY_HBASE_MASTER_KERBEROS_PRINCIPAL = "hbase.master.kerberos.principal";
    public final static String KEY_HBASE_REGIONSERVER_KEYTAB_FILE = "hbase.regionserver.keytab.file";
    public final static String KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";

    public final static String KEY_HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public final static String KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM = "zookeeper.znode.parent";

    public final static String KEY_HBASE_CLIENT_KEYTAB_FILE = "hbase.client.keytab.file";
    public final static String KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL = "hbase.client.kerberos.principal";


    public static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    public static final String KEY_ZOOKEEPER_SASL_CLIENT = "zookeeper.sasl.client";

    public static final String KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static final String KEY_HADOOP_SECURITY_AUTH_TO_LOCAL = "hadoop.security.auth_to_local";
    public static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";

    public static UserGroupInformation loginAndReturnUGI(Configuration conf, String principal, String keytab) throws IOException, KrbException {
        LOG.info("loginAndReturnUGI principal {}",principal);
        LOG.info("loginAndReturnUGI keytab {}",keytab);
        if (conf == null) {
            throw new IllegalArgumentException("kerberos conf can not be null");
        }

        if (org.apache.commons.lang.StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("principal can not be null");
        }

        if (org.apache.commons.lang.StringUtils.isEmpty(keytab)) {
            throw new IllegalArgumentException("keytab can not be null");
        }

        if (!new File(keytab).exists()){
            throw new IllegalArgumentIOException("keytab ["+ keytab + "] not exist");
        }

        conf.set(KEY_HADOOP_SECURITY_AUTHENTICATION, "Kerberos");
        //conf.set("hadoop.security.auth_to_local", "DEFAULT");
        conf.set(KEY_HADOOP_SECURITY_AUTH_TO_LOCAL, "RULE:[1:$1] RULE:[2:$1]");
        conf.set(KEY_HADOOP_SECURITY_AUTHORIZATION, "true");

        Config.refresh();
        KerberosName.resetDefaultRealm();
        UserGroupInformation.setConfiguration(conf);

        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }
}
