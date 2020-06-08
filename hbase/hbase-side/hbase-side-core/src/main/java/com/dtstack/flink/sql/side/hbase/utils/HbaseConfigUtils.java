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

package com.dtstack.flink.sql.side.hbase.utils;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
    private final static String AUTHENTICATION_TYPE = "Kerberos";
    private final static String KEY_HBASE_SECURITY_AUTHENTICATION = "hbase.security.authentication";
    private final static String KEY_HBASE_SECURITY_AUTHORIZATION =  "hbase.security.authorization";
    private final static String KEY_HBASE_MASTER_KERBEROS_PRINCIPAL = "hbase.master.kerberos.principal";
    private final static String KEY_HBASE_MASTER_KEYTAB_FILE = "hbase.master.keytab.file";
    private final static String KEY_HBASE_REGIONSERVER_KEYTAB_FILE = "hbase.regionserver.keytab.file";
    private final static String KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";

    // async side kerberos
    private final static String KEY_HBASE_SECURITY_AUTH_ENABLE = "hbase.security.auth.enable";
    private final static String KEY_HBASE_SASL_CLIENTCONFIG = "hbase.sasl.clientconfig";
    private final static String KEY_HBASE_KERBEROS_REGIONSERVER_PRINCIPAL = "hbase.kerberos.regionserver.principal";
    private static final String KEY_KEY_TAB = "hbase.keytab";
    private static final String KEY_PRINCIPAL = "hbase.principal";

    public final static String KEY_HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public final static String KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM = "hbase.zookeeper.znode.parent";


    private static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    public static final String KEY_JAVA_SECURITY_AUTH_LOGIN_CONF = "java.security.auth.login.config";


    private static final String SP = File.separator;
    private static final  String KEY_KRB5_CONF = "krb5.conf";


    private static List<String> KEYS_KERBEROS_REQUIRED = Arrays.asList(
            KEY_HBASE_SECURITY_AUTHENTICATION,
            KEY_HBASE_MASTER_KERBEROS_PRINCIPAL,
            KEY_HBASE_MASTER_KEYTAB_FILE,
            KEY_HBASE_REGIONSERVER_KEYTAB_FILE,
            KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL
    );

    private static List<String> ASYNC_KEYS_KERBEROS_REQUIRED = Arrays.asList(
            KEY_HBASE_SECURITY_AUTH_ENABLE,
            KEY_HBASE_SASL_CLIENTCONFIG,
            KEY_HBASE_KERBEROS_REGIONSERVER_PRINCIPAL,
            KEY_HBASE_SECURITY_AUTHENTICATION,
            KEY_KEY_TAB);


    public static Configuration getConfig(Map<String, Object> hbaseConfigMap) {
        Configuration hConfiguration = HBaseConfiguration.create();

        for (Map.Entry<String, Object> entry : hbaseConfigMap.entrySet()) {
            if (entry.getValue() != null && !(entry.getValue() instanceof Map)) {
                hConfiguration.set(entry.getKey(), entry.getValue().toString());
            }
        }
        return hConfiguration;
    }

    public static boolean openKerberos(Map<String, Object> hbaseConfigMap) {
        if (!MapUtils.getBooleanValue(hbaseConfigMap, KEY_HBASE_SECURITY_AUTHORIZATION)) {
            return false;
        }
        return AUTHENTICATION_TYPE.equalsIgnoreCase(MapUtils.getString(hbaseConfigMap, KEY_HBASE_SECURITY_AUTHENTICATION));
    }

    public static boolean asyncOpenKerberos(Map<String, Object> hbaseConfigMap) {
        if (!MapUtils.getBooleanValue(hbaseConfigMap, KEY_HBASE_SECURITY_AUTH_ENABLE)) {
            return false;
        }
        return AUTHENTICATION_TYPE.equalsIgnoreCase(MapUtils.getString(hbaseConfigMap, KEY_HBASE_SECURITY_AUTHENTICATION));
    }




    public static Configuration getHadoopConfiguration(Map<String, Object> hbaseConfigMap) {
        for (String key : KEYS_KERBEROS_REQUIRED) {
            if (StringUtils.isEmpty(MapUtils.getString(hbaseConfigMap, key))) {
                throw new IllegalArgumentException(String.format("Must provide [%s] when authentication is Kerberos", key));
            }
        }
        loadKrb5Conf(hbaseConfigMap);

        Configuration conf = new Configuration();
        if (hbaseConfigMap == null) {
            return conf;
        }

        hbaseConfigMap.forEach((key, val) -> {
            if (val != null) {
                conf.set(key, val.toString());
            }
        });

        return conf;
    }

    public static String getPrincipal(Map<String, Object> hbaseConfigMap) {
        String principal = MapUtils.getString(hbaseConfigMap, KEY_HBASE_MASTER_KERBEROS_PRINCIPAL);
        if (StringUtils.isNotEmpty(principal)) {
            return principal;
        }

        throw new IllegalArgumentException("");
    }

    public static String getKeytab(Map<String, Object> hbaseConfigMap) {
        String keytab = MapUtils.getString(hbaseConfigMap, KEY_HBASE_MASTER_KEYTAB_FILE);
        if (StringUtils.isNotEmpty(keytab)) {
            return keytab;
        }

        throw new IllegalArgumentException("");
    }

    public static void loadKrb5Conf(Map<String, Object> kerberosConfig) {
        String krb5FilePath = MapUtils.getString(kerberosConfig, KEY_JAVA_SECURITY_KRB5_CONF);
        if (!org.apache.commons.lang.StringUtils.isEmpty(krb5FilePath)) {
            System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);;
        }
    }

    public static String creatJassFile(String configStr) throws IOException {
        String fileName = System.getProperty("user.dir");
        File krbConf = new File(fileName);
        File temp = File.createTempFile("JAAS", ".conf", krbConf);
        temp.deleteOnExit();
        BufferedWriter out = new BufferedWriter(new FileWriter(temp, false));
        out.write(configStr + "\n");
        out.close();
        return temp.getAbsolutePath();
    }

    public static String buildJaasStr(Map<String, Object> kerberosConfig) {
        for (String key : ASYNC_KEYS_KERBEROS_REQUIRED) {
            if (StringUtils.isEmpty(MapUtils.getString(kerberosConfig, key))) {
                throw new IllegalArgumentException(String.format("Must provide [%s] when authentication is Kerberos", key));
            }
        }

        String keyTab = MapUtils.getString(kerberosConfig, KEY_KEY_TAB);
        String principal = MapUtils.getString(kerberosConfig, KEY_PRINCIPAL);

        StringBuilder jaasSB = new StringBuilder("Client {\n" +
                "  com.sun.security.auth.module.Krb5LoginModule required\n" +
                "  useKeyTab=true\n" +
                "  useTicketCache=false\n");
        jaasSB.append(" keyTab=\"").append(keyTab).append("\"").append("\n");
        jaasSB.append(" principal=\"").append(principal).append("\"").append(";\n");
        jaasSB.append("};");
        return jaasSB.toString();
    }



    public static UserGroupInformation loginAndReturnUGI(Configuration conf, String principal, String keytab) throws IOException {
        if (conf == null) {
            throw new IllegalArgumentException("kerberos conf can not be null");
        }

        if (org.apache.commons.lang.StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("principal can not be null");
        }

        if (org.apache.commons.lang.StringUtils.isEmpty(keytab)) {
            throw new IllegalArgumentException("keytab can not be null");
        }

        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);

        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }
}
