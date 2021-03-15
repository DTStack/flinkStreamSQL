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

package com.dtstack.flink.sql.util;

import com.esotericsoftware.minlog.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.io.IOException;

/**
 * @program: flinkStreamSQL
 * @author: wuren
 * @create: 2020/09/14
 **/
public class KrbUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KrbUtils.class);

    public static final String KRB5_CONF_KEY = "java.security.krb5.conf";
    public static final String HADOOP_AUTH_KEY = "hadoop.security.authentication";
    public static final String KRB_STR = "Kerberos";
//    public static final String FALSE_STR = "false";
//    public static final String SUBJECT_ONLY_KEY = "javax.security.auth.useSubjectCredsOnly";

    public static UserGroupInformation loginAndReturnUgi(String principal, String keytabPath, String krb5confPath) throws IOException {
        LOG.info("Kerberos login with principal: {} and keytab: {}", principal, keytabPath);
        System.setProperty(KRB5_CONF_KEY, krb5confPath);
        // 不刷新会读/etc/krb5.conf
        try {
            Config.refresh();
            KerberosName.resetDefaultRealm();
        } catch (KrbException e) {
            LOG.warn("resetting default realm failed, current default realm will still be used.", e);
        }
        // TODO 尚未探索出此选项的意义，以后研究明白方可打开
//        System.setProperty(SUBJECT_ONLY_KEY, FALSE_STR);
        Configuration configuration = new Configuration();
        configuration.set(HADOOP_AUTH_KEY , KRB_STR);
        UserGroupInformation.setConfiguration(configuration);
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
    }

}
