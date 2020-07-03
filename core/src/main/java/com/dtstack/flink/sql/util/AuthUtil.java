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

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *  Utility methods for helping with security tasks.
 * Date: 2019/12/28
 * Company: www.dtstack.com
 * @author maqi
 */
public class AuthUtil {

    public static String creatJaasFile(String prefix, String suffix, JAASConfig jaasConfig) throws IOException {
        File krbConf = new File(System.getProperty("user.dir"));
        File temp = File.createTempFile(prefix, suffix, krbConf);
        temp.deleteOnExit();
        FileUtils.writeStringToFile(temp, jaasConfig.toString());
        return temp.getAbsolutePath();
    }


    public static class JAASConfig {
        private String entryName;
        private String loginModule;
        private String loginModuleFlag;
        private Map<String, String> loginModuleOptions;

        public JAASConfig(String entryName, String loginModule, String loginModuleFlag, Map<String, String> loginModuleOptions) {
            this.entryName = entryName;
            this.loginModule = loginModule;
            this.loginModuleFlag = loginModuleFlag;
            this.loginModuleOptions = loginModuleOptions;
        }

        public static Builder builder() {
            return new Builder();
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder(entryName).append(" {\n\t")
                    .append(loginModule).append("  ").append(loginModuleFlag).append("\n\t");
            String[] keys = loginModuleOptions.keySet().toArray(new String[loginModuleOptions.size()]);
            for (int i = 0; i < keys.length; i++) {
                stringBuilder.append(keys[i]).append("=").append(loginModuleOptions.get(keys[i]));
                if (i != keys.length - 1) {
                    stringBuilder.append("\n\t");
                } else {
                    stringBuilder.append(";\n");
                }

            }
            stringBuilder.append("\n").append("};");
            return stringBuilder.toString();
        }

        public static class Builder {
            private String entryName;
            private String loginModule;
            private String loginModuleFlag;
            private Map<String, String> loginModuleOptions;

            public Builder setEntryName(String entryName) {
                this.entryName = entryName;
                return this;
            }

            public Builder setLoginModule(String loginModule) {
                this.loginModule = loginModule;
                return this;
            }

            public Builder setLoginModuleFlag(String loginModuleFlag) {
                this.loginModuleFlag = loginModuleFlag;
                return this;
            }

            public Builder setLoginModuleOptions(Map<String, String> loginModuleOptions) {
                this.loginModuleOptions = loginModuleOptions;
                return this;
            }

            public JAASConfig build() {
                return new JAASConfig(
                        entryName, loginModule, loginModuleFlag, loginModuleOptions);
            }
        }
    }
}
