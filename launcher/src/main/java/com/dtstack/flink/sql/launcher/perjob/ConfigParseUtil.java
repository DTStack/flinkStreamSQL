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

package com.dtstack.flink.sql.launcher.perjob;

import org.apache.commons.io.Charsets;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;

/**
 * Date: 2019/12/28
 * Company: www.dtstack.com
 * @author maqi
 */
public class ConfigParseUtil {

    public static List<String> parsePathFromStr(String pathStr) throws UnsupportedEncodingException {
        String addjarPath = URLDecoder.decode(pathStr, Charsets.UTF_8.toString());
        if (addjarPath.length() > 2) {
            addjarPath = addjarPath.substring(1,addjarPath.length()-1).replace("\"","");
        }
        return Arrays.asList(addjarPath.split(","));
    }
}
