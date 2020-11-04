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

package com.dtstack.flink.sql;

import com.dtstack.flink.sql.classloader.DtClassLoader;
import com.dtstack.flink.sql.exec.ApiResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Date: 2020/2/17
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class TestGetPlan {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testGetExecutionPlan() throws Exception {
        List<URL> urls = new ArrayList<URL>();
        urls.addAll(getJarUrl("/Users/chuixue/dtstack/soft/flink-1.10.1/lib"));

        ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
        DtClassLoader childClassLoader = new DtClassLoader(urls.toArray(new URL[urls.size()]), parentClassLoader);
        Class<?> aClass = childClassLoader.loadClass("com.dtstack.flink.sql.GetPlan");

        String[] params = {
                "-mode",
                "local",
                "-sql",
                readContent("/Users/chuixue/Desktop/tmp/sqlFile.sql"),
                "-localSqlPluginPath",
                "/Users/chuixue/dtstack/tmpworkspace/flinkStreamSQL/sqlplugins",
                "-name", "test"};

        Method getExecutionPlan = aClass.getMethod("getExecutionPlan", String[].class);
        String jsonStr = (String) getExecutionPlan.invoke(aClass.newInstance(), (Object) params);

        ObjectNode jsonNodes = OBJECT_MAPPER.readValue(jsonStr, ObjectNode.class);
        Assert.assertEquals(jsonNodes.get("code").asLong(), ApiResult.SUCCESS.longValue());
    }

    public static String readContent(String filePath) {
        StringBuilder sb = new StringBuilder();
        try {
            String str;
            BufferedReader in = new BufferedReader(new FileReader(filePath));
            while ((str = in.readLine()) != null) {
                sb.append(str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static List<URL> getJarUrl(String directory) {
        File flinkLibDir = new File(directory);
        return Arrays.stream(flinkLibDir.listFiles(((dir1, name) -> name.endsWith(".jar")))).map(file -> {
            try {
                return file.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }

        }).collect(Collectors.toList());
    }
}
