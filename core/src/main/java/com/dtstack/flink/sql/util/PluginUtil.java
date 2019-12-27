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

import com.dtstack.flink.sql.classloader.DtClassLoader;
import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reason:
 * Date: 2018/6/27
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class PluginUtil {

    private static String SP = File.separator;

    private static final String JAR_SUFFIX = ".jar";

    private static final String CLASS_PRE_STR = "com.dtstack.flink.sql";

    private static ObjectMapper objectMapper = new ObjectMapper();


    public static String getJarFileDirPath(String type, String sqlRootDir) {
        String jarPath = sqlRootDir + SP + type;
        File jarFile = new File(jarPath);

        if (!jarFile.exists()) {
            throw new RuntimeException(String.format("path %s not exists!!!", jarPath));
        }

        return jarPath;
    }

    public static String getSideJarFileDirPath(String pluginType, String sideOperator, String tableType, String sqlRootDir) throws MalformedURLException {
        String dirName = sqlRootDir + SP + pluginType + sideOperator + tableType.toLowerCase();
        File jarFile = new File(dirName);

        if (!jarFile.exists()) {
            throw new RuntimeException(String.format("path %s not exists!!!", dirName));
        }

        return dirName;
    }

    public static String getGenerClassName(String pluginTypeName, String type) throws IOException {
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + upperCaseFirstChar(type);
        return CLASS_PRE_STR + "." + type.toLowerCase() + "." + pluginTypeName + "." + pluginClassName;
    }

    public static String getRetractGenerClassName(String pluginTypeName, String type) throws IOException {
        String pluginClassName = upperCaseFirstChar("retract") + upperCaseFirstChar(pluginTypeName) + upperCaseFirstChar(type);
        return CLASS_PRE_STR + "." + type.toLowerCase() + "." + pluginTypeName + "." + "retract." + pluginClassName;
    }

    public static String getSqlParserClassName(String pluginTypeName, String type) {

        String pluginClassName = upperCaseFirstChar(pluginTypeName) + upperCaseFirstChar(type) + "Parser";
        return CLASS_PRE_STR + "." + type.toLowerCase() + "." + pluginTypeName + ".table." + pluginClassName;
    }


    public static String getSqlSideClassName(String pluginTypeName, String type, String operatorType) {
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + operatorType + "ReqRow";
        return CLASS_PRE_STR + "." + type.toLowerCase() + "." + pluginTypeName + "." + pluginClassName;
    }

    public static Map<String, Object> ObjectToMap(Object obj) throws Exception {
        return objectMapper.readValue(objectMapper.writeValueAsBytes(obj), Map.class);
    }

    public static <T> T jsonStrToObject(String jsonStr, Class<T> clazz) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException {
        return objectMapper.readValue(jsonStr, clazz);
    }

    public static Properties stringToProperties(String str) throws IOException {
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(str.getBytes("UTF-8")));
        return properties;
    }

    public static URL getRemoteJarFilePath(String pluginType, String tableType, String remoteSqlRootDir, String localSqlPluginPath) throws Exception {
        return buildFinalJarFilePath(pluginType, tableType, remoteSqlRootDir, localSqlPluginPath);
    }

    public static URL getLocalJarFilePath(String pluginType, String tableType, String localSqlPluginPath) throws Exception {
        return buildFinalJarFilePath(pluginType, tableType, null, localSqlPluginPath);
    }

    public static URL buildFinalJarFilePath(String pluginType, String tableType, String remoteSqlRootDir, String localSqlPluginPath) throws Exception {
        String dirName = pluginType + tableType.toLowerCase();
        String prefix = String.format("%s-%s", pluginType, tableType.toLowerCase());
        String jarPath = localSqlPluginPath + SP + dirName;
        String jarName = getCoreJarFileName(jarPath, prefix);
        String sqlRootDir = remoteSqlRootDir == null ? localSqlPluginPath : remoteSqlRootDir;
        return new URL("file:" + sqlRootDir + SP + dirName + SP + jarName);
    }

    public static URL getRemoteSideJarFilePath(String pluginType, String sideOperator, String tableType, String remoteSqlRootDir, String localSqlPluginPath) throws Exception {
        return buildFinalSideJarFilePath(pluginType, sideOperator, tableType, remoteSqlRootDir, localSqlPluginPath);
    }

    public static URL getLocalSideJarFilePath(String pluginType, String sideOperator, String tableType, String localSqlPluginPath) throws Exception {
        return buildFinalSideJarFilePath(pluginType, sideOperator, tableType, null, localSqlPluginPath);
    }

    public static URL buildFinalSideJarFilePath(String pluginType, String sideOperator, String tableType, String remoteSqlRootDir, String localSqlPluginPath) throws Exception {
        String dirName = pluginType + sideOperator + tableType.toLowerCase();
        String prefix = String.format("%s-%s-%s", pluginType, sideOperator, tableType.toLowerCase());
        String jarPath = localSqlPluginPath + SP + dirName;
        String jarName = getCoreJarFileName(jarPath, prefix);
        String sqlRootDir = remoteSqlRootDir == null ? localSqlPluginPath : remoteSqlRootDir;
        return new URL("file:" + sqlRootDir + SP + dirName + SP + jarName);
    }

    public static String upperCaseFirstChar(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    public static void addPluginJar(String pluginDir, DtClassLoader classLoader) throws MalformedURLException {
        File dirFile = new File(pluginDir);
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            throw new RuntimeException("plugin path:" + pluginDir + "is not exist.");
        }

        File[] files = dirFile.listFiles(tmpFile -> tmpFile.isFile() && tmpFile.getName().endsWith(JAR_SUFFIX));
        if (files == null || files.length == 0) {
            throw new RuntimeException("plugin path:" + pluginDir + " is null.");
        }

        for (File file : files) {
            URL pluginJarURL = file.toURI().toURL();
            classLoader.addURL(pluginJarURL);
        }
    }

    public static URL[] getPluginJarUrls(String pluginDir) throws MalformedURLException {
        List<URL> urlList = new ArrayList<>();
        File dirFile = new File(pluginDir);
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            throw new RuntimeException("plugin path:" + pluginDir + "is not exist.");
        }

        File[] files = dirFile.listFiles(tmpFile -> tmpFile.isFile() && tmpFile.getName().endsWith(JAR_SUFFIX));
        if (files == null || files.length == 0) {
            throw new RuntimeException("plugin path:" + pluginDir + " is null.");
        }

        for (File file : files) {
            URL pluginJarURL = file.toURI().toURL();
            urlList.add(pluginJarURL);
        }
        return urlList.toArray(new URL[urlList.size()]);
    }

    public static String getCoreJarFileName(String path, String prefix) throws Exception {
        String coreJarFileName = null;
        File pluginDir = new File(path);
        if (pluginDir.exists() && pluginDir.isDirectory()) {
            File[] jarFiles = pluginDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().startsWith(prefix) && name.toLowerCase().endsWith(".jar");
                }
            });

            if (jarFiles != null && jarFiles.length > 0) {
                coreJarFileName = jarFiles[0].getName();
            }
        }

        if (StringUtils.isEmpty(coreJarFileName)) {
            throw new Exception("Can not find core jar file in path:" + path);
        }

        return coreJarFileName;
    }

}
