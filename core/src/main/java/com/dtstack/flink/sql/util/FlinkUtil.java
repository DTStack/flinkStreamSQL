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


import com.dtstack.flink.sql.constrant.ConfigConstrant;
import com.dtstack.flink.sql.enums.EStateBackend;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.AggregateFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Reason:
 * Date: 2017/2/21
 * Company: www.dtstack.com
 * @author xuchao
 */

public class FlinkUtil {

    private static final Logger logger = LoggerFactory.getLogger(FlinkUtil.class);

    /**
     * TABLE|SCALA|AGGREGATE
     * 注册UDF到table env
     */
    public static void registerUDF(String type, String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader){
        if("SCALA".equalsIgnoreCase(type)){
            registerScalaUDF(classPath, funcName, tableEnv, classLoader);
        }else if("TABLE".equalsIgnoreCase(type)){
            registerTableUDF(classPath, funcName, tableEnv, classLoader);
        }else if("AGGREGATE".equalsIgnoreCase(type)){
            registerAggregateUDF(classPath, funcName, tableEnv, classLoader);
        }else{
            throw new RuntimeException("not support of UDF which is not in (TABLE, SCALA, AGGREGATE)");
        }

    }

    /**
     * 注册自定义方法到env上
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerScalaUDF(String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader){
        try{
            ScalarFunction udfFunc = Class.forName(classPath, false, classLoader)
                    .asSubclass(ScalarFunction.class).newInstance();
            tableEnv.registerFunction(funcName, udfFunc);
            logger.info("register scala function:{} success.", funcName);
        }catch (Exception e){
            logger.error("", e);
            throw new RuntimeException("register UDF exception:", e);
        }
    }

    /**
     * 注册自定义TABLEFFUNC方法到env上
     *
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerTableUDF(String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader){
        try {
            TableFunction udfFunc = Class.forName(classPath, false, classLoader)
                    .asSubclass(TableFunction.class).newInstance();
            if(tableEnv instanceof StreamTableEnvironment){
                ((StreamTableEnvironment)tableEnv).registerFunction(funcName, udfFunc);
            }else if(tableEnv instanceof BatchTableEnvironment){
                ((BatchTableEnvironment)tableEnv).registerFunction(funcName, udfFunc);
            }else{
                throw new RuntimeException("no support tableEnvironment class for " + tableEnv.getClass().getName());
            }

            logger.info("register table function:{} success.", funcName);
        }catch (Exception e){
            logger.error("", e);
            throw new RuntimeException("register Table UDF exception:", e);
        }
    }

    /**
     * 注册自定义Aggregate FUNC方法到env上
     *
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerAggregateUDF(String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader) {
        try {
            AggregateFunction udfFunc = Class.forName(classPath, false, classLoader)
                    .asSubclass(AggregateFunction.class).newInstance();
            if (tableEnv instanceof StreamTableEnvironment) {
                ((StreamTableEnvironment) tableEnv).registerFunction(funcName,  udfFunc);
            } else if (tableEnv instanceof BatchTableEnvironment) {
                ((BatchTableEnvironment) tableEnv).registerFunction(funcName,  udfFunc);
            } else {
                throw new RuntimeException("no support tableEnvironment class for " + tableEnv.getClass().getName());
            }

            logger.info("register Aggregate function:{} success.", funcName);
        } catch (Exception e) {
            logger.error("", e);
            throw new RuntimeException("register Aggregate UDF exception:", e);
        }
    }

    public static URLClassLoader loadExtraJar(List<URL> jarURLList, URLClassLoader classLoader) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        for(URL url : jarURLList){
            if(url.toString().endsWith(".jar")){
                urlClassLoaderAddUrl(classLoader, url);
            }
        }

        return classLoader;
    }

    private static void urlClassLoaderAddUrl(URLClassLoader classLoader, URL url) throws InvocationTargetException, IllegalAccessException {
        Method method = ReflectionUtils.getDeclaredMethod(classLoader, "addURL", URL.class);

        if(method == null){
            throw new RuntimeException("can't not find declared method addURL, curr classLoader is " + classLoader.getClass());
        }

        method.setAccessible(true);
        method.invoke(classLoader, url);
    }


    public static TypeInformation[] transformTypes(Class[] fieldTypes){
        TypeInformation[] types = new TypeInformation[fieldTypes.length];
        for(int i=0; i<fieldTypes.length; i++){
            types[i] = TypeInformation.of(fieldTypes[i]);
        }

        return types;
    }
}