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


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Properties;

/**
 * Reason:
 * Date: 2017/2/21
 * Company: www.dtstack.com
 * @author xuchao
 */

public class FlinkUtil {

    private static final Logger logger = LoggerFactory.getLogger(FlinkUtil.class);

    /**
     * 开启checkpoint
     * @param env
     * @throws IOException
     */
    public static void openCheckpoint(StreamExecutionEnvironment env, Properties properties) throws IOException {

        if(properties == null){
            return;
        }

        //设置了时间间隔才表明开启了checkpoint
        if(properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_INTERVAL_KEY) == null){
            return;
        }else{
            Long interval = Long.valueOf(properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_INTERVAL_KEY));
            //start checkpoint every ${interval}
            env.enableCheckpointing(interval);
        }

        String checkMode = properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_MODE_KEY);
        if(checkMode != null){
            if(checkMode.equalsIgnoreCase("EXACTLY_ONCE")){
                env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            }else if(checkMode.equalsIgnoreCase("AT_LEAST_ONCE")){
                env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            }else{
                throw new RuntimeException("not support of FLINK_CHECKPOINT_MODE_KEY :" + checkMode);
            }
        }

        String checkpointTimeoutStr = properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_TIMEOUT_KEY);
        if(checkpointTimeoutStr != null){
            Long checkpointTimeout = Long.valueOf(checkpointTimeoutStr);
            //checkpoints have to complete within one min,or are discard
            env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
        }

        String maxConcurrCheckpointsStr = properties.getProperty(ConfigConstrant.FLINK_MAXCONCURRENTCHECKPOINTS_KEY);
        if(maxConcurrCheckpointsStr != null){
            Integer maxConcurrCheckpoints = Integer.valueOf(maxConcurrCheckpointsStr);
            //allow only one checkpoint to be int porgress at the same time
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrCheckpoints);
        }

        String cleanupModeStr = properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_CLEANUPMODE_KEY);
        if ("true".equalsIgnoreCase(cleanupModeStr)){
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        } else if("false".equalsIgnoreCase(cleanupModeStr) || cleanupModeStr == null){
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        } else{
            throw new RuntimeException("not support value of cleanup mode :" + cleanupModeStr);
        }

        String backendPath = properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_DATAURI_KEY);
        if(backendPath != null){
            //set checkpoint save path on file system, 根据实际的需求设定文件路径,hdfs://, file://
            env.setStateBackend(new FsStateBackend(backendPath));
        }

    }

    /**
     * #ProcessingTime(默认),IngestionTime,EventTime
     * @param env
     * @param properties
     */
    public static void setStreamTimeCharacteristic(StreamExecutionEnvironment env, Properties properties){
        if(!properties.containsKey(ConfigConstrant.FLINK_TIME_CHARACTERISTIC_KEY)){
            //走默认值
            return;
        }

        String characteristicStr = properties.getProperty(ConfigConstrant.FLINK_TIME_CHARACTERISTIC_KEY);
        Boolean flag = false;
        for(TimeCharacteristic tmp : TimeCharacteristic.values()){
            if(characteristicStr.equalsIgnoreCase(tmp.toString())){
                env.setStreamTimeCharacteristic(tmp);
                flag = true;
            }
        }

        if(!flag){
            throw new RuntimeException("illegal property :" + ConfigConstrant.FLINK_TIME_CHARACTERISTIC_KEY);
        }
    }



    /**
     * FIXME 暂时不支持 UDF 实现类--有参构造方法
     * TABLE|SCALA
     * 注册UDF到table env
     */
    public static void registerUDF(String type, String classPath, String funcName, TableEnvironment tableEnv,
                                   ClassLoader classLoader){
        if("SCALA".equalsIgnoreCase(type)){
            registerScalaUDF(classPath, funcName, tableEnv, classLoader);
        }else if("TABLE".equalsIgnoreCase(type)){
            registerTableUDF(classPath, funcName, tableEnv, classLoader);
        }else{
            throw new RuntimeException("not support of UDF which is not in (TABLE, SCALA)");
        }

    }

    /**
     * 注册自定义方法到env上
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerScalaUDF(String classPath, String funcName, TableEnvironment tableEnv,
                                        ClassLoader classLoader){
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
     * TODO 对User-Defined Aggregate Functions的支持
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerTableUDF(String classPath, String funcName, TableEnvironment tableEnv,
                                        ClassLoader classLoader){
        try {
            UserDefinedFunction udfFunc = Class.forName(classPath,false, classLoader).asSubclass(UserDefinedFunction.class).newInstance();
            if(tableEnv instanceof StreamTableEnvironment){
                if (udfFunc instanceof AggregateFunction){
                    ((StreamTableEnvironment) tableEnv).registerFunction(funcName, (AggregateFunction)udfFunc);
                }else if (udfFunc instanceof TableFunction) {
                    ((StreamTableEnvironment) tableEnv).registerFunction(funcName, (TableFunction)udfFunc);
                }else{
                    throw new RuntimeException("no support UserDefinedFunction class for " + udfFunc.getClass().getName());
                }
            }else if(tableEnv instanceof BatchTableEnvironment){
                if (udfFunc instanceof AggregateFunction){
                    ((BatchTableEnvironment) tableEnv).registerFunction(funcName, (AggregateFunction)udfFunc);
                }else if (udfFunc instanceof TableFunction) {
                    ((BatchTableEnvironment) tableEnv).registerFunction(funcName, (TableFunction)udfFunc);
                }else{
                    throw new RuntimeException("no support UserDefinedFunction class for " + udfFunc.getClass().getName());
                }
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
     *
     * FIXME 仅针对sql执行方式,暂时未找到区分设置source,transform,sink 并行度的方式
     * 设置job运行的并行度
     * @param properties
     */
    public static int getEnvParallelism(Properties properties){
        String parallelismStr = properties.getProperty(ConfigConstrant.SQL_ENV_PARALLELISM);
        return StringUtils.isNotBlank(parallelismStr)?Integer.parseInt(parallelismStr):1;
    }


    /**
     * 最大并发度
     * @param properties
     * @return
     */
    public static int getMaxEnvParallelism(Properties properties){
        String parallelismStr = properties.getProperty(ConfigConstrant.SQL_MAX_ENV_PARALLELISM);
        return StringUtils.isNotBlank(parallelismStr)?Integer.parseInt(parallelismStr):0;
    }

    /**
     *
     * @param properties
     * @return
     */
    public static long getBufferTimeoutMillis(Properties properties){
        String mills = properties.getProperty(ConfigConstrant.SQL_BUFFER_TIMEOUT_MILLIS);
        return StringUtils.isNotBlank(mills)?Long.parseLong(mills):0L;
    }

    public static URLClassLoader loadExtraJar(List<URL> jarURLList, URLClassLoader classLoader) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        int size = 0;
        for(URL url : jarURLList){
            if(url.toString().endsWith(".jar")){
                size++;
            }
        }

        URL[] urlArray = new URL[size];
        int i=0;
        for(URL url : jarURLList){
            if(url.toString().endsWith(".jar")){
                urlArray[i] = url;
                urlClassLoaderAddUrl(classLoader, url);
                i++;
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
