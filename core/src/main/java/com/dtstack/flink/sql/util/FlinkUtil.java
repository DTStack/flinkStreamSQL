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

    private static final String TTL_PATTERN_STR = "^+?([1-9][0-9]*)([dDhHmMsS])$";
    private static final Pattern TTL_PATTERN = Pattern.compile(TTL_PATTERN_STR);

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
        if(properties.getProperty(ConfigConstrant.SQL_CHECKPOINT_INTERVAL_KEY) == null && properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_INTERVAL_KEY) == null){
            return;
        }else{
            Long sql_interval = Long.valueOf(properties.getProperty(ConfigConstrant.SQL_CHECKPOINT_INTERVAL_KEY,"0"));
            Long flink_interval = Long.valueOf(properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_INTERVAL_KEY, "0"));
            long checkpointInterval = Math.max(sql_interval, flink_interval);
            //start checkpoint every ${interval}
            env.enableCheckpointing(checkpointInterval);
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

        Boolean sqlCleanMode = MathUtil.getBoolean(properties.getProperty(ConfigConstrant.SQL_CHECKPOINT_CLEANUPMODE_KEY), false);
        Boolean flinkCleanMode = MathUtil.getBoolean(properties.getProperty(ConfigConstrant.FLINK_CHECKPOINT_CLEANUPMODE_KEY), false);

        String cleanupModeStr = "false";
        if (sqlCleanMode || flinkCleanMode ){
            cleanupModeStr = "true";
        }

        if ("true".equalsIgnoreCase(cleanupModeStr)){
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        } else if("false".equalsIgnoreCase(cleanupModeStr) || cleanupModeStr == null){
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        } else{
            throw new RuntimeException("not support value of cleanup mode :" + cleanupModeStr);
        }

        String backendType = properties.getProperty(ConfigConstrant.STATE_BACKEND_KEY);
        String checkpointDataUri = properties.getProperty(ConfigConstrant.CHECKPOINTS_DIRECTORY_KEY);
        String backendIncremental = properties.getProperty(ConfigConstrant.STATE_BACKEND_INCREMENTAL_KEY, "true");

        if(!StringUtils.isEmpty(backendType)){
            StateBackend stateBackend = createStateBackend(backendType, checkpointDataUri, backendIncremental);
            env.setStateBackend(stateBackend);
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
                break;
            }
        }

        if(!flag){
            throw new RuntimeException("illegal property :" + ConfigConstrant.FLINK_TIME_CHARACTERISTIC_KEY);
        }
    }


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
     * 设置ttl
     * @param properties
     * @param tableEnv
     * @return
     */
    public static StreamQueryConfig getTableEnvTTL(Properties properties, StreamTableEnvironment tableEnv) {
        StreamQueryConfig qConfig = null;
        String ttlMintimeStr = properties.getProperty(ConfigConstrant.SQL_TTL_MINTIME);
        String ttlMaxtimeStr = properties.getProperty(ConfigConstrant.SQL_TTL_MAXTIME);
        if (StringUtils.isNotEmpty(ttlMintimeStr) || StringUtils.isNotEmpty(ttlMaxtimeStr)) {
            verityTtl(ttlMintimeStr, ttlMaxtimeStr);
            Matcher ttlMintimeStrMatcher = TTL_PATTERN.matcher(ttlMintimeStr);
            Matcher ttlMaxtimeStrMatcher = TTL_PATTERN.matcher(ttlMaxtimeStr);

            Long ttlMintime = 0L;
            Long ttlMaxtime = 0L;
            if (ttlMintimeStrMatcher.find()) {
                ttlMintime = getTtlTime(Integer.parseInt(ttlMintimeStrMatcher.group(1)), ttlMintimeStrMatcher.group(2));
            }
            if (ttlMaxtimeStrMatcher.find()) {
                ttlMaxtime = getTtlTime(Integer.parseInt(ttlMaxtimeStrMatcher.group(1)), ttlMaxtimeStrMatcher.group(2));
            }
            if (0L != ttlMintime && 0L != ttlMaxtime) {
                qConfig = tableEnv.queryConfig();
                qConfig.withIdleStateRetentionTime(Time.milliseconds(ttlMintime), Time.milliseconds(ttlMaxtime));
            }
        }
        return qConfig;
    }

    /**
     * ttl 校验
     * @param ttlMintimeStr 最小时间
     * @param ttlMaxtimeStr 最大时间
     */
    private static void verityTtl(String ttlMintimeStr, String ttlMaxtimeStr) {
        if (null == ttlMintimeStr
                || null == ttlMaxtimeStr
                || !TTL_PATTERN.matcher(ttlMintimeStr).find()
                || !TTL_PATTERN.matcher(ttlMaxtimeStr).find()) {
            throw new RuntimeException("sql.ttl.min 、sql.ttl.max must be set at the same time . example sql.ttl.min=1h,sql.ttl.max=2h");
        }
    }

    /**
     * 不同单位时间到毫秒的转换
     * @param timeNumber 时间值，如：30
     * @param timeUnit 单位，d:天，h:小时，m:分，s:秒
     * @return
     */
    private static Long getTtlTime(Integer timeNumber,String timeUnit) {
        if (timeUnit.equalsIgnoreCase("d")) {
            return timeNumber * 1000l * 60 * 60 * 24;
        } else if (timeUnit.equalsIgnoreCase("h")) {
            return timeNumber * 1000l * 60 * 60;
        } else if (timeUnit.equalsIgnoreCase("m")) {
            return timeNumber * 1000l * 60;
        } else if (timeUnit.equalsIgnoreCase("s")) {
            return timeNumber * 1000l;
        } else {
            throw new RuntimeException("not support "+timeNumber+timeUnit);
        }
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

    private static StateBackend createStateBackend(String backendType, String checkpointDataUri, String backendIncremental) throws IOException {
        EStateBackend stateBackendType = EStateBackend.convertFromString(backendType);
        StateBackend stateBackend = null;
        switch (stateBackendType) {
            case MEMORY:
                stateBackend = new MemoryStateBackend();
                break;
            case FILESYSTEM:
                checkpointDataUriEmptyCheck(checkpointDataUri, backendType);
                stateBackend = new FsStateBackend(checkpointDataUri);
                break;
            case ROCKSDB:
                checkpointDataUriEmptyCheck(checkpointDataUri, backendType);
                stateBackend = new RocksDBStateBackend(checkpointDataUri, BooleanUtils.toBoolean(backendIncremental));
                break;
        }
        return stateBackend;
    }

    private static void checkpointDataUriEmptyCheck(String checkpointDataUri, String backendType) {
        if (StringUtils.isEmpty(checkpointDataUri)) {
            throw new RuntimeException(backendType + " backend checkpointDataUri not null!");
        }
    }
}