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

package com.dtstack.flink.sql.environment;

import com.dtstack.flink.sql.constrant.ConfigConstrant;
import com.dtstack.flink.sql.util.FlinkUtil;
import com.dtstack.flink.sql.util.PropertiesUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 *  流执行环境相关配置
 * Date: 2019/11/22
 * Company: www.dtstack.com
 * @author maqi
 */
public final class StreamEnvConfigManager {
    private StreamEnvConfigManager() {
        throw new AssertionError("Singleton class.");
    }

    /**
     * 配置StreamExecutionEnvironment运行时参数
     * @param streamEnv
     * @param confProperties
     */
    public static void streamExecutionEnvironmentConfig(StreamExecutionEnvironment streamEnv, Properties confProperties)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {

        confProperties = PropertiesUtils.propertiesTrim(confProperties);
        streamEnv.getConfig().disableClosureCleaner();

        Configuration globalJobParameters = new Configuration();
        //Configuration unsupported set properties key-value
        Method method = Configuration.class.getDeclaredMethod("setValueInternal", String.class, Object.class);
        method.setAccessible(true);
        for (Map.Entry<Object, Object> prop : confProperties.entrySet()) {
            method.invoke(globalJobParameters, prop.getKey(), prop.getValue());
        }

        ExecutionConfig exeConfig = streamEnv.getConfig();
        if (exeConfig.getGlobalJobParameters() == null) {
            exeConfig.setGlobalJobParameters(globalJobParameters);
        } else if (exeConfig.getGlobalJobParameters() instanceof Configuration) {
            ((Configuration) exeConfig.getGlobalJobParameters()).addAll(globalJobParameters);
        }

        Optional<Integer> envParallelism = getEnvParallelism(confProperties);
        if (envParallelism.isPresent()) {
            streamEnv.setParallelism(envParallelism.get());
        }

        Optional<Integer> maxParallelism = getMaxEnvParallelism(confProperties);
        if (maxParallelism.isPresent()) {
            streamEnv.setMaxParallelism(maxParallelism.get());
        }

        Optional<Long> bufferTimeoutMillis = getBufferTimeoutMillis(confProperties);
        if (bufferTimeoutMillis.isPresent()) {
            streamEnv.setBufferTimeout(bufferTimeoutMillis.get());
        }

        Optional<TimeCharacteristic> streamTimeCharacteristic = getStreamTimeCharacteristic(confProperties);
        if (streamTimeCharacteristic.isPresent()) {
            streamEnv.setStreamTimeCharacteristic(streamTimeCharacteristic.get());
        }

        streamEnv.setRestartStrategy(RestartStrategies.failureRateRestart(
                ConfigConstrant.failureRate,
                Time.of(ConfigConstrant.failureInterval, TimeUnit.MINUTES),
                Time.of(ConfigConstrant.delayInterval, TimeUnit.SECONDS)
        ));

        FlinkUtil.openCheckpoint(streamEnv, confProperties);
    }

    public static void streamTableEnvironmentStateTTLConfig(TableEnvironment tableEnv, Properties confProperties) {
        confProperties = PropertiesUtils.propertiesTrim(confProperties);
        FlinkUtil.setTableEnvTTL(confProperties, tableEnv);
    }

    public static Optional<Integer> getEnvParallelism(Properties properties) {
        String parallelismStr = properties.getProperty(ConfigConstrant.SQL_ENV_PARALLELISM);
        return StringUtils.isNotBlank(parallelismStr) ? Optional.of(Integer.valueOf(parallelismStr)) : Optional.empty();
    }

    public static Optional<Integer> getMaxEnvParallelism(Properties properties) {
        String parallelismStr = properties.getProperty(ConfigConstrant.SQL_MAX_ENV_PARALLELISM);
        return StringUtils.isNotBlank(parallelismStr) ? Optional.of(Integer.valueOf(parallelismStr)) : Optional.empty();
    }

    public static Optional<Long> getBufferTimeoutMillis(Properties properties) {
        String mills = properties.getProperty(ConfigConstrant.SQL_BUFFER_TIMEOUT_MILLIS);
        return StringUtils.isNotBlank(mills) ? Optional.of(Long.valueOf(mills)) : Optional.empty();
    }

    /**
     * #ProcessingTime(默认), IngestionTime, EventTime
     * @param properties
     */
    public static Optional<TimeCharacteristic> getStreamTimeCharacteristic(Properties properties) {
        if (!properties.containsKey(ConfigConstrant.FLINK_TIME_CHARACTERISTIC_KEY)) {
            return Optional.empty();
        }
        String characteristicStr = properties.getProperty(ConfigConstrant.FLINK_TIME_CHARACTERISTIC_KEY);
        Optional<TimeCharacteristic> characteristic = Arrays.stream(TimeCharacteristic.values())
                .filter(tc -> !characteristicStr.equalsIgnoreCase(tc.toString())).findAny();

        if (!characteristic.isPresent()) {
            throw new RuntimeException("illegal property :" + ConfigConstrant.FLINK_TIME_CHARACTERISTIC_KEY);
        }
        return characteristic;
    }
}
