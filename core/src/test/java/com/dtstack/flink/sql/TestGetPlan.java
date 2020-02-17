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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
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
 * @author maqi
 */
public class TestGetPlan {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testGetExecutionPlan() throws Exception {
        List<URL> urls = new ArrayList<URL>();
        urls.addAll(getJarUrl("/Users/maqi/code/dtstack/dt-center-flinkStreamSQL/plugins/"));
        urls.addAll(getJarUrl("/Users/maqi/tmp/flink/flink-1.8.1/lib"));

        ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
        DtClassLoader childClassLoader = new DtClassLoader(urls.toArray(new URL[urls.size()]), parentClassLoader);

        Thread.currentThread().setContextClassLoader(childClassLoader);
        Class<?> aClass = childClassLoader.loadClass("com.dtstack.flink.sql.GetPlan");

        String[] params = {
                "-mode",
                "local",
                "-sql",
                "%0d%0aCREATE+TABLE+MyTable(%0d%0a++++id+INT%2c%0d%0a++++channel+VARCHAR%2c%0d%0a++++pv+varchar%2c%0d%0a++++xctime+varchar%2c%0d%0a++++name+varchar%0d%0a+)WITH(%0d%0a++++type+%3d%27kafka11%27%2c%0d%0a++++bootstrapServers+%3d%27172.16.8.107%3a9092%27%2c%0d%0a++++zookeeperQuorum+%3d%27172.16.8.107%3a2181%2fkafka%27%2c%0d%0a++++offsetReset+%3d%27latest%27%2c%0d%0a++++topic+%3d%27mqTest02%27%2c%0d%0a++++timezone%3d%27Asia%2fShanghai%27%2c%0d%0a++++topicIsPattern+%3d%27false%27%2c%0d%0a++++parallelism+%3d%271%27%0d%0a+)%3b%0d%0a%0d%0a%0d%0aCREATE+TABLE+MyTable2(%0d%0a++++id2+INT%2c%0d%0a++++channel2+VARCHAR%2c%0d%0a++++pv2+varchar%2c%0d%0a++++xctime2+varchar%2c%0d%0a++++name2+varchar%0d%0a+)WITH(%0d%0a++++type+%3d%27kafka11%27%2c%0d%0a++++bootstrapServers+%3d%27172.16.8.107%3a9092%27%2c%0d%0a++++zookeeperQuorum+%3d%27172.16.8.107%3a2181%2fkafka%27%2c%0d%0a++++offsetReset+%3d%27latest%27%2c%0d%0a++++topic+%3d%27mqTest03%27%2c%0d%0a++++timezone%3d%27Asia%2fShanghai%27%2c%0d%0a++++topicIsPattern+%3d%27false%27%2c%0d%0a++++parallelism+%3d%271%27%0d%0a+)%3b%0d%0a%0d%0a%0d%0aCREATE+TABLE+sideTableA(%0d%0a++++id1+INT%2c%0d%0a++++channel1+varchar%2c%0d%0a++++time_info+varchar%2c%0d%0a++++name1+varchar%2c%0d%0a++++PRIMARY+KEY(channel1)+%2c%0d%0a++++PERIOD+FOR+SYSTEM_TIME%0d%0a+)WITH(%0d%0a++++type+%3d%27mysql%27%2c%0d%0a++++url+%3d%27jdbc%3amysql%3a%2f%2f172.16.8.109%3a3306%2ftest%27%2c%0d%0a++++userName+%3d%27dtstack%27%2c%0d%0a++++password+%3d%27abc123%27%2c%0d%0a++++tableName+%3d%27dimA%27%2c%0d%0a++++parallelism+%3d%271%27%2c%0d%0a++++cache+%3d+%27LRU%27%0d%0a%0d%0a+)%3b%0d%0a%0d%0a+CREATE+TABLE+sideTableB(%0d%0a++++id+INT%2c%0d%0a++++channel1+varchar%2c%0d%0a++++address+varchar%2c%0d%0a++++PRIMARY+KEY(channel1)%2c%0d%0a++++PERIOD+FOR+SYSTEM_TIME%0d%0a+)WITH(%0d%0a++++type+%3d%27mysql%27%2c%0d%0a++++url+%3d%27jdbc%3amysql%3a%2f%2f172.16.8.109%3a3306%2ftest%27%2c%0d%0a++++userName+%3d%27dtstack%27%2c%0d%0a++++password+%3d%27abc123%27%2c%0d%0a++++tableName+%3d%27dimB%27%2c%0d%0a++++parallelism+%3d%271%27%2c%0d%0a++++cache+%3d+%27LRU%27%2c%0d%0a++++asyncTimeout+%3d+%271000000%27%0d%0a+)%3b%0d%0a%0d%0aCREATE+TABLE+MyResult(%0d%0a++++xctime+VARCHAR%2c%0d%0a++++name+VARCHAR%2c%0d%0a++++time_info+VARCHAR%2c%0d%0a++++address+VARCHAR%0d%0a+)WITH(%0d%0a+++++type+%3d%27mysql%27%2c%0d%0a++++url+%3d%27jdbc%3amysql%3a%2f%2f172.16.8.109%3a3306%2ftest%27%2c%0d%0a++++userName+%3d%27dtstack%27%2c%0d%0a++++password+%3d%27abc123%27%2c%0d%0a++++tableName+%3d%27dimC%27%2c%0d%0a++++parallelism+%3d%271%27%2c%0d%0a+)%3b%0d%0a%0d%0ainsert+%0d%0ainto%0d%0a++MyResult%0d%0a++++select%0d%0a++++++++t1.xctime+as+xctime%2c%0d%0a++++++++t1.name+as+name%2c%0d%0a++++++++t3.channel1+as+time_info%2c%0d%0a++++++++t3.address+as+address+++++%0d%0a++++from%0d%0a++++++++MyTable+as+t1%0d%0a++++left+join%0d%0a++++++++--MyTable2+m2+%0d%0a++++++++sideTableA+m2+++++++++++++++++++++++++++%0d%0a++++++++on++t1.channel+%3d+m2.channel1%0d%0a++++left+join+sideTableB+t3%0d%0a++++++++on+t1.channel+%3d+t3.channel1%0d%0a%0d%0a%0d%0a",
                "-localSqlPluginPath",
                "/Users/maqi/code/dtstack/dt-center-flinkStreamSQL/plugins",
                "-remoteSqlPluginPath",
                "/Users/maqi/code/dtstack/dt-center-flinkStreamSQL/plugins",
                "-name","test"};

        Method getExecutionPlan = aClass.getMethod("getExecutionPlan", String[].class);
        String jsonStr = (String) getExecutionPlan.invoke(aClass.newInstance(), (Object)params);

        ObjectNode jsonNodes = OBJECT_MAPPER.readValue(jsonStr, ObjectNode.class);
        Assert.assertEquals(jsonNodes.get(GetPlan.STATUS_KEY).asLong(), GetPlan.SUCCESS.longValue());
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
