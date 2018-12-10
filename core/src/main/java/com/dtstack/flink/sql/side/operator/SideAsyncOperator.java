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


package com.dtstack.flink.sql.side.operator;

import com.dtstack.flink.sql.classloader.DtClassLoader;
import com.dtstack.flink.sql.side.AsyncReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * fill data with lru cache
 * get data from  External data source with async operator
 * Date: 2018/9/18
 * Company: www.dtstack.com
 * @author xuchao
 */

public class SideAsyncOperator {

    private static final String PATH_FORMAT = "%sasyncside";

    //TODO need to set by create table task
    private static int asyncCapacity = 100;

    private static AsyncReqRow loadAsyncReq(String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo,
                                            JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String pathOfType = String.format(PATH_FORMAT, sideType);
        String pluginJarPath = PluginUtil.getJarFileDirPath(pathOfType, sqlRootDir);
        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        String className = PluginUtil.getSqlSideClassName(sideType, "side", "Async");
        return dtClassLoader.loadClass(className).asSubclass(AsyncReqRow.class)
                .getConstructor(RowTypeInfo.class, JoinInfo.class, List.class, SideTableInfo.class).newInstance(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    public static DataStream getSideJoinDataStream(DataStream inputStream, String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo,  JoinInfo joinInfo,
                                            List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) throws Exception {
        AsyncReqRow asyncDbReq = loadAsyncReq(sideType, sqlRootDir, rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        //TODO How much should be set for the degree of parallelism? Timeout? capacity settings?
        return AsyncDataStream.orderedWait(inputStream, asyncDbReq, 10000, TimeUnit.MILLISECONDS, asyncCapacity)
                .setParallelism(sideTableInfo.getParallelism());
    }
}
