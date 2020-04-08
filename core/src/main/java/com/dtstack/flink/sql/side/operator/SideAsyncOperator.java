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

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
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

    private static final String OPERATOR_TYPE = "Async";

    private static final String ORDERED = "ordered";


    private static AsyncReqRow loadAsyncReq(String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo,
                                            JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) throws Exception {
        String pathOfType = String.format(PATH_FORMAT, sideType);
        String pluginJarPath = PluginUtil.getJarFileDirPath(pathOfType, sqlRootDir);
        String className = PluginUtil.getSqlSideClassName(sideType, "side", OPERATOR_TYPE);
        return ClassLoaderManager.newInstance(pluginJarPath, (cl) ->
                cl.loadClass(className).asSubclass(AsyncReqRow.class)
                        .getConstructor(RowTypeInfo.class, JoinInfo.class, List.class, SideTableInfo.class)
                        .newInstance(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    public static DataStream getSideJoinDataStream(DataStream inputStream, String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo,  JoinInfo joinInfo,
                                            List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) throws Exception {
        AsyncReqRow asyncDbReq = loadAsyncReq(sideType, sqlRootDir, rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);

        //TODO How much should be set for the degree of parallelism? Timeout? capacity settings?
        if (ORDERED.equals(sideTableInfo.getCacheMode())){
            return AsyncDataStream.orderedWait(inputStream, asyncDbReq, sideTableInfo.getAsyncTimeout(), TimeUnit.MILLISECONDS, sideTableInfo.getAsyncCapacity())
                    .setParallelism(sideTableInfo.getParallelism());
        }else {
            return AsyncDataStream.unorderedWait(inputStream, asyncDbReq, sideTableInfo.getAsyncTimeout(), TimeUnit.MILLISECONDS, sideTableInfo.getAsyncCapacity())
                    .setParallelism(sideTableInfo.getParallelism());
        }

    }
}
