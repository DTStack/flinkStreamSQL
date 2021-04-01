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

package com.dtstack.flink.sql.exec;

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.dirtyManager.manager.DirtyDataManager;
import com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys;
import com.dtstack.flink.sql.enums.ClusterMode;
import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.enums.EPluginLoadMode;
import com.dtstack.flink.sql.environment.MyLocalStreamEnvironment;
import com.dtstack.flink.sql.environment.StreamEnvConfigManager;
import com.dtstack.flink.sql.function.FunctionManager;
import com.dtstack.flink.sql.option.OptionParser;
import com.dtstack.flink.sql.option.Options;
import com.dtstack.flink.sql.parser.CreateFuncParser;
import com.dtstack.flink.sql.parser.CreateTmpTableParser;
import com.dtstack.flink.sql.parser.FlinkPlanner;
import com.dtstack.flink.sql.parser.InsertSqlParser;
import com.dtstack.flink.sql.parser.SqlParser;
import com.dtstack.flink.sql.parser.SqlTree;
import com.dtstack.flink.sql.resource.ResourceCheck;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.SideSqlExec;
import com.dtstack.flink.sql.sink.StreamSinkFactory;
import com.dtstack.flink.sql.source.StreamSourceFactory;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.util.PluginUtil;
import com.dtstack.flink.sql.util.TypeInfoDataTypeConverter;
import com.dtstack.flink.sql.watermarker.WaterMarkerAssigner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Stream;

/**
 * 任务执行时的流程方法
 * Date: 2020/2/17
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class ExecuteProcessHelper {

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";
    private static final Logger LOG = LoggerFactory.getLogger(ExecuteProcessHelper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String TIME_ZONE = "timezone";
    private static final String PLUGIN_PATH_STR = "pluginPath";
    private static final String PLUGIN_LOAD_STR = "pluginLoadMode";

    public static FlinkPlanner flinkPlanner = new FlinkPlanner();

    @SuppressWarnings("unchecked")
    public static ParamsInfo parseParams(String[] args) throws Exception {
        LOG.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));
        LOG.info("-------------------------------------------");

        OptionParser optionParser = new OptionParser(args);
        Options options = optionParser.getOptions();

        String sql = URLDecoder.decode(options.getSql(), Charsets.UTF_8.name());
        String name = options.getName();
        String localSqlPluginPath = options.getLocalSqlPluginPath();
        String remoteSqlPluginPath = options.getRemoteSqlPluginPath();
        String pluginLoadMode = options.getPluginLoadMode();
        String deployMode = options.getMode();
        String dirtyStr = options.getDirtyProperties();

        Preconditions.checkArgument(checkRemoteSqlPluginPath(remoteSqlPluginPath, deployMode, pluginLoadMode),
                "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");
        String confProp = URLDecoder.decode(options.getConfProp(), Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);
        Map<String, Object> dirtyProperties = (Map<String, Object>) PluginUtil.jsonStrToObject(Objects.isNull(dirtyStr) ?
                DirtyDataManager.buildDefaultDirty() : dirtyStr, Map.class);

        if (Objects.isNull(dirtyProperties.get(PLUGIN_LOAD_STR))) {
            dirtyProperties.put(PLUGIN_LOAD_STR, pluginLoadMode);
        }

        if (!pluginLoadMode.equalsIgnoreCase(EPluginLoadMode.LOCALTEST.name()) && Objects.isNull(dirtyProperties.get(PLUGIN_PATH_STR))) {
            dirtyProperties.put(PLUGIN_PATH_STR, localSqlPluginPath);
        }

        List<URL> jarUrlList = getExternalJarUrls(options.getAddjar());

        return ParamsInfo.builder()
                .setSql(sql)
                .setName(name)
                .setLocalSqlPluginPath(localSqlPluginPath)
                .setRemoteSqlPluginPath(remoteSqlPluginPath)
                .setPluginLoadMode(pluginLoadMode)
                .setDeployMode(deployMode)
                .setConfProp(confProperties)
                .setJarUrlList(jarUrlList)
                .setDirtyProperties(dirtyProperties)
                .build();

    }

    /**
     * 非local模式或者shipfile部署模式，remoteSqlPluginPath必填
     *
     * @param remoteSqlPluginPath
     * @param deployMode
     * @param pluginLoadMode
     * @return
     */
    public static boolean checkRemoteSqlPluginPath(String remoteSqlPluginPath, String deployMode, String pluginLoadMode) {
        if (StringUtils.isEmpty(remoteSqlPluginPath)) {
            return StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.SHIPFILE.name())
                    || StringUtils.equalsIgnoreCase(deployMode, ClusterMode.local.name());
        }
        return true;
    }


    public static StreamExecutionEnvironment getStreamExecution(ParamsInfo paramsInfo) throws Exception {
        StreamExecutionEnvironment env = ExecuteProcessHelper.getStreamExeEnv(paramsInfo.getConfProp(), paramsInfo.getDeployMode());
        StreamTableEnvironment tableEnv = getStreamTableEnv(env, paramsInfo.getConfProp());

        ResourceCheck.NEED_CHECK = Boolean.parseBoolean(paramsInfo.getConfProp().getProperty(ResourceCheck.CHECK_STR, "false"));

        SqlParser.setLocalSqlPluginRoot(paramsInfo.getLocalSqlPluginPath());
        SqlTree sqlTree = SqlParser.parseSql(paramsInfo.getSql(), paramsInfo.getPluginLoadMode());

        Map<String, AbstractSideTableInfo> sideTableMap = Maps.newHashMap();
        Map<String, Table> registerTableCache = Maps.newHashMap();

        //register udf
        ExecuteProcessHelper.registerUserDefinedFunction(sqlTree, paramsInfo.getJarUrlList(), tableEnv, paramsInfo.isGetPlan());
        //register table schema
        Set<URL> classPathSets = ExecuteProcessHelper.registerTable(
                sqlTree
                , env
                , tableEnv
                , paramsInfo.getLocalSqlPluginPath()
                , paramsInfo.getRemoteSqlPluginPath()
                , paramsInfo.getPluginLoadMode()
                , paramsInfo.getDirtyProperties()
                , sideTableMap
                , registerTableCache);
        // cache classPathSets
        ExecuteProcessHelper.registerPluginUrlToCachedFile(env, classPathSets);

        ExecuteProcessHelper.sqlTranslation(
                paramsInfo.getLocalSqlPluginPath(),
                paramsInfo.getPluginLoadMode(),
                tableEnv,
                sqlTree,
                sideTableMap,
                registerTableCache);

        if (env instanceof MyLocalStreamEnvironment) {
            ((MyLocalStreamEnvironment) env).setClasspaths(ClassLoaderManager.getClassPath());
        }
        return env;
    }

    @SuppressWarnings("unchecked")
    public static List<URL> getExternalJarUrls(String addJarListStr) throws java.io.IOException {
        List<URL> jarUrlList = Lists.newArrayList();
        if (Strings.isNullOrEmpty(addJarListStr)) {
            return jarUrlList;
        }

        List<String> addJarFileList = OBJECT_MAPPER.readValue(URLDecoder.decode(addJarListStr, Charsets.UTF_8.name()), List.class);
        //Get External jar to load
        for (String addJarPath : addJarFileList) {
            jarUrlList.add(new File(addJarPath).toURI().toURL());
        }
        return jarUrlList;
    }

    private static void sqlTranslation(String localSqlPluginPath,
                                       String pluginLoadMode,
                                       StreamTableEnvironment tableEnv,
                                       SqlTree sqlTree,
                                       Map<String, AbstractSideTableInfo> sideTableMap,
                                       Map<String, Table> registerTableCache) throws Exception {

        SideSqlExec sideSqlExec = new SideSqlExec();
        sideSqlExec.setLocalSqlPluginPath(localSqlPluginPath);
        sideSqlExec.setPluginLoadMode(pluginLoadMode);

        int scope = 0;
        for (CreateTmpTableParser.SqlParserResult result : sqlTree.getTmpSqlList()) {
            sideSqlExec.exec(result.getExecSql(), sideTableMap, tableEnv, registerTableCache, result, scope + "");
            scope++;
        }

        final Map<String, AbstractSideTableInfo> tmpTableMap = new HashMap<>();
        for (InsertSqlParser.SqlParseResult result : sqlTree.getExecSqlList()) {
            // prevent current sql use last sql's sideTableInfo
            sideTableMap.forEach((s, abstractSideTableInfo) -> tmpTableMap.put(s, SerializationUtils.clone(abstractSideTableInfo)));

            if (LOG.isInfoEnabled()) {
                LOG.info("exe-sql:\n" + result.getExecSql());
            }
            boolean isSide = false;
            for (String tableName : result.getTargetTableList()) {
                if (sqlTree.getTmpTableMap().containsKey(tableName)) {
                    CreateTmpTableParser.SqlParserResult tmp = sqlTree.getTmpTableMap().get(tableName);
                    String realSql = DtStringUtil.replaceIgnoreQuota(result.getExecSql(), "`", "");

                    SqlNode sqlNode = flinkPlanner.getParser().parse(realSql);
                    String tmpSql = ((SqlInsert) sqlNode).getSource().toString();
                    tmp.setExecSql(tmpSql);
                    sideSqlExec.exec(tmp.getExecSql(), tmpTableMap, tableEnv, registerTableCache, tmp, scope + "");
                } else {
                    for (String sourceTable : result.getSourceTableList()) {
                        if (tmpTableMap.containsKey(sourceTable)) {
                            isSide = true;
                            break;
                        }
                    }
                    if (isSide) {
                        //sql-dimensional table contains the dimension table of execution
                        sideSqlExec.exec(result.getExecSql(), tmpTableMap, tableEnv, registerTableCache, null, String.valueOf(scope));
                    } else {
                        LOG.info("----------exec sql without dimension join-----------");
                        LOG.info("----------real sql exec is--------------------------\n{}", result.getExecSql());

                        FlinkSQLExec.sqlInsert(tableEnv, result.getExecSql(), SideSqlExec.getDimTableNewTable().keySet() );
                        if (LOG.isInfoEnabled()) {
                            LOG.info("exec sql: " + result.getExecSql());
                        }
                    }
                }

                scope++;
            }
            tmpTableMap.clear();
        }
    }

    public static void registerUserDefinedFunction(SqlTree sqlTree, List<URL> jarUrlList, TableEnvironment tableEnv, boolean getPlan)
            throws IllegalAccessException, InvocationTargetException {
        // udf和tableEnv须由同一个类加载器加载
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        URLClassLoader classLoader = ClassLoaderManager.loadExtraJar(jarUrlList, (URLClassLoader) currentClassLoader);
        List<CreateFuncParser.SqlParserResult> funcList = sqlTree.getFunctionList();
        for (CreateFuncParser.SqlParserResult funcInfo : funcList) {
            FunctionManager.registerUDF(funcInfo.getType(), funcInfo.getClassName(), funcInfo.getName(), tableEnv, classLoader);
        }
    }

    /**
     * 向Flink注册源表和结果表，返回执行时插件包的全路径
     *
     * @param sqlTree
     * @param env
     * @param tableEnv
     * @param localSqlPluginPath
     * @param remoteSqlPluginPath
     * @param pluginLoadMode      插件加载模式 classpath or shipfile
     * @param sideTableMap
     * @param registerTableCache
     * @return
     * @throws Exception
     */
    public static Set<URL> registerTable(
            SqlTree sqlTree
            , StreamExecutionEnvironment env
            , StreamTableEnvironment tableEnv
            , String localSqlPluginPath
            , String remoteSqlPluginPath
            , String pluginLoadMode
            , Map<String, Object> dirtyProperties
            , Map<String, AbstractSideTableInfo> sideTableMap
            , Map<String, Table> registerTableCache
    ) throws Exception {
        Set<URL> pluginClassPathSets = Sets.newHashSet();
        WaterMarkerAssigner waterMarkerAssigner = new WaterMarkerAssigner();
        for (AbstractTableInfo tableInfo : sqlTree.getTableInfoMap().values()) {

            // 配置dirty manager
            tableInfo.setDirtyProperties(dirtyProperties);

            if (tableInfo instanceof AbstractSourceTableInfo) {

                AbstractSourceTableInfo sourceTableInfo = (AbstractSourceTableInfo) tableInfo;
                Table table = StreamSourceFactory.getStreamSource(sourceTableInfo, env, tableEnv, localSqlPluginPath, pluginLoadMode);
                tableEnv.registerTable(sourceTableInfo.getAdaptName(), table);
                //Note --- parameter conversion function can not be used inside a function of the type of polymerization
                //Create table in which the function is arranged only need adaptation sql
                String adaptSql = sourceTableInfo.getAdaptSelectSql();
                Table adaptTable = adaptSql == null ? table : tableEnv.sqlQuery(adaptSql);

                RowTypeInfo typeInfo = new RowTypeInfo(fromDataTypeToLegacyInfo(adaptTable.getSchema().getFieldDataTypes()), adaptTable.getSchema().getFieldNames());
                DataStream adaptStream = tableEnv.toAppendStream(adaptTable, typeInfo);

                String fields = String.join(",", typeInfo.getFieldNames());

                if (waterMarkerAssigner.checkNeedAssignWaterMarker(sourceTableInfo)) {
                    adaptStream = waterMarkerAssigner.assignWaterMarker(adaptStream, typeInfo, sourceTableInfo);
                    fields += ",ROWTIME.ROWTIME";
                } else {
                    fields += ",PROCTIME.PROCTIME";
                }

                Table regTable = tableEnv.fromDataStream(adaptStream, fields);
                tableEnv.createTemporaryView(tableInfo.getName(), regTable);
                if (LOG.isInfoEnabled()) {
                    LOG.info("registe table {} success.", tableInfo.getName());
                }
                registerTableCache.put(tableInfo.getName(), regTable);

                URL sourceTablePathUrl = PluginUtil.buildSourceAndSinkPathByLoadMode(tableInfo.getType(), AbstractSourceTableInfo.SOURCE_SUFFIX, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode);
                pluginClassPathSets.add(sourceTablePathUrl);
            } else if (tableInfo instanceof AbstractTargetTableInfo) {
                TableSink tableSink = StreamSinkFactory.getTableSink((AbstractTargetTableInfo) tableInfo, localSqlPluginPath, pluginLoadMode);
                // TODO Kafka Sink直接注册，其他的Sink要修复才可以。
                if (tableInfo.getType().startsWith("kafka")) {
                    tableEnv.registerTableSink(tableInfo.getName(), tableSink);
                } else {
                    TypeInformation[] flinkTypes = FunctionManager.transformTypes(tableInfo.getFieldClasses());
                    tableEnv.registerTableSink(tableInfo.getName(), tableInfo.getFields(), flinkTypes, tableSink);
                }

                URL sinkTablePathUrl = PluginUtil.buildSourceAndSinkPathByLoadMode(tableInfo.getType(), AbstractTargetTableInfo.TARGET_SUFFIX, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode);
                pluginClassPathSets.add(sinkTablePathUrl);
            } else if (tableInfo instanceof AbstractSideTableInfo) {
                String sideOperator = ECacheType.ALL.name().equalsIgnoreCase(((AbstractSideTableInfo) tableInfo).getCacheType()) ? "all" : "async";
                sideTableMap.put(tableInfo.getName(), (AbstractSideTableInfo) tableInfo);

                URL sideTablePathUrl = PluginUtil.buildSidePathByLoadMode(tableInfo.getType(), sideOperator, AbstractSideTableInfo.TARGET_SUFFIX, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode);
                pluginClassPathSets.add(sideTablePathUrl);
            } else {
                throw new RuntimeException("not support table type:" + tableInfo.getType());
            }
        }
        if (localSqlPluginPath == null || localSqlPluginPath.isEmpty()) {
            return Sets.newHashSet();
        }
        pluginClassPathSets.add(PluginUtil.buildDirtyPluginUrl(
               String.valueOf(dirtyProperties.get(DirtyKeys.PLUGIN_TYPE_STR)),
               String.valueOf(dirtyProperties.get(DirtyKeys.PLUGIN_PATH_STR)),
               String.valueOf(dirtyProperties.get(DirtyKeys.PLUGIN_LOAD_MODE_STR))
        ));
        return pluginClassPathSets;
    }

    /**
     * perjob模式将job依赖的插件包路径存储到cacheFile，在外围将插件包路径传递给jobgraph
     *
     * @param env
     * @param classPathSet
     */
    public static void registerPluginUrlToCachedFile(StreamExecutionEnvironment env, Set<URL> classPathSet) {
        int i = 0;
        for (URL url : classPathSet) {
            String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
            env.registerCachedFile(url.getPath(), classFileName, true);
            i++;
        }
    }

    public static StreamExecutionEnvironment getStreamExeEnv(Properties confProperties, String deployMode) throws Exception {
        StreamExecutionEnvironment env = !ClusterMode.local.name().equals(deployMode) ?
                StreamExecutionEnvironment.getExecutionEnvironment() :
                new MyLocalStreamEnvironment();

        StreamEnvConfigManager.streamExecutionEnvironmentConfig(env, confProperties);
        return env;
    }


    public static StreamTableEnvironment getStreamTableEnv(StreamExecutionEnvironment env, Properties confProperties) {
        // use blink and streammode
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableConfig tableConfig = new TableConfig();

        timeZoneCheck(confProperties.getProperty(TIME_ZONE, TimeZone.getDefault().getID()));

        tableConfig.setLocalTimeZone(ZoneId.of(confProperties.getProperty(TIME_ZONE, TimeZone.getDefault().getID())));

        StreamTableEnvironment tableEnv = StreamTableEnvironmentImpl.create(env, settings, tableConfig);
        StreamEnvConfigManager.streamTableEnvironmentStateTTLConfig(tableEnv, confProperties);
        StreamEnvConfigManager.streamTableEnvironmentEarlyTriggerConfig(tableEnv, confProperties);
        return tableEnv;
    }

    private static void timeZoneCheck(String timeZone) {
        ArrayList<String> zones = Lists.newArrayList(TimeZone.getAvailableIDs());
        if (!zones.contains(timeZone)) {
            throw new IllegalArgumentException(String.format(" timezone of %s is Incorrect!", timeZone));
        }
    }

    private static TypeInformation<?>[] fromDataTypeToLegacyInfo(DataType[] dataType) {
        return Stream.of(dataType)
                .map(TypeInfoDataTypeConverter::toLegacyTypeInfo)
                .toArray(TypeInformation[]::new);
    }
}