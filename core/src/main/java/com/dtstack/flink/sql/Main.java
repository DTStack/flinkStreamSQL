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

import com.dtstack.flink.sql.config.CalciteConfig;
import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.enums.ClusterMode;
import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.environment.MyLocalStreamEnvironment;
import com.dtstack.flink.sql.environment.StreamEnvConfigManager;
import com.dtstack.flink.sql.exec.FlinkSQLExec;
import com.dtstack.flink.sql.option.OptionParser;
import com.dtstack.flink.sql.parser.CreateFuncParser;
import com.dtstack.flink.sql.parser.CreateTmpTableParser;
import com.dtstack.flink.sql.parser.InsertSqlParser;
import com.dtstack.flink.sql.parser.SqlParser;
import com.dtstack.flink.sql.parser.SqlTree;
import com.dtstack.flink.sql.side.SideSqlExec;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.table.SourceTableInfo;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;
import com.dtstack.flink.sql.sink.StreamSinkFactory;
import com.dtstack.flink.sql.source.StreamSourceFactory;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.watermarker.WaterMarkerAssigner;
import com.dtstack.flink.sql.function.FunctionManager;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.Charsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import com.dtstack.flink.sql.option.Options;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

/**
 * Date: 2018/6/26
 * Company: www.dtstack.com
 * @author xuchao
 */

public class Main {

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";

    private static final ObjectMapper objMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws Exception {

        OptionParser optionParser = new OptionParser(args);
        Options options = optionParser.getOptions();
        String sql = options.getSql();
        String name = options.getName();
        String addJarListStr = options.getAddjar();
        String localSqlPluginPath = options.getLocalSqlPluginPath();
        String remoteSqlPluginPath = options.getRemoteSqlPluginPath();
        String pluginLoadMode = options.getPluginLoadMode();
        String deployMode = options.getMode();
        String confProp = options.getConfProp();
        sql = URLDecoder.decode(sql, Charsets.UTF_8.name());
        SqlParser.setLocalSqlPluginRoot(localSqlPluginPath);
        setLogLevel(options.getLogLevel());

        List<String> addJarFileList = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(addJarListStr)) {
            addJarListStr = URLDecoder.decode(addJarListStr, Charsets.UTF_8.name());
            addJarFileList = objMapper.readValue(addJarListStr, List.class);
        }

        confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);
        StreamExecutionEnvironment env = getStreamExeEnv(confProperties, deployMode);
        StreamTableEnvironment tableEnv =  StreamTableEnvironment.getTableEnvironment(env);
        StreamQueryConfig streamQueryConfig = StreamEnvConfigManager.getStreamQueryConfig(tableEnv, confProperties);

        List<URL> jarURList = Lists.newArrayList();
        SqlTree sqlTree = SqlParser.parseSql(sql);

        //Get External jar to load
        for (String addJarPath : addJarFileList) {
            File tmpFile = new File(addJarPath);
            jarURList.add(tmpFile.toURI().toURL());
        }

        Map<String, SideTableInfo> sideTableMap = Maps.newHashMap();
        Map<String, Table> registerTableCache = Maps.newHashMap();

        //register udf
        registerUserDefinedFunction(sqlTree, jarURList, tableEnv);
        //register table schema
        Set<URL> classPathSets = registerTable(sqlTree, env, tableEnv, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode, sideTableMap, registerTableCache);
        // cache classPathSets
        registerPluginUrlToCachedFile(env, classPathSets);

        sqlTranslation(localSqlPluginPath, tableEnv, sqlTree, sideTableMap, registerTableCache, streamQueryConfig);

        if (env instanceof MyLocalStreamEnvironment) {
            ((MyLocalStreamEnvironment) env).setClasspaths(ClassLoaderManager.getClassPath());
        }

        env.execute(name);
    }

    private static void sqlTranslation(String localSqlPluginPath, StreamTableEnvironment tableEnv,SqlTree sqlTree,Map<String, SideTableInfo> sideTableMap,Map<String, Table> registerTableCache, StreamQueryConfig queryConfig) throws Exception {
        SideSqlExec sideSqlExec = new SideSqlExec();
        sideSqlExec.setLocalSqlPluginPath(localSqlPluginPath);
        for (CreateTmpTableParser.SqlParserResult result : sqlTree.getTmpSqlList()) {
            sideSqlExec.registerTmpTable(result, sideTableMap, tableEnv, registerTableCache);
        }

        for (InsertSqlParser.SqlParseResult result : sqlTree.getExecSqlList()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("exe-sql:\n" + result.getExecSql());
            }
            boolean isSide = false;
            for (String tableName : result.getTargetTableList()) {
                if (sqlTree.getTmpTableMap().containsKey(tableName)) {
                    CreateTmpTableParser.SqlParserResult tmp = sqlTree.getTmpTableMap().get(tableName);
                    String realSql = DtStringUtil.replaceIgnoreQuota(result.getExecSql(), "`", "");

                    SqlNode sqlNode = org.apache.calcite.sql.parser.SqlParser.create(realSql, CalciteConfig.MYSQL_LEX_CONFIG).parseStmt();
                    String tmpSql = ((SqlInsert) sqlNode).getSource().toString();
                    tmp.setExecSql(tmpSql);
                    sideSqlExec.registerTmpTable(tmp, sideTableMap, tableEnv, registerTableCache);
                } else {
                    for (String sourceTable : result.getSourceTableList()) {
                        if (sideTableMap.containsKey(sourceTable)) {
                            isSide = true;
                            break;
                        }
                    }
                    if (isSide) {
                        //sql-dimensional table contains the dimension table of execution
                        sideSqlExec.exec(result.getExecSql(), sideTableMap, tableEnv, registerTableCache, queryConfig);
                    }else{
                        FlinkSQLExec.sqlUpdate(tableEnv, result.getExecSql(), queryConfig);
                        if(LOG.isInfoEnabled()){
                            LOG.info("exec sql: " + result.getExecSql());
                        }
                    }
                }
            }
        }


    }


    private static void registerUserDefinedFunction(SqlTree sqlTree, List<URL> jarURList, TableEnvironment tableEnv)
            throws IllegalAccessException, InvocationTargetException {
        // udf和tableEnv须由同一个类加载器加载
        ClassLoader levelClassLoader = tableEnv.getClass().getClassLoader();
        URLClassLoader classLoader = null;
        List<CreateFuncParser.SqlParserResult> funcList = sqlTree.getFunctionList();
        for (CreateFuncParser.SqlParserResult funcInfo : funcList) {
            //classloader
            if (classLoader == null) {
                classLoader = ClassLoaderManager.loadExtraJar(jarURList, (URLClassLoader) levelClassLoader);
            }
            FunctionManager.registerUDF(funcInfo.getType(), funcInfo.getClassName(), funcInfo.getName(), tableEnv, classLoader);
        }
    }

    /**
     *    向Flink注册源表和结果表，返回执行时插件包的全路径
     * @param sqlTree
     * @param env
     * @param tableEnv
     * @param localSqlPluginPath
     * @param remoteSqlPluginPath
     * @param pluginLoadMode   插件加载模式 classpath or shipfile
     * @param sideTableMap
     * @param registerTableCache
     * @return
     * @throws Exception
     */
    private static Set<URL> registerTable(SqlTree sqlTree, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String localSqlPluginPath,
                                          String remoteSqlPluginPath, String pluginLoadMode, Map<String, SideTableInfo> sideTableMap, Map<String, Table> registerTableCache) throws Exception {
        Set<URL> pluginClassPatshSets = Sets.newHashSet();
        WaterMarkerAssigner waterMarkerAssigner = new WaterMarkerAssigner();
        for (TableInfo tableInfo : sqlTree.getTableInfoMap().values()) {

            if (tableInfo instanceof SourceTableInfo) {

                SourceTableInfo sourceTableInfo = (SourceTableInfo) tableInfo;
                Table table = StreamSourceFactory.getStreamSource(sourceTableInfo, env, tableEnv, localSqlPluginPath);
                tableEnv.registerTable(sourceTableInfo.getAdaptName(), table);
                //Note --- parameter conversion function can not be used inside a function of the type of polymerization
                //Create table in which the function is arranged only need adaptation sql
                String adaptSql = sourceTableInfo.getAdaptSelectSql();
                Table adaptTable = adaptSql == null ? table : tableEnv.sqlQuery(adaptSql);

                RowTypeInfo typeInfo = new RowTypeInfo(adaptTable.getSchema().getTypes(), adaptTable.getSchema().getColumnNames());
                DataStream adaptStream = tableEnv.toRetractStream(adaptTable, typeInfo)
                        .map((Tuple2<Boolean, Row> f0) -> { return f0.f1; })
                        .returns(typeInfo);

                String fields = String.join(",", typeInfo.getFieldNames());

                if (waterMarkerAssigner.checkNeedAssignWaterMarker(sourceTableInfo)) {
                    adaptStream = waterMarkerAssigner.assignWaterMarker(adaptStream, typeInfo, sourceTableInfo);
                    fields += ",ROWTIME.ROWTIME";
                } else {
                    fields += ",PROCTIME.PROCTIME";
                }

                Table regTable = tableEnv.fromDataStream(adaptStream, fields);
                tableEnv.registerTable(tableInfo.getName(), regTable);
                if (LOG.isInfoEnabled()) {
                    LOG.info("registe table {} success.", tableInfo.getName());
                }
                registerTableCache.put(tableInfo.getName(), regTable);

                URL sourceTablePathUrl = PluginUtil.buildSourceAndSinkPathByLoadMode(tableInfo.getType(), SourceTableInfo.SOURCE_SUFFIX, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode);
                pluginClassPatshSets.add(sourceTablePathUrl);
            } else if (tableInfo instanceof TargetTableInfo) {

                TableSink tableSink = StreamSinkFactory.getTableSink((TargetTableInfo) tableInfo, localSqlPluginPath);
                TypeInformation[] flinkTypes = FunctionManager.transformTypes(tableInfo.getFieldClasses());
                tableEnv.registerTableSink(tableInfo.getName(), tableInfo.getFields(), flinkTypes, tableSink);

                URL sinkTablePathUrl = PluginUtil.buildSourceAndSinkPathByLoadMode(tableInfo.getType(), TargetTableInfo.TARGET_SUFFIX, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode);
                pluginClassPatshSets.add(sinkTablePathUrl);
            } else if (tableInfo instanceof SideTableInfo) {
                String sideOperator = ECacheType.ALL.name().equals(((SideTableInfo) tableInfo).getCacheType()) ? "all" : "async";
                sideTableMap.put(tableInfo.getName(), (SideTableInfo) tableInfo);

                URL sideTablePathUrl = PluginUtil.buildSidePathByLoadMode(tableInfo.getType(), sideOperator, SideTableInfo.TARGET_SUFFIX, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode);
                pluginClassPatshSets.add(sideTablePathUrl);
            } else {
                throw new RuntimeException("not support table type:" + tableInfo.getType());
            }
        }
        return pluginClassPatshSets;
    }

    /**
     *   perjob模式将job依赖的插件包路径存储到cacheFile，在外围将插件包路径传递给jobgraph
     * @param env
     * @param classPathSet
     */
    private static void registerPluginUrlToCachedFile(StreamExecutionEnvironment env, Set<URL> classPathSet) {
        int i = 0;
        for (URL url : classPathSet) {
            String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
            env.registerCachedFile(url.getPath(), classFileName, true);
            i++;
        }
    }

    private static StreamExecutionEnvironment getStreamExeEnv(Properties confProperties, String deployMode) throws Exception {
        StreamExecutionEnvironment env = !ClusterMode.local.name().equals(deployMode) ?
                StreamExecutionEnvironment.getExecutionEnvironment() :
                new MyLocalStreamEnvironment();

        StreamEnvConfigManager.streamExecutionEnvironmentConfig(env, confProperties);
        return env;
    }
    private static void setLogLevel(String level){
        LoggerContext loggerContext= (LoggerContext) LoggerFactory.getILoggerFactory();
        //设置全局日志级别
        ch.qos.logback.classic.Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.toLevel(level, Level.INFO));
    }
}
