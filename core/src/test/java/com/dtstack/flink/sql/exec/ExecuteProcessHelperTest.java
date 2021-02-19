package com.dtstack.flink.sql.exec;

import com.dtstack.flink.sql.enums.ClusterMode;
import com.dtstack.flink.sql.enums.EPluginLoadMode;
import com.dtstack.flink.sql.parser.SqlParser;
import com.dtstack.flink.sql.parser.SqlTree;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.sink.StreamSinkFactory;
import com.dtstack.flink.sql.source.StreamSourceFactory;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.dtstack.flink.sql.util.PluginUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SqlParser.class, PluginUtil.class, StreamSourceFactory.class, StreamSinkFactory.class})
public class ExecuteProcessHelperTest {

    @Test
    public void parseParams() throws Exception {
        String[] sql = new String[]{"-mode", "yarnPer", "-sql", "/Users/maqi/tmp/json/group_tmp4.txt", "-name", "PluginLoadModeTest",
                "-localSqlPluginPath", "/Users/maqi/code/dtstack/dt-center-flinkStreamSQL/plugins",
                "-remoteSqlPluginPath", "/Users/maqi/code/dtstack/dt-center-flinkStreamSQL/plugins",
                "-flinkconf", "/Users/maqi/tmp/flink-1.8.1/conf",
                "-confProp", "{\"sql.checkpoint.cleanup.mode\":\"false\",\"sql.checkpoint.interval\":10000,\"time.characteristic\":\"EventTime\"}",
                "-yarnconf", "/Users/maqi/tmp/hadoop", "-flinkJarPath", "/Users/maqi/tmp/flink-1.8.1/lib", "-queue", "c", "-pluginLoadMode", "shipfile"};

        ExecuteProcessHelper.parseParams(sql);
    }

    @Test
    public void checkRemoteSqlPluginPath(){
        ExecuteProcessHelper.checkRemoteSqlPluginPath(null, EPluginLoadMode.SHIPFILE.name(),  ClusterMode.local.name());

    }

    // @Test
    public void getStreamExecution() throws Exception {
        String[] sql = new String[]{"-mode", "yarnPer", "-sql", "/Users/maqi/tmp/json/group_tmp4.txt", "-name", "PluginLoadModeTest",
                "-localSqlPluginPath", "/Users/maqi/code/dtstack/dt-center-flinkStreamSQL/plugins",
                "-remoteSqlPluginPath", "/Users/maqi/code/dtstack/dt-center-flinkStreamSQL/plugins",
                "-flinkconf", "/Users/maqi/tmp/flink-1.8.1/conf",
                "-confProp", "{\"sql.checkpoint.cleanup.mode\":\"false\",\"sql.checkpoint.interval\":10000,\"time.characteristic\":\"EventTime\"}",
                "-yarnconf", "/Users/maqi/tmp/hadoop", "-flinkJarPath", "/Users/maqi/tmp/flink-1.8.1/lib", "-queue", "c", "-pluginLoadMode", "shipfile"};
        ParamsInfo paramsInfo = ExecuteProcessHelper.parseParams(sql);
        PowerMockito.mockStatic(SqlParser.class);
        SqlTree sqlTree = mock(SqlTree.class);
        when(SqlParser.parseSql(anyString(), anyString())).thenReturn(sqlTree);
        when(sqlTree.getFunctionList()).thenReturn(Lists.newArrayList());
        ExecuteProcessHelper.getStreamExecution(paramsInfo);

    }

    @Test
    public void getExternalJarUrls() throws IOException {
        ExecuteProcessHelper.getExternalJarUrls("[\"/test\"]");
    }

    // @Test
    public void registerTable() throws Exception {
        SqlTree sqlTree = mock(SqlTree.class);
        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);
        StreamTableEnvironment tableEnv = mock(StreamTableEnvironment.class);
        Table table = mock(Table.class);
        TableSchema tableSchema = mock(TableSchema.class);
        DataStream dataStream = mock(DataStream.class);
        when(tableEnv.sqlQuery(anyString())).thenReturn(table);
        when(tableEnv.fromDataStream(anyObject(), anyObject())).thenReturn(table);

        when(tableEnv.toRetractStream(any(Table.class), any(RowTypeInfo.class))).thenReturn(dataStream);
        SingleOutputStreamOperator singleOutputStreamOperator = mock(SingleOutputStreamOperator.class);
        when(dataStream.map(anyObject())).thenReturn(singleOutputStreamOperator);
        when(singleOutputStreamOperator.returns(any(RowTypeInfo.class))).thenReturn(singleOutputStreamOperator);


        when(table.getSchema()).thenReturn(tableSchema);
        when(tableSchema.getFieldTypes()).thenReturn(new TypeInformation[]{Types.STRING});
        when(tableSchema.getFieldNames()).thenReturn(new String[]{"a"});
        String localSqlPluginPath = "/";
        String remoteSqlPluginPath = "/";
        String pluginLoadMode = "shipfile";
        Map<String, AbstractSideTableInfo> sideTableMap = Maps.newHashMap();
        Map<String, Table> registerTableCache = Maps.newHashMap();
        PowerMockito.mockStatic(PluginUtil.class);

        PowerMockito.mockStatic(StreamSourceFactory.class);
        when(StreamSourceFactory.getStreamSource(anyObject(), anyObject(), anyObject(), anyString(),anyString())).thenReturn(table);

        TableSink tableSink = mock(TableSink.class);
        PowerMockito.mockStatic(StreamSinkFactory.class);
        when(StreamSinkFactory.getTableSink(anyObject(), anyString(), anyString())).thenReturn(tableSink);


        Map tableMap = Maps.newHashMap();

        AbstractSourceTableInfo sourceTableInfo = mock(AbstractSourceTableInfo.class);
        when(sourceTableInfo.getAdaptName()).thenReturn("a");
        when(sourceTableInfo.getAdaptSelectSql()).thenReturn("s");
        when(sourceTableInfo.getType()).thenReturn("kafka");

        when(PluginUtil.buildSourceAndSinkPathByLoadMode(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(new URL("file://a"));

        AbstractSideTableInfo sideTableInfo = mock(AbstractSideTableInfo.class);
        when(sideTableInfo.getCacheType()).thenReturn("all");
        when(sideTableInfo.getName()).thenReturn("sideTable");
        when(sideTableInfo.getType()).thenReturn("redis");
        when(PluginUtil.buildSidePathByLoadMode(anyString(), anyString(), anyString(), anyString(), anyString(),anyString())).thenReturn(new URL("file://a"));

        AbstractTargetTableInfo targetTableInfo = mock(AbstractTargetTableInfo.class);
        when(targetTableInfo.getName()).thenReturn("sinkTable");
        when(targetTableInfo.getType()).thenReturn("kafka");
        when(targetTableInfo.getFieldClasses()).thenReturn(new Class[]{String.class});
        when(targetTableInfo.getFields()).thenReturn(new String[]{"a"});


        tableMap.put("source", sourceTableInfo);
        tableMap.put("side", sideTableInfo);
        tableMap.put("target", targetTableInfo);
        when(sqlTree.getTableInfoMap()).thenReturn(tableMap);

        ExecuteProcessHelper.registerTable(sqlTree, env, tableEnv, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode, sideTableMap, registerTableCache);
    }

    @Test
    public void registerPluginUrlToCachedFile() throws Exception {
        StreamExecutionEnvironment executionEnvironment =  ExecuteProcessHelper.getStreamExeEnv(new Properties(), "local");
        Set<URL> classPathSet = Sets.newHashSet();
        classPathSet.add(new URL("file://"));
        ExecuteProcessHelper.registerPluginUrlToCachedFile(executionEnvironment, classPathSet);
    }

    @Test
    public void getStreamExeEnv() throws Exception {
        ExecuteProcessHelper.getStreamExeEnv(new Properties(), "local");
    }



}
