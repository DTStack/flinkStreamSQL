package com.dtstack.flink.sql.source;

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URL;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamSourceFactory.class, PluginUtil.class, DtStringUtil.class})
public class StreamSourceFactoryTest {

    @Test(expected = Exception.class)
    public void testGetSqlParser() throws Exception {
        PowerMockito.mockStatic(PluginUtil.class);
        when(PluginUtil.getJarFileDirPath(anyString(), anyString(), anyString())).thenReturn("./test.jar");

        PowerMockito.mockStatic(DtStringUtil.class);
        when(DtStringUtil.getPluginTypeWithoutVersion(anyString())).thenReturn("10");

        when(PluginUtil.getSqlParserClassName(anyString(), anyString())).thenReturn("test");

        StreamSourceFactory.getSqlParser("source", "./test.jar", "");
    }

    @Test(expected = Exception.class)
    public void testGetTableSink() throws Exception {
        AbstractSourceTableInfo sourceTableInfo = mock(AbstractSourceTableInfo.class);
        PowerMockito.mockStatic(PluginUtil.class);
        when(PluginUtil.getJarFileDirPath(anyString(), anyString(), anyString())).thenReturn("./test.jar");

        PowerMockito.mockStatic(DtStringUtil.class);
        when(DtStringUtil.getPluginTypeWithoutVersion(anyString())).thenReturn("10");

        when(PluginUtil.getGenerClassName(anyString(), anyString())).thenReturn("test");

        when(PluginUtil.getPluginJarUrls(anyString())).thenReturn(new URL[1]);

        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);
        StreamTableEnvironment tableEnv = mock(StreamTableEnvironment.class);

        StreamSourceFactory.getStreamSource(sourceTableInfo, env, tableEnv, "./test.jar", "");
    }

}
