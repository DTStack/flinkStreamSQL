package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PluginUtil.class, ClassLoaderManager.class, AsyncDataStream.class})
public class StreamSideFactoryTest {

    @Test
    public void getSqlParser() throws Exception {

        PowerMockito.mockStatic(PluginUtil.class);
        when(PluginUtil.getJarFileDirPath(anyString(), anyString(), anyString())).thenReturn("/");
        when(PluginUtil.getSqlSideClassName(anyString(), anyString(), anyString())).thenReturn("clazz");

        PowerMockito.mockStatic(ClassLoaderManager.class);
        AbstractTableParser tableParser = mock(AbstractTableParser.class);
        when(ClassLoaderManager.newInstance(anyString(), anyObject())).thenReturn(tableParser);
        StreamSideFactory.getSqlParser("kafka", "/", "all", "");
    }
}
