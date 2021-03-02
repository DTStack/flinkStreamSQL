package com.dtstack.flink.sql.classloader;

import com.dtstack.flink.sql.util.PluginUtil;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URL;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ClassLoaderManager.class, PluginUtil.class})
public class ClassLoaderManagerTest {

    @Test
    public void testNewInstance() throws Exception {
        String jarPath = "./test.jar";
        DtClassLoader classLoader = mock(DtClassLoader.class);
        ClassLoaderSupplier classLoaderSupplier = mock(ClassLoaderSupplier.class);
        when(classLoaderSupplier.get(any())).thenReturn(classLoader);


        PowerMockito.mockStatic(PluginUtil.class);
        when(PluginUtil.getPluginJarUrls(anyString())).thenReturn(new URL[1]);
        ClassLoaderManager.newInstance(jarPath, classLoaderSupplier);

    }

    // @Test
    public void testNewInstanceWithList() throws Exception {
        URL jarUrl = new URL("file:./test.jar");
        DtClassLoader classLoader = mock(DtClassLoader.class);
        ClassLoaderSupplier classLoaderSupplier = mock(ClassLoaderSupplier.class);
        when(classLoaderSupplier.get(any())).thenReturn(classLoader);


        PowerMockito.mockStatic(PluginUtil.class);
        when(PluginUtil.getPluginJarUrls(anyString())).thenReturn(new URL[1]);
        ClassLoaderManager.newInstance(Lists.newArrayList(jarUrl), classLoaderSupplier);

    }
}
