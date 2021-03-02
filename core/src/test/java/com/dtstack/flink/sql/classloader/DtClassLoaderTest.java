package com.dtstack.flink.sql.classloader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URL;

@RunWith(PowerMockRunner.class)
public class DtClassLoaderTest {

    @Test(expected = ClassNotFoundException.class)
    public void testLoadClass() throws Exception {
        URL[] urls = new URL[1];
        urls[0] = new URL("file:./test.jar");
        DtClassLoader dtClassLoader = new DtClassLoader(urls);
        dtClassLoader.loadClass("test");
    }

    @Test
    public void testGetResource() throws Exception {
        URL[] urls = new URL[1];
        urls[0] = new URL("file:./test.jar");
        DtClassLoader dtClassLoader = new DtClassLoader(urls, Thread.currentThread().getContextClassLoader());
        dtClassLoader.getResource("test");
    }

    @Test
    public void testAddURL() throws Exception {
        URL[] urls = new URL[1];
        urls[0] = new URL("file:./test.jar");
        DtClassLoader dtClassLoader = new DtClassLoader(urls);
        dtClassLoader.addURL(urls[0]);
    }

    @Test
    public void testGetResources() throws Exception {
        URL[] urls = new URL[1];
        urls[0] = new URL("file:./test.jar");
        DtClassLoader dtClassLoader = new DtClassLoader(urls, Thread.currentThread().getContextClassLoader());
        dtClassLoader.getResources("test");
    }

    @Test
    public void testFindResources() throws Exception {
        URL[] urls = new URL[1];
        urls[0] = new URL("file:./test.jar");
        DtClassLoader dtClassLoader = new DtClassLoader(urls, Thread.currentThread().getContextClassLoader());
        dtClassLoader.findResources("test");
    }
}
