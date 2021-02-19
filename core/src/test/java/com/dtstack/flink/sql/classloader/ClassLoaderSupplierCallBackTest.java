package com.dtstack.flink.sql.classloader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URL;

import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
public class ClassLoaderSupplierCallBackTest {

    @Test
    public void testCallBackAndRest() throws Exception {
        ClassLoaderSupplier classLoaderSupplier = mock(ClassLoaderSupplier.class);
        URL[] urls = new URL[1];
        ClassLoaderSupplierCallBack.callbackAndReset(classLoaderSupplier, new DtClassLoader(urls));
    }

}
