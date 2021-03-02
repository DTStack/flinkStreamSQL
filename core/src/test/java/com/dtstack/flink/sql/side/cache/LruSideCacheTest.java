package com.dtstack.flink.sql.side.cache;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LruSideCacheTest {
    private LRUSideCache lruSideCache ;

    @Before
    public void before(){
        AbstractSideTableInfo sideTableInfo = mock(AbstractSideTableInfo.class);
        when(sideTableInfo.getCacheSize()).thenReturn(10);
        when(sideTableInfo.getCacheTimeout()).thenReturn(1000000L);

        lruSideCache = new LRUSideCache(sideTableInfo);
        lruSideCache.initCache();
    }


    @Test
    public void getFromCache(){
       lruSideCache.getFromCache("test");

    }

    @Test
    public void putCache(){
        lruSideCache.putCache("test", CacheObj.buildCacheObj(null, null));
    }
}
