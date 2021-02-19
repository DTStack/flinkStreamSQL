package com.dtstack.flink.sql.util;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;

public class PluginUtilTest {

    @Test(expected = Exception.class)
    public void buildSourceAndSinkPathByLoadMode() throws Exception {
        String type = "source";
        String prefix = "";
        String localSqlPluginPath = ".";
        String remoteSqlPluginPath = ".";
        try {
            PluginUtil.buildSourceAndSinkPathByLoadMode(type, prefix, localSqlPluginPath, remoteSqlPluginPath, "classpath");
        } catch (Exception e){
            PluginUtil.buildSourceAndSinkPathByLoadMode(type, prefix, localSqlPluginPath, remoteSqlPluginPath, "shipfile");
        }
    }

    @Test(expected = Exception.class)
    public void buildSidePathByLoadMode() throws Exception {
        PluginUtil.buildSidePathByLoadMode("", "", "", "", "", "");
        String type = "source";
        String prefix = "";
        String localSqlPluginPath = ".";
        String remoteSqlPluginPath = ".";
        try {
            PluginUtil.buildSidePathByLoadMode(type, "s", prefix, localSqlPluginPath, remoteSqlPluginPath, "classpath");
        } catch (Exception e){
            PluginUtil.buildSidePathByLoadMode(type, "a", prefix, localSqlPluginPath, remoteSqlPluginPath, "shipfile");
        }
    }


    @Test(expected = RuntimeException.class)
    public void getJarFileDirPath() {
        PluginUtil.getJarFileDirPath("a", "b", "");
    }

    @Test(expected = Exception.class)
    public void getSideJarFileDirPath() throws MalformedURLException {
        PluginUtil.getSideJarFileDirPath("a", "b", "c", "d", "");
    }

    @Test
    public void getGenerClassName() throws IOException {
        PluginUtil.getGenerClassName("a", "b");
    }

    @Test
    public void getSqlParserClassName(){
        PluginUtil.getSqlParserClassName("a", "b");
    }
    @Test
    public void getSqlSideClassName(){
        PluginUtil.getSqlSideClassName("a", "b", "c");
    }

    @Test
    public void objectToMap() throws Exception{
        Map<String, Object> cache = Maps.newHashMap();
        cache.put("k","v");
        PluginUtil.objectToMap(cache);
    }

    @Test
    public void jsonStrToObject() throws IOException {
        Assert.assertEquals(PluginUtil.jsonStrToObject("true", Boolean.class), true);
    }

    @Test
    public void stringToProperties() throws IOException{
        PluginUtil.stringToProperties("{}");
    }

}
