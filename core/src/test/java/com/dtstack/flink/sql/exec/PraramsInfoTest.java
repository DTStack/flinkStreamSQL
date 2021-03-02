package com.dtstack.flink.sql.exec;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Properties;

public class PraramsInfoTest {

    @Test
    public void paramInfo(){
         ParamsInfo paramsInfo = ParamsInfo.builder()
                 .setConfProp(new Properties())
                 .setDeployMode("local")
                 .setName("test")
                 .setJarUrlList(Lists.newArrayList())
                 .setLocalSqlPluginPath(".")
                 .setRemoteSqlPluginPath(".")
                 .setPluginLoadMode("classpath")
                 .setSql("select a from b")
                 .build();
        System.out.println(paramsInfo.toString());

    }
}
