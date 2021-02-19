package com.dtstack.flink.sql.option;

import org.junit.Test;

public class OptionParseTest {

    @Test
    public void testOption() throws Exception {
        String[] sql = new String[]{"-mode", "yarnPer", "-sql", "/Users/maqi/tmp/json/group_tmp4.txt", "-name", "PluginLoadModeTest",
                "-localSqlPluginPath", "/Users/maqi/code/dtstack/dt-center-flinkStreamSQL/plugins",
                "-remoteSqlPluginPath", "/Users/maqi/code/dtstack/dt-center-flinkStreamSQL/plugins",
                "-flinkconf", "/Users/maqi/tmp/flink-1.8.1/conf",
                "-confProp", "{\"sql.checkpoint.cleanup.mode\":\"false\",\"sql.checkpoint.interval\":10000,\"time.characteristic\":\"EventTime\"}",
                "-yarnconf", "/Users/maqi/tmp/hadoop", "-flinkJarPath", "/Users/maqi/tmp/flink-1.8.1/lib", "-queue", "c", "-pluginLoadMode", "shipfile"};
        OptionParser optionParser = new OptionParser(sql);
    }
}
