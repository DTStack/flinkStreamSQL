package com.dtstack.flink.sql.enums;

public enum PluginLoadMode {
    classpath(0),shipfile(1);

    private int type;

    PluginLoadMode(int type){
        this.type = type;
    }

    public int getType(){
        return this.type;
    }
}
