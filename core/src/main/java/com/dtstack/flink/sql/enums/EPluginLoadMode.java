package com.dtstack.flink.sql.enums;

/**
 *
 * CLASSPATH： plugin jar depends on each machine node.
 * SHIPFILE:  plugin jar only depends on the client submitted by the task.
 *
 */
public enum EPluginLoadMode {

    CLASSPATH(0),
    SHIPFILE(1);

    private int type;

    EPluginLoadMode(int type){
        this.type = type;
    }

    public int getType(){
        return this.type;
    }
}
