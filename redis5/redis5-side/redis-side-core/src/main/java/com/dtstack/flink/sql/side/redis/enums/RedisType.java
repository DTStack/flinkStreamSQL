package com.dtstack.flink.sql.side.redis.enums;

public enum RedisType {
    /**
     * 单机
     */
    STANDALONE(1),
    /**
     * 哨兵
     */
    SENTINEL(2),
    /**
     * 集群
     */
    CLUSTER(3);
    int type;
    RedisType(int type){
        this.type = type;
    }

    public int getType(){
        return type;
    }

    public static RedisType parse(int redisType){
        for(RedisType type : RedisType.values()){
            if(type.getType() == redisType){
                return type;
            }
        }
        throw new RuntimeException("unsupport redis type["+ redisType + "]");
    }
}
