package com.dtstack.flink.sql.side.hbase.factory;

import com.dtstack.flink.sql.side.hbase.operators.AbstractReplaceOperator;
import com.dtstack.flink.sql.side.hbase.enums.EReplaceOpType;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 遍历EReplaceOpType当中的所有枚举方法，自动生成对应的operate类
 * 新增rowkey算法，这个类不需要修改
 */
public class ReplaceOperatorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ReplaceOperatorFactory.class);

    /**
     * 创建对应的算法枚举类
     */
    public static AbstractReplaceOperator createReplaceOperator(EReplaceOpType eReplaceOpType){
        try {
            Class clazz = Class.forName(eReplaceOpType.getReference());
            return (AbstractReplaceOperator) clazz.newInstance();
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    /**
     * 创建所有的operators
     * @return
     */
    public static List<AbstractReplaceOperator> createAllOperators(){
        return Arrays.stream(EReplaceOpType.values()).map(ReplaceOperatorFactory::createReplaceOperator)
                .filter(Objects::nonNull).collect(Collectors.toList());
    }
}
