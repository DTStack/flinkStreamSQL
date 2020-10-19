package com.dtstack.flink.sql.side.hbase.operators;

import com.dtstack.flink.sql.side.hbase.enums.EReplaceOpType;

import java.util.regex.Pattern;

public class AddReplaceOperator extends AbstractReplaceOperator {

    public AddReplaceOperator() {
        super(EReplaceOpType.ADD, Pattern.compile("(?i)^add\\(\\s*(.*)\\s*\\)$"));
    }

    /**
     * 简单的举个例子
     *
     * @param replaceStr
     * @return
     */
    @Override
    public String doFunc(String replaceStr) {
        return replaceStr + "add";
    }
}
