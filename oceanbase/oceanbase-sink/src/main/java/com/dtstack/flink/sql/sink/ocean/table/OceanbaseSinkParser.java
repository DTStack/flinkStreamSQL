package com.dtstack.flink.sql.sink.ocean.table;

import com.dtstack.flink.sql.sink.rdb.table.RdbSinkParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;

import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/24
 */
public class OceanbaseSinkParser extends RdbSinkParser {

    private static final String CURRENT_TYPE = "oceanbase";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {

        AbstractTableInfo oceanbaseTableInfo = super.getTableInfo(tableName, fieldsInfo, props);

        oceanbaseTableInfo.setType(CURRENT_TYPE);

        return oceanbaseTableInfo;
    }
}
