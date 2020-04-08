package com.dtstack.flink.sql.side.polardb;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.rdb.all.RdbAllSideInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;

public class PolardbAllSideInfo extends RdbAllSideInfo {
    public PolardbAllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}

