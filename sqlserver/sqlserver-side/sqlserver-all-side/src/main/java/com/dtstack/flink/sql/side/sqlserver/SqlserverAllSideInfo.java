package com.dtstack.flink.sql.side.sqlserver;


import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.rdb.all.RdbAllSideInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import java.util.List;

public class SqlserverAllSideInfo extends RdbAllSideInfo {

    public SqlserverAllSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }
}
