package com.dtstack.flink.sql.side.sqlserver;


import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRow;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

/**
 * side operator with cache for all(period reload)
 */
public class SqlserverAllReqRow extends RdbAllReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(SqlserverAllReqRow.class);

    private static final String SQLSERVER_DRIVER = "net.sourceforge.jtds.jdbc.Driver";

    public SqlserverAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public Connection getConn(String dbURL, String userName, String password) {
        try {
            Class.forName(SQLSERVER_DRIVER);
            //add param useCursorFetch=true
            Map<String, String> addParams = Maps.newHashMap();
            //addParams.put("useCursorFetch", "true");
            String targetDbUrl = DtStringUtil.addJdbcParam(dbURL, addParams, true);
            return DriverManager.getConnection(targetDbUrl, userName, password);
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException("", e);
        }
    }

}
