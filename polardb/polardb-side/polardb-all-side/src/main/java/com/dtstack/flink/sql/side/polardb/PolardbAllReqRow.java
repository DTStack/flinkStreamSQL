package com.dtstack.flink.sql.side.polardb;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRow;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

public class PolardbAllReqRow extends RdbAllReqRow {

    private static final long serialVersionUID = 2098635140857937717L;

    private static final Logger LOG = LoggerFactory.getLogger(PolardbAllReqRow.class);

    private static final String POLARDB_DRIVER = "com.mysql.cj.jdbc.Driver";

    public PolardbAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new PolardbAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public Connection getConn(String dbURL, String userName, String password) {
        try {
            Class.forName(POLARDB_DRIVER);
            //add param useCursorFetch=true
            Map<String, String> addParams = Maps.newHashMap();
            addParams.put("useCursorFetch", "true");
            String targetDbUrl = DtStringUtil.addJdbcParam(dbURL, addParams, true);
            return DriverManager.getConnection(targetDbUrl, userName, password);
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException("", e);
        }
    }

    @Override
    public int getFetchSize() {
        return Integer.MIN_VALUE;
    }
}
