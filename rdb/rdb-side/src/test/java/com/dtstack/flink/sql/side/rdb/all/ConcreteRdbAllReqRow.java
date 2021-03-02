package com.dtstack.flink.sql.side.rdb.all;

import com.dtstack.flink.sql.side.BaseSideInfo;
import org.powermock.api.mockito.PowerMockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * @program: flink.sql
 * @author: wuren
 * @create: 2020/07/29
 **/
public class ConcreteRdbAllReqRow extends AbstractRdbAllReqRow {

    public ConcreteRdbAllReqRow(BaseSideInfo sideInfo) {
        super(sideInfo);
    }

    @Override
    public Connection getConn(String dbURL, String userName, String password) {
        Connection connMock = mock(Connection.class);
        try {
            PowerMockito.doNothing().when(connMock).close();
            Statement stmtMock = mock(Statement.class);
            PowerMockito.doReturn(stmtMock).when(connMock).createStatement();
//        PowerMockito.doNothing().when(stmtMock).setFetchSize(any());
            ResultSet setMock = mock(ResultSet.class);
            PowerMockito.when(setMock.next()).thenReturn(true).thenReturn(false);
            PowerMockito.doReturn(setMock).when(stmtMock).executeQuery(any());
            PowerMockito.doReturn(1).when(setMock).getObject(any());
        } catch (SQLException e) {

        }
        return connMock;
    }
}
