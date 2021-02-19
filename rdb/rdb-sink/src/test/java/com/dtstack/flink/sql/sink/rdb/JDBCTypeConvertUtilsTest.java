package com.dtstack.flink.sql.sink.rdb;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;

public class JDBCTypeConvertUtilsTest {

    @Test
    public void testSetField() {

    }

    @Test
    public void testBuildSqlTypes() {
        List<Class> fieldTypes = Lists.newArrayList();
        fieldTypes.add(Integer.class);
        fieldTypes.add(Boolean.class);
        fieldTypes.add(Long.class);
        fieldTypes.add(Byte.class);
        fieldTypes.add(Short.class);
        fieldTypes.add(String.class);

        fieldTypes.add(Float.class);
        fieldTypes.add(Double.class);
        fieldTypes.add(Timestamp.class);
        fieldTypes.add(BigDecimal.class);
        fieldTypes.add(Date.class);
        fieldTypes.add(Double.class);
        JDBCTypeConvertUtils.buildSqlTypes(fieldTypes);
    }

    @Test
    public void testSetRecordToStatement() throws SQLException {
        PreparedStatement upload = mock(PreparedStatement.class);
        int[] typesArray = new int[] {java.sql.Types.INTEGER, Types.VARCHAR};
        Row row = Row.of(1, "a");
//        PowerMockito.doNothing().when(upload).setObject(any(), any());
        JDBCTypeConvertUtils.setRecordToStatement(upload, typesArray, row);
    }
}