package com.dtstack.flink.sql.sink.rdb;

import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class AbstractRdbSinkTest {

    AbstractRdbSink rdbSink;

    @Before
    public void setUp() {
        rdbSink = Whitebox.newInstance(ConcreteRdbSink.class);
    }

    @Test
    public void testGenStreamSink() {
        RdbTableInfo tableInfo = Whitebox.newInstance(RdbTableInfo.class);
        tableInfo.setType("mysql");
        tableInfo.setFieldClasses(new Class[] {Integer.class});
        AbstractRdbSink streamSink = rdbSink.genStreamSink(tableInfo);
        Assert.assertEquals(streamSink, rdbSink);
    }

    @Test
    public void testConfigure() {
        final String[] fieldNames = new String[] {"id", "name"};
        final TypeInformation<?>[] fieldTypes = new TypeInformation[] {
            TypeInformation.of(Integer.class),
            TypeInformation.of(String.class)
        };
        rdbSink.configure(fieldNames, fieldTypes);
        Assert.assertArrayEquals(fieldNames, rdbSink.getFieldNames());
        Assert.assertArrayEquals(fieldTypes, rdbSink.getFieldTypes());
        Assert.assertEquals(
            new RowTypeInfo(fieldTypes, fieldNames),
            rdbSink.getRecordType()
        );
        Assert.assertEquals(
            new TupleTypeInfo(
                org.apache.flink.table.api.Types.BOOLEAN(),
                rdbSink.getRecordType()
            ),
            rdbSink.getOutputType()
        );
    }
}