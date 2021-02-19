package com.dtstack.flink.sql.sink.console;

import com.dtstack.flink.sql.sink.console.table.TablePrintUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
 * @program: flink.sql
 * @description:
 * @author: wuren
 * @create: 2020-06-16 20:43
 **/
@RunWith(PowerMockRunner. class)
@PrepareForTest({TablePrintUtil.class })
public class ConsoleOutputFormatTest {

    @Mock
    Counter counter;

    ConsoleOutputFormat.ConsoleOutputFormatBuilder builder;

    ConsoleOutputFormat consoleOutputFormat;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        builder = ConsoleOutputFormat.buildOutputFormat();
        String[] fieldNames = new String[]{"id", "name"};
        TypeInformation<?>[] fieldTypes = new TypeInformation[]{
                TypeInformation.of(Integer.class),
                TypeInformation.of(String.class)
        };
        builder
                .setFieldNames(fieldNames)
                .setFieldTypes(fieldTypes);
        consoleOutputFormat = builder.finish();
    }

    @Test
    public void testWriteRecord() throws IOException {
        Row row = Row.of(1, "foo_name");
        Tuple2<Boolean, Row> tuple = Tuple2.of(true, row);
        doNothing().when(counter).inc();
//        PowerMockito.mockStatic(TablePrintUtil.class);
//        TablePrintUtil printUtilMock = Whitebox.newInstance(TablePrintUtil.class);
//        Whitebox.newInstance(printUtilMock, "print")
//        TablePrintUtil printUtilMock = PowerMockito.mock(TablePrintUtil.class);
//        PowerMockito.doNothing().when(printUtilMock).print();
//        PowerMockito.when(TablePrintUtil.build((String[][]) any())).thenReturn(printUtilMock);
//        TablePrintUtil.build((String[][]) null);
        consoleOutputFormat.outRecords = counter;
        consoleOutputFormat.writeRecord(tuple);
    }

}
