package com.dtstack.flink.sql.sink.console.table;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;

public class TablePrintUtilTest {

    TablePrintUtil tablePrintUtil;

    @Before
    public void setUp() {
        List<String[]> data = new ArrayList<>();
        String[] fieldNames = new String[] {"id", "name"};
        data.add(fieldNames);
        String[] recordStr = new String[] {"1", "foo_name"};
        data.add(recordStr);
        tablePrintUtil = TablePrintUtil.build(data);
    }

    @Test
    public void testBuild() {
        List<String[]> data = Whitebox.getInternalState(tablePrintUtil, "data");
        String[] tableHead = data.get(0);
        String[] tableBody = data.get(1);
        Assert.assertTrue("id".equals(tableHead[0]));
        Assert.assertTrue("name".equals(tableHead[1]));
        Assert.assertTrue("1".equals(tableBody[0]));
        Assert.assertTrue("foo_name".equals(tableBody[1]));
    }

    @Test
    public void testGetTableString() {
        String s = tablePrintUtil.getTableString();
        final String normal =
            "+----+----------+\r\n" +
            "| id |   name   |\r\n" +
            "+----+----------+\r\n" +
            "| 1  | foo_name |\r\n" +
            "+----+----------+\r\n";
        Assert.assertTrue(normal.equals(s));
    }

    @Test
    public void testGetTableString2() {
//        List<String[]> data1 = new ArrayList<>();
//        data1.add(new String[]{"用户名", "密码", "姓名"});
//        data1.add(new String[]{"xiaoming", "xm123", "小明"});
//        data1.add(new String[]{"xiaohong", "xh123", "小红"});
//        TablePrintUtil.build(data1).print();

        List<List<String>> data2 = new ArrayList<>();

        data2.add(
            Lists.newArrayList("用户名", "密码", "姓名")
        );
        data2.add(
            Lists.newArrayList("xiaoming", "xm123", "小明")
        );
        data2.add(
            Lists.newArrayList("xiaohong", "xh123", "小红")
        );

        TablePrintUtil printUtil = TablePrintUtil.build(data2)
            .setAlign(TablePrintUtil.ALIGN_LEFT)
            .setPadding(5)
            .setEquilong(true);
        final String normal =
            "+------------------+------------------+------------------+\r\n" +
            "|     用户名       |     密码         |     姓名         |\r\n" +
            "+------------------+------------------+------------------+\r\n" +
            "|     xiaoming     |     xm123        |     小明         |\r\n" +
            "+------------------+------------------+------------------+\r\n" +
            "|     xiaohong     |     xh123        |     小红         |\r\n" +
            "+------------------+------------------+------------------+\r\n"
            ;
        String s = printUtil.getTableString();
        Assert.assertTrue(normal.equals(s));

    }

    @Test
    public void testGetTableString3() {

        class User {
            String name;

            User(String name) {
                this.name = name;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }
        }
        List<User> data3 = new ArrayList<>();
        data3.add(new User("xiaoming"));
        data3.add(new User("xiaohong"));
        TablePrintUtil printUtil = TablePrintUtil.build(data3)
            .setAlign(TablePrintUtil.ALIGN_RIGHT)
            .setH('=')
            .setV('!');

        final String normal =
            "+==========+\r\n" +
            "!     name !\r\n" +
            "+==========+\r\n" +
            "! xiaoming !\r\n" +
            "+==========+\r\n" +
            "! xiaohong !\r\n" +
            "+==========+\r\n";
        String s = printUtil.getTableString();
        Assert.assertTrue(normal.equals(s));
    }
}