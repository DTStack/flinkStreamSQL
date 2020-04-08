package com.dtstack.flink.sql.sink.console.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/12/19
 *
 * @author xuqianjin
 */
public class TablePrintUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TablePrintUtil.class);
    public static final int ALIGN_LEFT = 1;//左对齐
    public static final int ALIGN_RIGHT = 2;//右对齐
    public static final int ALIGN_CENTER = 3;//居中对齐

    private int align = ALIGN_CENTER;//默认居中对齐
    private boolean equilong = false;//默认不等宽
    private int padding = 1;//左右边距默认为1
    private char h = '-';//默认水平分隔符
    private char v = '|';//默认竖直分隔符
    private char o = '+';//默认交叉分隔符
    private char s = ' ';//默认空白填充符
    private List<String[]> data;//数据

    private TablePrintUtil() {
    }

    /**
     * 链式调用入口方法
     *
     * @param data
     * @return
     */
    public static TablePrintUtil build(String[][] data) {
        TablePrintUtil self = new TablePrintUtil();
        self.data = new ArrayList<>(Arrays.asList(data));
        return self;
    }

    /**
     * 链式调用入口方法，T可以是String[]、List<String>、任意实体类
     * 由于java泛型不同无法重载，所以这里要写if instanceof进行类型判断
     *
     * @param data
     * @param <T>
     * @return
     */
    public static <T> TablePrintUtil build(List<T> data) {
        TablePrintUtil self = new TablePrintUtil();
        self.data = new ArrayList<>();
        if (data.size() <= 0) throw new RuntimeException("数据源至少得有一行吧");
        Object obj = data.get(0);


        if (obj instanceof String[]) {
            //如果泛型为String数组，则直接设置
            self.data = (List<String[]>) data;
        } else if (obj instanceof List) {
            //如果泛型为List，则把list中的item依次转为String[]，再设置
            int length = ((List) obj).size();
            for (Object item : data) {
                List<String> col = (List<String>) item;
                if (col.size() != length) throw new RuntimeException("数据源每列长度必须一致");
                self.data.add(col.toArray(new String[length]));
            }
        } else {
            //如果泛型为实体类，则利用反射获取get方法列表，从而推算出属性列表。
            //根据反射得来的属性列表设置表格第一行thead
            List<Col> colList = getColList(obj);
            String[] header = new String[colList.size()];
            for (int i = 0; i < colList.size(); i++) {
                header[i] = colList.get(i).colName;
            }
            self.data.add(header);
            //利用反射调用相应get方法获取属性值来设置表格tbody
            for (int i = 0; i < data.size(); i++) {
                String[] item = new String[colList.size()];
                for (int j = 0; j < colList.size(); j++) {
                    String value = null;
                    try {
                        value = obj.getClass().getMethod(colList.get(j).getMethodName).invoke(data.get(i)).toString();
                    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                        e.printStackTrace();
                    }
                    item[j] = value == null ? "null" : value;
                }
                self.data.add(item);
            }
        }
        return self;
    }

    private static class Col {
        private String colName;//列名
        private String getMethodName;//get方法名
    }

    /**
     * 利用反射获取get方法名和属性名
     *
     * @return
     */
    private static List<Col> getColList(Object obj) {
        List<Col> colList = new ArrayList<>();
        Method[] methods = obj.getClass().getMethods();
        for (Method m : methods) {
            StringBuilder getMethodName = new StringBuilder(m.getName());
            if (getMethodName.substring(0, 3).equals("get") && !m.getName().equals("getClass")) {
                Col col = new Col();
                col.getMethodName = getMethodName.toString();
                char first = Character.toLowerCase(getMethodName.delete(0, 3).charAt(0));
                getMethodName.delete(0, 1).insert(0, first);
                col.colName = getMethodName.toString();
                colList.add(col);
            }
        }
        return colList;
    }

    /**
     * 获取字符串占的字符位数
     *
     * @param str
     * @return
     */
    private int getStringCharLength(String str) {
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");//利用正则找到中文
        Matcher m = p.matcher(str);
        int count = 0;
        while (m.find()) {
            count++;
        }
        return str.length() + count;
    }

    /**
     * 纵向遍历获取数据每列的长度
     *
     * @return
     */
    private int[] getColLengths() {
        int[] result = new int[data.get(0).length];
        for (int x = 0; x < result.length; x++) {
            int max = 0;
            for (int y = 0; y < data.size(); y++) {
                int len = getStringCharLength(data.get(y)[x]);
                if (len > max) {
                    max = len;
                }
            }
            result[x] = max;
        }
        if (equilong) {//如果等宽表格
            int max = 0;
            for (int len : result) {
                if (len > max) max = len;
            }
            for (int i = 0; i < result.length; i++) {
                result[i] = max;
            }
        }
        return result;
    }

    /**
     * 取得表格字符串
     *
     * @return
     */
    public String getTableString() {
        StringBuilder sb = new StringBuilder();
        int[] colLengths = getColLengths();//获取每列文字宽度
        StringBuilder line = new StringBuilder();//表格横向分隔线
        line.append(o);
        for (int len : colLengths) {
            int allLen = len + padding * 2;//还需要加上边距和分隔符的长度
            for (int i = 0; i < allLen; i++) {
                line.append(h);
            }
            line.append(o);
        }
        sb.append(line).append("\r\n");
        for (int y = 0; y < data.size(); y++) {
            sb.append(v);
            for (int x = 0; x < data.get(y).length; x++) {
                String cell = data.get(y)[x];
                switch (align) {
                    case ALIGN_LEFT:
                        for (int i = 0; i < padding; i++) {sb.append(s);}
                        sb.append(cell);
                        for (int i = 0; i < colLengths[x] - getStringCharLength(cell) + padding; i++) {sb.append(s);}
                        break;
                    case ALIGN_RIGHT:
                        for (int i = 0; i < colLengths[x] - getStringCharLength(cell) + padding; i++) {sb.append(s);}
                        sb.append(cell);
                        for (int i = 0; i < padding; i++) {sb.append(s);}
                        break;
                    case ALIGN_CENTER:
                        int space = colLengths[x] - getStringCharLength(cell);
                        int left = space / 2;
                        int right = space - left;
                        for (int i = 0; i < left + padding; i++) {sb.append(s);}
                        sb.append(cell);
                        for (int i = 0; i < right + padding; i++) {sb.append(s);}
                        break;
                }
                sb.append(v);
            }
            sb.append("\r\n");
            sb.append(line).append("\r\n");
        }
        return sb.toString();
    }

    /**
     * 直接打印表格
     */
    public void print() {
        LOG.info("\n"+getTableString());
        System.out.println(getTableString());
    }

    //下面是链式调用的set方法
    public TablePrintUtil setAlign(int align) {
        this.align = align;
        return this;
    }

    public TablePrintUtil setEquilong(boolean equilong) {
        this.equilong = equilong;
        return this;
    }

    public TablePrintUtil setPadding(int padding) {
        this.padding = padding;
        return this;
    }

    public TablePrintUtil setH(char h) {
        this.h = h;
        return this;
    }

    public TablePrintUtil setV(char v) {
        this.v = v;
        return this;
    }

    public TablePrintUtil setO(char o) {
        this.o = o;
        return this;
    }

    public TablePrintUtil setS(char s) {
        this.s = s;
        return this;
    }

    /**
     * 使用示例
     *
     * @param args
     */
    public static void main(String[] args) {
        List<String[]> data1 = new ArrayList<>();
        data1.add(new String[]{"用户名", "密码", "姓名"});
        data1.add(new String[]{"xiaoming", "xm123", "小明"});
        data1.add(new String[]{"xiaohong", "xh123", "小红"});
        TablePrintUtil.build(data1).print();

        List<List<String>> data2 = new ArrayList<>();
        data2.add(new ArrayList<>());
        data2.add(new ArrayList<>());
        data2.add(new ArrayList<>());
        data2.get(0).add("用户名");
        data2.get(0).add("密码");
        data2.get(0).add("姓名");
        data2.get(1).add("xiaoming");
        data2.get(1).add("xm123");
        data2.get(1).add("小明");
        data2.get(2).add("xiaohong");
        data2.get(2).add("xh123");
        data2.get(2).add("小红");
        TablePrintUtil.build(data2)
                .setAlign(TablePrintUtil.ALIGN_LEFT)
                .setPadding(5)
                .setEquilong(true)
                .print();


        class User {
            String username;
            String password;
            String name;

            User(String username, String password, String name) {
                this.username = username;
                this.password = password;
                this.name = name;
            }

            public String getUsername() {
                return username;
            }

            public void setUsername(String username) {
                this.username = username;
            }

            public String getPassword() {
                return password;
            }

            public void setPassword(String password) {
                this.password = password;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }
        }
        List<User> data3 = new ArrayList<>();
        data3.add(new User("xiaoming", "xm123", "小明"));
        data3.add(new User("xiaohong", "xh123", "小红"));
        TablePrintUtil.build(data3).setH('=').setV('!').print();
    }
}
