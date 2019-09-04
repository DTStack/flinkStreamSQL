package com.dtstack.flink.sql.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;

public class JDBCUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ClassUtil.class);

    public final static String lock_str = "jdbc_lock_str";

    public static void forName(String clazz, ClassLoader classLoader)  {
        synchronized (lock_str){
            try {
                Class.forName(clazz, true, classLoader);
                DriverManager.setLoginTimeout(10);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    public synchronized static void forName(String clazz) {
        try {
            Class<?> driverClass = Class.forName(clazz);
            driverClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
