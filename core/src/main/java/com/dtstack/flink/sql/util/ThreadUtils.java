package com.dtstack.flink.sql.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author tiezhu
 * date 2020/12/4
 * company dtstack
 */
public class ThreadUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadUtils.class);

    public static void sleepMilliseconds(long timeout) {
        try {
            TimeUnit.MILLISECONDS.sleep(timeout);
        } catch(InterruptedException ie) {
            LOG.error("", ie);
        }
    }

    public static void sleepMinutes(long timeout) {
        try {
            TimeUnit.MINUTES.sleep(timeout);
        } catch(InterruptedException ie) {
            LOG.error("", ie);
        }
    }

    public static void sleepMicroseconds(long timeout) {
        try {
            TimeUnit.MICROSECONDS.sleep(timeout);
        } catch(InterruptedException ie) {
            LOG.error("", ie);
        }
    }
}
