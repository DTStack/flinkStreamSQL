package com.dtstack.flink.sql.exception;

import org.apache.flink.runtime.execution.SuppressRestartsException;

import java.util.Objects;

/**
 * @author tiezhu
 * @date 2021/2/2 星期二
 * Company dtstack
 */
public class ExceptionTrace {
    // 追溯当前异常的最原始异常信息
    public static String traceOriginalCause(Throwable e) {
        String errorMsg;
        if (Objects.nonNull(e.getCause())) {
            errorMsg = traceOriginalCause(e.getCause());
        } else {
            errorMsg = e.getMessage();
        }
        return errorMsg;
    }

    /**
     * 根据异常的种类来判断是否需要强制跳过Flink的重启{@link SuppressRestartsException}
      * @param e exception
     * @param errorMsg 需要抛出的异常信息
     */
    public static void dealExceptionWithSuppressStart(Exception e, String errorMsg) {
        if (e instanceof SuppressRestartsException) {
            throw new SuppressRestartsException(
                new Throwable(
                    errorMsg
                )
            );
        } else {
            throw new RuntimeException(errorMsg);
        }
    }
}
