package com.dtstack.flink.sql.exception;

/**
 * 错误码
 */
public interface ErrorCode {

    /**
     * 获取错误码
     *
     * @return
     */
    String getCode();

    /**
     * 获取错误信息
     *
     * @return
     */
    String getDescription();
}