package com.dtstack.flink;

@FunctionalInterface
public interface ThrowableRunable<EXCEPTION extends Throwable> {

    void run() throws EXCEPTION;

}
