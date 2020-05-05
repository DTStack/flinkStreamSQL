package com.dtstack.flink;

@FunctionalInterface
public interface ThrowableSupplier<OUT, EXCEPTION extends Throwable> {

    OUT get() throws EXCEPTION;

}
