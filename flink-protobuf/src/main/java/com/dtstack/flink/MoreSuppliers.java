package com.dtstack.flink;


public class MoreSuppliers {

    private MoreSuppliers() {
        throw new UnsupportedOperationException();
    }

    public static <OUT> OUT throwing(ThrowableSupplier<OUT, Throwable> throwableSupplier) {
        try {
            return throwableSupplier.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
