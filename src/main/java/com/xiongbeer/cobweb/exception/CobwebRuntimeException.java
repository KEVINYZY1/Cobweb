package com.xiongbeer.cobweb.exception;

/**
 * Created by shaoxiong on 17-4-15.
 */
public abstract class CobwebRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public CobwebRuntimeException() {
    }

    public CobwebRuntimeException(String message) {
        super(message);
    }

    public static class FilterOverflowException extends CobwebRuntimeException {

        public FilterOverflowException() {
        }

        public FilterOverflowException(String message) {
            super(message);
        }
    }

    public static class IllegalFilterCacheNameException extends CobwebRuntimeException {

        public IllegalFilterCacheNameException() {
        }

        public IllegalFilterCacheNameException(String message) {
            super(message);
        }
    }

    public static class OperationFailedException extends CobwebRuntimeException {

        public OperationFailedException() {
        }

        public OperationFailedException(String message) {
            super(message);
        }
    }
}
