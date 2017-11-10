package com.xiongbeer.cobweb.exception;

/**
 * Created by shaoxiong on 17-11-10.
 */
public class FilterException extends Exception {
    public FilterException() {
        super();
    }

    public FilterException(String message) {
        super(message);
    }

    public FilterException(String message, Throwable cause) {
        super(message, cause);
    }

    public FilterException(Throwable cause) {
        super(cause);
    }


    public static class IllegalFilterCacheNameException extends FilterException {
        public IllegalFilterCacheNameException() {
            super();
        }

        public IllegalFilterCacheNameException(String message) {
            super(message);
        }

        public IllegalFilterCacheNameException(String message, Throwable cause) {
            super(message, cause);
        }

        public IllegalFilterCacheNameException(Throwable cause) {
            super(cause);
        }
    }
}
