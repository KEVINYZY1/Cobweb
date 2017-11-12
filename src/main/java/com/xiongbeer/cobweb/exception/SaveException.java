package com.xiongbeer.cobweb.exception;

import java.io.IOException;

/**
 * Created by shaoxiong on 17-11-12.
 */
public class SaveException extends IOException {
    public SaveException() {
        super();
    }

    public SaveException(String message) {
        super(message);
    }

    public SaveException(String message, Throwable cause) {
        super(message, cause);
    }

    public SaveException(Throwable cause) {
        super(cause);
    }

    public static class SliceSaveIOException extends SaveException {
        public SliceSaveIOException() {
            super();
        }

        public SliceSaveIOException(String message) {
            super(message);
        }

        public SliceSaveIOException(String message, Throwable cause) {
            super(message, cause);
        }

        public SliceSaveIOException(Throwable cause) {
            super(cause);
        }
    }
}
