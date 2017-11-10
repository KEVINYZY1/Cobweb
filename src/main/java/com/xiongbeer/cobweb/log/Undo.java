package com.xiongbeer.cobweb.log;

import com.xiongbeer.cobweb.exception.CobwebRuntimeException;

/**
 * Created by shaoxiong on 17-11-10.
 */
@FunctionalInterface
public interface Undo {
    void rollback() throws CobwebRuntimeException.OperationFailedException;
}
