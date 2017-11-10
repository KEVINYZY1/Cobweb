package com.xiongbeer.cobweb.log;

import com.xiongbeer.cobweb.exception.CobwebRuntimeException;

/**
 * Created by shaoxiong on 17-11-10.
 */
@FunctionalInterface
public interface Do {
    void roll() throws CobwebRuntimeException.OperationFailedException;
}
