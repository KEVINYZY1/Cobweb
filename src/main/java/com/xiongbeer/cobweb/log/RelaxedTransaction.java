package com.xiongbeer.cobweb.log;

import com.xiongbeer.cobweb.exception.CobwebRuntimeException;

import org.slf4j.Logger;

/**
 * Created by shaoxiong on 17-11-10.
 */
public class RelaxedTransaction implements Transaction {

    public static void doProcess(Do doOp, Undo undoOp) {
        try {
            doOp.roll();
        } catch (CobwebRuntimeException.OperationFailedException e) {
            undoOp.rollback();
        }
    }

    public static void doProcess(Do doOp, Undo undoOp, Logger logger) {
        try {
            doOp.roll();
        } catch (CobwebRuntimeException.OperationFailedException e) {
            logger.error(e.getMessage());
            undoOp.rollback();
        }
    }
}
