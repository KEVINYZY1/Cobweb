package com.xiongbeer.cobweb.api.job;

import com.xiongbeer.cobweb.Configuration;
import com.xiongbeer.cobweb.api.SimpleJob;
import com.xiongbeer.cobweb.exception.CobwebRuntimeException;
import com.xiongbeer.cobweb.saver.dfs.DFSManager;

import java.io.IOException;
import java.util.List;

/**
 * Created by shaoxiong on 17-5-18.
 */
public class DFSJob implements SimpleJob {
    private DFSManager dfsManager;

    public DFSJob(DFSManager dfsManager) {
        this.dfsManager = dfsManager;
    }

    public void EmptyTrash() {
        try {
            List<String> files
                    = dfsManager.listFiles(Configuration.INSTANCE.FINISHED_TASKS_URLS, false);
            for (String file : files) {
                dfsManager.delete(file, false);
            }
        } catch (IOException e) {
            throw new CobwebRuntimeException.OperationFailedException(e.getMessage());
        }
    }

    @Override
    public void sumbit() {

    }
}
