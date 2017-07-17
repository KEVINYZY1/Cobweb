package com.xiongbeer.webveins.api.job;

import com.xiongbeer.webveins.Configuration;
import com.xiongbeer.webveins.api.SimpleJob;
import com.xiongbeer.webveins.exception.VeinsException;
import com.xiongbeer.webveins.saver.DFSManager;
import com.xiongbeer.webveins.saver.HDFSManager;

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
                    = dfsManager.listFiles(Configuration.FINISHED_TASKS_URLS, false);
            for (String file : files) {
                dfsManager.delete(file, false);
            }
        } catch (IOException e) {
            throw new VeinsException.OperationFailedException(e.getMessage());
        }
    }

    @Override
    public void sumbit() {

    }
}
