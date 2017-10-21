package com.xiongbeer.webveins.service.rpc;

import com.sun.istack.Nullable;
import com.xiongbeer.webveins.zk.worker.Worker;
import org.apache.thrift.TException;

import java.util.Optional;

/**
 * Created by shaoxiong on 17-10-9.
 */
public class WorkerCrawlerServiceImpl implements LocalWorkerCrawlerService.Iface {

    private Worker worker;

    public WorkerCrawlerServiceImpl(Worker worker) {
        this.worker = worker;
    }

    @Override
    public boolean giveUpTask(String taskId, @Nullable String reason) throws TException {
        worker.discardTask(taskId);
        /*TODO*/
        Optional.of(reason).ifPresent(System.out::println);
        return true;
    }

    @Override
    public String getLastProgressRate(String taskId) throws TException {
        return null;
    }

    @Override
    public boolean finishTask(String taskId) throws TException {
        worker.finishTask(taskId);
        return false;
    }

    @Override
    public boolean getNewTask(String taskFilePath, String lastProgressRate) throws TException {
        return false;
    }

    @Override
    public boolean updateTaskProgressRate(String taskId, String newProgressRate) throws TException {
        return false;
    }

    @Override
    public String getWorkersStatus() throws TException {
        return null;
    }

    @Override
    public String getFiltersStatus() throws TException {
        return null;
    }

    @Override
    public String getManagersStatus() throws TException {
        return "Managers status";
    }
}
