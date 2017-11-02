package com.xiongbeer.webveins.service.rpc;

import com.sun.istack.Nullable;
import com.xiongbeer.webveins.zk.task.Task;
import com.xiongbeer.webveins.zk.task.TaskData;
import com.xiongbeer.webveins.zk.worker.Worker;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by shaoxiong on 17-10-9.
 */
public class WorkerCrawlerServiceImpl implements LocalWorkerCrawlerServiceBase.Iface {

    private Worker worker;

    public WorkerCrawlerServiceImpl(Worker worker) {
        this.worker = worker;
    }

    @Override
    public boolean giveUpTask(String taskId, @Nullable String reason) throws TException {
        boolean res = worker.discardTask(taskId);
        /*TODO*/
        Optional.ofNullable(reason).ifPresent(System.out::println);
        return res;
    }

    @Override
    public int getLastProgressRate(String taskId) throws TException {
        return worker.getTaskWorker()
                .checkTask(taskId)
                .getTaskData()
                .getProgress();
    }

    @Override
    public boolean finishTask(String taskId) throws TException {
        return worker.finishTask(taskId);
    }

    @Override
    public List<String> getNewTask() throws TException {
        List<String> res = new ArrayList<>();
        res.add(worker.takeTask().toJson());
        return res;
    }

    @Override
    public List<String> getBlackList() throws TException {
        return null;
    }

    @Override
    public boolean addToBlackList(String taskId) throws TException {
        return false;
    }

    @Override
    public boolean removeFromBlackList(String taskId) throws TException {
        return false;
    }

    @Override
    public boolean clearBlackList() throws TException {
        return false;
    }

    @Override
    public boolean updateTaskProgressRate(String taskId, int newProgressRate, int markup, byte status) throws TException {
        TaskData taskData = new TaskData();
        taskData.setProgress(newProgressRate)
                .setUniqueMarkup(markup)
                .setStatus(Task.Status.get(Byte.toString(status)));
        return worker.beat(taskId, taskData);
    }

    @Override
    public String getWorkersStatus() throws TException {
        return "here";
    }

    @Override
    public String getFiltersStatus() throws TException {
        return null;
    }

    @Override
    public String getManagersStatus() throws TException {
        return null;
    }
}
