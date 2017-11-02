package com.xiongbeer.webveins.zk.worker;

import com.xiongbeer.webveins.ZnodeInfo;
import com.xiongbeer.webveins.exception.VeinsException;
import com.xiongbeer.webveins.zk.task.Epoch;
import com.xiongbeer.webveins.zk.task.TaskData;
import com.xiongbeer.webveins.zk.task.TaskWatcher;
import com.xiongbeer.webveins.zk.task.TaskWorker;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


/**
 * Created by shaoxiong on 17-4-9.
 */
public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private CuratorFramework client;

    private String serverId;

    private String workerPath;

    private TaskWorker taskWorker;

    private TaskWatcher taskWatcher;

    public Worker(CuratorFramework client, String serverId) {
        this.client = client;
        this.serverId = serverId;
        taskWorker = new TaskWorker(client);
        taskWatcher = new TaskWatcher(client);
        signUpWorker();
    }

    public static void addToBlackList(String taskName) {
        TaskWorker.addToBlackList(taskName);
    }

    public static void clearBlackList() {
        TaskWorker.clearTaskBlackList();
    }

    public TaskWorker getTaskWorker() {
        return taskWorker;
    }

    public TaskWatcher getTaskWatcher() {
        return taskWatcher;
    }

    public String getWorkerPath() {
        return workerPath;
    }

    public void waitForTask() {
        taskWatcher.waitForTask();
    }

    /**
     * 设置Worker的工作状态
     *
     * @param taskName 正在执行任务的ID，为空则说明当前没有任务
     */
    public void setStatus(String taskName) {
        try {
            client.setData().forPath(workerPath, taskName.getBytes());
        } catch (Exception e) {
            logger.warn("failed to set task.", e);
        }
    }

    public Epoch takeTask() {
        Optional<Epoch> task = Optional.ofNullable(taskWorker.takeTask());
        task.ifPresent((val) -> setStatus(val.getTaskName()));
        return task.get();
    }

    public boolean beat(String taskName, TaskData taskData) {
        boolean res = taskWorker.setRunningTask(ZnodeInfo.TASKS_PATH + '/' + taskName, -1, taskData);
        setStatus(taskName);
        return res;
    }

    public boolean discardTask(String taskPath) {
        return taskWorker.discardTask(taskPath);
    }

    public boolean finishTask(String taskPath) {
        boolean res = taskWorker.finishTask(taskPath);
        setStatus("");
        return res;
    }

    private void signUpWorker() {
        try {
            workerPath = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(ZnodeInfo.NEW_WORKER_PATH + serverId, serverId.getBytes());
        } catch (KeeperException.ConnectionLossException e) {
            signUpWorker();
        } catch (Exception e) {
            throw new VeinsException.OperationFailedException("\nfailed to sign up worker. " + e.getMessage());
        }
    }
}
