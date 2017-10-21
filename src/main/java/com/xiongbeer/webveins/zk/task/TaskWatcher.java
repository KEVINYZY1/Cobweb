package com.xiongbeer.webveins.zk.task;

import com.xiongbeer.webveins.ZnodeInfo;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by shaoxiong on 17-5-6.
 */
public class TaskWatcher {
    public static int WAITING_TIME = 2 * 1000;

    private CuratorFramework client;

    private Logger logger = LoggerFactory.getLogger(TaskWatcher.class);

    public TaskWatcher(CuratorFramework client) {
        this.client = client;
    }

    /**
     * 在没有可领取任务时阻塞
     * 刷新频率为WAITING_TIME/次
     */
    public void waitForTask() {
        try {
            while (true) {
                List<String> children = client.getChildren().forPath(ZnodeInfo.TASKS_PATH);
                for (String child : children) {
                    byte[] data = client.getData()
                            .forPath(ZnodeInfo.NEW_TASK_PATH + child);
                    TaskData taskData = new TaskData(data);
                    if (taskData.getStatus() == Task.Status.WAITING) {
                        return;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(WAITING_TIME);
            }
        } catch (Exception e) {
            logger.warn("some thing get wrong when waiting for task.", e);
        }
    }

}
