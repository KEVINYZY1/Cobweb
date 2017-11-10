package com.xiongbeer.cobweb.api.info;

import com.xiongbeer.cobweb.conf.ZNodeStaticSetting;
import com.xiongbeer.cobweb.api.SimpleInfo;
import com.xiongbeer.cobweb.api.jsondata.JData;
import com.xiongbeer.cobweb.api.jsondata.TaskJson;
import com.xiongbeer.cobweb.exception.CobwebRuntimeException.OperationFailedException;

import com.xiongbeer.cobweb.zk.task.TaskData;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by shaoxiong on 17-5-12.
 */
public class TaskInfo implements SimpleInfo {
    private List<JData> info;

    private CuratorFramework client;

    public TaskInfo(CuratorFramework client) {
        this.client = client;
        info = new LinkedList<>();
    }

    public TaskInfo getCurrentTasks() {
        try {
            List<String> children =
                    client.getChildren().forPath(ZNodeStaticSetting.TASKS_PATH);
            byte[] data;
            for (String child : children) {
                Stat stat = new Stat();
                data = client.getData()
                        .storingStatIn(stat)
                        .forPath(ZNodeStaticSetting.NEW_TASK_PATH + child);
                info.add(taskInfo(stat, child, data));
            }
            return this;
        } catch (KeeperException.ConnectionLossException e) {
            throw new OperationFailedException("[Error] Connection loss" +
                    ", you may have to wait for a while.");
        } catch (KeeperException.AuthFailedException e) {
            throw new OperationFailedException("[Error] Authentication failed.");
        } catch (KeeperException.NoAuthException e) {
            throw new OperationFailedException("[Error] Permission denied.");
        } catch (Exception e) {
            throw new OperationFailedException("[Error] Unknow reason." + e.getMessage());
        }
    }

    private TaskJson taskInfo(Stat taskStat, String name, byte[] data) {
        TaskJson foo = new TaskJson();
        TaskData taskData = new TaskData(data);
        foo.setName(name);
        foo.setCtime(taskStat.getCtime());
        foo.setMtime(taskStat.getMtime());
        foo.setStatus(taskData.getStatus());
        foo.setProgress(taskData.getProgress());
        foo.setMarkup(taskData.getUniqueMarkup());
        return foo;
    }

    @Override
    public List<JData> getInfo() {
        return info;
    }
}
